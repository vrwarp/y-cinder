/**
 * Unit tests for the provider's policy decisions.
 *
 * FireProvider is wired to Firestore listeners, timers and the window
 * lifecycle, so these were only reachable by standing one up against the
 * emulator. Two of them are load-bearing: the aggregation ceiling is what
 * stops continuous typing from deferring a save forever, and the squash
 * guards are what stop a squash from silently dropping data.
 */
import { describe, it, expect } from 'vitest';
import {
    computeSaveDelay,
    isRemoteOrigin,
    squashBlockedBy,
    validateProviderConfig,
} from '../../src/provider-policy';
import { FIREBASE_ORIGINS } from '../../src/types';

describe('validateProviderConfig', () => {
    const valid = { path: 'docs/a', maxUpdatesThreshold: 10, maxAggregationTime: 1_000, depth: 0 };

    it('accepts a valid configuration', () => {
        expect(() => validateProviderConfig(valid)).not.toThrow();
    });

    it.each([
        ['', 'empty'],
        ['/docs/a', 'leading slash'],
        ['docs/a/', 'trailing slash'],
        ['docs//a', 'double slash'],
    ])('rejects path %s (%s)', (path) => {
        expect(() => validateProviderConfig({ ...valid, path })).toThrow(/Invalid Firestore path/);
    });

    it('accepts a deep but well-formed path', () => {
        expect(() => validateProviderConfig({ ...valid, path: 'a/b/c/d/e' })).not.toThrow();
    });

    it('rejects a non-positive update threshold', () => {
        expect(() => validateProviderConfig({ ...valid, maxUpdatesThreshold: 0 }))
            .toThrow(/Invalid maxUpdatesThreshold/);
        expect(() => validateProviderConfig({ ...valid, maxUpdatesThreshold: -1 }))
            .toThrow(/Invalid maxUpdatesThreshold/);
    });

    it('accepts a threshold of one', () => {
        expect(() => validateProviderConfig({ ...valid, maxUpdatesThreshold: 1 })).not.toThrow();
    });

    it('rejects a non-positive aggregation time', () => {
        expect(() => validateProviderConfig({ ...valid, maxAggregationTime: 0 }))
            .toThrow(/Invalid maxAggregationTime/);
    });

    it('rejects depth outside 0..100', () => {
        expect(() => validateProviderConfig({ ...valid, depth: -1 })).toThrow(/Invalid depth/);
        expect(() => validateProviderConfig({ ...valid, depth: 101 })).toThrow(/Invalid depth/);
    });

    it('accepts both ends of the depth range', () => {
        expect(() => validateProviderConfig({ ...valid, depth: 0 })).not.toThrow();
        expect(() => validateProviderConfig({ ...valid, depth: 100 })).not.toThrow();
    });

    it('reports the path problem first when several are wrong', () => {
        expect(() => validateProviderConfig({ path: '', maxUpdatesThreshold: 0, maxAggregationTime: 0, depth: -5 }))
            .toThrow(/Invalid Firestore path/);
    });
});

describe('isRemoteOrigin', () => {
    it.each([FIREBASE_ORIGINS.SNAPSHOT, FIREBASE_ORIGINS.HISTORY, FIREBASE_ORIGINS.UPDATE])(
        'treats %s as remote',
        (origin) => {
            expect(isRemoteOrigin(origin)).toBe(true);
        },
    );

    it('treats a local edit as not remote', () => {
        expect(isRemoteOrigin(undefined)).toBe(false);
        expect(isRemoteOrigin(null)).toBe(false);
        expect(isRemoteOrigin('user-typing')).toBe(false);
        expect(isRemoteOrigin({})).toBe(false);
    });
});

describe('computeSaveDelay', () => {
    const base = { maxWaitTime: 1_000, maxAggregationTime: 5_000, pendingSince: null, now: 10_000 };

    it('waits the full debounce when nothing is buffered yet', () => {
        expect(computeSaveDelay(base)).toBe(1_000);
    });

    it('waits the full debounce while well inside the aggregation window', () => {
        expect(computeSaveDelay({ ...base, pendingSince: 9_500 })).toBe(1_000);
    });

    /*
     * The ceiling is what makes continuous typing terminate: the debounce
     * slides forever, so the delay must shrink as the deadline nears.
     */
    it('shortens the delay as the aggregation deadline approaches', () => {
        // Buffered at 6,000; deadline 11,000; now 10,000 -> only 1,000 left,
        // and at now 10,500 only 500.
        expect(computeSaveDelay({ ...base, pendingSince: 6_000, now: 10_500 })).toBe(500);
    });

    it('collapses to zero once the deadline has passed', () => {
        expect(computeSaveDelay({ ...base, pendingSince: 1_000, now: 10_000 })).toBe(0);
    });

    it('never returns a negative delay', () => {
        expect(computeSaveDelay({ ...base, pendingSince: 0, now: 1_000_000 })).toBe(0);
    });

    it('returns zero exactly at the deadline', () => {
        expect(computeSaveDelay({ ...base, pendingSince: 5_000, now: 10_000 })).toBe(0);
    });

    /* An explicit delay is a retry backoff, not aggregation. */
    it('uses an explicit delay verbatim, ignoring the ceiling', () => {
        expect(computeSaveDelay({ ...base, explicitDelayMs: 250, pendingSince: 0 })).toBe(250);
        expect(computeSaveDelay({ ...base, explicitDelayMs: 30_000, pendingSince: 0 })).toBe(30_000);
    });

    it('honours an explicit zero rather than falling back to the debounce', () => {
        expect(computeSaveDelay({ ...base, explicitDelayMs: 0 })).toBe(0);
    });
});

describe('squashBlockedBy', () => {
    const ready = { isDestroyed: false, synced: true, epochFenced: false, subProviderCount: 0, depth: 0 };

    it('allows a squash when everything is ready', () => {
        expect(squashBlockedBy(ready)).toBeNull();
    });

    it('blocks a destroyed provider first of all', () => {
        expect(squashBlockedBy({ ...ready, isDestroyed: true, synced: false }))
            .toEqual({ kind: 'destroyed' });
    });

    /*
     * Squashing while unsynced rebuilds the document from a copy that is
     * missing other clients' data — and nothing reports it afterwards.
     */
    it('blocks an unsynced provider', () => {
        expect(squashBlockedBy({ ...ready, synced: false })).toEqual({ kind: 'local-behind' });
    });

    it('blocks an epoch-fenced provider', () => {
        expect(squashBlockedBy({ ...ready, epochFenced: true })).toEqual({ kind: 'local-behind' });
    });

    it('blocks a provider that owns subdocuments', () => {
        expect(squashBlockedBy({ ...ready, subProviderCount: 1 }))
            .toEqual({ kind: 'subdocs-unsupported' });
    });

    it('blocks a provider that is itself a subdocument', () => {
        expect(squashBlockedBy({ ...ready, depth: 1 })).toEqual({ kind: 'subdocs-unsupported' });
    });
});
