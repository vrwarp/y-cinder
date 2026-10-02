/**
 * Regression: squash() must not break the lock of a compaction that is
 * already in flight on the same provider.
 *
 * Bug: squash() calls `await this.compact()` to fold the backlog first, but
 * compact() returns immediately while a compaction is already running
 * (`_isCompacting`), so squash proceeds alongside it. squashDocument then
 * "acquires" the compaction lock — which succeeds because the lock is
 * re-entrant for the same uid — and its `finally` releases (deletes) the
 * lock doc on every exit path. The in-flight compaction is still working
 * (downloading / merging / uploading) but no longer holds anything, so any
 * other client can take the lock and fold the same version concurrently.
 *
 * Contract pinned here: while a compaction is in flight, no other client
 * can acquire the compaction lock — regardless of the app calling squash()
 * in the meantime. (A fix may make squash wait for the compaction, skip,
 * or never share/release the compaction's lock; all of those keep this
 * test green.)
 *
 * The in-flight compaction is parked deterministically on the provider's
 * `testHooks.beforeTransaction` gate (a listener-triggered compaction runs
 * the exact same compact() path as the explicit call used here).
 *
 * Two squash outcomes are covered: a squash that would succeed (the backlog
 * is our own update), and one that skips as 'local-behind' because another
 * client's pending update is not covered locally. The skip is the harmful
 * path: the server stays at the same version, so a second compactor that
 * grabs the released lock folds exactly what the parked compaction folds.
 *
 * @file squash_during_compaction_lock.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as Y from 'yjs';
import { collection, getDocs, getDoc, doc, addDoc, Bytes, serverTimestamp } from 'firebase/firestore';
import { FireProvider } from '../../src/provider';
import { acquireLock } from '../../src/locking';
import type { SquashResult } from '../../src/squash';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** Long enough that the lease can never lapse during the test. */
const LOCK_TTL = 120_000;

/**
 * How long squash() is given to finish on its own while the compaction is
 * parked. On the buggy code it settles in well under a second; a fix that
 * makes squash wait for the compaction keeps it pending past this window.
 */
const SQUASH_SETTLE_WINDOW_MS = 10_000;

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function describeOutcome(outcome: SquashResult | 'pending'): string {
    if (outcome === 'pending') return 'pending';
    return JSON.stringify({
        success: outcome.success,
        skippedReason: outcome.skippedReason,
        error: outcome.error?.message,
    });
}

describe('squash() during an in-flight compaction', () => {
    let app: any;
    let db: any;
    let path: string;
    let counter = 0;

    let provider: FireProvider | null = null;
    let releaseCompaction: (() => void) | null = null;
    let compaction: Promise<void> | null = null;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        path = `tests/squash-inflight-compaction-lock-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    afterEach(async () => {
        releaseCompaction?.();
        releaseCompaction = null;
        if (provider) {
            await provider.destroy();
            provider = null;
        }
        if (compaction) {
            await Promise.race([compaction.catch(() => { }), sleep(15_000)]);
            compaction = null;
        }
    });

    /** Loads the document the way any new device would and returns its content. */
    async function loadAsFreshClient(): Promise<Record<string, unknown>> {
        const ydoc = new Y.Doc();
        const reader = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 1000, // no compaction from the reader
        });
        try {
            await waitForConditionTruthy(() => reader.synced, { timeout: 30000, message: 'fresh client synced' });
            return ydoc.getMap('m').toJSON();
        } finally {
            await reader.destroy();
            ydoc.destroy();
        }
    }

    /**
     * Parks a compaction of one local edit, runs `beforeSquash`, squashes,
     * and checks the compaction's lock stayed exclusive throughout.
     */
    async function squashDuringParkedCompaction(opts: {
        beforeSquash?: () => Promise<void>;
        expectedContent: Record<string, unknown>;
    }): Promise<void> {
        let markParked!: () => void;
        const compactionParked = new Promise<void>((r) => { markParked = r; });
        let release!: () => void;
        const gate = new Promise<void>((r) => { release = r; });
        releaseCompaction = release;
        let hookCalls = 0;

        const ydoc = new Y.Doc();
        provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 1000, // the compaction is started explicitly below
            maxWaitTime: 50,
            lockTTL: LOCK_TTL,
            testHooks: {
                // Park only the first compaction run, after it took the lock
                // and listed its work items.
                beforeTransaction: async () => {
                    if (hookCalls++ === 0) {
                        markParked();
                        await gate;
                    }
                },
            },
        });
        const p = provider;
        await waitForConditionTruthy(() => p.synced, { timeout: 30000, message: 'provider synced' });

        // Give the compaction some work: one persisted local edit.
        ydoc.getMap('m').set('k', 'v');
        await waitForConditionTruthy(
            async () => (await getDocs(collection(db, path, 'updates'))).size >= 1,
            { timeout: 20000, message: 'local edit persisted as an update doc' }
        );

        // A compaction is in flight: it holds the lock and is parked mid-run.
        compaction = p.compact();
        await compactionParked;
        expect(p.isCompacting).toBe(true);
        // Precondition: the running compaction excludes other clients.
        expect(await acquireLock({ db, path, uid: 'client-B', lockTTL: LOCK_TTL, cachedClockOffset: 0 })).toBe(false);

        await opts.beforeSquash?.();

        // The app squashes while that compaction is still running.
        let squashOutcome: SquashResult | 'pending' = 'pending';
        const squash = p.squash().then((r) => { squashOutcome = r; return r; });
        await Promise.race([squash, sleep(SQUASH_SETTLE_WINDOW_MS)]);

        // The compaction has not finished...
        expect(p.isCompacting).toBe(true);
        // ...so its lock must still exclude every other client.
        const lockDocExists = (await getDoc(doc(db, path, 'metadata/lock_compaction'))).exists();
        const otherClientGotLock = await acquireLock({ db, path, uid: 'client-B', lockTTL: LOCK_TTL, cachedClockOffset: 0 });
        expect(
            otherClientGotLock,
            `another client acquired the compaction lock while the compaction was still in flight ` +
            `(squash outcome: ${describeOutcome(squashOutcome)}, lock doc existed: ${lockDocExists})`
        ).toBe(false);

        // Let the compaction finish; the squash completes after it.
        release();
        await compaction;
        const result = await squash;
        expect(result.error).toBeUndefined();

        // Nothing was lost along the way.
        expect(await loadAsFreshClient()).toEqual(opts.expectedContent);
    }

    it('keeps the in-flight compaction\'s lock exclusive while the app squashes', { timeout: 120000 }, async () => {
        await squashDuringParkedCompaction({ expectedContent: { k: 'v' } });
    });

    it('keeps the lock exclusive when that squash skips as local-behind', { timeout: 120000 }, async () => {
        await squashDuringParkedCompaction({
            // Another client's pending update whose metadata claims clocks the
            // local doc lacks: squash must skip rather than drop its data.
            beforeSquash: async () => {
                const foreign = new Y.Doc();
                foreign.clientID = 123456789;
                foreign.getMap('m').set('f', 1);
                await addDoc(collection(db, path, 'updates'), {
                    update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(foreign)),
                    createdAt: serverTimestamp(),
                    createdBy: 'client-F',
                    clientIDs: [123456789],
                    clientClocks: [50],
                });
                foreign.destroy();
            },
            expectedContent: { k: 'v', f: 1 },
        });
    });
});
