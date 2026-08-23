/**
 * Unit tests for the distributed compaction lock's decisions.
 *
 * Wrong in one direction and two clients compact the same document at
 * once; wrong in the other and one crashed client stops compaction
 * forever. Both were previously only reachable inside a Firestore
 * transaction.
 */
import { describe, it, expect } from 'vitest';
import {
    estimateServerNow,
    isLockBusy,
    isLockExpired,
    offsetFromProbe,
    readLockCreatedAt,
    readProbeServerTime,
} from '../../src/locking-policy';

const stamp = (millis: number) => ({ toMillis: () => millis });

describe('readLockCreatedAt', () => {
    it('reads a Firestore Timestamp', () => {
        expect(readLockCreatedAt(stamp(1_700_000))).toBe(1_700_000);
    });

    it('accepts a raw number written by an older client', () => {
        expect(readLockCreatedAt(1_500)).toBe(1_500);
    });

    /*
     * 0 makes the lock look infinitely old, so it is treated as expired
     * and can be reclaimed. The alternative — treating an unreadable
     * timestamp as "now" — creates a lock nobody can ever break.
     */
    it('reads anything unusable as 0, so the lock is reclaimable', () => {
        expect(readLockCreatedAt(undefined)).toBe(0);
        expect(readLockCreatedAt(null)).toBe(0);
        expect(readLockCreatedAt({})).toBe(0);
        expect(readLockCreatedAt('2026-01-01')).toBe(0);
        expect(readLockCreatedAt({ toMillis: 'not a function' })).toBe(0);
    });

    it('preserves a zero timestamp rather than treating it as missing', () => {
        expect(readLockCreatedAt(stamp(0))).toBe(0);
        expect(readLockCreatedAt(0)).toBe(0);
    });
});

describe('estimateServerNow', () => {
    it('adds the offset to the local clock', () => {
        expect(estimateServerNow(1_000, 250)).toBe(1_250);
    });

    it('handles a local clock ahead of the server', () => {
        expect(estimateServerNow(1_000, -250)).toBe(750);
    });

    it('is the identity at zero offset', () => {
        expect(estimateServerNow(1_000, 0)).toBe(1_000);
    });
});

describe('isLockBusy', () => {
    const base = { createdAt: stamp(1_000), owner: 'other', uid: 'me', serverNow: 1_500, lockTTL: 1_000 };

    it('blocks on a fresh lock held by someone else', () => {
        expect(isLockBusy(base)).toBe(true);
    });

    /* Re-entrant: our own lock never blocks us. */
    it('does not block on our own lock, however fresh', () => {
        expect(isLockBusy({ ...base, owner: 'me' })).toBe(false);
        expect(isLockBusy({ ...base, owner: 'me', serverNow: 1_000 })).toBe(false);
    });

    it('does not block once the lock has aged past the TTL', () => {
        expect(isLockBusy({ ...base, serverNow: 2_500 })).toBe(false);
    });

    /*
     * The boundary must agree with isLockExpired: age === TTL is expired,
     * so it must not also be "busy" — otherwise a lock at exactly its TTL
     * is both expired and unbreakable.
     */
    it('treats age exactly at the TTL as free, matching isLockExpired', () => {
        expect(isLockBusy({ ...base, serverNow: 2_000 })).toBe(false);
        expect(isLockExpired(1_000, 1_000)).toBe(true);
    });

    it('blocks one millisecond before the TTL', () => {
        expect(isLockBusy({ ...base, serverNow: 1_999 })).toBe(true);
    });

    it('treats an unreadable createdAt as expired', () => {
        expect(isLockBusy({ ...base, createdAt: undefined })).toBe(false);
    });

    it('blocks when the owner field is missing but the lock is fresh', () => {
        expect(isLockBusy({ ...base, owner: undefined })).toBe(true);
    });

    it('uses server time, not local time, to judge age', () => {
        // A client whose clock is an hour fast must not consider a fresh
        // lock expired.
        expect(isLockBusy({ ...base, serverNow: 1_500 })).toBe(true);
    });
});

describe('isLockExpired', () => {
    it('is false before the TTL', () => {
        expect(isLockExpired(999, 1_000)).toBe(false);
    });

    it('is true at and after the TTL', () => {
        expect(isLockExpired(1_000, 1_000)).toBe(true);
        expect(isLockExpired(5_000, 1_000)).toBe(true);
    });

    it('treats a negative age (clock skew) as unexpired', () => {
        expect(isLockExpired(-500, 1_000)).toBe(false);
    });
});

describe('clock skew probe', () => {
    it('reads the server timestamp from the probe document', () => {
        expect(readProbeServerTime({ t: stamp(9_000) })).toBe(9_000);
    });

    it('returns null when the probe field is missing or unusable', () => {
        expect(readProbeServerTime({})).toBeNull();
        expect(readProbeServerTime({ t: 9_000 })).toBeNull();
        expect(readProbeServerTime(null)).toBeNull();
        expect(readProbeServerTime(undefined)).toBeNull();
    });

    it('derives a positive offset when the server is ahead', () => {
        expect(offsetFromProbe(1_500, 1_000)).toBe(500);
    });

    it('derives a negative offset when the server is behind', () => {
        expect(offsetFromProbe(900, 1_000)).toBe(-100);
    });

    /* An unreadable probe must degrade to "no skew", not to a wild guess. */
    it('falls back to zero offset when the probe could not be read', () => {
        expect(offsetFromProbe(null, 1_000)).toBe(0);
    });
});
