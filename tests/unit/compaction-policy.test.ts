/**
 * Unit tests for compaction's decision logic.
 *
 * These choices decide whether a compaction cycle costs O(new data) or
 * O(whole document), whether a pre-squash document gets merged into the
 * snapshot (which poisons it permanently), and whether a failure is retried
 * or given up on. They were previously interleaved with Firestore I/O and
 * so reachable only through the emulator suite — which a mutation run
 * cannot execute once per mutant.
 */
import { describe, it, expect } from 'vitest';
import {
    blobsReplacedByFold,
    buildDeltaSegmentDoc,
    buildSnapshotResult,
    chooseDeleteSetField,
    deleteSetFitsInline,
    deltaSegmentFitsInline,
    epochOf,
    foldDeleteSetPath,
    foldSnapshotPath,
    nextSnapshotVersion,
    isLockLostError,
    isRetryableCompactionError,
    planHistoryDoc,
    planUpdateDoc,
    readMainDocState,
    shouldRetryCompaction,
    shouldUseDelta,
} from '../../src/compaction-policy';
import { DEFAULTS } from '../../src/types';

describe('epochOf', () => {
    it('reads a numeric epoch', () => {
        expect(epochOf({ epoch: 3 })).toBe(3);
    });

    it('treats a pre-squash document with no epoch as epoch 0', () => {
        expect(epochOf({})).toBe(0);
    });

    it('treats epoch 0 as 0 rather than falling through', () => {
        expect(epochOf({ epoch: 0 })).toBe(0);
    });

    it('ignores a non-numeric epoch', () => {
        expect(epochOf({ epoch: '5' } as any)).toBe(0);
        expect(epochOf({ epoch: null } as any)).toBe(0);
    });

    it('handles missing data', () => {
        expect(epochOf(null)).toBe(0);
        expect(epochOf(undefined)).toBe(0);
    });
});

describe('readMainDocState', () => {
    it('defaults everything when the main document does not exist', () => {
        expect(readMainDocState(null)).toEqual({
            hasBase: false,
            baseStoragePath: null,
            baseDeleteSetStoragePath: null,
            baseInline: null,
            currentVersion: 0,
            currentEpoch: 0,
        });
    });

    it('reads a Storage-backed base', () => {
        const state = readMainDocState({ snapshotStoragePath: 'gs://snap', version: 4, epoch: 2 });

        expect(state.hasBase).toBe(true);
        expect(state.baseStoragePath).toBe('gs://snap');
        expect(state.baseInline).toBeNull();
        expect(state.currentVersion).toBe(4);
        expect(state.currentEpoch).toBe(2);
    });

    it('reads a legacy inline base', () => {
        const content = { marker: 'inline' };
        const state = readMainDocState({ content });

        expect(state.hasBase).toBe(true);
        expect(state.baseInline).toBe(content);
        expect(state.baseStoragePath).toBeNull();
    });

    /*
     * Both fields can coexist on a document written before the Storage
     * migration. Preferring the stale inline copy would roll the snapshot
     * back to an older state.
     */
    it('prefers the Storage path when a stale inline copy is also present', () => {
        const state = readMainDocState({ snapshotStoragePath: 'gs://snap', content: { stale: true } });

        expect(state.baseStoragePath).toBe('gs://snap');
        expect(state.baseInline).toBeNull();
    });

    it('reads an offloaded delete-set path', () => {
        const state = readMainDocState({ snapshotStoragePath: 'gs://snap', deleteSetStoragePath: 'gs://ds' });

        expect(state.baseDeleteSetStoragePath).toBe('gs://ds');
        expect(readMainDocState({ snapshotStoragePath: 'gs://snap' }).baseDeleteSetStoragePath).toBeNull();
    });

    it('reports no base when the document exists but carries neither field', () => {
        const state = readMainDocState({ version: 9 });

        expect(state.hasBase).toBe(false);
        expect(state.currentVersion).toBe(9);
    });

    /* Fold GC deletes the replaced blobs through these stored paths. */
    it('reads the offloaded delete-set path alongside the snapshot path', () => {
        const state = readMainDocState({ snapshotStoragePath: 'gs://snap', deleteSetStoragePath: 'gs://ds' });

        expect(state.baseStoragePath).toBe('gs://snap');
        expect(state.baseDeleteSetStoragePath).toBe('gs://ds');
    });

    it('reports no delete-set path when the fingerprint is inline', () => {
        expect(readMainDocState({ snapshotStoragePath: 'gs://snap', deleteSet: {} }).baseDeleteSetStoragePath)
            .toBeNull();
    });

    it('ignores a non-numeric version or epoch', () => {
        const state = readMainDocState({ version: 'four', epoch: {} });

        expect(state.currentVersion).toBe(0);
        expect(state.currentEpoch).toBe(0);
    });
});

describe('shouldUseDelta', () => {
    const base = { hasBase: true, updateCount: 3, historyCount: 0, historyFoldThreshold: 8 };

    it('runs delta in the steady state', () => {
        expect(shouldUseDelta(base)).toBe(true);
    });

    it('folds when there is no base snapshot to build on', () => {
        expect(shouldUseDelta({ ...base, hasBase: false })).toBe(false);
    });

    it('folds when there is nothing new to add', () => {
        expect(shouldUseDelta({ ...base, updateCount: 0 })).toBe(false);
    });

    /*
     * The +1 counts the segment this cycle would write, so the fold must
     * happen one cycle before the threshold is reached, not after.
     */
    it('folds once the segment this cycle adds would reach the threshold', () => {
        expect(shouldUseDelta({ ...base, historyCount: 6, historyFoldThreshold: 8 })).toBe(true);
        expect(shouldUseDelta({ ...base, historyCount: 7, historyFoldThreshold: 8 })).toBe(false);
        expect(shouldUseDelta({ ...base, historyCount: 8, historyFoldThreshold: 8 })).toBe(false);
    });

    it('always folds when the threshold is 1', () => {
        expect(shouldUseDelta({ ...base, historyCount: 0, historyFoldThreshold: 1 })).toBe(false);
    });

    it('never goes negative on the comparison', () => {
        expect(shouldUseDelta({ ...base, historyCount: 0, historyFoldThreshold: 0 })).toBe(false);
    });
});

describe('isRetryableCompactionError', () => {
    it.each(['aborted', 'unavailable', 'deadline-exceeded'])('retries on %s', (code) => {
        expect(isRetryableCompactionError({ code })).toBe(true);
    });

    it.each(['permission-denied', 'invalid-argument', 'not-found', 'resource-exhausted'])(
        'does not retry on %s',
        (code) => {
            expect(isRetryableCompactionError({ code })).toBe(false);
        },
    );

    it('does not retry an error with no code', () => {
        expect(isRetryableCompactionError(new Error('merge validation failed'))).toBe(false);
        expect(isRetryableCompactionError(null)).toBe(false);
        expect(isRetryableCompactionError(undefined)).toBe(false);
    });
});

describe('isLockLostError', () => {
    it('detects a lost lock from the message', () => {
        expect(isLockLostError(new Error('Lock lost during transaction'))).toBe(true);
    });

    it('is false for unrelated messages', () => {
        expect(isLockLostError(new Error('aborted'))).toBe(false);
    });

    it('tolerates a missing or non-string message', () => {
        expect(isLockLostError({})).toBe(false);
        expect(isLockLostError({ message: 42 })).toBe(false);
        expect(isLockLostError(null)).toBe(false);
    });
});

describe('shouldRetryCompaction', () => {
    const retryable = { code: 'aborted' };

    it('retries a contention failure on an early attempt', () => {
        expect(shouldRetryCompaction({ error: retryable, attempt: 1, isDestroyed: false })).toBe(true);
    });

    it('stops at the retry ceiling', () => {
        expect(shouldRetryCompaction({
            error: retryable, attempt: DEFAULTS.MAX_RETRIES - 1, isDestroyed: false,
        })).toBe(true);
        expect(shouldRetryCompaction({
            error: retryable, attempt: DEFAULTS.MAX_RETRIES, isDestroyed: false,
        })).toBe(false);
        expect(shouldRetryCompaction({
            error: retryable, attempt: DEFAULTS.MAX_RETRIES + 5, isDestroyed: false,
        })).toBe(false);
    });

    it('does not retry once the provider is destroyed', () => {
        expect(shouldRetryCompaction({ error: retryable, attempt: 1, isDestroyed: true })).toBe(false);
    });

    /*
     * A lost lock means another client is already compacting. Retrying
     * would contend with it rather than help.
     */
    it('does not retry a lost lock even though the code is retryable', () => {
        const error = Object.assign(new Error('Lock lost'), { code: 'aborted' });

        expect(shouldRetryCompaction({ error, attempt: 1, isDestroyed: false })).toBe(false);
    });

    it('does not retry a non-retryable code', () => {
        expect(shouldRetryCompaction({
            error: { code: 'permission-denied' }, attempt: 1, isDestroyed: false,
        })).toBe(false);
    });

    it('honours an explicit retry ceiling', () => {
        expect(shouldRetryCompaction({ error: retryable, attempt: 2, isDestroyed: false, maxRetries: 2 }))
            .toBe(false);
        expect(shouldRetryCompaction({ error: retryable, attempt: 1, isDestroyed: false, maxRetries: 2 }))
            .toBe(true);
    });
});

describe('planUpdateDoc', () => {
    it('merges an inline update from the current epoch', () => {
        expect(planUpdateDoc({ epoch: 2, update: 'bytes' }, 2)).toEqual({ kind: 'inline' });
    });

    it('downloads a Storage-backed update', () => {
        expect(planUpdateDoc({ epoch: 2, updateStoragePath: 'gs://u' }, 2))
            .toEqual({ kind: 'storage', storagePath: 'gs://u' });
    });

    it('prefers the inline payload when both are present (no download needed)', () => {
        expect(planUpdateDoc({ epoch: 2, update: 'bytes', updateStoragePath: 'gs://u' }, 2))
            .toEqual({ kind: 'inline' });
    });

    /*
     * The epoch check is absolute and comes first: merging a pre-squash
     * update parks unresolvable structs in the snapshot forever.
     */
    it('marks a foreign-epoch update stale regardless of payload', () => {
        expect(planUpdateDoc({ epoch: 1, update: 'bytes' }, 2)).toEqual({ kind: 'stale' });
        expect(planUpdateDoc({ epoch: 3, updateStoragePath: 'gs://u' }, 2)).toEqual({ kind: 'stale' });
    });

    it('treats an epoch-less document as epoch 0', () => {
        expect(planUpdateDoc({ update: 'bytes' }, 0)).toEqual({ kind: 'inline' });
        expect(planUpdateDoc({ update: 'bytes' }, 1)).toEqual({ kind: 'stale' });
    });

    it('skips a current-epoch document with no payload at all', () => {
        expect(planUpdateDoc({ epoch: 2 }, 2)).toEqual({ kind: 'skip' });
        expect(planUpdateDoc(null, 0)).toEqual({ kind: 'skip' });
    });
});

describe('planHistoryDoc', () => {
    it('merges a segment from the current epoch', () => {
        expect(planHistoryDoc({ epoch: 1, segment: 'bytes' }, 1)).toEqual({ kind: 'merge' });
    });

    it('marks a foreign-epoch segment stale', () => {
        expect(planHistoryDoc({ epoch: 0, segment: 'bytes' }, 1)).toEqual({ kind: 'stale' });
    });

    it('skips a current-epoch segment with no payload', () => {
        expect(planHistoryDoc({ epoch: 1 }, 1)).toEqual({ kind: 'skip' });
        expect(planHistoryDoc(null, 0)).toEqual({ kind: 'skip' });
    });
});

describe('deltaSegmentFitsInline', () => {
    it('fits below and at the limit', () => {
        expect(deltaSegmentFitsInline(999, 0, 1_000)).toBe(true);
        expect(deltaSegmentFitsInline(1_000, 0, 1_000)).toBe(true);
    });

    it('does not fit one byte over', () => {
        expect(deltaSegmentFitsInline(1_001, 0, 1_000)).toBe(false);
    });

    it('fits an empty segment', () => {
        expect(deltaSegmentFitsInline(0, 0, 1_000)).toBe(true);
    });

    /*
     * The state vector rides on the same history document. A segment just
     * under the limit plus a many-client state vector is rejected by
     * Firestore with a non-retryable INVALID_ARGUMENT.
     */
    it('counts the state vector stored beside the segment', () => {
        expect(deltaSegmentFitsInline(900, 100, 1_000)).toBe(true);
        expect(deltaSegmentFitsInline(900, 101, 1_000)).toBe(false);
    });
});

describe('deleteSetFitsInline', () => {
    const fits = (deleteSetBytes: number, stateVectorB64Length: number) =>
        deleteSetFitsInline({ deleteSetBytes, stateVectorB64Length, maxFieldBytes: 700, inlineLimit: 1_000 });

    it('inlines a fingerprint under the field cap with room beside the state vector', () => {
        expect(fits(700, 300)).toBe(true);
        expect(fits(0, 0)).toBe(true);
    });

    it('offloads a fingerprint over the field cap', () => {
        expect(fits(701, 0)).toBe(false);
    });

    /*
     * Under the field cap is not enough: on an aged many-client document
     * the state vector and the fingerprint together pass the Firestore
     * limit, and the snapshot write would fail permanently.
     */
    it('offloads a fingerprint that would not fit beside the state vector', () => {
        expect(fits(600, 401)).toBe(false);
        expect(fits(1, 1_000)).toBe(false);
    });

    it('applies the real defaults to the regression scenario', () => {
        // ~75k clients: base64 state vector ~600 KB, fingerprint ~600 KB.
        expect(deleteSetFitsInline({
            deleteSetBytes: 599_996,
            stateVectorB64Length: 600_004,
            maxFieldBytes: DEFAULTS.MAX_DELETE_SET_FIELD_BYTES,
            inlineLimit: DEFAULTS.INLINE_UPDATE_LIMIT,
        })).toBe(false);
    });
});

describe('buildDeltaSegmentDoc', () => {
    it('carries the state vector and author', () => {
        expect(buildDeltaSegmentDoc({ stateVectorB64: 'sv', hasDeletions: false, uid: 'me', epoch: 0 }))
            .toEqual({ stateVector: 'sv', createdBy: 'me' });
    });

    /*
     * Omitted rather than written as 0, so a never-squashed database keeps
     * producing documents identical to what older clients wrote.
     */
    it('omits the epoch field entirely at epoch 0', () => {
        expect('epoch' in buildDeltaSegmentDoc({ stateVectorB64: 'sv', hasDeletions: false, uid: 'me', epoch: 0 })).toBe(false);
    });

    it('writes the epoch once past 0', () => {
        expect(buildDeltaSegmentDoc({ stateVectorB64: 'sv', hasDeletions: false, uid: 'me', epoch: 3 }))
            .toEqual({ stateVector: 'sv', createdBy: 'me', epoch: 3 });
    });

    /*
     * The state vector cannot show deletions, so readers rely on this flag
     * to apply a segment whose structs they already hold.
     */
    it('flags a segment that carries deletions', () => {
        expect(buildDeltaSegmentDoc({ stateVectorB64: 'sv', hasDeletions: true, uid: 'me', epoch: 0 }))
            .toEqual({ stateVector: 'sv', hasDeletions: true, createdBy: 'me' });
    });

    it('omits the deletions flag entirely when there are none', () => {
        expect('hasDeletions' in buildDeltaSegmentDoc({ stateVectorB64: 'sv', hasDeletions: false, uid: 'me', epoch: 3 })).toBe(false);
    });
});

describe('chooseDeleteSetField', () => {
    it('writes inline when a fingerprint was produced', () => {
        expect(chooseDeleteSetField({ deleteSetUpdate: new Uint8Array([1]), deleteSetStoragePath: null }))
            .toEqual({ writeInline: true, writeStoragePath: false });
    });

    it('writes a storage pointer when the fingerprint was offloaded', () => {
        expect(chooseDeleteSetField({ deleteSetUpdate: null, deleteSetStoragePath: 'gs://ds' }))
            .toEqual({ writeInline: false, writeStoragePath: true });
    });

    it('writes neither when there is no fingerprint at all', () => {
        expect(chooseDeleteSetField({ deleteSetUpdate: null, deleteSetStoragePath: null }))
            .toEqual({ writeInline: false, writeStoragePath: false });
    });

    it('treats an empty fingerprint as present, not absent', () => {
        expect(chooseDeleteSetField({ deleteSetUpdate: new Uint8Array(0), deleteSetStoragePath: null }).writeInline)
            .toBe(true);
    });
});

describe('buildSnapshotResult', () => {
    it('reports what the cycle processed', () => {
        expect(buildSnapshotResult({ updatesCompacted: 5, historySegmentsMerged: 2, currentVersion: 7 }))
            .toEqual({
                success: true,
                type: 'snapshot',
                updatesCompacted: 5,
                historySegmentsMerged: 2,
                previousVersion: 7,
            });
    });

    /* A first-ever compaction must be distinguishable from replacing v0. */
    it('omits previousVersion when there was no previous snapshot', () => {
        expect(buildSnapshotResult({ updatesCompacted: 1, historySegmentsMerged: 0, currentVersion: 0 }).previousVersion)
            .toBeUndefined();
    });

    it('reports version 1 as a real previous version', () => {
        expect(buildSnapshotResult({ updatesCompacted: 1, historySegmentsMerged: 0, currentVersion: 1 }).previousVersion)
            .toBe(1);
    });
});

describe('nextSnapshotVersion', () => {
    it('increments by one', () => {
        expect(nextSnapshotVersion(0)).toBe(1);
        expect(nextSnapshotVersion(41)).toBe(42);
    });
});

/*
 * Candidates are uploaded before the lock-checked commit. Two attempts
 * folding the same version must never share an object, or a compactor
 * whose lease lapsed mid-upload replaces the winner's committed snapshot.
 */
describe('foldSnapshotPath / foldDeleteSetPath', () => {
    it('names the blobs by version and attempt', () => {
        expect(foldSnapshotPath('docs/a', 3, 'x1')).toBe('docs/a/snapshot_v3_x1.bin');
        expect(foldDeleteSetPath('docs/a', 3, 'x1')).toBe('docs/a/ds_v3_x1.bin');
    });

    it('distinguishes attempts folding the same version', () => {
        expect(foldSnapshotPath('docs/a', 3, 'x1')).not.toBe(foldSnapshotPath('docs/a', 3, 'x2'));
        expect(foldDeleteSetPath('docs/a', 3, 'x1')).not.toBe(foldDeleteSetPath('docs/a', 3, 'x2'));
    });

    it('distinguishes versions', () => {
        expect(foldSnapshotPath('docs/a', 3, 'x1')).not.toBe(foldSnapshotPath('docs/a', 4, 'x1'));
        expect(foldDeleteSetPath('docs/a', 3, 'x1')).not.toBe(foldDeleteSetPath('docs/a', 4, 'x1'));
    });
});

describe('blobsReplacedByFold', () => {
    const written = ['docs/a/snapshot_v4.bin', null];

    /*
     * squash() names its blob snapshot_e{E}_v{V}_{id}.bin. Rebuilding the old
     * name from the version (snapshot_v3.bin) leaked it forever.
     */
    it('returns the stored squash snapshot path, not a name rebuilt from the version', () => {
        expect(blobsReplacedByFold(
            { baseStoragePath: 'docs/a/snapshot_e1_v3_k9.bin', baseDeleteSetStoragePath: null },
            written,
        )).toEqual(['docs/a/snapshot_e1_v3_k9.bin']);
    });

    it('returns the stored snapshot and offloaded delete-set paths', () => {
        expect(blobsReplacedByFold(
            { baseStoragePath: 'docs/a/snapshot_v3.bin', baseDeleteSetStoragePath: 'docs/a/ds_v3.bin' },
            ['docs/a/snapshot_v4.bin', 'docs/a/ds_v4.bin'],
        )).toEqual(['docs/a/snapshot_v3.bin', 'docs/a/ds_v3.bin']);
    });

    it('returns nothing when the replaced document had no Storage blobs', () => {
        expect(blobsReplacedByFold({ baseStoragePath: null, baseDeleteSetStoragePath: null }, written))
            .toEqual([]);
    });

    /* Deleting a path the fold just committed would destroy the live snapshot. */
    it('never returns a path the fold itself wrote', () => {
        expect(blobsReplacedByFold(
            { baseStoragePath: 'docs/a/snapshot_v4.bin', baseDeleteSetStoragePath: 'docs/a/ds_v4.bin' },
            ['docs/a/snapshot_v4.bin', 'docs/a/ds_v4.bin'],
        )).toEqual([]);
    });
});
