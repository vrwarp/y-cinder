/**
 * Pure decision logic for compaction.
 *
 * compaction.ts interleaves these choices with Firestore reads, writes and
 * Storage downloads, which made them reachable only through the emulator
 * integration suite — and therefore invisible to mutation testing, since a
 * mutation run cannot execute that suite once per mutant. Nothing here
 * touches the Firestore SDK: every function takes plain document data and
 * returns a decision, so the fast unit suite can pin it.
 *
 * These are the choices that decide whether a compaction cycle costs
 * O(new data) or O(whole document), whether a pre-squash document is merged
 * into the snapshot (which would poison it permanently), and whether a
 * failure is retried or given up on.
 */
import { DEFAULTS } from './types';

/** Anything with an `epoch` field, or nothing at all. */
export type EpochBearing = { epoch?: unknown } | null | undefined;

/**
 * The epoch a document belongs to. Documents written before the squash
 * protocol existed carry no epoch and are treated as epoch 0.
 *
 * @param data - Firestore document data.
 * @returns The document's epoch, defaulting to 0.
 */
export function epochOf(data: EpochBearing): number {
    return typeof data?.epoch === 'number' ? data.epoch : 0;
}

/** What compaction needs to know about the main document before it starts. */
export interface MainDocState {
    /** Whether a base snapshot exists (inline or in Cloud Storage). */
    hasBase: boolean;
    /** Cloud Storage path of the base snapshot, when it lives there. */
    baseStoragePath: string | null;
    /** Cloud Storage path of the base's offloaded delete-set fingerprint. */
    baseDeleteSetStoragePath: string | null;
    /**
     * Cloud Storage path of the tail published by the fold that wrote the
     * base. Read whatever version it is bound to: a stale one left behind
     * by an older client is still a blob to garbage-collect.
     */
    baseFoldTailStoragePath: string | null;
    /** The base snapshot's base64 state vector, when the document has one. */
    baseStateVector: string | null;
    /** Legacy inline snapshot content, when the base is still inline. */
    baseInline: unknown | null;
    /** Snapshot version, used for optimistic concurrency. */
    currentVersion: number;
    /** Current epoch; documents from other epochs must not be merged. */
    currentEpoch: number;
}

/**
 * Reads the main document's compaction-relevant state.
 *
 * A Storage-backed snapshot wins over a legacy inline one: both fields can
 * be present on a document written before the Storage migration, and
 * treating the stale inline copy as the base would roll the snapshot back.
 *
 * @param data - Main document data, or null when the document is absent.
 * @returns The parsed state, with safe defaults for a missing document.
 */
export function readMainDocState(data: Record<string, any> | null | undefined): MainDocState {
    const state: MainDocState = {
        hasBase: false,
        baseStoragePath: null,
        baseDeleteSetStoragePath: null,
        baseFoldTailStoragePath: null,
        baseStateVector: null,
        baseInline: null,
        currentVersion: 0,
        currentEpoch: 0,
    };

    if (!data) {
        return state;
    }

    if (data.snapshotStoragePath) {
        state.hasBase = true;
        state.baseStoragePath = data.snapshotStoragePath;
    } else if (data.content) {
        state.hasBase = true;
        state.baseInline = data.content;
    }
    if (data.deleteSetStoragePath) {
        state.baseDeleteSetStoragePath = data.deleteSetStoragePath;
    }
    if (typeof data.foldTailStoragePath === 'string') {
        state.baseFoldTailStoragePath = data.foldTailStoragePath;
    }
    if (typeof data.stateVector === 'string') {
        state.baseStateVector = data.stateVector;
    }
    if (typeof data.version === 'number') {
        state.currentVersion = data.version;
    }
    if (typeof data.epoch === 'number') {
        state.currentEpoch = data.epoch;
    }

    return state;
}

/**
 * Chooses DELTA over FOLD for this cycle.
 *
 * DELTA merges only the pending updates into one new history segment, so it
 * costs O(new data) and never downloads or re-uploads the base snapshot.
 * FOLD rebuilds the snapshot from base + history + updates and costs
 * O(document), so it must stay amortized: it runs when there is no base to
 * build on, when there is nothing new, or when history has grown to the
 * fold threshold (counting the segment this cycle would add).
 *
 * The threshold is capped at maxHistory + 1. One fold can merge at most
 * maxHistory segments (the transaction write budget), so compaction never
 * counts more than that: a larger threshold could never be reached, and
 * history would grow without bound, never folding.
 *
 * @param params - Base presence, pending counts, the fold threshold and the
 * per-cycle history cap.
 * @returns true to run DELTA, false to FOLD.
 */
export function shouldUseDelta(params: {
    hasBase: boolean;
    updateCount: number;
    historyCount: number;
    historyFoldThreshold: number;
    maxHistory?: number;
}): boolean {
    const { hasBase, updateCount, historyCount, historyFoldThreshold, maxHistory = DEFAULTS.MAX_COMPACTION_HISTORY } = params;
    const foldThreshold = Math.min(historyFoldThreshold, maxHistory + 1);

    return hasBase && updateCount > 0 && historyCount + 1 < foldThreshold;
}

/**
 * Whether a Firestore failure is worth another attempt.
 *
 * Only contention and transport failures are; anything else (permission
 * denied, invalid argument, a validation throw from the merge) will fail
 * identically on retry and must surface instead of spinning.
 *
 * @param error - The thrown value.
 * @returns true when retrying could plausibly succeed.
 */
export function isRetryableCompactionError(error: any): boolean {
    return error?.code === 'aborted'
        || error?.code === 'unavailable'
        || error?.code === 'deadline-exceeded';
}

/**
 * Whether losing the distributed lock caused this failure.
 *
 * Retrying is pointless and actively harmful here: another client holds the
 * lock and is already compacting, so a retry would contend with it.
 *
 * @param error - The thrown value.
 * @returns true when the error reports a lost lock.
 */
export function isLockLostError(error: any): boolean {
    return typeof error?.message === 'string' && error.message.includes('Lock lost');
}

/**
 * The full retry decision for a failed compaction attempt.
 *
 * @param params - The error, the 1-based attempt number, and whether the
 * provider has been destroyed.
 * @returns true to retry after a backoff.
 */
export function shouldRetryCompaction(params: {
    error: any;
    attempt: number;
    isDestroyed: boolean;
    maxRetries?: number;
}): boolean {
    const { error, attempt, isDestroyed, maxRetries = DEFAULTS.MAX_RETRIES } = params;

    return attempt < maxRetries
        && isRetryableCompactionError(error)
        && !isLockLostError(error)
        && !isDestroyed;
}

/** How compaction should treat one pending update document. */
export type UpdateDocPlan =
    /** Belongs to another epoch: delete without merging. */
    | { kind: 'stale' }
    /** Payload is inline on the document. */
    | { kind: 'inline' }
    /** Payload must be downloaded from Cloud Storage first. */
    | { kind: 'storage'; storagePath: string }
    /** Nothing usable on the document: ignore it this cycle. */
    | { kind: 'skip' };

/**
 * Decides how to handle one update document, before any I/O.
 *
 * The epoch check comes first and is absolute: an update written before a
 * squash belongs to an unrelated id space, and merging it would park
 * unresolvable structs in the snapshot forever (which also disables GC).
 *
 * @param data - Update document data.
 * @param currentEpoch - The epoch the local document is on.
 * @returns The plan for this document.
 */
export function planUpdateDoc(data: Record<string, any> | null | undefined, currentEpoch: number): UpdateDocPlan {
    if (epochOf(data) !== currentEpoch) {
        return { kind: 'stale' };
    }
    if (data?.updateStoragePath && !data?.update) {
        return { kind: 'storage', storagePath: data.updateStoragePath };
    }
    if (data?.update) {
        return { kind: 'inline' };
    }

    return { kind: 'skip' };
}

/** How compaction should treat one history segment document. */
export type HistoryDocPlan =
    | { kind: 'stale' }
    | { kind: 'merge' }
    | { kind: 'skip' };

/**
 * Decides how to handle one history segment, before any I/O.
 *
 * @param data - History document data.
 * @param currentEpoch - The epoch the local document is on.
 * @returns The plan for this document.
 */
export function planHistoryDoc(data: Record<string, any> | null | undefined, currentEpoch: number): HistoryDocPlan {
    if (epochOf(data) !== currentEpoch) {
        return { kind: 'stale' };
    }

    return data?.segment ? { kind: 'merge' } : { kind: 'skip' };
}

/**
 * Whether a merged delta segment is small enough to store inline on a
 * Firestore document.
 *
 * The segment document also carries the segment's base64 state vector
 * (~8 bytes per client in the segment), so both count: Firestore rejects
 * an oversized document with INVALID_ARGUMENT, which is not retryable,
 * and the same pending updates would re-merge to the same segment on
 * every later cycle.
 *
 * Over the limit, delta mode is abandoned for this cycle and compaction
 * folds instead — the segment would not fit, and splitting it would defeat
 * the point of a delta.
 *
 * @param byteLength - Size of the merged segment.
 * @param stateVectorB64Length - Length of the segment's base64 state vector.
 * @param inlineLimit - The inline payload ceiling.
 * @returns true when the segment fits inline.
 */
export function deltaSegmentFitsInline(byteLength: number, stateVectorB64Length: number, inlineLimit: number): boolean {
    return byteLength + stateVectorB64Length <= inlineLimit;
}

/**
 * Builds the history-segment document a delta compaction writes.
 *
 * `epoch` is omitted entirely at epoch 0 rather than written as 0, so
 * documents from a never-squashed database stay byte-identical to what
 * older clients produced; `hasDeletions` is likewise omitted when false.
 *
 * `hasDeletions` lets readers apply a segment whose state vector they
 * already cover: the vector only spans structs and a deletion adds none
 * (a delete-only segment's vector is empty), so without the flag every
 * redundancy check would skip the segment and resurrect what it deleted.
 *
 * @param params - Segment bytes, its state vector, whether it carries
 * deletions, author and epoch.
 * @returns The document fields, minus server-generated timestamps.
 */
export function buildDeltaSegmentDoc(params: {
    stateVectorB64: string;
    hasDeletions: boolean;
    uid: string;
    epoch: number;
}): Record<string, unknown> {
    const { stateVectorB64, hasDeletions, uid, epoch } = params;

    return {
        stateVector: stateVectorB64,
        ...(hasDeletions ? { hasDeletions: true } : {}),
        createdBy: uid,
        ...(epoch > 0 ? { epoch } : {}),
    };
}

/**
 * Whether a snapshot's delete-set fingerprint can be stored inline on the
 * main document, or must be offloaded to Cloud Storage.
 *
 * The per-field cap alone does not keep the main document under the
 * Firestore limit: the fingerprint shares it with the base64 state vector
 * (~8 bytes per client ever seen), which has no offload path. On an aged
 * many-client document the two together exceed the limit, Firestore
 * rejects the snapshot write with a non-retryable INVALID_ARGUMENT, and
 * every later fold recomputes the same fields — compaction stops for
 * good. So the fingerprint must also fit in the room the state vector
 * leaves.
 *
 * @param params - Fingerprint size, base64 state vector length, the
 * per-field cap and the inline payload ceiling.
 * @returns true to store the fingerprint inline, false to offload it.
 */
export function deleteSetFitsInline(params: {
    deleteSetBytes: number;
    stateVectorB64Length: number;
    maxFieldBytes: number;
    inlineLimit: number;
}): boolean {
    const { deleteSetBytes, stateVectorB64Length, maxFieldBytes, inlineLimit } = params;

    return deleteSetBytes <= maxFieldBytes
        && deleteSetBytes + stateVectorB64Length <= inlineLimit;
}

/**
 * Which of the two delete-set fingerprint fields a snapshot write should
 * carry.
 *
 * Exactly one survives: inline for the normal case, a Cloud Storage
 * pointer once the fingerprint outgrows the inline cap. Dropping it
 * entirely is not an option — every future reconnect would then take the
 * spurious-push slow path — and keeping a stale one would hide newer
 * deletions, so the other field is always explicitly cleared.
 *
 * @param params - Whichever fingerprint form was produced.
 * @returns Which field to write and which to delete.
 */
export function chooseDeleteSetField(params: {
    deleteSetUpdate: Uint8Array | null;
    deleteSetStoragePath: string | null;
}): { writeInline: boolean; writeStoragePath: boolean } {
    return {
        writeInline: params.deleteSetUpdate !== null,
        writeStoragePath: params.deleteSetStoragePath !== null,
    };
}

/**
 * The result a compaction cycle reports.
 *
 * previousVersion is reported only when there actually was one, so a
 * first-ever compaction is distinguishable from one that replaced version 0.
 *
 * @param params - What the cycle processed.
 * @returns The result record.
 */
export function buildSnapshotResult(params: {
    updatesCompacted: number;
    historySegmentsMerged: number;
    currentVersion: number;
}): {
    success: true;
    type: 'snapshot';
    updatesCompacted: number;
    historySegmentsMerged: number;
    previousVersion?: number;
} {
    const { updatesCompacted, historySegmentsMerged, currentVersion } = params;

    return {
        success: true,
        type: 'snapshot',
        updatesCompacted,
        historySegmentsMerged,
        previousVersion: currentVersion > 0 ? currentVersion : undefined,
    };
}

/**
 * The Storage blobs a committed fold replaced, which it should then delete.
 *
 * These are the paths stored on the replaced main document, never names
 * rebuilt from its version: blob names carry a per-attempt id (see
 * foldSnapshotPath / squashSnapshotPath), and squash writes
 * `snapshot_e{E}_v{V}_{id}.bin`, which a rebuilt `snapshot_v{n}.bin` never
 * matched, so every squash blob would leak. The fold's transaction verified the version, and every writer of
 * these paths bumps it, so the stored paths are exactly what the fold
 * replaced. A path the fold itself just wrote is never returned: deleting
 * it would destroy the live snapshot.
 *
 * The previous fold's tail goes too: the committed fold replaced (or
 * cleared) the pointer to it, and a tail only ever describes the snapshot
 * it was published with.
 *
 * @param previous - The main document state read before the fold.
 * @param written - The Storage paths the fold committed.
 * @returns The paths to delete.
 */
export function blobsReplacedByFold(
    previous: Pick<MainDocState, 'baseStoragePath' | 'baseDeleteSetStoragePath' | 'baseFoldTailStoragePath'>,
    written: (string | null)[],
): string[] {
    return [previous.baseStoragePath, previous.baseDeleteSetStoragePath, previous.baseFoldTailStoragePath]
        .filter((p): p is string => p !== null && !written.includes(p));
}

/**
 * The next snapshot version.
 *
 * @param currentVersion - The version read before the transaction.
 * @returns The version to write.
 */
export function nextSnapshotVersion(currentVersion: number): number {
    return currentVersion + 1;
}

/**
 * The Cloud Storage path for a fold's candidate snapshot.
 *
 * The candidate is uploaded BEFORE the lock-checked commit, and the lock
 * is a lease with no fencing token: a compactor that stalls past it can
 * still be uploading after another client folded the same version and
 * committed. The per-attempt id gives every candidate its own object, so
 * such a late upload lands where nothing points instead of replacing the
 * snapshot the winner committed.
 *
 * @param basePath - The document's base path.
 * @param version - The version the fold will commit.
 * @param attemptId - Unique to this compaction attempt.
 * @returns The storage object path.
 */
export function foldSnapshotPath(basePath: string, version: number, attemptId: string): string {
    return `${basePath}/snapshot_v${version}_${attemptId}.bin`;
}

/**
 * The Cloud Storage path for a fold's offloaded delete-set fingerprint.
 * Attempt-unique for the same reason as foldSnapshotPath.
 *
 * @param basePath - The document's base path.
 * @param version - The version the fold will commit.
 * @param attemptId - Unique to this compaction attempt.
 * @returns The storage object path.
 */
export function foldDeleteSetPath(basePath: string, version: number, attemptId: string): string {
    return `${basePath}/ds_v${version}_${attemptId}.bin`;
}

/**
 * The Cloud Storage path for a fold's tail. Attempt-unique for the same
 * reason as foldSnapshotPath.
 *
 * @param basePath - The document's base path.
 * @param version - The version the fold will commit.
 * @param attemptId - Unique to this compaction attempt.
 * @returns The storage object path.
 */
export function foldTailPath(basePath: string, version: number, attemptId: string): string {
    return `${basePath}/tail_v${version}_${attemptId}.bin`;
}

/**
 * The base clocks a fold's tail is published with: the replaced snapshot's
 * state vector, restricted to the clients the tail touches (0 for a client
 * the base never saw).
 *
 * A reader that holds the replaced snapshot lacks only the tail, but the
 * full vector is O(every client ever seen) and shares the main document's
 * size budget with the new one. The restriction loses nothing: a client
 * the tail does not touch has the same clock in both snapshots, so "covers
 * the new snapshot wherever it is behind, and every client it is behind on
 * is listed here at a clock it covers" is exactly "covers the replaced
 * snapshot".
 *
 * @param baseSV - The replaced snapshot's state vector.
 * @param tailSV - The tail's state vector (clock ends per client).
 * @returns client -> base clock, for every client in the tail.
 */
export function foldTailBaseClocks(baseSV: Map<number, number>, tailSV: Map<number, number>): Map<number, number> {
    const clocks = new Map<number, number>();
    for (const client of tailSV.keys()) {
        clocks.set(client, baseSV.get(client) ?? 0);
    }
    return clocks;
}

/**
 * Whether a fold should publish its tail beside the new snapshot.
 *
 * The tail is un-GC'd, so early in a document's life it can be nearly the
 * size of the snapshot (the first fold over a small base) — a reader would
 * gain little, and pay for both when the tail does not suffice. Publish it
 * only when it is at most half the snapshot.
 *
 * Its base clocks are stored on the main document, which already carries
 * the state vector and possibly the inline delete-set fingerprint (see
 * deleteSetFitsInline). The tail is the optional one: when its fields do
 * not fit in the room those two leave, the fold publishes none.
 *
 * @param params - Tail and snapshot sizes, the length of the tail's main
 * document fields, the state vector and inline fingerprint they share the
 * budget with, and the inline payload ceiling.
 * @returns true to publish the tail.
 */
export function shouldPublishFoldTail(params: {
    tailBytes: number;
    snapshotBytes: number;
    tailFieldsLength: number;
    stateVectorB64Length: number;
    inlineDeleteSetBytes: number;
    inlineLimit: number;
}): boolean {
    const { tailBytes, snapshotBytes, tailFieldsLength, stateVectorB64Length, inlineDeleteSetBytes, inlineLimit } = params;

    return tailBytes * 2 <= snapshotBytes
        && tailFieldsLength + stateVectorB64Length + inlineDeleteSetBytes <= inlineLimit;
}
