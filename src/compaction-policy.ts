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
 * @param params - Base presence, pending counts and the fold threshold.
 * @returns true to run DELTA, false to FOLD.
 */
export function shouldUseDelta(params: {
    hasBase: boolean;
    updateCount: number;
    historyCount: number;
    historyFoldThreshold: number;
}): boolean {
    const { hasBase, updateCount, historyCount, historyFoldThreshold } = params;

    return hasBase && updateCount > 0 && historyCount + 1 < historyFoldThreshold;
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
 * Over the limit, delta mode is abandoned for this cycle and compaction
 * folds instead — the segment would not fit, and splitting it would defeat
 * the point of a delta.
 *
 * @param byteLength - Size of the merged segment.
 * @param inlineLimit - The inline payload ceiling.
 * @returns true when the segment fits inline.
 */
export function deltaSegmentFitsInline(byteLength: number, inlineLimit: number): boolean {
    return byteLength <= inlineLimit;
}

/**
 * Builds the history-segment document a delta compaction writes.
 *
 * `epoch` is omitted entirely at epoch 0 rather than written as 0, so
 * documents from a never-squashed database stay byte-identical to what
 * older clients produced.
 *
 * @param params - Segment bytes, its state vector, author and epoch.
 * @returns The document fields, minus server-generated timestamps.
 */
export function buildDeltaSegmentDoc(params: {
    stateVectorB64: string;
    uid: string;
    epoch: number;
}): Record<string, unknown> {
    const { stateVectorB64, uid, epoch } = params;

    return {
        stateVector: stateVectorB64,
        createdBy: uid,
        ...(epoch > 0 ? { epoch } : {}),
    };
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
 * The next snapshot version.
 *
 * @param currentVersion - The version read before the transaction.
 * @returns The version to write.
 */
export function nextSnapshotVersion(currentVersion: number): number {
    return currentVersion + 1;
}
