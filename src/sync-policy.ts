/**
 * Pure decision logic for the sync listeners and initial sync.
 *
 * sync.ts makes these choices inside Firestore `onSnapshot` callbacks and
 * around `getDocs` pagination, which made them reachable only through the
 * emulator suite — and so invisible to mutation testing, which cannot run
 * that suite once per mutant. Nothing here touches the Firestore SDK.
 *
 * These decide which remote documents are applied and which are dropped.
 * Getting one wrong is silent in both directions: a document wrongly
 * dropped is data the client never receives, and a foreign-epoch document
 * wrongly applied parks unresolvable structs in the local doc forever.
 */
import { isUpdateRedundant } from './update-metadata';
import { epochOf } from './compaction-policy';

/** What the update listener should do with one incoming document. */
export type IncomingUpdatePlan =
    /** Written before a squash: belongs to an unrelated id space. */
    | { kind: 'drop-foreign-epoch' }
    /** This client wrote it; the local doc already has it. */
    | { kind: 'skip-own' }
    /** Already covered by the local state vector. */
    | { kind: 'skip-redundant' }
    /** Previously failed to apply; quarantined to avoid a retry loop. */
    | { kind: 'skip-quarantined' }
    /** Payload lives in Cloud Storage and must be downloaded. */
    | { kind: 'download'; storagePath: string }
    /** Payload is inline and can be applied directly. */
    | { kind: 'apply-inline' }
    /** Nothing usable on the document. */
    | { kind: 'skip-empty' };

/**
 * Decides what to do with one document delivered by the update listener.
 *
 * Order matters and is deliberate. The epoch fence runs first so a
 * pre-squash document can never reach the doc by any later path. The
 * own-write check comes before redundancy because our own updates still
 * need their metadata folded into the cached state vector even though the
 * blob is not applied — the caller relies on that distinction.
 *
 * @param data - The Firestore document data.
 * @param context - Local identity, epoch, quarantine set and state vector.
 * @returns The plan for this document.
 */
export function planIncomingUpdate(
    data: Record<string, any> | null | undefined,
    context: {
        uid: string;
        currentEpoch: number;
        docId?: string;
        corruptedDocIds?: Set<string>;
        localSVMap: Map<number, number>;
    },
): IncomingUpdatePlan {
    const { uid, currentEpoch, docId, corruptedDocIds, localSVMap } = context;

    if (epochOf(data) !== currentEpoch) {
        return { kind: 'drop-foreign-epoch' };
    }
    if (data?.createdBy === uid) {
        return { kind: 'skip-own' };
    }
    if (data?.clientIDs?.length > 0 && data?.clientClocks?.length > 0
        && isUpdateRedundant(localSVMap, data.clientIDs, data.clientClocks)) {
        return { kind: 'skip-redundant' };
    }
    if (docId !== undefined && corruptedDocIds?.has(docId)) {
        return { kind: 'skip-quarantined' };
    }
    if (data?.updateStoragePath && !data?.update) {
        return { kind: 'download', storagePath: data.updateStoragePath };
    }
    if (data?.update) {
        return { kind: 'apply-inline' };
    }

    return { kind: 'skip-empty' };
}

/**
 * Whether this snapshot delivery should trigger a compaction.
 *
 * Above the threshold every client would otherwise fire a mostly futile
 * lock transaction on every delivery, so triggers are rate limited: the
 * first fires immediately and later ones wait out a cooldown — unless the
 * collection has reached the realtime hard cap, where falling further
 * behind is worse than the wasted contention.
 *
 * @param params - Delivery size, threshold, clock and cooldown state.
 * @returns true to trigger a compaction now.
 */
export function shouldTriggerCompaction(params: {
    size: number;
    maxUpdatesThreshold: number;
    now: number;
    lastTriggerAt: number;
    cooldownMs: number;
    hardCap: number;
}): boolean {
    const { size, maxUpdatesThreshold, now, lastTriggerAt, cooldownMs, hardCap } = params;

    if (size <= maxUpdatesThreshold) {
        return false;
    }

    return size >= hardCap || now - lastTriggerAt >= cooldownMs;
}

/**
 * Whether a pending item survives the epoch fence during initial sync.
 *
 * Snapshots always pass: the main document defines the current epoch.
 * Update and history documents must match it exactly, and the fence must
 * run before their metadata is folded into the server state vector — a
 * stale update that slipped through would otherwise suppress the local
 * push and lose the client's own edits.
 *
 * @param item - The pending item's type and data.
 * @param serverEpoch - Epoch declared by the main document.
 * @returns true when the item may be kept.
 */
export function survivesEpochFence(
    item: { type: 'snapshot' | 'history' | 'update'; data: Record<string, any> },
    serverEpoch: number,
): boolean {
    if (item.type === 'snapshot') {
        return true;
    }

    return epochOf(item.data) === serverEpoch;
}

/**
 * Chooses the pagination cursor for the next page of a Firestore query.
 *
 * A document whose `serverTimestamp()` has not committed yet sorts
 * unpredictably, and using it as a cursor makes the follow-up query
 * invalid. Walk back to the newest committed document instead; if none of
 * the page has committed, there is no usable cursor and the caller must
 * keep the one it had.
 *
 * @param docs - The page, in query order, each reporting pending writes.
 * @returns The index to use as the cursor, or -1 when none is usable.
 */
export function pickPaginationCursorIndex(
    docs: { metadata: { hasPendingWrites: boolean } }[],
): number {
    for (let index = docs.length - 1; index >= 0; index -= 1) {
        if (!docs[index].metadata.hasPendingWrites) {
            return index;
        }
    }

    return -1;
}

/**
 * Whether a page result means there are more pages to fetch.
 *
 * A short page is the last one; a full page might not be, so it is
 * followed by another query.
 *
 * @param pageSize - Documents returned by this query.
 * @param batchSize - The query's limit.
 * @returns true when another page should be requested.
 */
export function hasMorePages(pageSize: number, batchSize: number): boolean {
    return pageSize === batchSize;
}
