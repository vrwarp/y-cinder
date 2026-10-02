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
 * Whether a failed download of a storage-backed update can never succeed.
 *
 * Update documents are immutable and are not delivered again, so giving up
 * on one drops the update for the session — and parks every later update
 * from the same author as pending. Only a missing object qualifies: the
 * writer uploads the blob before the pointer document exists, so a blob
 * missing behind a visible pointer is gone for good. Everything else —
 * notably `storage/retry-limit-exceeded`, which is what the SDK surfaces
 * once its own retries on network errors, 5xx and 429 run out — may
 * succeed on a later attempt and must be retried, never skipped or
 * quarantined.
 *
 * @param error - The value `getBytes` rejected with.
 * @returns true when retrying is pointless.
 */
export function isPermanentDownloadError(error: any): boolean {
    return error?.code === 'storage/object-not-found';
}

/** Error code initial sync fails with when the client is offline */
export const CLIENT_OFFLINE = 'client-offline';

/**
 * Whether a read was answered from the local cache instead of the server.
 *
 * The SDK serves getDocs/getDoc from its cache only once it considers the
 * client offline. Initial sync must not complete on such a read: it would
 * report 'sync' while offline, and its Updates → History → Snapshot read
 * order only rules out missed data when each tier reflects the server at
 * the time it is read.
 *
 * @param snapshot - A query or document snapshot. Test doubles of the SDK
 *   may omit its metadata.
 * @returns true when the read did not reach the server.
 */
export function isServedFromCache(snapshot: { metadata?: { fromCache?: boolean } }): boolean {
    return snapshot.metadata?.fromCache === true;
}

/**
 * Whether initial sync failed only because the client is offline.
 *
 * The provider then waits for the connection without spending its retry
 * budget. Thrown errors, 'unavailable' included, remain failures.
 *
 * @param error - The error initial sync failed with.
 * @returns true for the CLIENT_OFFLINE error.
 */
export function isClientOfflineError(error: any): boolean {
    return error?.code === CLIENT_OFFLINE;
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
 * A backoff after a compaction failure that retrying cannot fix overrides
 * even the hard cap. A document compaction cannot get past (e.g. an
 * undecodable update) never drains, so it stays at the hard cap, and
 * every delivery would start another attempt that re-reads the whole
 * backlog and fails the same way.
 *
 * @param params - Delivery size, threshold, clock, cooldown state, and
 * when the provider's failure backoff ends (0 when none is running).
 * @returns true to trigger a compaction now.
 */
export function shouldTriggerCompaction(params: {
    size: number;
    maxUpdatesThreshold: number;
    now: number;
    lastTriggerAt: number;
    cooldownMs: number;
    hardCap: number;
    backoffUntil?: number;
}): boolean {
    const { size, maxUpdatesThreshold, now, lastTriggerAt, cooldownMs, hardCap, backoffUntil = 0 } = params;

    if (size <= maxUpdatesThreshold || now < backoffUntil) {
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

/**
 * Whether the server's state vector already covers every local struct.
 *
 * This is the aged-document fast path. `Y.encodeStateAsUpdate(doc, sv)`
 * embeds the document's FULL delete-set, so its cost grows with total
 * historical churn — paying it on every reconnect of every client makes
 * startup linearly slower as the document ages. When the server is level
 * on structs, the diff would be structs-empty anyway and pushability
 * reduces to delete-set coverage, which is provable without encoding
 * anything.
 *
 * @param localSV - The local document's exact state vector.
 * @param serverSVMap - Per client, the clock up to which the server holds
 *   every struct (not merely its highest clock — see buildServerCoverage).
 * @returns true when no local struct is missing server-side.
 */
export function serverCoversLocalStructs(
    localSV: Map<number, number>,
    serverSVMap: Map<number, number>,
): boolean {
    for (const [client, clock] of localSV) {
        if ((serverSVMap.get(client) || 0) < clock) {
            return false;
        }
    }

    return true;
}

/**
 * Whether an encoded diff is large enough to be worth pushing.
 *
 * A structs-empty, deletions-empty update encodes to a two-byte header, so
 * anything at or below that carries nothing.
 *
 * @param byteLength - Size of the encoded diff.
 * @returns true when the diff has content.
 */
export function diffHasPayload(byteLength: number): boolean {
    return byteLength > 2;
}

/**
 * Whether a local diff must be offloaded to Cloud Storage rather than
 * written inline on the update document.
 *
 * @param byteLength - Size of the encoded diff.
 * @param inlineLimit - The inline payload ceiling.
 * @returns true when the diff needs Storage.
 */
export function diffNeedsStorage(byteLength: number, inlineLimit: number): boolean {
    return byteLength > inlineLimit;
}

/**
 * The epoch tag to spread onto a written document.
 *
 * Omitted entirely at epoch 0 so a never-squashed database keeps producing
 * documents identical to what older clients wrote.
 *
 * @param epoch - The current epoch.
 * @returns An object to spread, empty at epoch 0.
 */
export function epochTag(epoch: number): { epoch?: number } {
    return epoch > 0 ? { epoch } : {};
}

/**
 * Orders fetched items so they apply in dependency order: the base
 * snapshot first, then history segments, then individual updates.
 *
 * Applying an update before the snapshot it depends on leaves it parked in
 * pendingStructs, which also disables GC on the document.
 *
 * @param items - Items with a priority field.
 * @returns A new array in application order.
 */
export function orderByApplyPriority<T extends { priority: number }>(items: T[]): T[] {
    return [...items].sort((a, b) => a.priority - b.priority);
}

/**
 * The Cloud Storage path for an oversized local diff.
 *
 * Writers pass a per-attempt id: compaction and squash delete a blob once
 * the transaction deleting its pointer document commits, which is only
 * safe while every blob has exactly one pointer. Millis alone can repeat
 * (a save and the initial-sync push of the same client in the same
 * millisecond), and a second pointer to the same object would then
 * dangle once the first is consumed.
 *
 * @param basePath - The document's base path.
 * @param uid - This client's id.
 * @param timestamp - Millis (keeps names time-ordered).
 * @param attemptId - Unique to this upload attempt; every library
 *   writer passes one.
 * @returns The storage object path.
 */
export function largeUpdatePath(basePath: string, uid: string, timestamp: number, attemptId?: string): string {
    const suffix = attemptId === undefined ? '' : `_${attemptId}`;

    return `${basePath}/large_updates/${uid}_${timestamp}${suffix}.bin`;
}
