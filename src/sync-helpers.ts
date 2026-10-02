/**
 * Pure helpers extracted from sync.ts.
 *
 * These decide what the client already has and what it must apply: state
 * vector folding, redundancy checks, and blob application. They touch only
 * plain document data, `Bytes` and Yjs — never the Firestore SDK — so they
 * can be unit tested (and therefore mutation tested) without an emulator.
 * Keeping them here rather than private to sync.ts is what makes that
 * possible; sync.ts re-exports nothing and simply imports them.
 */
import { Bytes } from "@firebase/firestore";
import * as Y from "yjs";
import { fromBase64 } from "lib0/buffer";
import { FIREBASE_ORIGINS } from "./types";
import { extractClockEnds, isUpdateRedundant } from "./update-metadata";

/**
 * A server item fetched during sync, before it is applied to the local doc.
 */
export interface PendingUpdate {
    type: 'snapshot' | 'history' | 'update';
    data: any;
    priority: number;
}

/**
 * Extracts the raw binary blobs from all fetched server items.
 * Used for delete-set coverage checks before pushing a local diff.
 *
 * Items without an inline blob (e.g., a legacy main document carrying only
 * a stateVector) are skipped — missing coverage can only cause a redundant
 * (idempotent) push, never data loss.
 *
 * @param items - Pending updates collected during initial sync
 * @returns Array of update/segment/content blobs
 */
export function collectServerBlobs(items: PendingUpdate[]): Uint8Array[] {
    const blobs: Uint8Array[] = [];
    for (const item of items) {
        const raw = item.type === 'snapshot' ? item.data.content
            : item.type === 'history' ? item.data.segment
                : item.data.update;
        if (raw) {
            blobs.push((raw as Bytes).toUint8Array());
        }
    }
    return blobs;
}

/**
 * Ensures that the document data has a decoded state vector map cached.
 * P3.1 OPTIMIZATION: Cache decoded state vector to avoid repeated parsing.
 *
 * @param data - Firestore document data containing stateVector
 * @returns The decoded state vector map
 */
export function ensureDecodedSV(data: any): Map<number, number> {
    if (!data._decodedSV) {
        const vector = fromBase64(data.stateVector);
        data._decodedSV = Y.decodeStateVector(vector);
    }
    return data._decodedSV;
}

/**
 * Checks whether the local document already covers a snapshot's state
 * vector, i.e. downloading/applying the snapshot blob would be a no-op.
 *
 * Returns false when the stateVector is missing or malformed, so callers
 * fall back to fetching and applying the content (the safe direction).
 *
 * @param data - Main document data containing a stateVector field
 * @param ydoc - Local Yjs document
 */
export function localCoversSnapshot(data: any, ydoc: Y.Doc): boolean {
    if (!data.stateVector) return false;
    try {
        const remoteSV = ensureDecodedSV(data);
        const localSV = Y.decodeStateVector(Y.encodeStateVector(ydoc));
        for (const [client, clock] of remoteSV) {
            if ((localSV.get(client) || 0) < clock) {
                return false;
            }
        }
        return true;
    } catch (e) {
        console.warn("Failed to decode snapshot stateVector", e);
        return false;
    }
}

/**
 * Extracts and aggregates clock values from an update document into the server state vector.
 * Tries stored metadata first, falls back to parsing the update blob.
 * 
 * @param data - Firestore document data containing update and/or metadata
 * @param serverSVMap - Map to populate with client -> clock mappings
 */
export function processUpdateMetadata(data: any, serverSVMap: Map<number, number>): void {
    if (data.clientIDs?.length > 0 && data.clientClocks?.length > 0) {
        data.clientIDs.forEach((cid: number, i: number) => {
            const clock = data.clientClocks[i];
            const current = serverSVMap.get(cid) || 0;
            if (clock > current) {
                serverSVMap.set(cid, clock);
            }
        });
    } else if (data.update) {
        // Lazy clock extraction — avoids materializing the struct tree of
        // potentially large update blobs (extractClockEnds handles parse
        // errors internally by returning an empty map).
        const clockEnds = extractClockEnds((data.update as Bytes).toUint8Array());
        for (const [clientID, clock] of clockEnds) {
            const current = serverSVMap.get(clientID) || 0;
            if (clock > current) {
                serverSVMap.set(clientID, clock);
            }
        }
    }
}

/**
 * Extracts clock values from a history segment into the server state vector.
 * Uses stateVector field if present, otherwise parses the segment blob.
 * 
 * @param data - Firestore document data containing history segment
 * @param serverSVMap - Map to populate with client -> clock mappings
 */
export function processHistoryMetadata(data: any, serverSVMap: Map<number, number>): void {
    if (data.stateVector) {
        const map = ensureDecodedSV(data);
        for (const [client, clock] of map.entries()) {
            const current = serverSVMap.get(client) || 0;
            if (clock > current) {
                serverSVMap.set(client, clock);
            }
        }
    } else if (data.segment) {
        // Lazy clock extraction for history segments, which are large by
        // construction (merged batches of updates).
        const clockEnds = extractClockEnds((data.segment as Bytes).toUint8Array());
        for (const [clientID, clock] of clockEnds) {
            const current = serverSVMap.get(clientID) || 0;
            if (clock > current) {
                serverSVMap.set(clientID, clock);
            }
        }
    }
}

/**
 * Extracts clock values from the base snapshot into the server state vector.
 * Only uses the stateVector field (snapshots always have this).
 * 
 * @param data - Firestore document data from the main document
 * @param serverSVMap - Map to populate with client -> clock mappings
 */
export function processSnapshotMetadata(data: any, serverSVMap: Map<number, number>): void {
    if (data.stateVector) {
        const map = ensureDecodedSV(data);
        for (const [client, clock] of map.entries()) {
            const current = serverSVMap.get(client) || 0;
            if (clock > current) {
                serverSVMap.set(client, clock);
            }
        }
    }
}

/**
 * Builds the state vector the initial-sync push diffs against: for each
 * client, the clock up to which the server holds EVERY struct.
 *
 * Unlike the local state vector, this cannot be folded from metadata.
 * Update and history metadata record only END clocks, which cannot tell
 * "the server holds X:[0,11)" from "the server holds X:[5,11)". Reading
 * the end as coverage made the push guard skip X:[0,5) forever, leaving
 * every other client with X:[5,11) parked in pendingStructs. Such gaps
 * appear whenever an update document is written without what precedes
 * it — e.g. a save that commits before initial sync has pushed the doc's
 * pre-existing local content.
 *
 * Each update/history blob therefore contributes the [from, to) range
 * `Y.parseUpdateMeta` reports per client (the same lazy walk as
 * `extractClockEnds`; it never reads the delete-set), and a range only
 * extends the coverage when it starts within it. The base snapshot counts
 * as [0, sv) and is never parsed — it is O(document) and often not even
 * downloaded. The update and history tiers are bounded by compaction, so
 * this stays flat as the document ages.
 *
 * @param snapshotSVMap - The base snapshot's state vector (empty if none)
 * @param items - Pending updates collected during initial sync
 * @returns Map of clientID -> clock the server holds contiguously from 0
 */
export function buildServerCoverage(snapshotSVMap: Map<number, number>, items: PendingUpdate[]): Map<number, number> {
    const ranges = new Map<number, [number, number][]>();
    for (const item of items) {
        const raw = item.type === 'history' ? item.data.segment
            : item.type === 'update' ? item.data.update
                : null;
        if (!raw) continue;
        let meta: { from: Map<number, number>; to: Map<number, number> };
        try {
            meta = Y.parseUpdateMeta((raw as Bytes).toUint8Array());
        } catch (e) {
            // A corrupted blob proves no coverage; worst case we push a
            // redundant (idempotent) diff.
            console.warn("Failed to parse server blob clock ranges", e);
            continue;
        }
        for (const [client, from] of meta.from) {
            const clientRanges = ranges.get(client) || [];
            clientRanges.push([from, meta.to.get(client) || from]);
            ranges.set(client, clientRanges);
        }
    }

    const coverage = new Map(snapshotSVMap);
    for (const [client, clientRanges] of ranges) {
        let covered = coverage.get(client) || 0;
        clientRanges.sort((a, b) => a[0] - b[0]);
        for (const [from, to] of clientRanges) {
            if (from > covered) break;
            if (to > covered) covered = to;
        }
        if (covered > 0) {
            coverage.set(client, covered);
        }
    }
    return coverage;
}

/**
 * Updates a cached local state vector after an item has been applied.
 *
 * The item's metadata only says which clients it touches; their clocks are
 * re-read from the document instead of folding in the metadata's END
 * clocks. An item that starts past the local clock (a server-side gap) is
 * parked in pendingStructs and does not advance it — folding its end in
 * would make the later item that fills the gap look redundant, and the gap
 * would never close.
 *
 * @param item - The item just applied (or skipped as our own write)
 * @param ydoc - The local Yjs document
 * @param localSVMap - Cached local state vector to update
 */
export function refreshLocalClocks(
    item: Pick<PendingUpdate, 'type' | 'data'>,
    ydoc: Y.Doc,
    localSVMap: Map<number, number>
): void {
    const touched = new Map<number, number>();
    if (item.type === 'snapshot') {
        processSnapshotMetadata(item.data, touched);
    } else if (item.type === 'history') {
        processHistoryMetadata(item.data, touched);
    } else {
        processUpdateMetadata(item.data, touched);
    }
    for (const client of touched.keys()) {
        localSVMap.set(client, Y.getState(ydoc.store, client));
    }
}

/**
 * Determines if a pending update is already contained in the local document.
 * Uses clock comparison to avoid re-applying known data.
 * 
 * P1.3 FIX: Now handles history segments with stateVector field.
 * 
 * @param item - The pending update to check
 * @param localSVMap - Local document's state vector
 * @returns true if local document already has all data from this item
 */
export function isItemRedundant(item: PendingUpdate, localSVMap: Map<number, number>): boolean {
    if (item.type === 'snapshot' && item.data.stateVector) {
        const map = ensureDecodedSV(item.data);
        for (const [client, clock] of map) {
            const localClock = localSVMap.get(client) || 0;
            if (clock > localClock) return false;
        }
        return true;
    }

    // P1.3 FIX: Handle history segments with stateVector
    if (item.type === 'history' && item.data.stateVector) {
        try {
            const map = ensureDecodedSV(item.data);
            for (const [client, clock] of map) {
                const localClock = localSVMap.get(client) || 0;
                if (clock > localClock) return false;
            }
            return true;
        } catch (e) {
            // If stateVector parsing fails, treat as not redundant
            return false;
        }
    }

    if (item.type === 'update') {
        const data = item.data;
        if (data.clientIDs?.length > 0 && data.clientClocks?.length > 0) {
            return isUpdateRedundant(localSVMap, data.clientIDs, data.clientClocks);
        }
    }

    return false;
}

/**
 * Applies a pending update to the Yjs document.
 * Handles different update types (snapshot, history, update) appropriately.
 * 
 * @param item - The pending update to apply
 * @param ydoc - Target Yjs document
 * @returns true if update was successfully applied
 */
export function applyItem(item: PendingUpdate, ydoc: Y.Doc): boolean {
    try {
        if (item.type === 'snapshot' && item.data.content) {
            Y.applyUpdate(ydoc, (item.data.content as Bytes).toUint8Array(), FIREBASE_ORIGINS.SNAPSHOT);
            return true;
        } else if (item.type === 'history' && item.data.segment) {
            Y.applyUpdate(ydoc, (item.data.segment as Bytes).toUint8Array(), FIREBASE_ORIGINS.HISTORY);
            return true;
        } else if (item.type === 'update' && item.data.update) {
            Y.applyUpdate(ydoc, (item.data.update as Bytes).toUint8Array(), FIREBASE_ORIGINS.UPDATE);
            return true;
        }
    } catch (e) {
        console.error(`Failed to apply ${item.type}`, e);
    }
    return false;
}
