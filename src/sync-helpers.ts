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
import { extractClockEnds, isUpdateRedundant, deleteSetContains } from "./update-metadata";
import { diffUpdateAsync, isWorkerMergeAvailable } from "./merge-utils";

/**
 * A server item fetched during sync, before it is applied to the local doc.
 *
 * Its blob (`content`, `segment` or `update`) is Firestore `Bytes` when it
 * came inline with the document, or the Uint8Array downloaded from Cloud
 * Storage; read it with {@link blobOf}.
 */
export interface PendingUpdate {
    type: 'snapshot' | 'history' | 'update';
    data: any;
    priority: number;
    /**
     * Marks the main document's delete-set fingerprint, an 'update' item
     * that only carries deletions (see `fingerprintIsRedundant`).
     */
    fingerprint?: boolean;
}

/**
 * Inline blobs already copied out of their Firestore `Bytes`, keyed by the
 * `Bytes` value.
 */
const inlineBlobs = new WeakMap<Bytes, Uint8Array>();

/**
 * Returns a blob field of fetched document data as a Uint8Array.
 *
 * Blobs initial sync downloads from Cloud Storage stay the Uint8Array they
 * arrive as: wrapping one in `Bytes` costs the SDK a string concatenation
 * per byte (a transient rope ~20-30x the blob size), plus a full copy back
 * out for every reader. Inline `Bytes` are copied out once, and coverage,
 * apply, metadata and the push guard share that copy. The document data
 * itself is left untouched.
 *
 * @param raw - Inline Firestore `Bytes` or a downloaded Uint8Array
 * @returns The blob's bytes
 */
export function blobOf(raw: Bytes | Uint8Array): Uint8Array {
    if (raw instanceof Uint8Array) {
        return raw;
    }
    let blob = inlineBlobs.get(raw);
    if (blob === undefined) {
        blob = raw.toUint8Array();
        inlineBlobs.set(raw, blob);
    }
    return blob;
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
            blobs.push(blobOf(raw));
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
 * Checks whether the snapshot's fold tail may be all the local document
 * lacks of it, i.e. the tail is worth downloading instead of the snapshot.
 *
 * Compaction publishes the tail (what the fold merged on top of the
 * replaced snapshot) with its base clocks: the replaced snapshot's state
 * vector, restricted to the clients the tail touches. Wherever the local
 * doc is behind the snapshot, that client must be listed at a clock the
 * local doc covers — which is exactly "covers the replaced snapshot".
 *
 * Only a pre-check: the caller applies the tail and re-runs
 * localCoversSnapshot, which alone decides whether the snapshot is still
 * needed (a tail with an internal gap leaves structs pending). A tail
 * bound to another version is ignored: a main-document writer that
 * predates tails bumps the version and leaves the fields behind.
 *
 * Returns false whenever the fields are missing or malformed, so callers
 * download the snapshot (the safe direction).
 *
 * @param data - Main document data
 * @param ydoc - Local Yjs document
 */
export function foldTailMayCatchUp(data: any, ydoc: Y.Doc): boolean {
    if (typeof data.foldTailStoragePath !== 'string' || typeof data.foldTailBaseClocks !== 'string'
        || !data.stateVector || typeof data.version !== 'number' || data.foldTailVersion !== data.version) {
        return false;
    }
    try {
        const baseClocks = Y.decodeStateVector(fromBase64(data.foldTailBaseClocks));
        const localSV = Y.decodeStateVector(Y.encodeStateVector(ydoc));
        for (const [client, clock] of ensureDecodedSV(data)) {
            const localClock = localSV.get(client) || 0;
            if (localClock >= clock) continue;
            const baseClock = baseClocks.get(client);
            if (baseClock === undefined || localClock < baseClock) {
                return false;
            }
        }
        return true;
    } catch (e) {
        console.warn("Failed to decode fold tail base clocks", e);
        return false;
    }
}

/**
 * Share of a snapshot the local document already holds, estimated from the
 * two state vectors in O(clients): the sum over the snapshot's clients of
 * min(local clock, snapshot clock), divided by the sum of the snapshot's
 * clocks. On versicle-shaped documents it tracks the share of structs
 * within a percentage point.
 *
 * @param snapshotSV - The snapshot's state vector
 * @param localSV - The local document's state vector
 * @returns A fraction in [0, 1]; 1 for a snapshot without structs
 */
export function snapshotOverlap(snapshotSV: Map<number, number>, localSV: Map<number, number>): number {
    let total = 0;
    let held = 0;
    for (const [client, clock] of snapshotSV) {
        total += clock;
        held += Math.min(localSV.get(client) || 0, clock);
    }
    return total === 0 ? 1 : held / total;
}

/**
 * Below this share of the snapshot held locally, a diff costs more than it
 * saves: computing it walks the whole snapshot, and the structs it leaves
 * are most of the snapshot anyway (e.g. a fresh device that wrote app
 * defaults before its first sync). With the merge worker the main thread
 * only applies the diff, so this only bounds the worker's added latency.
 */
export const SNAPSHOT_DIFF_MIN_OVERLAP = 0.5;

/**
 * The same bound when the diff has to run on the main thread (no Worker,
 * e.g. a CSP that blocks blob workers). The main thread then pays the walk
 * too, and diff + apply only beats applying the whole snapshot from about
 * 60% overlap (measured at 3.3 MB: break-even at 61%, 24% faster at 71%).
 */
export const SNAPSHOT_DIFF_MIN_OVERLAP_MAIN_THREAD = 0.75;

/**
 * Strips from a downloaded snapshot what the local document already holds.
 *
 * A local doc that is behind the snapshot (another device ran a fold since
 * it last synced) typically holds 90%+ of it. `Y.applyUpdate` of the whole
 * blob decodes every struct and resolves its origins against the store
 * before skipping the ones already held — O(snapshot) main-thread work that
 * grows with document age (~300 ms at 3.3 MB). `Y.diffUpdate` against the
 * local state vector, computed in the merge worker when available, leaves
 * only the missing structs plus the FULL delete set, so applying it reaches
 * the same state: structs the doc parks in pendingStructs lie beyond its
 * state vector and stay in the diff, and local edits made while the diff is
 * computed only add local structs (the diff is then a superset, and
 * applying it is idempotent). The delete set also keeps the result valid
 * as delete-set proof for the push guard (collectServerBlobs).
 *
 * The snapshot's metadata (stateVector, version) is unaffected: coverage
 * and redundancy keep treating the snapshot as [0, stateVector).
 *
 * @param content - The snapshot blob
 * @param data - Main document data carrying the snapshot's stateVector
 * @param ydoc - Local Yjs document the snapshot will be applied to
 * @returns The diff, or `content` itself when the local doc holds too
 *          little of the snapshot for a diff to pay off, or when diffing
 *          fails — the caller then applies the full blob exactly as before,
 *          so corruption still fails (and quarantines) in the apply
 */
export async function diffSnapshotForLocal(content: Uint8Array, data: any, ydoc: Y.Doc): Promise<Uint8Array> {
    if (!data.stateVector) return content;
    try {
        const localSV = Y.encodeStateVector(ydoc);
        const overlap = snapshotOverlap(ensureDecodedSV(data), Y.decodeStateVector(localSV));
        // At 1 the local doc covers the snapshot: it is skipped as redundant
        if (overlap < SNAPSHOT_DIFF_MIN_OVERLAP || overlap === 1) return content;
        if (overlap < SNAPSHOT_DIFF_MIN_OVERLAP_MAIN_THREAD && !isWorkerMergeAvailable()) return content;
        return await diffUpdateAsync(content, localSV);
    } catch (e) {
        console.warn("Failed to diff snapshot against the local document; applying it whole", e);
        return content;
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
        const clockEnds = extractClockEnds(blobOf(data.update));
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
        const clockEnds = extractClockEnds(blobOf(data.segment));
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
 * Each update/history blob therefore contributes the [from, to) ranges
 * its structs hold per client (one `Y.decodeUpdate` walk, about the cost
 * of `Y.parseUpdateMeta`, which would report a delta segment merged
 * across a gap as one range), and a range only extends the coverage when
 * it starts within it. The base snapshot counts as [0, sv) and is never
 * parsed — it is O(document) and often not even downloaded; a fold stores
 * as sv only what the snapshot holds contiguously from clock 0 (see
 * mergeUpdatesWithMeta). The update and history tiers are bounded by
 * compaction, so this stays flat as the document ages.
 *
 * @param snapshotSVMap - The base snapshot's state vector (empty if none)
 * @param items - Pending updates collected during initial sync
 * @returns Map of clientID -> clock the server holds contiguously from 0
 */
export function buildServerCoverage(snapshotSVMap: Map<number, number>, items: PendingUpdate[]): Map<number, number> {
    const ranges = new Map<number, [number, number][]>();
    for (const item of items) {
        // The fingerprint holds no structs; decoding it would only read
        // its O(delete-set) deletions.
        if (item.fingerprint) continue;
        const raw = item.type === 'history' ? item.data.segment
            : item.type === 'update' ? item.data.update
                : null;
        if (!raw) continue;
        let structs: ReturnType<typeof Y.decodeUpdate>['structs'];
        try {
            structs = Y.decodeUpdate(blobOf(raw)).structs;
        } catch (e) {
            // A corrupted blob proves no coverage; worst case we push a
            // redundant (idempotent) diff.
            console.warn("Failed to parse server blob clock ranges", e);
            continue;
        }
        // One range per run of contiguous structs: a merged blob (a delta
        // segment over a gap) marks the clocks it lacks with a Skip, so
        // its first and last clock do not bound what it holds.
        let run: [number, number] | null = null;
        let runClient = 0;
        for (const struct of structs) {
            if (struct instanceof Y.Skip) {
                run = null;
                continue;
            }
            const { client, clock } = struct.id;
            if (run !== null && client === runClient && clock === run[1]) {
                run[1] = clock + struct.length;
                continue;
            }
            run = [clock, clock + struct.length];
            runClient = client;
            const clientRanges = ranges.get(client) || [];
            clientRanges.push(run);
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
 * Re-bases a cached local state vector on the document's real one while
 * structs are parked in pendingStructs.
 *
 * Initial sync and the listeners update their cache incrementally after
 * each apply (see `refreshLocalClocks`) instead of re-encoding the state
 * vector every time. A cached clock only holds once Yjs has integrated the
 * item's structs. When a same-client dependency is missing (a later range
 * ordered before the earlier one), the structs wait in pendingStructs and
 * the real state vector does not move. An inflated cache would then judge
 * the document that fills the gap redundant and skip it, so the gap would
 * never close. While anything is pending, the cache is therefore re-based
 * on the real state vector as a backstop; with nothing pending, every
 * cached clock is integrated and the incremental update stands.
 *
 * @param ydoc - Local Yjs document the cache tracks
 * @param localSVMap - Cached local state vector, corrected in place
 */
export function rebaseIfPending(ydoc: Y.Doc, localSVMap: Map<number, number>): void {
    if (ydoc.store.pendingStructs === null) {
        return;
    }
    localSVMap.clear();
    for (const [client, clock] of Y.decodeStateVector(Y.encodeStateVector(ydoc))) {
        localSVMap.set(client, clock);
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
        // A state vector only spans structs and deletions add none, so it
        // can never prove a segment's deletions are known: a segment that
        // carries any is always applied (idempotent).
        if (item.data.hasDeletions) return false;
        try {
            const map = ensureDecodedSV(item.data);
            // An empty vector proves nothing either: such a segment holds
            // only deletions (written before the hasDeletions flag existed).
            if (map.size === 0) return false;
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
        // Same for an update document's clocks (see update-metadata)
        if (data.hasDeletions) return false;
        if (data.clientIDs?.length > 0 && data.clientClocks?.length > 0) {
            return isUpdateRedundant(localSVMap, data.clientIDs, data.clientClocks);
        }
    }

    return false;
}

/**
 * Whether applying the main document's delete-set fingerprint would change
 * nothing: it carries no structs, and every range it deletes is already
 * deleted locally.
 *
 * The fingerprint is the snapshot's WHOLE delete-set, so applying it costs
 * O(delete-set) even when it deletes nothing — Yjs looks up every range and
 * walks every dead struct inside it, which on a document used for years is
 * tens of thousands of structs. A state vector cannot prove deletions (they
 * add no structs), so `isItemRedundant` can never skip it; this proves it
 * against the local delete-set instead. A range at or past the local clock
 * is absent from `localDs`, so the fingerprint is then applied and Yjs
 * keeps that range pending.
 *
 * @param fingerprint - The fingerprint update blob
 * @param localDs - The local delete-set, as built by
 *                  `Y.createDeleteSetFromStructStore` (not modified)
 * @returns true only when the apply is provably a no-op; false when the
 *          blob fails to parse (applying it is the safe direction)
 */
export function fingerprintIsRedundant(
    fingerprint: Uint8Array,
    localDs: ReturnType<typeof Y.decodeUpdate>['ds']
): boolean {
    try {
        const { structs, ds } = Y.decodeUpdate(fingerprint);
        return structs.length === 0 && deleteSetContains(localDs, ds);
    } catch (e) {
        return false;
    }
}

/**
 * Whether a finished transaction changed the document, i.e. integrated or
 * deleted any struct. One that did neither left the state vector and the
 * delete-set as they were and called no type observers, so a delete-set
 * read from the struct store during it is still exact afterwards.
 *
 * @param tr - A transaction whose cleanup has run (`afterState` is set)
 */
export function transactionChangedDoc(tr: Y.Transaction): boolean {
    if (tr.deleteSet.clients.size > 0) {
        return true;
    }
    for (const [client, clock] of tr.afterState) {
        if (tr.beforeState.get(client) !== clock) {
            return true;
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
            Y.applyUpdate(ydoc, blobOf(item.data.content), FIREBASE_ORIGINS.SNAPSHOT);
            return true;
        } else if (item.type === 'history' && item.data.segment) {
            Y.applyUpdate(ydoc, blobOf(item.data.segment), FIREBASE_ORIGINS.HISTORY);
            return true;
        } else if (item.type === 'update' && item.data.update) {
            Y.applyUpdate(ydoc, blobOf(item.data.update), FIREBASE_ORIGINS.UPDATE);
            return true;
        }
    } catch (e) {
        console.error(`Failed to apply ${item.type}`, e);
    }
    return false;
}
