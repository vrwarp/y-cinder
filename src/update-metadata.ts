/**
 * Update Metadata Extraction Module
 *
 * Provides functions for extracting and working with metadata from Yjs updates.
 * This metadata enables efficient sync by allowing clock-based comparisons
 * instead of full content comparisons.
 *
 * ## How It Works
 *
 * Yjs updates contain internal structures with:
 * - **Client ID**: Unique identifier for each editing client
 * - **Clock**: Monotonically increasing counter per client
 *
 * By extracting these values, we can determine:
 * 1. Whether we already have this update (redundancy check)
 * 2. What data a remote peer is missing (differential sync)
 *
 * ## Metadata Fields Stored in Firestore
 *
 * ```typescript
 * {
 *   clientIDs: number[],    // All client IDs in the update
 *   clientClocks: number[], // Per-client clockEnd values (paired with clientIDs)
 * }
 * ```
 *
 * @module update-metadata
 */

import * as Y from "yjs";
import { UpdateMetadata } from "./types";

/**
 * Maximum number of distinct client IDs to store in metadata.
 * If an update exceeds this, we skip metadata optimization entirely
 * to avoid Firestore document bloat from massive offline merges.
 */
const MAX_METADATA_CLIENTS = 50;

/**
 * Result of metadata extraction.
 * P1.9 FIX: Distinguishes between empty update and parse error.
 */
export interface MetadataResult {
    metadata: UpdateMetadata[];
    parseError?: boolean;
}

/**
 * Extracts metadata from all clients within a Yjs update.
 * 
 * Parses the internal structure of a Yjs update to extract:
 * - Client IDs
 * - Clock ranges (start and end)
 * 
 * This metadata is used for:
 * - Efficient sync (compare clocks instead of full content)
 * - Deduplication (avoid re-applying already-seen updates)
 * - Debugging and audit trails
 * 
 * P1.9 FIX: Returns result object to distinguish parse errors from empty updates.
 * 
 * @param update - The Yjs update blob to parse
 * @returns Array of metadata objects (backwards compatible). 
 *          Returns empty array on parse error (logs warning).
 * 
 * @example
 * ```typescript
 * const update = Y.encodeStateAsUpdate(doc);
 * const metas = extractAllMetadata(update);
 * // [{ clientID: 1, clockStart: 0, clockEnd: 5 }, ...]
 * ```
 */
export function extractAllMetadata(update: Uint8Array): UpdateMetadata[] {
    try {
        const decoded = Y.decodeUpdate(update);
        const results: UpdateMetadata[] = [];

        if (decoded.structs) {
            // Group by client to compute accurate ranges
            const clientRanges = new Map<number, { start: number; end: number }>();

            for (const struct of decoded.structs) {
                const clientID = struct.id.client;
                const clockStart = struct.id.clock;
                const clockEnd = struct.id.clock + struct.length;

                const existing = clientRanges.get(clientID);
                if (existing) {
                    existing.start = Math.min(existing.start, clockStart);
                    existing.end = Math.max(existing.end, clockEnd);
                } else {
                    clientRanges.set(clientID, { start: clockStart, end: clockEnd });
                }
            }

            // Convert to array
            for (const [clientID, range] of clientRanges) {
                results.push({
                    clientID,
                    clockStart: range.start,
                    clockEnd: range.end
                });
            }
        }

        return results;
    } catch (e) {
        // P1.9 FIX: Log parse error for debugging
        console.warn("Failed to parse update metadata:", e);
        return [];
    }
}

/**
 * Extracts per-client end clocks from a Yjs update without materializing
 * its struct tree.
 *
 * This is the hot-path replacement for `extractAllMetadata` + `clockEnd`:
 * `Y.parseUpdateMeta` walks the update with the lazy decoder (no
 * Item/content objects are allocated), which matters when the update is a
 * large merged blob — e.g. the debounced save after a long offline
 * session, or a snapshot-sized diff. The resulting map is identical to the
 * `clockEnd` values `extractAllMetadata` computes from the decoded structs.
 *
 * IMPORTANT: this must NOT be implemented with
 * `Y.encodeStateVectorFromUpdate`. That function answers "what document
 * state does this update produce from scratch" — for any update whose
 * structs do not start at clock 0 (i.e. every mid-life incremental save)
 * the leading gap makes the answer EMPTY, which silently disabled the
 * redundancy-skip metadata on all such updates. `parseUpdateMeta` reports
 * the actual [from, to) clock ranges the blob contains.
 *
 * @param update - The Yjs update blob to parse
 * @returns Map of clientID -> end clock. Empty map on parse error.
 */
export function extractClockEnds(update: Uint8Array): Map<number, number> {
    try {
        return Y.parseUpdateMeta(update).to;
    } catch (e) {
        console.warn("Failed to extract update clock metadata:", e);
        return new Map();
    }
}

/**
 * Whether a Yjs update carries any deletions.
 *
 * Clock metadata cannot answer this: a deletion adds no struct, so it never
 * moves a state vector. Decodes the whole blob, so it is meant for small
 * structs-empty updates such as a merge's `dsUpdate`.
 *
 * @param update - The Yjs update blob to inspect
 * @returns true when the delete-set is non-empty, or when the blob fails to
 *          parse (claiming deletions only costs a redundant apply)
 */
export function updateHasDeletions(update: Uint8Array): boolean {
    try {
        return Y.decodeUpdate(update).ds.clients.size > 0;
    } catch (e) {
        console.warn("Failed to read update delete-set:", e);
        return true;
    }
}

/**
 * Aggregates a clock-ends map into a Firestore document payload.
 *
 * Same output shape and MAX_METADATA_CLIENTS capping as
 * `aggregateMetadata`, but consumes the map produced by `extractClockEnds`.
 *
 * @param clockEnds - Map of clientID -> end clock
 * @returns Object with aggregated metadata fields, or empty object
 */
export function aggregateClockEnds(clockEnds: Map<number, number>): {
    clientIDs?: number[];
    clientClocks?: number[];
} {
    if (clockEnds.size === 0 || clockEnds.size > MAX_METADATA_CLIENTS) {
        return {};
    }

    const clientIDs: number[] = [];
    const clientClocks: number[] = [];
    for (const [clientID, clock] of clockEnds) {
        clientIDs.push(clientID);
        clientClocks.push(clock);
    }
    return { clientIDs, clientClocks };
}

/**
 * Aggregates metadata from multiple clients into a document payload.
 *
 * Creates a metadata object suitable for storing alongside an update
 * in Firestore, including backwards-compatible single-client fields.
 *
 * @param metas - Array of metadata from extractAllMetadata
 * @returns Object with aggregated metadata fields, or empty object if no metadata
 *
 * @example
 * ```typescript
 * const metas = extractAllMetadata(update);
 * const pkg = {
 *   update: Bytes.fromUint8Array(update),
 *   ...aggregateMetadata(metas)
 * };
 * ```
 */
export function aggregateMetadata(metas: UpdateMetadata[]): {
    clientIDs?: number[];
    clientClocks?: number[];
} {
    if (metas.length === 0) {
        return {};
    }

    // Cap: if too many clients (e.g. massive offline merge with full history),
    // skip metadata optimization entirely. It's cheaper to let Yjs handle
    // the binary merge than to serialize/parse thousands of clock entries.
    if (metas.length > MAX_METADATA_CLIENTS) {
        return {};
    }

    return {
        clientIDs: metas.map(m => m.clientID),
        clientClocks: metas.map(m => m.clockEnd),
    };
}

/**
 * Checks if a local document already contains the data represented by metadata.
 * 
 * Compares the local state vector against update metadata to determine
 * if the update would be redundant (already applied).
 * 
 * @param localSVMap - Map of client IDs to local clock values
 * @param clientIDs - Array of client IDs in the update
 * @param clockEnd - The maximum clock value in the update
 * @returns true if all update data is already in the local document
 * 
 * @example
 * ```typescript
 * const localSV = Y.decodeStateVector(Y.encodeStateVector(doc));
 * if (isUpdateRedundant(localSV, data.clientIDs, data.clockEnd)) {
 *   return; // Skip - already have this data
 * }
 * ```
 */
export function isUpdateRedundant(
    localSVMap: Map<number, number>,
    clientIDs: number[],
    clientClocks: number[]
): boolean {
    if (clientIDs.length !== clientClocks.length) {
        return false; // Malformed metadata
    }

    for (let i = 0; i < clientIDs.length; i++) {
        const cid = clientIDs[i];
        const localClock = localSVMap.get(cid) || 0;
        if (localClock < clientClocks[i]) {
            return false; // Missing data for this client
        }
    }
    return true;
}

/**
 * Determines whether a diff produced by `Y.encodeStateAsUpdate(doc, serverSV)`
 * actually carries data the server is missing.
 *
 * Yjs always embeds the document's *complete* delete-set in such diffs —
 * state vectors don't cover deletions — so a fully-synced document whose
 * history contains any deletion still produces a non-empty diff. Pushing
 * those no-op diffs writes a spurious update document on every connect.
 *
 * A diff carries new data iff:
 * - it contains any structs (insertions the server lacks), or
 * - its delete-set is not fully covered by the union of the server blobs'
 *   delete-sets (genuine offline deletions).
 *
 * @param diff - Diff produced against the server state vector
 * @param getServerBlobs - Lazily provides all update/history/snapshot blobs
 *                         fetched from the server (only invoked when the
 *                         diff contains no structs)
 * @returns true if the diff should be pushed
 */
export function diffCarriesNewData(diff: Uint8Array, getServerBlobs: () => Uint8Array[]): boolean {
    let localDs: ReturnType<typeof Y.decodeUpdate>['ds'];
    try {
        const decoded = Y.decodeUpdate(diff);
        if (decoded.structs.length > 0) {
            return true;
        }
        localDs = decoded.ds;
    } catch (e) {
        // Unparseable diff — push it and let the server-side consumers decide
        return true;
    }

    // Structs are empty: the diff is push-worthy only if it contains
    // deletions the server doesn't already have.
    return !deleteSetCoveredByBlobs(localDs, getServerBlobs);
}

/**
 * Proves (or fails to prove) that the union of the server blobs' delete-sets
 * covers `localDs`.
 *
 * Blobs are decoded smallest-first with an early exit once coverage is
 * proven. On a long-lived document, the snapshot's delete-set fingerprint
 * (small by construction) almost always proves coverage on its own, so a
 * reconnecting client never decodes the multi-megabyte snapshot or history
 * blobs just to conclude "nothing to push".
 *
 * Coverage is monotone in the union of the server sets, so it is only
 * checked when the next blob would more than double the bytes decoded at
 * the previous check, and once at the end. That always checks before a
 * blob larger than everything decoded so far (the early exit still skips
 * the snapshot), decodes less than twice what checking after every blob
 * would, and bounds the O(|DS|) checks to a logarithmic number: the dozens
 * of tiny pending update documents that sort ahead of the fingerprint no
 * longer cost one full check each.
 *
 * Used by the reconnect push guard both via {@link diffCarriesNewData}
 * (when a diff was already encoded) and directly with a delete-set obtained
 * from `Y.createDeleteSetFromStructStore` — the fast path that avoids
 * encoding an O(document) diff on every clean reconnect.
 *
 * @param localDs - The local document's delete-set
 * @param getServerBlobs - Lazily provides all update/segment/content blobs
 * @returns true when every local deletion is provably on the server
 */
export function deleteSetCoveredByBlobs(
    localDs: ReturnType<typeof Y.decodeUpdate>['ds'],
    getServerBlobs: () => Uint8Array[]
): boolean {
    let serverDs = Y.mergeDeleteSets([]);
    let unmerged: ReturnType<typeof Y.decodeUpdate>['ds'][] = [];
    let decodedBytes = 0;
    let checkedBytes = 0;
    // Only the private accumulator and freshly decoded sets are merged:
    // Y.mergeDeleteSets reuses the input DeleteItems and widens them in
    // place, so localDs must never go in (the caller's set would change,
    // and serverDs stretched over adjacent local deletions would "prove"
    // its own coverage). The merged result is canonical, as
    // deleteSetContains requires.
    const covered = () => {
        if (unmerged.length > 0) {
            serverDs = Y.mergeDeleteSets([serverDs, ...unmerged]);
            unmerged = [];
        }
        checkedBytes = decodedBytes;
        return deleteSetContains(serverDs, localDs);
    };

    // Handles the trivial case (empty local delete-set) without decoding
    // any server blob at all.
    if (covered()) {
        return true;
    }

    const blobs = getServerBlobs().slice().sort((a, b) => a.byteLength - b.byteLength);
    for (const blob of blobs) {
        if (unmerged.length > 0 && decodedBytes + blob.byteLength > 2 * checkedBytes && covered()) {
            return true;
        }
        try {
            unmerged.push(Y.decodeUpdate(blob).ds);
        } catch (e) {
            // Corrupted server blob contributes nothing to coverage;
            // worst case we push a redundant (idempotent) diff.
            continue;
        }
        decodedBytes += blob.byteLength;
    }
    return covered();
}

/**
 * Strips from a diff the deletions the server provably holds already.
 *
 * Yjs embeds the document's COMPLETE delete-set in every diff, so pushing
 * a few bytes of offline structs would re-upload every deletion the
 * document has ever seen: an O(delete-set) update document that every
 * online peer integrates and the next delta compaction copies into a
 * history segment.
 *
 * The result keeps the diff's structs section byte for byte and replaces
 * its trailing delete-set with the ranges the server blobs do not prove: a
 * range is dropped only when one range of the blobs' merged delete-set
 * contains it. Merging joins adjacent and overlapping ranges, so that is
 * exactly "the server's union contains it"; a range only partly on the
 * server is kept whole (re-deleting the rest is idempotent).
 *
 * The delete-set is read from the diff itself and its re-encoding must
 * match the diff's trailing bytes, so the split point is the diff's own;
 * otherwise, and on any error, the diff is returned unchanged. A blob that
 * fails to parse proves nothing, so the result only errs towards pushing
 * more.
 *
 * @param diff - A V1 update, typically `Y.encodeStateAsUpdate(doc, serverSV)`
 * @param getServerBlobs - Lazily provides the server blobs whose delete-sets
 *                         prove coverage (only invoked when the diff
 *                         carries deletions)
 * @returns The diff carrying only the deletions the server lacks, or `diff`
 *          itself when none can be dropped
 */
export function withoutServerDeletions(diff: Uint8Array, getServerBlobs: () => Uint8Array[]): Uint8Array {
    try {
        const diffDs = Y.decodeUpdate(diff).ds;
        if (diffDs.clients.size === 0) {
            return diff;
        }
        const dsSection = encodeDeleteSet(diffDs);
        if (!endsWith(diff, dsSection)) {
            return diff;
        }

        // One merge over all blobs: merging them one at a time re-sorts the
        // accumulated set per blob, O(blobs × delete-set). The decoded sets
        // are throwaway, so Y.mergeDeleteSets widening them in place is
        // harmless here.
        const serverDss: ReturnType<typeof Y.decodeUpdate>['ds'][] = [];
        for (const blob of getServerBlobs()) {
            try {
                serverDss.push(Y.decodeUpdate(blob).ds);
            } catch (e) {
                // Corrupted server blob contributes nothing to coverage
                continue;
            }
        }
        const missing = deletionsMissingFrom(diffDs, Y.mergeDeleteSets(serverDss));
        if (missing === null) {
            return diff;
        }

        const structsEnd = diff.byteLength - dsSection.byteLength;
        const missingSection = encodeDeleteSet(missing);
        const trimmed = new Uint8Array(structsEnd + missingSection.byteLength);
        trimmed.set(diff.subarray(0, structsEnd));
        trimmed.set(missingSection, structsEnd);
        return trimmed;
    } catch (e) {
        console.warn("Failed to strip server deletions from diff:", e);
        return diff;
    }
}

/**
 * The ranges of `ds` that no single range of `serverDs` contains.
 *
 * @param ds - Delete-set with each client's ranges sorted by clock
 * @param serverDs - Sorted and merged delete-set, as Y.mergeDeleteSets
 *                   returns it
 * @returns The ranges to keep, or null when no range can be dropped
 */
function deletionsMissingFrom(
    ds: ReturnType<typeof Y.decodeUpdate>['ds'],
    serverDs: ReturnType<typeof Y.decodeUpdate>['ds']
): ReturnType<typeof Y.decodeUpdate>['ds'] | null {
    const missing = Y.createDeleteSet();
    let dropped = false;
    ds.clients.forEach((items, client) => {
        const serverItems = serverDs.clients.get(client) || [];
        const kept: typeof items = [];
        let j = 0;
        for (const item of items) {
            // Both lists are sorted: skip server ranges ending before it
            while (j < serverItems.length && serverItems[j].clock + serverItems[j].len <= item.clock) {
                j++;
            }
            const range = serverItems[j];
            if (range !== undefined && range.clock <= item.clock &&
                item.clock + item.len <= range.clock + range.len) {
                dropped = true;
            } else {
                kept.push(item);
            }
        }
        if (kept.length > 0) {
            missing.clients.set(client, kept);
        }
    });
    return dropped ? missing : null;
}

/**
 * Encodes a delete-set exactly as it trails a V1 update: Y.encodeSnapshot
 * writes it with the same writer as Y.encodeStateAsUpdate, followed by the
 * snapshot's state vector (empty here: a single zero byte).
 */
function encodeDeleteSet(ds: ReturnType<typeof Y.decodeUpdate>['ds']): Uint8Array {
    const encoded = Y.encodeSnapshot(Y.createSnapshot(ds, new Map()));
    return encoded.subarray(0, encoded.byteLength - 1);
}

function endsWith(bytes: Uint8Array, suffix: Uint8Array): boolean {
    const offset = bytes.byteLength - suffix.byteLength;
    if (offset < 0) {
        return false;
    }
    for (let i = 0; i < suffix.byteLength; i++) {
        if (bytes[offset + i] !== suffix[i]) {
            return false;
        }
    }
    return true;
}

/**
 * Whether every deletion in `ds` lies within one range of `canonicalDs`,
 * which must be sorted and merged the way Y.mergeDeleteSets leaves it
 * (overlapping AND adjacent ranges joined). Equivalent to
 * `Y.equalDeleteSets(canonicalDs, Y.mergeDeleteSets([canonicalDs, ds]))`,
 * without copying, sorting or mutating either set.
 */
function deleteSetContains(
    canonicalDs: ReturnType<typeof Y.decodeUpdate>['ds'],
    ds: ReturnType<typeof Y.decodeUpdate>['ds']
): boolean {
    for (const [client, items] of ds.clients) {
        const ranges = canonicalDs.clients.get(client);
        if (ranges === undefined) {
            return false;
        }
        for (let i = 0; i < items.length; i++) {
            const { clock, len } = items[i];
            // Only the last range starting at or before `clock` can contain it
            let lo = 0;
            let hi = ranges.length - 1;
            while (lo <= hi) {
                const mid = (lo + hi) >>> 1;
                if (ranges[mid].clock <= clock) {
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }
            if (hi < 0 || ranges[hi].clock + ranges[hi].len < clock + len) {
                return false;
            }
        }
    }
    return true;
}
