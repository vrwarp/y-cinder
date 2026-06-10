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
/**
 * Maximum number of distinct client IDs to store in metadata.
 * If an update exceeds this, we skip metadata optimization entirely
 * to avoid Firestore document bloat from massive offline merges.
 */
const MAX_METADATA_CLIENTS = 50;
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
export function extractAllMetadata(update) {
    try {
        const decoded = Y.decodeUpdate(update);
        const results = [];
        if (decoded.structs) {
            // Group by client to compute accurate ranges
            const clientRanges = new Map();
            for (const struct of decoded.structs) {
                const clientID = struct.id.client;
                const clockStart = struct.id.clock;
                const clockEnd = struct.id.clock + struct.length;
                const existing = clientRanges.get(clientID);
                if (existing) {
                    existing.start = Math.min(existing.start, clockStart);
                    existing.end = Math.max(existing.end, clockEnd);
                }
                else {
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
    }
    catch (e) {
        // P1.9 FIX: Log parse error for debugging
        console.warn("Failed to parse update metadata:", e);
        return [];
    }
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
export function aggregateMetadata(metas) {
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
export function isUpdateRedundant(localSVMap, clientIDs, clientClocks) {
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
export function diffCarriesNewData(diff, getServerBlobs) {
    let localDs;
    try {
        const decoded = Y.decodeUpdate(diff);
        if (decoded.structs.length > 0) {
            return true;
        }
        localDs = decoded.ds;
    }
    catch (e) {
        // Unparseable diff — push it and let the server-side consumers decide
        return true;
    }
    // Structs are empty: the diff is push-worthy only if it contains
    // deletions the server doesn't already have.
    const serverDeleteSets = [];
    for (const blob of getServerBlobs()) {
        try {
            serverDeleteSets.push(Y.decodeUpdate(blob).ds);
        }
        catch (e) {
            // Corrupted server blob contributes nothing to coverage;
            // worst case we push a redundant (idempotent) diff.
        }
    }
    const serverDs = Y.mergeDeleteSets(serverDeleteSets);
    const unionDs = Y.mergeDeleteSets([serverDs, localDs]);
    // If adding our delete-set changes nothing, the server already has it all
    return !Y.equalDeleteSets(serverDs, unionDs);
}
