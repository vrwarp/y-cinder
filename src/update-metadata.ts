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
 * `Y.encodeStateVectorFromUpdate` walks the update with the lazy decoder
 * (no Item/content objects are allocated), which matters when the update is
 * a large merged blob — e.g. the debounced save after a long offline
 * session, or a snapshot-sized diff. The resulting map is identical to the
 * `clockEnd` values `extractAllMetadata` computes from the decoded structs.
 *
 * @param update - The Yjs update blob to parse
 * @returns Map of clientID -> end clock. Empty map on parse error.
 */
export function extractClockEnds(update: Uint8Array): Map<number, number> {
    try {
        return Y.decodeStateVector(Y.encodeStateVectorFromUpdate(update));
    } catch (e) {
        console.warn("Failed to extract update clock metadata:", e);
        return new Map();
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
    //
    // Blobs are checked smallest-first with an early exit once coverage is
    // proven. On a long-lived document, the snapshot's inline delete-set
    // fingerprint (a few KB) almost always proves coverage on its own, so a
    // reconnecting client never decodes the multi-megabyte snapshot or
    // history blobs just to conclude "nothing to push".
    let serverDs = Y.mergeDeleteSets([]);
    const covered = () =>
        Y.equalDeleteSets(serverDs, Y.mergeDeleteSets([serverDs, localDs]));

    // Handles the trivial case (empty local delete-set) without decoding
    // any server blob at all.
    if (covered()) {
        return false;
    }

    const blobs = getServerBlobs().slice().sort((a, b) => a.byteLength - b.byteLength);
    for (const blob of blobs) {
        try {
            serverDs = Y.mergeDeleteSets([serverDs, Y.decodeUpdate(blob).ds]);
        } catch (e) {
            // Corrupted server blob contributes nothing to coverage;
            // worst case we push a redundant (idempotent) diff.
            continue;
        }
        if (covered()) {
            return false;
        }
    }
    return true;
}
