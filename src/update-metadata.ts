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
