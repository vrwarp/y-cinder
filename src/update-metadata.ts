import * as Y from "yjs";
import { UpdateMetadata } from "./types";

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
 * @param update - The Yjs update blob to parse
 * @returns Array of metadata objects, one per client in the update.
 *          Returns empty array if parsing fails.
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
        // Malformed update or internal Yjs API change
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
    clientID?: number;
    clockStart?: number;
    clockEnd?: number;
} {
    if (metas.length === 0) {
        return {};
    }

    return {
        clientIDs: metas.map(m => m.clientID),
        clientID: metas[0].clientID, // Backwards compatibility
        clockStart: Math.min(...metas.map(m => m.clockStart)),
        clockEnd: Math.max(...metas.map(m => m.clockEnd)),
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
    clockEnd: number
): boolean {
    for (const cid of clientIDs) {
        const localClock = localSVMap.get(cid) || 0;
        if (localClock < clockEnd) {
            return false; // Missing data for this client
        }
    }
    return true;
}
