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
import { UpdateMetadata } from "./types";
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
export declare function extractAllMetadata(update: Uint8Array): UpdateMetadata[];
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
export declare function aggregateMetadata(metas: UpdateMetadata[]): {
    clientIDs?: number[];
    clientClocks?: number[];
};
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
export declare function isUpdateRedundant(localSVMap: Map<number, number>, clientIDs: number[], clientClocks: number[]): boolean;
//# sourceMappingURL=update-metadata.d.ts.map