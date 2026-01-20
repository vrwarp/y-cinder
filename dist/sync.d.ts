/**
 * Synchronization Module
 *
 * Implements the core sync algorithm for bidirectional synchronization between
 * local Yjs documents and Firestore. Uses metadata-based comparison to minimize
 * data transfer and avoid re-applying already-seen updates.
 *
 * ## Sync Algorithm
 *
 * ### Initial Sync (performInitialSync)
 * 1. Fetch all server data (updates, history, snapshot)
 * 2. Extract metadata (client IDs and clock values) from each item
 * 3. Build a server state vector from the metadata
 * 4. Compare with local state vector
 * 5. Apply only items that contain data missing locally
 * 6. Push any local data that's missing on the server
 *
 * ### Real-time Sync (createUpdateListener)
 * - Listens to the updates collection via onSnapshot
 * - Applies new updates from other clients
 * - Skips our own updates (using createdBy)
 * - Skips redundant updates (using clientID/clockEnd metadata)
 * - Probabilistically triggers compaction when threshold exceeded
 *
 * ## Priority Order
 *
 * Updates are applied in this order to ensure correct CRDT merge:
 * 1. Base Snapshot (Tier 1) - oldest, most compacted data
 * 2. History Segments (Tier 2) - intermediate merges
 * 3. Individual Updates (Tier 3) - newest data
 *
 * @module sync
 */
import { Firestore, Unsubscribe } from "@firebase/firestore";
import * as Y from "yjs";
/**
 * Context required for sync operations.
 */
export interface SyncContext {
    /** Firestore instance */
    db: Firestore;
    /** Base document path */
    path: string;
    /** The Yjs document to sync */
    doc: Y.Doc;
    /** Unique client ID */
    uid: string;
    /** Maximum updates before triggering compaction consideration */
    maxUpdatesThreshold: number;
    /** Probability of attempting compaction */
    compactionProbability: number;
    /** Callback to trigger compaction */
    onCompactionNeeded?: () => void;
    /** Flag to check if provider is destroyed */
    isDestroyed: () => boolean;
}
/**
 * Result of initial sync operation.
 */
export interface SyncResult {
    /** Whether sync completed successfully */
    success: boolean;
    /** Error if sync failed */
    error?: Error;
    /** Number of updates applied */
    updatesApplied: number;
    /** Whether local updates were pushed */
    localUpdatesPushed: boolean;
}
/**
 * Performs the initial sync operation.
 *
 * This is the core sync algorithm using metadata-only comparison:
 * 1. Fetch all data (updates, history, snapshot) and extract metadata
 * 2. Build a server state vector from metadata
 * 3. Compare with local state vector
 * 4. Apply only missing data
 * 5. Push local updates not on server
 *
 * @param ctx - Sync context
 * @returns Sync result with statistics
 *
 * @example
 * ```typescript
 * const result = await performInitialSync({
 *   db, path, doc: ydoc, uid,
 *   maxUpdatesThreshold: 50,
 *   compactionProbability: 0.01,
 *   isDestroyed: () => false
 * });
 * ```
 */
export declare function performInitialSync(ctx: SyncContext): Promise<SyncResult>;
/**
 * Creates a real-time listener for new updates.
 *
 * @param ctx - Sync context
 * @returns Unsubscribe function
 */
export declare function createUpdateListener(ctx: SyncContext): Unsubscribe;
//# sourceMappingURL=sync.d.ts.map