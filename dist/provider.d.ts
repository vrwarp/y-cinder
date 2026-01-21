/**
 * FireProvider - Yjs persistence provider for Firebase Firestore
 *
 * This is the main orchestration class that coordinates:
 * - Document synchronization with Firestore
 * - Debounced update batching
 * - Tiered compaction (snapshot → history → updates)
 * - Distributed locking for safe concurrent operations
 * - Subdocument lifecycle management
 *
 * @module FireProvider
 */
import { FirebaseApp } from "@firebase/app";
import { Firestore } from "@firebase/firestore";
import * as Y from "yjs";
import { ObservableV2 } from "lib0/observable";
import { FireProviderConfig } from "./types";
export { FireProviderConfig } from "./types";
/**
 * Yjs persistence provider for Firebase Firestore.
 *
 * Provides real-time synchronization of Yjs documents with Firestore,
 * including automatic compaction, distributed locking, and subdocument support.
 *
 * @example
 * ```typescript
 * import { FireProvider } from 'y-cinder';
 *
 * const provider = new FireProvider({
 *   firebaseApp: app,
 *   ydoc: doc,
 *   path: 'documents/my-doc'
 * });
 *
 * // Later...
 * await provider.destroy();
 * ```
 */
export declare class FireProvider extends ObservableV2<any> {
    /** The Yjs document being synced */
    readonly doc: Y.Doc;
    /** Firestore document path */
    readonly path: string;
    /** Firestore instance */
    readonly db: Firestore;
    /** Firebase app instance */
    readonly firebaseApp: FirebaseApp;
    /** Unique session ID for this provider instance */
    readonly uid: string;
    /** Map of subdocument providers */
    private subProviders;
    /** Whether compaction is currently in progress */
    private _isCompacting;
    /** Pending update cache for debouncing */
    private updateCache;
    private readonly maxUpdatesThreshold;
    private readonly maxWaitTime;
    private readonly compactionProbability;
    private readonly compactionLimit;
    private readonly depth;
    private readonly lockTTL;
    private readonly _testHooks?;
    private _unsubscribers;
    private _unsubscribeHistory;
    private _lastHistoryDoc;
    private _debouncedSave;
    private _isDestroyed;
    /** P0.3 FIX: Cached clock offset to avoid measuring on every lock attempt */
    private _cachedClockOffset;
    /** P0.5 FIX: Flag to prevent race condition during save */
    private _isSaving;
    /** P1.4 FIX: Sync retry counter for exponential backoff */
    private _syncRetryCount;
    /** P1.5 FIX: Debounce timer ID for cancellation on destroy */
    private _debounceTimerId;
    private _boundBeforeUnload;
    /**
     * Creates a new FireProvider instance.
     *
     * @param config - Configuration options
     * @throws {Error} If config parameters (path, depth, maxUpdatesThreshold) are invalid.
     */
    constructor(config: FireProviderConfig);
    /**
     * Whether compaction is currently in progress.
     */
    get isCompacting(): boolean;
    /**
     * Manually trigger compaction.
     * Normally handled automatically when update threshold is exceeded.
     *
     * @param attempt - Internal retry counter (do not set manually)
     * @throws {Error} If locking fails or Firestore operations error
     */
    compact(attempt?: number): Promise<void>;
    /**
     * Destroys the provider and releases all resources.
     *
     * This method:
     * 1. Stops listening for remote updates
     * 2. Destroys all subdocument providers
     * 3. Flushes any pending local updates
     * 4. Cleans up event handlers
     * 5. P1.5: Cancels pending debounce timer
     */
    destroy(): Promise<void>;
    /**
     * Performs initial synchronization and sets up real-time listener.
     *
     * P0.7 NOTE: The sync algorithm uses eventual consistency.
     * Read order (Updates → History → Snapshot) ensures we never miss data,
     * though we may occasionally apply duplicates (Yjs handles this safely).
     */
    private sync;
    /**
     * Handles local document updates.
     * Batches updates and triggers debounced save to Firestore.
     */
    private handleUpdate;
    /**
     * Handles subdocument events.
     */
    private handleSubdocs;
    /**
     * CRITICAL FIX: Handles beforeunload event to prevent data loss on tab close.
     *
     * Uses navigator.sendBeacon for best-effort delivery of pending updates.
     * sendBeacon is designed for this exact use case - it queues data for
     * delivery even after the page unloads.
     *
     * Limitations:
     * - sendBeacon payload is limited to ~64KB
     * - Firestore SDK doesn't support sendBeacon directly, so we encode minimal payload
     * - This is BEST EFFORT - not guaranteed delivery
     */
    private handleBeforeUnload;
    /**
     * Saves the cached update to Firestore.
     * P0.5 FIX: Uses _isSaving flag to prevent race condition where
     * updates arriving during save could be duplicated or lost.
     */
    private saveToFirestore;
}
//# sourceMappingURL=provider.d.ts.map