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
import { FirebaseStorage } from "@firebase/storage";
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
    /** Firebase Storage instance */
    readonly storage: FirebaseStorage;
    /** Unique session ID for this provider instance */
    readonly uid: string;
    /** Map of subdocument providers */
    private subProviders;
    /** Whether compaction is currently in progress */
    private _isCompacting;
    /**
     * Buffered local updates awaiting the debounced save.
     * Kept as an array and merged once at save time — merging on every
     * update event would be quadratic across editing bursts.
     */
    private _pendingUpdates;
    private readonly maxUpdatesThreshold;
    private readonly maxWaitTime;
    private readonly compactionLimit;
    private readonly depth;
    private readonly lockTTL;
    private readonly persistence?;
    private readonly _testHooks?;
    private _unsubscribers;
    private _unsubscribeHistory;
    private _lastHistoryDoc;
    private _isDestroyed;
    /** Whether initial sync has completed and listeners are attached */
    private _synced;
    /** P0.3 FIX: Cached clock offset to avoid measuring on every lock attempt */
    private _cachedClockOffset;
    /**
     * P0.5 FIX: The in-flight save operation, if any. Prevents concurrent
     * saves, and lets destroy() wait it out so a final flush is never
     * silently skipped while a save is mid-flight.
     */
    private _inflightSave;
    /** Consecutive save failure counter for circuit breaker */
    private _saveRetryCount;
    /** P1.4 FIX: Sync retry counter for exponential backoff */
    private _syncRetryCount;
    /** P1.5 FIX: Debounce timer ID for cancellation on destroy */
    private _debounceTimerId;
    /** P1.4 FIX: Sync retry timer ID for cancellation on destroy */
    private _syncRetryTimerId;
    private _boundBeforeUnload;
    /** Per-session quarantine set for corrupted Firestore documents */
    private _corruptedDocIds;
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
     * Whether initial sync has completed and real-time listeners are active.
     * Also emitted as a 'synced' event when the state becomes true.
     */
    get synced(): boolean;
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
     * Schedules a save after a delay, resetting any pending timer.
     * P1.5 FIX: Timer is tracked for cancellation on destroy.
     *
     * @param delayMs - Delay before saving. Defaults to the debounce window;
     *                  failure retries pass an exponential backoff delay.
     */
    private _scheduleSave;
    /**
     * Saves buffered updates to Firestore.
     *
     * P0.5 FIX: Only one save runs at a time; while one is in flight this
     * returns the in-flight promise. Updates arriving during a save stay
     * buffered and are flushed by a follow-up save.
     *
     * Updates too large to inline are offloaded to Cloud Storage with a
     * lightweight pointer document (same mechanism as oversized initial-sync
     * diffs) instead of being rejected.
     *
     * Circuit breaker: persistent failures retry with exponential backoff,
     * then emit 'save-rejected' after MAX_SAVE_RETRIES attempts.
     */
    private saveToFirestore;
    private _executeSave;
}
//# sourceMappingURL=provider.d.ts.map