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
import {
  getFirestore,
  initializeFirestore,
  persistentLocalCache,
  Firestore,
  Unsubscribe,
  QueryDocumentSnapshot,
  collection,
  addDoc,
  Bytes,
  serverTimestamp,
} from "@firebase/firestore";
import { getStorage, FirebaseStorage, ref, uploadBytes } from "@firebase/storage";
import * as Y from "yjs";
import { ObservableV2 } from "lib0/observable";

// Module imports
import {
  FireProviderConfig,
  DEFAULTS,
  FIREBASE_ORIGINS,
  FIRESTORE_PATHS,
} from "./types";
import { generateSessionId, calculateBackoff } from "./utils";
import { extractClockEnds, aggregateClockEnds } from "./update-metadata";
import { performInitialSync, createUpdateListener, createSnapshotListener, createHistoryListener, SyncContext } from "./sync";
import { compact as performTieredCompaction, CompactionContext } from "./compaction";
import { measureClockSkew } from "./locking";
import {
  handleSubdocs as handleSubdocsEvent,
  destroyAllSubdocs,
  SubdocContext,
  SubProviderMap,
} from "./subdocs";

// Re-export types for external consumers
export type { FireProviderConfig } from "./types";

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
export class FireProvider extends ObservableV2<any> {
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
  private subProviders: SubProviderMap = new Map();

  /** Whether compaction is currently in progress */
  private _isCompacting: boolean = false;

  /**
   * Buffered local updates awaiting the debounced save.
   * Kept as an array and merged once at save time — merging on every
   * update event would be quadratic across editing bursts.
   */
  private _pendingUpdates: Uint8Array[] = [];

  // Configuration
  private readonly maxUpdatesThreshold: number;
  private readonly maxWaitTime: number;
  /** Hard cap on how long the sliding debounce may defer buffered updates */
  private readonly maxAggregationTime: number;
  /** Whether compaction garbage-collects deleted content */
  private readonly gcCompaction: boolean;
  private readonly compactionLimit: number;
  private readonly depth: number;
  private readonly lockTTL: number;
  private readonly persistence?: FireProviderConfig['persistence'];
  private readonly _testHooks?: FireProviderConfig['testHooks'];

  // State
  // FIX: Manage multiple listeners (updates, history, snapshot)
  private _unsubscribers: Unsubscribe[] = [];
  // P1.9 FIX: Store history listener separately to pause during compaction
  private _unsubscribeHistory: Unsubscribe | null = null;
  private _lastHistoryDoc: QueryDocumentSnapshot | null = null;

  private _isDestroyed = false;
  /** Whether initial sync has completed and listeners are attached */
  private _synced = false;
  /** P0.3 FIX: Cached clock offset to avoid measuring on every lock attempt */
  private _cachedClockOffset: number | undefined = undefined;
  /**
   * P0.5 FIX: The in-flight save operation, if any. Prevents concurrent
   * saves, and lets destroy() wait it out so a final flush is never
   * silently skipped while a save is mid-flight.
   */
  private _inflightSave: Promise<void> | null = null;
  /** Consecutive save failure counter for circuit breaker */
  private _saveRetryCount = 0;
  /** P1.4 FIX: Sync retry counter for exponential backoff */
  private _syncRetryCount = 0;
  /**
   * Wall-clock time when the oldest currently-buffered update arrived.
   * Used to enforce maxAggregationTime against the sliding debounce.
   */
  private _pendingSince: number | null = null;
  /** P1.5 FIX: Debounce timer ID for cancellation on destroy */
  private _debounceTimerId: ReturnType<typeof setTimeout> | null = null;
  /** P1.4 FIX: Sync retry timer ID for cancellation on destroy */
  private _syncRetryTimerId: ReturnType<typeof setTimeout> | null = null;
  private _boundBeforeUnload: (() => void) | null = null;
  /** Per-session quarantine set for corrupted Firestore documents */
  private _corruptedDocIds = new Set<string>();

  /**
   * Creates a new FireProvider instance.
   * 
   * @param config - Configuration options
   * @throws {Error} If config parameters (path, depth, maxUpdatesThreshold) are invalid.
   */
  constructor(config: FireProviderConfig) {
    super();

    // Initialize from config
    const {
      firebaseApp,
      ydoc,
      path,
      maxUpdatesThreshold = DEFAULTS.MAX_UPDATES_THRESHOLD,
      maxWaitTime = DEFAULTS.MAX_WAIT_TIME,
      maxAggregationTime = maxWaitTime * DEFAULTS.MAX_AGGREGATION_MULTIPLIER,
      gcCompaction = true,
      depth = DEFAULTS.DEPTH,
      lockTTL = DEFAULTS.LOCK_TTL,
      compactionLimit = DEFAULTS.COMPACTION_LIMIT,
      testHooks,
    }: FireProviderConfig = config;

    // P1.8 / P2.20 FIX: Validate path and config BEFORE any Firebase SDK calls
    // This ensures validation errors are thrown with clear messages before
    // getFirestore() which could fail with cryptic errors on invalid app.
    if (!path || path.includes('//') || path.startsWith('/') || path.endsWith('/')) {
      throw new Error(`Invalid Firestore path: '${path}'. Path must not be empty, start/end with '/', or contain '//'`);
    }

    if (maxUpdatesThreshold <= 0) {
      throw new Error(`Invalid maxUpdatesThreshold: ${maxUpdatesThreshold}. Must be positive.`);
    }

    if (maxAggregationTime <= 0) {
      throw new Error(`Invalid maxAggregationTime: ${maxAggregationTime}. Must be positive.`);
    }

    if (depth < 0 || depth > 100) {
      throw new Error(`Invalid depth: ${depth}. Must be between 0 and 100.`);
    }

    this.firebaseApp = firebaseApp;
    this.storage = getStorage(firebaseApp);

    // Check if offline persistence is enabled
    if (config.persistence?.enabled) {
      try {
        this.db = initializeFirestore(firebaseApp, {
          localCache: persistentLocalCache({})
        });
      } catch (err: any) {
        if (err.code === 'failed-precondition') {
          // Firestore has already been initialized in another tab/instance
          this.db = getFirestore(firebaseApp);
        } else {
          throw err;
        }
      }
    } else {
      this.db = getFirestore(firebaseApp);
    }

    this.path = path;
    this.doc = ydoc;
    this.uid = generateSessionId();
    this.depth = depth;

    this.maxUpdatesThreshold = maxUpdatesThreshold;
    this.maxWaitTime = maxWaitTime;
    this.maxAggregationTime = maxAggregationTime;
    this.gcCompaction = gcCompaction;
    this.lockTTL = lockTTL;
    this.compactionLimit = compactionLimit;
    this.persistence = config.persistence;
    this._testHooks = testHooks;

    // Attach document event handlers
    this.doc.on('update', this.handleUpdate);
    this.doc.on('subdocs', this.handleSubdocs);

    // CRITICAL FIX: Register beforeunload handler to prevent data loss on tab close
    // This attempts a best-effort save when the user closes/refreshes the tab
    if (typeof window !== 'undefined' && typeof window.addEventListener === 'function') {
      this._boundBeforeUnload = this.handleBeforeUnload.bind(this);
      window.addEventListener('beforeunload', this._boundBeforeUnload);
    }

    // Start synchronization
    this.sync().catch(err => {
      // P1.7: errors during initial sync are handled by retry logic in sync()
      // but we catch here to prevent unhandled promise rejection
      console.debug('Initial sync handled error:', err);
    });
  }

  // --- Public API ---

  /**
   * Whether compaction is currently in progress.
   */
  get isCompacting(): boolean {
    return this._isCompacting;
  }

  /**
   * Whether initial sync has completed and real-time listeners are active.
   * Also emitted as a 'sync' event when the state becomes true.
   */
  get synced(): boolean {
    return this._synced;
  }

  /**
   * Manually trigger compaction.
   * Normally handled automatically when update threshold is exceeded.
   * 
   * @param attempt - Internal retry counter (do not set manually)
   * @throws {Error} If locking fails or Firestore operations error
   */
  async compact(attempt: number = 1): Promise<void> {
    // Prevent concurrent compaction from same instance
    if (this._isCompacting && attempt === 1) return;

    this._isCompacting = true;

    const ctx: CompactionContext = {
      db: this.db,
      path: this.path,
      uid: this.uid,
      lockTTL: this.lockTTL,
      compactionLimit: this.compactionLimit,
      isDestroyed: () => this._isDestroyed,
      testHooks: this._testHooks,
      // P0.3 FIX: Pass cached clock offset to avoid re-measuring
      cachedClockOffset: this._cachedClockOffset,
      storage: this.storage,
      gc: this.gcCompaction,
    };

    // FIX: Pause history listener during compaction to avoid contention/deadlock in emulator
    if (this._unsubscribeHistory) {
      this._unsubscribeHistory();
      this._unsubscribeHistory = null;
    }

    try {
      await performTieredCompaction(ctx, attempt);
    } finally {
      this._isCompacting = false;

      // FIX: Resume history listener
      if (!this._isDestroyed && !this._unsubscribeHistory) {
        // Use SyncContext to recreate listener
        // We need to re-construct SyncContext or store it.
        // Re-constructing is cheap.
        const syncCtx: SyncContext = {
          db: this.db,
          path: this.path,
          doc: this.doc,
          uid: this.uid,
          maxUpdatesThreshold: this.maxUpdatesThreshold,
          onCompactionNeeded: () => this.compact(),
          isDestroyed: () => this._isDestroyed,
          onListenerError: (error) => {
            console.error('Listener error (resumed):', error);
            this.emit('connection-error', [{ code: 'listener-error', message: error.message, error }]);
          },
          storage: this.storage,
          corruptedDocIds: this._corruptedDocIds,
          onCorruptedDocument: (docId, error) => {
            this.emit('corrupted-document', [{ docId, error }]);
          },
        };

        // We resume listening from the last known checkpoint.
        // If compaction created new segments, they will be picked up now.
        // If we are the ones who created them, we will assume them redundant (correct).
        this._unsubscribeHistory = createHistoryListener(syncCtx, this._lastHistoryDoc);
      }
    }
  }

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
  async destroy(): Promise<void> {
    this._isDestroyed = true;

    // P1.5 FIX: Cancel pending debounce timer
    if (this._debounceTimerId) {
      clearTimeout(this._debounceTimerId);
      this._debounceTimerId = null;
    }

    if (this._syncRetryTimerId) {
      clearTimeout(this._syncRetryTimerId);
      this._syncRetryTimerId = null;
    }

    // Clear all listeners
    this._unsubscribers.forEach(unsub => unsub());
    this._unsubscribers = [];

    if (this._unsubscribeHistory) {
      this._unsubscribeHistory();
      this._unsubscribeHistory = null;
    }

    // Remove document event handlers
    this.doc.off('update', this.handleUpdate);
    this.doc.off('subdocs', this.handleSubdocs);

    // CRITICAL FIX: Remove beforeunload handler
    if (this._boundBeforeUnload && typeof window !== 'undefined') {
      window.removeEventListener('beforeunload', this._boundBeforeUnload);
      this._boundBeforeUnload = null;
    }

    // Destroy all subdocument providers
    await destroyAllSubdocs(this.subProviders);

    // Wait out any in-flight save: updates that arrived while it was
    // running are sitting in _pendingUpdates and would otherwise be
    // silently dropped (the in-flight save won't reschedule once
    // _isDestroyed is set, and saveToFirestore() would have returned
    // the in-flight promise instead of flushing).
    if (this._inflightSave) {
      try {
        await this._inflightSave;
      } catch (err) {
        // Failure already logged and handled inside the save
      }
    }

    // Flush pending updates (single best-effort attempt; retries are
    // suppressed after destroy)
    if (this._pendingUpdates.length > 0) {
      await this.saveToFirestore();
    }

    super.destroy();
  }

  // --- Private Methods ---

  /**
   * Performs initial synchronization and sets up real-time listener.
   * 
   * P0.7 NOTE: The sync algorithm uses eventual consistency.
   * Read order (Updates → History → Snapshot) ensures we never miss data,
   * though we may occasionally apply duplicates (Yjs handles this safely).
   */
  private async sync(): Promise<void> {
    // P0.3 FIX: Measure clock offset once per session and cache it
    // This avoids 3 Firestore ops on every lock attempt
    if (this._cachedClockOffset === undefined) {
      try {
        this._cachedClockOffset = await measureClockSkew(this.db, this.path, this.uid);
        console.log(`Clock offset measured: ${this._cachedClockOffset}ms`);
      } catch (e) {
        console.warn("Failed to measure clock skew, using 0:", e);
        this._cachedClockOffset = 0;
      }
    }

    const syncCtx: SyncContext = {
      db: this.db,
      path: this.path,
      doc: this.doc,
      uid: this.uid,
      maxUpdatesThreshold: this.maxUpdatesThreshold,
      onCompactionNeeded: () => this.compact(),
      isDestroyed: () => this._isDestroyed,
      // FIX: Wire listener error to event emitter
      onListenerError: (error) => {
        console.error('Listener error:', error);
        this.emit('connection-error', [{ code: 'listener-error', message: error.message, error }]);
      },
      storage: this.storage,
      corruptedDocIds: this._corruptedDocIds,
      onCorruptedDocument: (docId, error) => {
        this.emit('corrupted-document', [{ docId, error }]);
      },
    };

    try {
      // Perform initial sync
      const result = await performInitialSync(syncCtx);
      if (this._isDestroyed) return;

      // performInitialSync reports failures via its result rather than
      // throwing — route them into the retry/backoff path below, otherwise
      // a failed sync would be silently treated as success (no retry, no
      // sync-failure event, local changes never pushed).
      if (!result.success) {
        throw result.error ?? new Error("Initial sync failed");
      }

      // Reset retry count on successful sync
      this._syncRetryCount = 0;

      // Cleanup any previous listeners
      this._unsubscribers.forEach(unsub => unsub());
      this._unsubscribers = [];

      if (this._unsubscribeHistory) {
        this._unsubscribeHistory();
        this._unsubscribeHistory = null;
      }

      // Setup real-time listeners (Updates, Snapshot, and History)
      // Pass cursor to prevent sync gaps
      this._unsubscribers.push(createUpdateListener(syncCtx, result.lastSyncedDoc));
      this._unsubscribers.push(createSnapshotListener(syncCtx));

      // Store history listener separately so it can be paused during compaction
      this._lastHistoryDoc = result.lastHistoryDoc;
      this._unsubscribeHistory = createHistoryListener(syncCtx, result.lastHistoryDoc);

      // Initial sync complete and listeners attached. The 'sync' event name
      // follows the y-fire / y-* provider convention (y-websocket, y-indexeddb)
      // so consumers can treat this provider as a drop-in.
      this._synced = true;
      this.emit('sync', [true]);

    } catch (err) {
      console.error("Sync failed", err);

      // Circuit breaker - stop retrying after MAX_RETRIES
      if (!this._isDestroyed) {
        this._syncRetryCount++;

        if (this._syncRetryCount >= DEFAULTS.MAX_RETRIES) {
          console.error(`Sync failed after ${DEFAULTS.MAX_RETRIES} attempts, giving up.`);
          this.emit('sync-failure', [new Error(`Sync failed after ${DEFAULTS.MAX_RETRIES} attempts`)]);
          return;
        }

        const backoffMs = calculateBackoff(this._syncRetryCount);
        console.log(`Retrying sync in ${backoffMs}ms (attempt ${this._syncRetryCount}/${DEFAULTS.MAX_RETRIES})...`);

        if (this._syncRetryTimerId) {
          clearTimeout(this._syncRetryTimerId);
        }

        this._syncRetryTimerId = setTimeout(() => {
          this._syncRetryTimerId = null;
          if (!this._isDestroyed) this.sync();
        }, backoffMs);
      }
    }
  }

  /**
   * Handles local document updates.
   * Batches updates and triggers debounced save to Firestore.
   */
  private handleUpdate = (update: Uint8Array, origin: unknown): void => {
    // Prevent echo loops from remote updates
    if (origin === FIREBASE_ORIGINS.SNAPSHOT ||
      origin === FIREBASE_ORIGINS.HISTORY ||
      origin === FIREBASE_ORIGINS.UPDATE) {
      return;
    }

    // Buffer the update; merging happens once at save time
    if (this._pendingUpdates.length === 0) {
      this._pendingSince = Date.now();
    }
    this._pendingUpdates.push(update);

    // Trigger debounced write
    this._scheduleSave();
  };

  /**
   * Handles subdocument events.
   */
  private handleSubdocs = (event: { added: Set<Y.Doc>; removed: Set<Y.Doc>; loaded: Set<Y.Doc> }): void => {
    const ctx: SubdocContext = {
      firebaseApp: this.firebaseApp,
      parentPath: this.path,
      depth: this.depth,
      maxUpdatesThreshold: this.maxUpdatesThreshold,
      maxWaitTime: this.maxWaitTime,
      lockTTL: this.lockTTL,
      compactionLimit: this.compactionLimit,
      persistence: this.persistence,
      createProvider: (config) => new FireProvider(config),
      onConnectionError: (error) => {
        this.emit('connection-error', [error]);
      },
    };

    handleSubdocsEvent(event, ctx, this.subProviders);
  };

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
  private handleBeforeUnload = (): void => {
    if (this._pendingUpdates.length === 0 || this._isDestroyed) return;

    // Cancel any pending debounce - we're saving now
    if (this._debounceTimerId) {
      clearTimeout(this._debounceTimerId);
      this._debounceTimerId = null;
    }

    // Start the save operation - browser gives us a small window.
    // If a save is already in flight, chain a follow-up flush for the
    // updates that arrived during it.
    const flush = this._inflightSave
      ? this._inflightSave.then(() => this.saveToFirestore())
      : this.saveToFirestore();

    flush.catch(err => {
      console.warn('Best-effort save on unload failed:', err);
    });

    // Note: For guaranteed delivery, implement a Cloud Function endpoint
    // that accepts navigator.sendBeacon data and writes to Firestore.
  };

  /**
   * Schedules a save after a delay, resetting any pending timer.
   * P1.5 FIX: Timer is tracked for cancellation on destroy.
   *
   * The default (debounce) path is additionally capped by
   * maxAggregationTime: because the timer resets on every local update,
   * continuous editing would otherwise defer the save indefinitely while
   * the update buffer grows without bound. Once the oldest buffered update
   * has waited maxAggregationTime, the save fires even mid-burst.
   *
   * @param delayMs - Explicit delay before saving (used by failure retries
   *                  with exponential backoff, exempt from the aggregation
   *                  cap). When omitted, the debounce window applies.
   */
  private _scheduleSave(delayMs?: number): void {
    if (this._isDestroyed) return;

    let delay = delayMs ?? this.maxWaitTime;
    if (delayMs === undefined && this._pendingSince !== null) {
      const deadline = this._pendingSince + this.maxAggregationTime;
      delay = Math.max(0, Math.min(delay, deadline - Date.now()));
    }

    if (this._debounceTimerId) {
      clearTimeout(this._debounceTimerId);
    }
    this._debounceTimerId = setTimeout(() => {
      this._debounceTimerId = null;
      if (!this._isDestroyed) {
        this.saveToFirestore();
      }
    }, delay);
  }

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
  private saveToFirestore(): Promise<void> {
    if (this._inflightSave) return this._inflightSave;
    if (this._pendingUpdates.length === 0) return Promise.resolve();

    this._inflightSave = this._executeSave().finally(() => {
      this._inflightSave = null;
    });
    return this._inflightSave;
  }

  private async _executeSave(): Promise<void> {
    // Take the buffered updates for this save operation
    const batch = this._pendingUpdates;
    this._pendingUpdates = [];
    this._pendingSince = null;
    const update = batch.length === 1 ? batch[0] : Y.mergeUpdates(batch);

    // Lazy clock extraction: on large batches (long offline sessions) a
    // full Y.decodeUpdate here would materialize every struct on the main
    // thread right on the save path.
    const clockEnds = extractClockEnds(update);
    const baseData: Record<string, any> = {
      createdAt: serverTimestamp(),
      createdBy: this.uid,
      ...aggregateClockEnds(clockEnds),
    };

    try {
      if (update.byteLength > DEFAULTS.INLINE_UPDATE_LIMIT) {
        // Storage-backed update: upload binary to Cloud Storage and write
        // a lightweight pointer document to the updates collection
        const storagePath = `${this.path}/large_updates/${this.uid}_${Date.now()}.bin`;
        await uploadBytes(ref(this.storage, storagePath), update);
        await addDoc(collection(this.db, this.path, FIRESTORE_PATHS.UPDATES), {
          ...baseData,
          updateStoragePath: storagePath,
        });
        console.log(`Oversized update (${update.byteLength} bytes) offloaded to Cloud Storage: ${storagePath}`);
      } else {
        await addDoc(collection(this.db, this.path, FIRESTORE_PATHS.UPDATES), {
          ...baseData,
          update: Bytes.fromUint8Array(update),
        });
      }

      // Reset retry counter on success
      this._saveRetryCount = 0;

      // Announce the committed save with the commit wall-clock time — the
      // success half of the persistence event surface (every failure mode
      // already emits 'save-rejected'). Fires for the debounced path, the
      // threshold-forced path, and the destroy() final flush alike, since
      // they all funnel through here. Consumers map it to a last-sync time.
      this.emit('saved', [Date.now()]);

      // P0.5 FIX: Check if new updates arrived during save
      // If so, schedule another save
      if (this._pendingUpdates.length > 0 && !this._isDestroyed) {
        this._scheduleSave();
      }
    } catch (err: any) {
      console.error("Failed to save update to Firestore", err);

      // Detect Firestore size-limit error (server-side rejection of an
      // inline write; rare now that oversized updates are offloaded with
      // headroom below the limit)
      const isDocTooLarge =
        err?.code === 'invalid-argument' ||
        err?.message?.includes('exceeds the maximum') ||
        err?.message?.includes('too large');

      if (isDocTooLarge) {
        // Terminal: the data will never fit, do not retry
        this.emit('save-rejected', [{
          code: 'document-too-large' as const,
          sizeBytes: update.byteLength,
          limitBytes: DEFAULTS.FIRESTORE_DOC_LIMIT,
          error: err instanceof Error ? err : new Error(String(err)),
          update,
        }]);
        if (this._pendingUpdates.length > 0) {
          this._scheduleSave();
        }
        return;
      }

      // Generic failure: apply retry cap
      this._saveRetryCount++;

      if (this._saveRetryCount >= DEFAULTS.MAX_SAVE_RETRIES) {
        console.error(
          `Save failed after ${this._saveRetryCount} consecutive attempts, giving up.`
        );
        this.emit('save-rejected', [{
          code: 'max-retries-exceeded' as const,
          retries: this._saveRetryCount,
          error: err instanceof Error ? err : new Error(String(err)),
          update,
        }]);
        this._saveRetryCount = 0;
        if (this._pendingUpdates.length > 0) {
          this._scheduleSave();
        }
        return;
      }

      // Recovery: put the failed batch back ahead of any updates
      // that arrived during the attempt
      this._pendingUpdates.unshift(update);
      // Restart the aggregation clock: retries pace themselves via
      // explicit backoff, the cap only guards the debounce path
      if (this._pendingSince === null) {
        this._pendingSince = Date.now();
      }

      // Retry with exponential backoff
      if (!this._isDestroyed) {
        this._scheduleSave(calculateBackoff(this._saveRetryCount));
      }
    }
  }
}
