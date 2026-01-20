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
  Firestore,
  Unsubscribe,
  collection,
  addDoc,
  Bytes,
  serverTimestamp,
} from "@firebase/firestore";
import * as Y from "yjs";
import { ObservableV2 } from "lib0/observable";

// Module imports
import {
  FireProviderConfig,
  DEFAULTS,
  FIREBASE_ORIGINS,
  FIRESTORE_PATHS,
} from "./types";
import { debounce, generateSessionId, calculateBackoff } from "./utils";
import { extractAllMetadata, aggregateMetadata } from "./update-metadata";
import { performInitialSync, createUpdateListener, SyncContext } from "./sync";
import { compact, CompactionContext } from "./compaction";
import { measureClockSkew } from "./locking";
import {
  handleSubdocs as handleSubdocsEvent,
  destroyAllSubdocs,
  SubdocContext,
  SubProviderMap,
} from "./subdocs";

// Re-export types for external consumers
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
export class FireProvider extends ObservableV2<any> {
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
  private subProviders: SubProviderMap = new Map();

  /** Whether compaction is currently in progress */
  private _isCompacting: boolean = false;

  /** Pending update cache for debouncing */
  private updateCache: Uint8Array | null = null;

  // Configuration
  private readonly maxUpdatesThreshold: number;
  private readonly maxWaitTime: number;
  private readonly compactionProbability: number;
  private readonly compactionLimit: number;
  private readonly depth: number;
  private readonly lockTTL: number;
  private readonly _testHooks?: FireProviderConfig['testHooks'];

  // State
  private _unsubscribeUpdates: Unsubscribe | null = null;
  private _debouncedSave: () => void;
  private _isDestroyed = false;
  /** P0.3 FIX: Cached clock offset to avoid measuring on every lock attempt */
  private _cachedClockOffset: number | undefined = undefined;
  /** P0.5 FIX: Flag to prevent race condition during save */
  private _isSaving = false;
  /** P1.4 FIX: Sync retry counter for exponential backoff */
  private _syncRetryCount = 0;
  /** P1.5 FIX: Debounce timer ID for cancellation on destroy */
  private _debounceTimerId: ReturnType<typeof setTimeout> | null = null;

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
      compactionProbability = DEFAULTS.COMPACTION_PROBABILITY,
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

    if (depth < 0 || depth > 100) {
      throw new Error(`Invalid depth: ${depth}. Must be between 0 and 100.`);
    }

    this.firebaseApp = firebaseApp;
    this.db = getFirestore(firebaseApp);
    this.path = path;
    this.doc = ydoc;
    this.uid = generateSessionId();
    this.depth = depth;

    this.maxUpdatesThreshold = maxUpdatesThreshold;
    this.maxWaitTime = maxWaitTime;
    this.compactionProbability = compactionProbability;
    this.lockTTL = lockTTL;
    this.compactionLimit = compactionLimit;
    this._testHooks = testHooks;

    // P1.5 FIX: Setup debounced save with timer tracking
    this._debouncedSave = () => {
      if (this._debounceTimerId) {
        clearTimeout(this._debounceTimerId);
      }
      this._debounceTimerId = setTimeout(() => {
        this._debounceTimerId = null;
        this.saveToFirestore();
      }, this.maxWaitTime);
    };

    // Attach document event handlers
    this.doc.on('update', this.handleUpdate);
    this.doc.on('subdocs', this.handleSubdocs);

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
   * Manually trigger compaction.
   * Normally handled automatically when update threshold is exceeded.
   * 
   * @param attempt - Internal retry counter (do not set manually)
   * @throws {Error} If locking fails or Firestore operations error
   */
  async compact(attempt: number = 1): Promise<void> {
    // Prevent concurrent compaction from same instance
    if (this._isCompacting && attempt === 1) return;

    const ctx: CompactionContext = {
      db: this.db,
      path: this.path,
      uid: this.uid,
      lockTTL: this.lockTTL,
      compactionLimit: this.compactionLimit,
      isDestroyed: () => this._isDestroyed,
      testHooks: this._testHooks,
      onCompactionStateChange: (isCompacting) => {
        this._isCompacting = isCompacting;
      },
      // P0.3 FIX: Pass cached clock offset to avoid re-measuring
      cachedClockOffset: this._cachedClockOffset,
    };

    await compact(ctx, attempt);
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

    // Stop listening
    if (this._unsubscribeUpdates) {
      this._unsubscribeUpdates();
      this._unsubscribeUpdates = null;
    }

    // Remove document event handlers
    this.doc.off('update', this.handleUpdate);
    this.doc.off('subdocs', this.handleSubdocs);

    // Destroy all subdocument providers
    await destroyAllSubdocs(this.subProviders);

    // Flush pending updates
    if (this.updateCache) {
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
      compactionProbability: this.compactionProbability,
      onCompactionNeeded: () => this.compact(),
      isDestroyed: () => this._isDestroyed,
    };

    try {
      // Perform initial sync
      await performInitialSync(syncCtx);
      if (this._isDestroyed) return;

      // Cleanup any previous listener
      if (this._unsubscribeUpdates) {
        this._unsubscribeUpdates();
      }

      // Setup real-time listener
      this._unsubscribeUpdates = createUpdateListener(syncCtx);

    } catch (err) {
      console.error("Sync failed", err);

      // P1.4 FIX: Exponential backoff instead of fixed retry
      if (!this._isDestroyed) {
        this._syncRetryCount++;
        const backoffMs = calculateBackoff(this._syncRetryCount);
        console.log(`Retrying sync in ${backoffMs}ms (attempt ${this._syncRetryCount})...`);
        setTimeout(() => {
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

    // Merge into cache
    this.updateCache = this.updateCache
      ? Y.mergeUpdates([this.updateCache, update])
      : update;

    // Trigger debounced write
    this._debouncedSave();
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
      compactionProbability: this.compactionProbability,
      lockTTL: this.lockTTL,
      compactionLimit: this.compactionLimit,
      createProvider: (config) => new FireProvider(config),
      onConnectionError: (error) => {
        this.emit('connection-error', [error]);
      },
    };

    handleSubdocsEvent(event, ctx, this.subProviders);
  };

  /**
   * Saves the cached update to Firestore.
   * P0.5 FIX: Uses _isSaving flag to prevent race condition where
   * updates arriving during save could be duplicated or lost.
   */
  private async saveToFirestore(): Promise<void> {
    if (!this.updateCache || this._isSaving) return;

    this._isSaving = true;

    // Take the current cache for this save operation
    const update = this.updateCache;
    this.updateCache = null;

    const metas = extractAllMetadata(update);
    const docData: any = {
      update: Bytes.fromUint8Array(update),
      createdAt: serverTimestamp(),
      createdBy: this.uid,
      ...aggregateMetadata(metas),
    } as Record<string, any>;

    try {
      await addDoc(collection(this.db, this.path, FIRESTORE_PATHS.UPDATES), docData);

      // P0.5 FIX: Check if new updates arrived during save
      // If so, schedule another save
      if (this.updateCache) {
        this._debouncedSave();
      }
    } catch (err) {
      console.error("Failed to save update to Firestore", err);

      // Recovery: Merge back the update we failed to save
      // with any new updates that arrived during the attempt
      if (this.updateCache) {
        this.updateCache = Y.mergeUpdates([update, this.updateCache]);
      } else {
        this.updateCache = update;
      }

      // Retry
      this._debouncedSave();
    } finally {
      this._isSaving = false;
    }
  }
}
