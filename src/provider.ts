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
import { debounce, generateSessionId } from "./utils";
import { extractAllMetadata, aggregateMetadata } from "./update-metadata";
import { performInitialSync, createUpdateListener, SyncContext } from "./sync";
import { compact, CompactionContext } from "./compaction";
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
    } = config;

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

    // Setup debounced save
    this._debouncedSave = debounce(this.saveToFirestore.bind(this), this.maxWaitTime);

    // Attach document event handlers
    this.doc.on('update', this.handleUpdate);
    this.doc.on('subdocs', this.handleSubdocs);

    // Start synchronization
    this.sync();
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
   */
  async destroy(): Promise<void> {
    this._isDestroyed = true;

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
   */
  private async sync(): Promise<void> {
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

      // Retry after delay
      if (!this._isDestroyed) {
        console.log("Retrying sync in 5 seconds...");
        setTimeout(() => {
          if (!this._isDestroyed) this.sync();
        }, 5000);
      }
    }
  }

  /**
   * Handles local document updates.
   * Batches updates and triggers debounced save to Firestore.
   */
  private handleUpdate = (update: Uint8Array, origin: any): void => {
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
   */
  private async saveToFirestore(): Promise<void> {
    if (!this.updateCache) return;

    const update = this.updateCache;
    this.updateCache = null;

    const metas = extractAllMetadata(update);
    const docData: any = {
      update: Bytes.fromUint8Array(update),
      createdAt: serverTimestamp(),
      createdBy: this.uid,
      ...aggregateMetadata(metas),
    };

    try {
      await addDoc(collection(this.db, this.path, FIRESTORE_PATHS.UPDATES), docData);
    } catch (err) {
      console.error("Failed to save update to Firestore", err);

      // Recovery: Put back the updates we failed to save
      if (this.updateCache) {
        this.updateCache = Y.mergeUpdates([update, this.updateCache]);
      } else {
        this.updateCache = update;
      }

      // Retry
      this._debouncedSave();
    }
  }
}
