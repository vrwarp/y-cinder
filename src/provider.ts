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
import { getStorage, FirebaseStorage, ref, deleteObject } from "@firebase/storage";
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
import { extractClockEnds, aggregateClockEnds, updateEndsWithDeletions } from "./update-metadata";
import { performInitialSync, createUpdateListener, createSnapshotListener, createHistoryListener, SyncContext, SyncResult } from "./sync";
import { isClientOfflineError, isLostAckCommit, largeUpdatePath } from "./sync-policy";
import { compact as performTieredCompaction, deleteUpdateBlobs, CompactionContext, CompactionResult, UpdateBlobReclaim } from "./compaction";
import { isPersistentCompactionFailure } from "./compaction-policy";
import { squashDocument, readDocEpoch, SquashResult } from "./squash";
import { sharedClockOffset } from "./locking";
import { uploadBlob, restoreMissingBlob } from "./storage-blobs";
import {
  handleSubdocs as handleSubdocsEvent,
  destroyAllSubdocs,
  SubdocContext,
  SubProviderMap,
} from "./subdocs";
import {
  compactionBackoffMs,
  computeSaveDelay,
  initialSyncSaveHold,
  isRemoteOrigin,
  planListenerRecovery,
  squashBlockedBy,
  validateProviderConfig,
} from './provider-policy';

// Re-export types for external consumers
export type { FireProviderConfig } from "./types";

/** The update buffer, and how many entries it held, when initial sync read the local doc */
type SyncCapture = { buffer: Uint8Array[]; count: number };

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

  /**
   * The in-flight compaction, if any. The compaction lock is re-entrant
   * for this provider's uid, so compaction and squash must never overlap:
   * the second would share the first's lock, and its release would strip
   * the first of its exclusivity mid-run.
   */
  private _inflightCompaction: Promise<void> | null = null;
  /** The in-flight squash, if any (see _inflightCompaction) */
  private _inflightSquash: Promise<SquashResult> | null = null;
  /**
   * Consecutive compaction failures that retrying cannot fix, and when
   * automatic triggers may resume. Without this memory, a document no
   * compaction can get past (an undecodable update, a missing Storage
   * blob, Storage rejecting the fold upload) re-ran the same failing
   * attempt, re-reading the whole backlog, on every trigger: at the
   * realtime hard cap that is every listener delivery.
   */
  private _compactionFailures = 0;
  private _compactionBackoffUntil = 0;
  /**
   * Blobs of update pointers this provider's compactions or squash
   * deleted, reclaimed by its next compaction, or by destroy() when none
   * follows (see CompactionContext.deferredUpdateBlobs).
   */
  private _deferredUpdateBlobs: UpdateBlobReclaim[] = [];

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
  /** History segments accumulated before compaction folds into the snapshot */
  private readonly historyFoldThreshold: number;
  /** Subdocument sync strategy: eager (all) or lazy (shouldLoad only) */
  private readonly subdocLoadingMode: 'eager' | 'lazy';
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
  /**
   * P0.3 FIX: Cached clock offset to avoid measuring on every lock attempt.
   * Measured on the first lock need (see _clockOffset), not before sync.
   */
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
  /** Consecutive initial syncs that found the client offline */
  private _offlineRetryCount = 0;
  /** Consecutive listener errors, for re-sync backoff */
  private _listenerRetryCount = 0;
  /** When the current real-time listeners were attached */
  private _listenersAttachedAt = 0;
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
   * Epoch of the local document (see squash.ts). Initialized from the
   * document's own epoch marker, refreshed from the server on sync.
   */
  private _epoch = 0;
  /**
   * Set once the server is discovered to be at a newer epoch. All saves
   * and syncing stop; the application must rebuild from the new epoch
   * (see the 'epoch-changed' event).
   */
  private _epochFenced = false;
  /**
   * An initial sync (performInitialSync) is running. Its push covers what
   * is buffered when it reads the local doc, so due saves wait for it
   * (see initialSyncSaveHold).
   */
  private _initialSyncInFlight = false;
  /**
   * Resolves once local persistence has loaded into the doc, failed to,
   * or timed out (see FireProviderConfig.localReady). Bounded once for
   * the provider's lifetime, so retried syncs do not wait again.
   */
  private _localReady: Promise<void> | undefined;
  /** Ends the _localReady wait early (on destroy) */
  private _endLocalReadyWait: (() => void) | null = null;

  /**
   * Creates a new FireProvider instance.
   * 
   * @param config - Configuration options
   * @throws {Error} If config parameters (path, depth, maxUpdatesThreshold, maxWaitTime) are invalid.
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
      // Floored at 1ms: maxWaitTime 0 (save immediately) never defers a
      // save, so its cap is moot, but deriving 0 would fail validation
      // with an error about an option the caller never set.
      maxAggregationTime = Math.max(1, maxWaitTime * DEFAULTS.MAX_AGGREGATION_MULTIPLIER),
      gcCompaction = true,
      historyFoldThreshold = DEFAULTS.HISTORY_FOLD_THRESHOLD,
      subdocLoadingMode = 'eager',
      depth = DEFAULTS.DEPTH,
      lockTTL = DEFAULTS.LOCK_TTL,
      compactionLimit = DEFAULTS.COMPACTION_LIMIT,
      testHooks,
    }: FireProviderConfig = config;

    // P1.8 / P2.20 FIX: Validate path and config BEFORE any Firebase SDK calls
    // This ensures validation errors are thrown with clear messages before
    // getFirestore() which could fail with cryptic errors on invalid app.
    validateProviderConfig({ path, maxUpdatesThreshold, maxWaitTime, maxAggregationTime, depth });

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
    this.historyFoldThreshold = historyFoldThreshold;
    this.subdocLoadingMode = subdocLoadingMode;
    // Reuse a parent provider's measured clock offset (subdoc case): skew
    // is per-client, and measuring costs 3 Firestore ops per provider.
    this._cachedClockOffset = config.cachedClockOffset;
    this.lockTTL = lockTTL;
    this.compactionLimit = compactionLimit;
    this.persistence = config.persistence;
    this._testHooks = testHooks;
    // Bounded from construction on: the initial-sync reads overlap with
    // the load it waits for
    if (config.localReady) {
      this._localReady = this._boundLocalReady(config.localReady);
    }

    // The document knows which epoch it belongs to (stamped by squash);
    // fresh documents are epoch 0 until initial sync reports otherwise.
    this._epoch = readDocEpoch(this.doc);

    // Attach document event handlers
    this.doc.on('update', this.handleUpdate);
    this.doc.on('subdocs', this.handleSubdocs);

    // 'subdocs' fires only when a subdoc is integrated, so subdocs already
    // in the document (local-first content, or a provider recreated on the
    // same doc) would never get a provider. Treat them as just added; lazy
    // mode still skips those with shouldLoad === false.
    this.handleSubdocs({
      added: new Set(this.doc.getSubdocs()),
      removed: new Set(),
      loaded: new Set(),
    });

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
    return this._inflightCompaction !== null;
  }

  /**
   * Whether initial sync has completed and real-time listeners are active.
   * Also emitted as a 'sync' event when the state becomes true. Drops back
   * to false while recovering from a listener error.
   */
  get synced(): boolean {
    return this._synced;
  }

  /**
   * Manually trigger compaction.
   * Normally handled automatically when update threshold is exceeded.
   * Runs even while automatic compaction is backing off after a failure
   * (see the 'compaction-failed' event).
   * 
   * @param attempt - Internal retry counter (do not set manually)
   * @param minUpdates - Internal: smallest backlog worth compacting, set by
   *   the threshold trigger (do not set manually; 0 drains everything)
   * @throws {Error} If locking fails or Firestore operations error
   */
  compact(attempt: number = 1, minUpdates: number = 0): Promise<void> {
    return this._compact(attempt, false, minUpdates);
  }

  /**
   * Starts a compaction unless one (or a squash) is already in flight.
   *
   * @param beforeSquash - Run as squash()'s preparatory cycle (see
   *   CompactionContext.beforeSquash)
   * @param minUpdates - Smallest backlog worth compacting (see
   *   CompactionContext.minUpdates; 0 drains everything)
   */
  private _compact(attempt: number, beforeSquash: boolean, minUpdates: number = 0): Promise<void> {
    // Prevent concurrent compaction from same instance, and never start
    // while a squash holds the lock (see _inflightCompaction)
    if ((this._inflightCompaction || this._inflightSquash) && attempt === 1) {
      return Promise.resolve();
    }

    this._inflightCompaction = this._executeCompaction(attempt, beforeSquash, minUpdates).finally(() => {
      this._inflightCompaction = null;
    });
    return this._inflightCompaction;
  }

  /**
   * The clock offset lock decisions use (serverTime - clientTime).
   *
   * Measured on the first lock need rather than before initial sync, which
   * never uses it: the probe's write ack and read used to add 2 round trips
   * to every launch. The measurement is shared by every provider on the
   * same Firestore instance (see sharedClockOffset).
   */
  private async _clockOffset(): Promise<number> {
    if (this._cachedClockOffset === undefined) {
      this._cachedClockOffset = await sharedClockOffset(this.db, this.path, this.uid);
    }
    return this._cachedClockOffset;
  }

  /**
   * Waits for the clock offset before a lock, at most
   * CLOCK_SKEW_PROBE_TIMEOUT_MS.
   *
   * The probe's write resolves only once the server acknowledges it, so
   * offline the offset never arrives: compact() and squash() would stay
   * pending until the connection returns, then run long after their
   * caller gave up. They give up instead, as their lock transaction does
   * offline. The probe keeps running and caches the offset once
   * acknowledged, so a later lock need finds it.
   *
   * @returns Whether the offset is known.
   */
  private async _awaitClockOffset(): Promise<boolean> {
    let timerId: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<void>(resolve => {
      timerId = setTimeout(resolve, DEFAULTS.CLOCK_SKEW_PROBE_TIMEOUT_MS);
    });
    try {
      await Promise.race([this._clockOffset(), deadline]);
    } finally {
      clearTimeout(timerId);
    }
    if (this._cachedClockOffset === undefined) {
      console.warn(`Clock skew not measured after ${DEFAULTS.CLOCK_SKEW_PROBE_TIMEOUT_MS}ms (client offline?), skipping the lock`);
      return false;
    }
    return true;
  }

  /**
   * Compaction requested by the update listener's threshold trigger.
   *
   * Every online client's listener fires on the same crossing, and a
   * client whose lock attempt lands after the winner's finds only the
   * leftovers. Triggered cycles therefore act on at least half a
   * threshold's worth of updates, or when a fold is due; smaller
   * leftovers wait for the next crossing (see shouldDeferCompaction).
   */
  private _compactOnTrigger(): Promise<void> {
    return this.compact(1, Math.ceil(this.maxUpdatesThreshold / 2));
  }

  private async _executeCompaction(attempt: number, beforeSquash: boolean, minUpdates: number): Promise<void> {
    // acquireLock needs the measured offset: given none, it would measure
    // again on every call. Only the first compaction waits for it, before
    // the history listener is paused; offline, the cycle is skipped (see
    // _awaitClockOffset).
    if (this._cachedClockOffset === undefined) {
      if (!(await this._awaitClockOffset())) return;
      if (this._isDestroyed || this._epochFenced) return;
    }

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
      historyFoldThreshold: this.historyFoldThreshold,
      beforeSquash,
      minUpdates,
      deferredUpdateBlobs: this._deferredUpdateBlobs,
    };

    // FIX: Pause history listener during compaction to avoid contention/deadlock in emulator
    if (this._unsubscribeHistory) {
      this._unsubscribeHistory();
      this._unsubscribeHistory = null;
    }

    try {
      this._recordCompactionResult(await performTieredCompaction(ctx, attempt));
    } finally {
      // FIX: Resume history listener — unless the provider was epoch-fenced
      // meanwhile (see _stopSyncing): a resumed listener would keep reading
      // and, on the squasher, apply new-epoch segments onto the old doc.
      if (!this._isDestroyed && !this._epochFenced && !this._unsubscribeHistory) {
        // Use SyncContext to recreate listener
        // We need to re-construct SyncContext or store it.
        // Re-constructing is cheap.
        const syncCtx: SyncContext = {
          db: this.db,
          path: this.path,
          doc: this.doc,
          uid: this.uid,
          maxUpdatesThreshold: this.maxUpdatesThreshold,
          onCompactionNeeded: () => this._compactOnTrigger(),
          getCompactionBackoffUntil: () => this._compactionBackoffUntil,
          onCompactionProgress: () => this._endCompactionBackoff(),
          isDestroyed: () => this._isDestroyed,
          onListenerError: (error) => this._handleListenerError(error),
          storage: this.storage,
          corruptedDocIds: this._corruptedDocIds,
          onCorruptedDocument: (docId, error) => {
            this.emit('corrupted-document', [{ docId, error }]);
          },
          getEpoch: () => this._epoch,
          onEpochChanged: (serverEpoch) => this._handleEpochChanged(serverEpoch),
        };

        // We resume listening from the last known checkpoint.
        // If compaction created new segments, they will be picked up now.
        // If we are the ones who created them, we will assume them redundant (correct).
        this._unsubscribeHistory = createHistoryListener(syncCtx, this._lastHistoryDoc);
      }
    }
  }

  /**
   * Updates the automatic-compaction backoff from a finished compaction.
   *
   * Progress (anything compacted) ends the backoff. A failure counts only
   * when it will repeat (see isPersistentCompactionFailure); a lost lock,
   * a version race or a transport error leaves the backoff as it was. A
   * counted failure is surfaced: the document's backlog now grows until
   * its cause is fixed, and nothing else would tell the application.
   */
  private _recordCompactionResult(result: CompactionResult): void {
    if (result.success) {
      if (result.updatesCompacted > 0 || result.historySegmentsMerged > 0) {
        this._endCompactionBackoff();
      }
      return;
    }
    if (this._isDestroyed || !isPersistentCompactionFailure(result.error)) return;

    this._compactionFailures++;
    const retryInMs = compactionBackoffMs({
      failures: this._compactionFailures,
      baseMs: DEFAULTS.COMPACTION_TRIGGER_COOLDOWN_MS,
      maxMs: DEFAULTS.COMPACTION_FAILURE_BACKOFF_MAX_MS,
      random: Math.random(),
    });
    this._compactionBackoffUntil = Date.now() + retryInMs;
    console.warn(`Compaction failed ${this._compactionFailures} time(s) in a row; automatic compaction paused for ${Math.round(retryInMs / 1000)}s.`);

    this.emit('compaction-failed', [{
      error: result.error,
      consecutiveFailures: this._compactionFailures,
      retryInMs,
    }]);
  }

  /** Ends the compaction failure backoff (see _compactionFailures). */
  private _endCompactionBackoff(): void {
    this._compactionFailures = 0;
    this._compactionBackoffUntil = 0;
  }

  /**
   * The epoch this provider is currently syncing (0 until a squash has
   * ever happened on the document).
   */
  get epoch(): number {
    return this._epoch;
  }

  /**
   * Rebuilds the document into a brand-new epoch on the server — the
   * long-lived-document floor reset (see squash.ts for the full model).
   *
   * A garbage-collected snapshot still accretes tombstone structure,
   * delete-set ranges, and one state-vector entry per client that ever
   * wrote. Squashing clones the CONTENT into a fresh Yjs id space, so all
   * three reset to the live-content floor. In exchange, edits made
   * concurrently across the squash boundary cannot merge automatically:
   * other clients receive an 'epoch-changed' event carrying their local
   * state and must rebuild (see event docs). Use for single-user /
   * few-device documents; do not use for high-concurrency collaboration.
   *
   * Preconditions enforced here: initial sync completed, no subdocuments
   * (loaded or not), provider not destroyed. The server-side backlog
   * must fit one transaction — a compaction is run first to shrink it,
   * without the fold the squash snapshot would supersede.
   *
   * @returns The squash outcome; `skippedReason` distinguishes benign
   *          skips (lock contention, backlog, stale local doc, local
   *          edits during the squash) from errors.
   */
  async squash(): Promise<SquashResult> {
    const blocked = squashBlockedBy({
      isDestroyed: this._isDestroyed,
      synced: this._synced,
      epochFenced: this._epochFenced,
      subProviderCount: this.subProviders.size,
      subdocCount: this.doc.subdocs.size,
      depth: this.depth,
    });

    if (blocked?.kind === 'destroyed') {
      return { success: false, error: new Error('Provider is destroyed') };
    }
    if (blocked?.kind === 'local-behind') {
      return { success: false, skippedReason: 'local-behind' };
    }
    if (blocked?.kind === 'subdocs-unsupported') {
      return { success: false, error: new Error('squash() does not support subdocuments') };
    }

    // Both locks below need the measured offset. Awaited once, up front:
    // offline, the squash is reported as not done (see _awaitClockOffset)
    // instead of waiting for the probe in the compaction and again before
    // its own lock. The wait for in-flight operations below also stays the
    // last await before squashDocument, so no compaction starts between.
    if (this._cachedClockOffset === undefined && !(await this._awaitClockOffset())) {
      return {
        success: false,
        error: Object.assign(new Error('Clock skew not measured: client is offline'), { code: 'unavailable' }),
      };
    }

    // Compact the backlog first so the squash transaction stays within
    // Firestore's write budget. This also removes old-epoch documents and
    // drains update documents too old to carry redundancy metadata into a
    // segment that does — squash cannot verify either. A fold that is
    // merely due is skipped: the squash snapshot replaces it.
    await this._compact(1, true);

    // Flush our own pending updates so the clone reflects them.
    if (this._inflightSave) {
      try { await this._inflightSave; } catch { /* handled inside save */ }
    }
    if (this._pendingUpdates.length > 0) {
      await this.saveToFirestore();
    }

    // compact() returns at once while a compaction (or another squash) is
    // already running. Wait it out: squashDocument would re-enter its lock
    // and then release it while that operation still relies on it.
    while (this._inflightCompaction || this._inflightSquash) {
      try { await (this._inflightCompaction ?? this._inflightSquash); } catch { /* handled by its caller */ }
    }

    this._inflightSquash = squashDocument({
      db: this.db,
      path: this.path,
      uid: this.uid,
      lockTTL: this.lockTTL,
      cachedClockOffset: this._cachedClockOffset,
      storage: this.storage,
      isDestroyed: () => this._isDestroyed,
      doc: this.doc,
      deferredUpdateBlobs: this._deferredUpdateBlobs,
    }).finally(() => {
      this._inflightSquash = null;
    });
    const result = await this._inflightSquash;

    if (result.success && result.epoch !== undefined) {
      // The LIVE doc still carries the old epoch's structure, so this
      // provider must stop syncing NOW: saves tagged with the new epoch
      // would reference old-epoch struct ids no rebuilt client can
      // integrate, and incoming new-epoch updates cannot apply onto the
      // old doc. The application rebuilds the doc from the new snapshot
      // (e.g. versicle's staged swap + reload) and recreates providers.
      // A local edit that raced the commit itself is in the live doc but
      // not in the new epoch (and is never saved now), so the full local
      // state is surfaced exactly as 'epoch-changed' does.
      this._epoch = result.epoch;
      this._stopSyncing();

      let localState: Uint8Array | null = null;
      try {
        localState = Y.encodeStateAsUpdate(this.doc);
      } catch (e) {
        console.error('Failed to encode local state for squashed event', e);
      }

      this.emit('squashed', [{ epoch: result.epoch, localState }]);
    }
    return result;
  }

  /**
   * Handles discovery of a newer server epoch (someone squashed).
   *
   * Stops syncing and surfaces 'epoch-changed' with the full local state
   * so the application can (a) rebuild its local doc from the new epoch's
   * snapshot, and (b) decide whether anything from the old-epoch local
   * state needs semantic re-application (data written strictly before the
   * squash is already inside the new snapshot).
   */
  private _handleEpochChanged(serverEpoch: number): void {
    if (this._epochFenced || this._isDestroyed) return;
    this._stopSyncing();

    let localState: Uint8Array | null = null;
    try {
      localState = Y.encodeStateAsUpdate(this.doc);
    } catch (e) {
      console.error('Failed to encode local state for epoch-changed event', e);
    }

    this.emit('epoch-changed', [{
      previousEpoch: this._epoch,
      epoch: serverEpoch,
      localState,
    }]);
  }

  /**
   * Fences the provider after an epoch transition: stops all listeners,
   * cancels timers, and blocks future saves. The provider stays alive so
   * the application can read events/state, but no further data crosses
   * the epoch boundary in either direction until it rebuilds.
   */
  private _stopSyncing(): void {
    this._epochFenced = true;

    if (this._debounceTimerId) {
      clearTimeout(this._debounceTimerId);
      this._debounceTimerId = null;
    }
    if (this._syncRetryTimerId) {
      clearTimeout(this._syncRetryTimerId);
      this._syncRetryTimerId = null;
    }
    this._unsubscribers.forEach(unsub => unsub());
    this._unsubscribers = [];
    if (this._unsubscribeHistory) {
      this._unsubscribeHistory();
      this._unsubscribeHistory = null;
    }
  }

  /**
   * Destroys the provider and releases all resources.
   *
   * This method:
   * 1. Stops listening for remote updates
   * 2. Destroys all subdocument providers
   * 3. Flushes any pending local updates (waiting at most
   *    DESTROY_FLUSH_TIMEOUT_MS for Firestore to acknowledge them)
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

    // An initial sync waiting for local persistence returns now (it
    // re-checks isDestroyed) instead of when the timeout fires
    this._endLocalReadyWait?.();

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

    // No compaction follows to reclaim the blobs earlier ones left (see
    // _deferredUpdateBlobs): in the background, best effort
    // (deleteUpdateBlobs never rejects).
    void deleteUpdateBlobs(this.storage, this._deferredUpdateBlobs.splice(0));

    // Bound the waits below. A Firestore write resolves only when the
    // server acknowledges it, which never happens while offline (the SDK
    // keeps the write queued and sends it on reconnect). Online, resolving
    // still means committed; offline, give up waiting after the deadline
    // instead of hanging until the network returns. Subdocument providers
    // start their own deadlines in this same tick, so a whole tree of
    // providers settles within one timeout.
    let deadlineTimerId: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<void>(resolve => {
      deadlineTimerId = setTimeout(resolve, DEFAULTS.DESTROY_FLUSH_TIMEOUT_MS);
    });

    try {
      // Destroy all subdocument providers
      await destroyAllSubdocs(this.subProviders);

      // Wait out any in-flight save: updates that arrived while it was
      // running are sitting in _pendingUpdates and would otherwise be
      // silently dropped (the in-flight save won't reschedule once
      // _isDestroyed is set, and saveToFirestore() would have returned
      // the in-flight promise instead of flushing).
      if (this._inflightSave) {
        await Promise.race([
          this._inflightSave.catch(() => {
            // Failure already logged and handled inside the save
          }),
          deadline,
        ]);
      }

      // Flush pending updates (single best-effort attempt; retries are
      // suppressed after destroy). If the in-flight save is still
      // unacknowledged at the deadline, write them alongside it instead of
      // behind it, so they too reach the SDK's queue before destroy()
      // settles. The epoch fence is checked here because that path
      // bypasses saveToFirestore().
      if (this._pendingUpdates.length > 0 && !this._epochFenced) {
        await Promise.race([
          this._inflightSave ? this._executeSave() : this.saveToFirestore(),
          deadline,
        ]);
      }
    } finally {
      clearTimeout(deadlineTimerId);
    }

    super.destroy();
  }

  /**
   * Emits an event without letting a listener's exception escape.
   *
   * lib0's emit() calls listeners bare, so a throwing consumer handler
   * would unwind into whichever save or sync step fired the event: a
   * 'saved' listener throwing after a committed write was treated as a
   * failed write (re-queued and re-written forever, since each successful
   * attempt reset the retry counter), a throwing 'sync' listener re-ran
   * initial sync the same way, and a throwing 'save-rejected' listener
   * skipped the reschedule of buffered updates. Listener errors are
   * consumer bugs: log them and leave provider state alone.
   */
  emit(name: string, args: any[]): void {
    try {
      super.emit(name, args);
    } catch (err) {
      console.error(`Error in '${name}' event listener`, err);
    }
  }

  // --- Private Methods ---

  /**
   * Bounds the wait for local persistence (FireProviderConfig.localReady).
   * y-idb's whenSynced never rejects, and never settles if the persistence
   * is destroyed first, so the wait also ends after
   * LOCAL_READY_TIMEOUT_MS or on destroy. It always resolves: a failed or
   * timed-out load leaves sync exactly as it is without the option.
   */
  private _boundLocalReady(localReady: Promise<unknown>): Promise<void> {
    let timerId: ReturnType<typeof setTimeout> | undefined;
    return new Promise<void>(resolve => {
      this._endLocalReadyWait = resolve;
      timerId = setTimeout(() => {
        console.warn(`Local persistence not ready after ${DEFAULTS.LOCAL_READY_TIMEOUT_MS}ms, syncing without it`);
        resolve();
      }, DEFAULTS.LOCAL_READY_TIMEOUT_MS);
      Promise.resolve(localReady).then(() => resolve(), (err) => {
        console.warn("Local persistence failed to load, syncing without it:", err);
        resolve();
      });
    }).finally(() => {
      clearTimeout(timerId);
      this._endLocalReadyWait = null;
    });
  }

  /**
   * Performs initial synchronization and sets up real-time listener.
   * 
   * P0.7 NOTE: The sync algorithm uses eventual consistency.
   * Read order (Updates → History → Snapshot) ensures we never miss data,
   * though we may occasionally apply duplicates (Yjs handles this safely).
   */
  private async sync(): Promise<void> {
    let captured: SyncCapture | null = null;
    const syncCtx: SyncContext = {
      db: this.db,
      path: this.path,
      doc: this.doc,
      uid: this.uid,
      maxUpdatesThreshold: this.maxUpdatesThreshold,
      onCompactionNeeded: () => this._compactOnTrigger(),
      getCompactionBackoffUntil: () => this._compactionBackoffUntil,
      onCompactionProgress: () => this._endCompactionBackoff(),
      isDestroyed: () => this._isDestroyed,
      // FIX: Wire listener error to event emitter and re-sync
      onListenerError: (error) => this._handleListenerError(error),
      storage: this.storage,
      corruptedDocIds: this._corruptedDocIds,
      onCorruptedDocument: (docId, error) => {
        this.emit('corrupted-document', [{ docId, error }]);
      },
      getEpoch: () => this._epoch,
      onEpochChanged: (serverEpoch) => this._handleEpochChanged(serverEpoch),
      // Adopt the server epoch as soon as initial sync knows it (an empty
      // local doc bootstraps straight into whatever epoch the server is
      // at; the marker itself arrives inside the snapshot content). The
      // constructor's value can be stale — local persistence may hydrate
      // an epoch-N doc after construction — and saves that start while
      // the initial-sync push is in flight must carry the push's epoch.
      // A fenced provider keeps its epoch: a re-sync that read the main
      // document before this client's squash committed must not roll it
      // back to the old one.
      onEpochAdopted: (serverEpoch) => {
        if (!this._epochFenced) this._epoch = serverEpoch;
      },
      // Everything buffered so far is in the doc the push is computed from
      onLocalStateCaptured: () => {
        captured = { buffer: this._pendingUpdates, count: this._pendingUpdates.length };
      },
      localReady: this._localReady,
    };

    try {
      // Perform initial sync; due saves wait for it meanwhile
      this._initialSyncInFlight = true;
      let result: SyncResult;
      try {
        result = await performInitialSync(syncCtx);
      } finally {
        this._initialSyncInFlight = false;
      }
      if (result.success) {
        this._retireSyncedUpdates(captured);
      }
      // The listeners below keep syncCtx alive: do not pin the buffer
      captured = null;
      // Whatever the outcome, saves held back meanwhile resume
      if (this._pendingUpdates.length > 0) {
        this._scheduleSave();
      }
      // A re-sync (e.g. after a listener error) can still be running when
      // the provider is epoch-fenced (see _stopSyncing): attaching its
      // listeners would undo the fence.
      if (this._isDestroyed || this._epochFenced) return;

      // The server was squashed past this document's history — do not
      // retry (the state cannot converge); surface the event and stop.
      if (result.epochConflict) {
        this._handleEpochChanged(result.epochConflict.serverEpoch);
        return;
      }

      // performInitialSync reports failures via its result rather than
      // throwing — route them into the retry/backoff path below, otherwise
      // a failed sync would be silently treated as success (no retry, no
      // sync-failure event, local changes never pushed).
      if (!result.success) {
        throw result.error ?? new Error("Initial sync failed");
      }

      // Reset retry count on successful sync
      this._syncRetryCount = 0;
      this._offlineRetryCount = 0;

      // Cleanup any previous listeners
      this._unsubscribers.forEach(unsub => unsub());
      this._unsubscribers = [];

      if (this._unsubscribeHistory) {
        this._unsubscribeHistory();
        this._unsubscribeHistory = null;
      }

      // Setup real-time listeners (Updates, Snapshot, and History)
      // Pass cursor to prevent sync gaps, plus the number of update
      // documents behind it so they still count toward compaction; pass
      // the processed snapshot version so the listener's attach-delivery
      // is skipped instead of re-applying the delete-set fingerprint on
      // every (re)connect.
      this._unsubscribers.push(createUpdateListener(syncCtx, result.lastSyncedDoc, result.syncedUpdateCount));
      this._unsubscribers.push(createSnapshotListener(syncCtx, result.snapshotVersion));

      // Store history listener separately so it can be paused during compaction
      this._lastHistoryDoc = result.lastHistoryDoc;
      this._unsubscribeHistory = createHistoryListener(syncCtx, result.lastHistoryDoc);

      this._listenersAttachedAt = Date.now();

      // Initial sync complete and listeners attached. The 'sync' event name
      // follows the y-fire / y-* provider convention (y-websocket, y-indexeddb)
      // so consumers can treat this provider as a drop-in.
      this._synced = true;
      this.emit('sync', [true]);

    } catch (err) {
      // No retry once fenced either: _stopSyncing already ran and could
      // not cancel a retry scheduled after it.
      if (isClientOfflineError(err)) {
        if (!this._isDestroyed && !this._epochFenced) this._retrySyncWhenOnline();
        return;
      }

      console.error("Sync failed", err);

      // Circuit breaker - stop retrying after MAX_RETRIES
      if (!this._isDestroyed && !this._epochFenced) {
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
   * Drops the buffered local updates a successful initial sync covered.
   *
   * handleUpdate only buffers updates the doc already holds, so every
   * entry buffered when initial sync read the local doc is on the server
   * once the sync succeeds: already, or inside its push. Saving them again
   * would upload them twice; on a cold start where local persistence
   * hydrated after construction, that is the whole document. Entries
   * buffered later stay queued.
   *
   * A save that started since then took the buffer with it (the array was
   * replaced), so nothing is dropped and the worst case is that save's
   * duplicate. A save that started before may fail and put its batch back
   * at the front, but that batch is inside the push as well: initial sync
   * does not count a write the server has not acknowledged as server data.
   *
   * @param captured - The buffer and its length when the doc was read
   */
  private _retireSyncedUpdates(captured: SyncCapture | null): void {
    if (!captured || captured.count === 0 || captured.buffer !== this._pendingUpdates) return;

    this._pendingUpdates.splice(0, captured.count);
    if (this._pendingUpdates.length === 0) {
      this._pendingSince = null;
      if (this._debounceTimerId) {
        clearTimeout(this._debounceTimerId);
        this._debounceTimerId = null;
      }
    }

    // They are committed now: report them like a committed save
    this.emit('saved', [Date.now()]);
  }

  /**
   * Retries an initial sync that found the client offline (its reads were
   * served from the local cache), without spending the MAX_RETRIES budget:
   * being offline is not a failure, and an app launched offline must sync
   * once the connection returns.
   *
   * While the clock offset is still unmeasured, the retry waits for the
   * clock-skew probe: its write resolves only once the server acknowledges
   * it, so the retry runs as soon as the client is back online (this was
   * initial sync's only connectivity gate when it measured up front), and
   * the first lock needs the offset anyway. Afterwards, retries back off,
   * capped at OFFLINE_SYNC_RETRY_MAX_MS.
   */
  private _retrySyncWhenOnline(): void {
    if (this._cachedClockOffset === undefined) {
      this._clockOffset().then(() => {
        if (!this._isDestroyed && !this._epochFenced) this.sync();
      });
      return;
    }

    this._offlineRetryCount++;
    const backoffMs = Math.min(calculateBackoff(this._offlineRetryCount), DEFAULTS.OFFLINE_SYNC_RETRY_MAX_MS);
    console.log(`Client offline, retrying sync in ${Math.round(backoffMs)}ms...`);

    if (this._syncRetryTimerId) {
      clearTimeout(this._syncRetryTimerId);
    }

    this._syncRetryTimerId = setTimeout(() => {
      this._syncRetryTimerId = null;
      if (!this._isDestroyed) this.sync();
    }, backoffMs);
  }

  /**
   * Handles a real-time listener error.
   *
   * Firestore terminates an onSnapshot listener for good once its error
   * callback fires (e.g. permission-denied while auth is briefly
   * invalid). Keeping the dead handles left the provider deaf while it
   * still reported `synced` and kept uploading its own edits. Instead,
   * drop `synced`, detach every listener and re-run sync() with backoff:
   * it catches up on whatever was missed and attaches fresh listeners.
   */
  private _handleListenerError(error: Error): void {
    console.error('Listener error:', error);
    this.emit('connection-error', [{ code: 'listener-error', message: error.message, error }]);

    // The listeners usually fail together: the first error starts the
    // recovery, and a re-sync already pending replaces them all.
    if (!this._synced || this._isDestroyed || this._epochFenced) return;
    this._synced = false;

    this._unsubscribers.forEach(unsub => unsub());
    this._unsubscribers = [];
    if (this._unsubscribeHistory) {
      this._unsubscribeHistory();
      this._unsubscribeHistory = null;
    }

    const recovery = planListenerRecovery({
      retryCount: this._listenerRetryCount,
      attachedAt: this._listenersAttachedAt,
      now: Date.now(),
      healthyMs: DEFAULTS.LISTENER_HEALTHY_MS,
      maxRetries: DEFAULTS.MAX_RETRIES,
    });
    this._listenerRetryCount = recovery.retryCount;

    if (recovery.giveUp) {
      console.error(`Listeners failed ${recovery.retryCount} times in a row, giving up.`);
      this.emit('sync-failure', [new Error(`Listeners failed ${recovery.retryCount} times in a row`)]);
      return;
    }

    const backoffMs = calculateBackoff(recovery.retryCount);
    console.log(`Re-syncing in ${backoffMs}ms after listener error (attempt ${recovery.retryCount}/${DEFAULTS.MAX_RETRIES})...`);

    this._syncRetryTimerId = setTimeout(() => {
      this._syncRetryTimerId = null;
      if (!this._isDestroyed) this.sync();
    }, backoffMs);
  }

  /**
   * Handles local document updates.
   * Batches updates and triggers debounced save to Firestore.
   */
  private handleUpdate = (update: Uint8Array, origin: unknown): void => {
    // Prevent echo loops from remote updates
    if (isRemoteOrigin(origin)) {
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
      maxAggregationTime: this.maxAggregationTime,
      gcCompaction: this.gcCompaction,
      historyFoldThreshold: this.historyFoldThreshold,
      lockTTL: this.lockTTL,
      compactionLimit: this.compactionLimit,
      persistence: this.persistence,
      // Undefined until the parent's first lock need: the child then
      // awaits the measurement shared per Firestore instance itself.
      cachedClockOffset: this._cachedClockOffset,
      subdocLoadingMode: this.subdocLoadingMode,
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
   * While initial sync runs, a due save waits for it, up to that same cap:
   * its push covers what is buffered, and it is rescheduled when the sync
   * settles (see initialSyncSaveHold).
   *
   * @param delayMs - Explicit delay before saving (used by failure retries
   *                  with exponential backoff, exempt from the aggregation
   *                  cap, and by the initial-sync hold, which ends at it).
   *                  When omitted, the debounce window applies.
   */
  private _scheduleSave(delayMs?: number): void {
    if (this._isDestroyed) return;

    const delay = computeSaveDelay({
      explicitDelayMs: delayMs,
      maxWaitTime: this.maxWaitTime,
      maxAggregationTime: this.maxAggregationTime,
      pendingSince: this._pendingSince,
      now: Date.now(),
    });

    if (this._debounceTimerId) {
      clearTimeout(this._debounceTimerId);
    }
    this._debounceTimerId = setTimeout(() => {
      this._debounceTimerId = null;
      if (this._isDestroyed) return;

      const holdMs = initialSyncSaveHold({
        syncInFlight: this._initialSyncInFlight,
        maxAggregationTime: this.maxAggregationTime,
        pendingSince: this._pendingSince,
        now: Date.now(),
      });
      if (holdMs > 0) {
        this._scheduleSave(holdMs);
        return;
      }
      this.saveToFirestore();
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
   * then emit 'save-rejected' after MAX_SAVE_RETRIES attempts. A rejected
   * batch is never dropped: it stays queued ahead of newer updates, since
   * later batches cannot integrate on peers without its clock range.
   */
  private saveToFirestore(): Promise<void> {
    if (this._inflightSave) return this._inflightSave;
    if (this._pendingUpdates.length === 0) return Promise.resolve();
    // Epoch fence: after a squash, old-epoch updates would be ignored by
    // every other client anyway. The buffered updates stay in memory and
    // are surfaced through the 'epoch-changed' / 'squashed' payload.
    if (this._epochFenced) return Promise.resolve();

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
      // Squashed documents fence updates by epoch; omit for epoch 0 so
      // never-squashed documents keep their historical schema.
      ...(this._epoch > 0 ? { epoch: this._epoch } : {}),
      ...aggregateClockEnds(clockEnds),
      // Readers may already hold every struct (from the initial-sync push,
      // when this batch was taken while that push was unacknowledged); the
      // flag keeps them from skipping its deletions as redundant.
      ...(updateEndsWithDeletions(update) ? { hasDeletions: true } : {}),
    };

    try {
      if (update.byteLength > DEFAULTS.INLINE_UPDATE_LIMIT) {
        // Storage-backed update: upload binary to Cloud Storage and write
        // a lightweight pointer document to the updates collection
        const storagePath = largeUpdatePath(this.path, this.uid, Date.now(), generateSessionId());
        await uploadBlob(this.storage, storagePath, update);
        try {
          await addDoc(collection(this.db, this.path, FIRESTORE_PATHS.UPDATES), {
            ...baseData,
            updateStoragePath: storagePath,
          });
        } catch (pointerErr) {
          // Committed by an earlier send whose ack was lost: the pointer
          // is live, so the save succeeded (see isLostAckCommit).
          if (!isLostAckCommit(pointerErr)) {
            // Any other rejected write never commits (the SDK retries
            // transient errors itself), so no pointer to this blob exists
            // or ever will, and the retry uploads the batch again under a
            // new path. Delete this copy instead of orphaning one per
            // failed attempt (in the background: the retry need not wait
            // for it).
            deleteObject(ref(this.storage, storagePath)).catch(err => {
              console.warn(`Failed to delete unreferenced update blob ${storagePath}`, err);
            });
            throw pointerErr;
          }
        }
        // A re-send may have re-created a pointer whose blob a compaction
        // already reclaimed
        await restoreMissingBlob(this.storage, storagePath, update);
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

      // Recovery: put the failed batch back ahead of any updates that
      // arrived during the attempt — on every path, including the
      // terminal ones below. Dropping it would leave a hole in this
      // client's clock range: every later batch starts past it, so no
      // peer could ever integrate them (they would sit in pendingStructs
      // even though 'saved' fired for them).
      const hasNewerUpdates = this._pendingUpdates.length > 0;
      this._pendingUpdates.unshift(update);
      // Restart the aggregation clock: retries pace themselves via
      // explicit backoff, the cap only guards the debounce path
      if (this._pendingSince === null) {
        this._pendingSince = Date.now();
      }

      // Detect Firestore size-limit error (server-side rejection of an
      // inline write; rare now that oversized updates are offloaded with
      // headroom below the limit)
      const isDocTooLarge =
        err?.code === 'invalid-argument' ||
        err?.message?.includes('exceeds the maximum') ||
        err?.message?.includes('too large');

      if (isDocTooLarge) {
        // Terminal: the data will never fit as is, do not retry on a
        // timer. The batch stays queued and goes out merged with the next
        // save (newer updates or the destroy() flush).
        this.emit('save-rejected', [{
          code: 'document-too-large' as const,
          sizeBytes: update.byteLength,
          limitBytes: DEFAULTS.FIRESTORE_DOC_LIMIT,
          error: err instanceof Error ? err : new Error(String(err)),
          update,
        }]);
        if (hasNewerUpdates) {
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
        // Stop retrying on a timer; the queued batch goes out with the
        // next save (newer updates or the destroy() flush)
        if (hasNewerUpdates) {
          this._scheduleSave();
        }
        return;
      }

      // Retry with exponential backoff
      if (!this._isDestroyed) {
        this._scheduleSave(calculateBackoff(this._saveRetryCount));
      }
    }
  }
}
