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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
import { getFirestore, initializeFirestore, persistentLocalCache, collection, addDoc, Bytes, serverTimestamp, } from "@firebase/firestore";
import { getStorage } from "@firebase/storage";
import * as Y from "yjs";
import { ObservableV2 } from "lib0/observable";
// Module imports
import { DEFAULTS, FIREBASE_ORIGINS, FIRESTORE_PATHS, } from "./types";
import { generateSessionId, calculateBackoff } from "./utils";
import { extractAllMetadata, aggregateMetadata } from "./update-metadata";
import { performInitialSync, createUpdateListener, createSnapshotListener, createHistoryListener } from "./sync";
import { compact as performTieredCompaction } from "./compaction";
import { measureClockSkew } from "./locking";
import { handleSubdocs as handleSubdocsEvent, destroyAllSubdocs, } from "./subdocs";
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
export class FireProvider extends ObservableV2 {
    /**
     * Creates a new FireProvider instance.
     *
     * @param config - Configuration options
     * @throws {Error} If config parameters (path, depth, maxUpdatesThreshold) are invalid.
     */
    constructor(config) {
        var _a;
        super();
        /** Map of subdocument providers */
        this.subProviders = new Map();
        /** Whether compaction is currently in progress */
        this._isCompacting = false;
        /** Pending update cache for debouncing */
        this.updateCache = null;
        // State
        // FIX: Manage multiple listeners (updates, history, snapshot)
        this._unsubscribers = [];
        // P1.9 FIX: Store history listener separately to pause during compaction
        this._unsubscribeHistory = null;
        this._lastHistoryDoc = null; // QueryDocumentSnapshot
        this._isDestroyed = false;
        /** P0.3 FIX: Cached clock offset to avoid measuring on every lock attempt */
        this._cachedClockOffset = undefined;
        /** P0.5 FIX: Flag to prevent race condition during save */
        this._isSaving = false;
        /** Consecutive save failure counter for circuit breaker */
        this._saveRetryCount = 0;
        /** P1.4 FIX: Sync retry counter for exponential backoff */
        this._syncRetryCount = 0;
        /** P1.5 FIX: Debounce timer ID for cancellation on destroy */
        this._debounceTimerId = null;
        this._boundBeforeUnload = null;
        /** Per-session quarantine set for corrupted Firestore documents */
        this._corruptedDocIds = new Set();
        /**
         * Handles local document updates.
         * Batches updates and triggers debounced save to Firestore.
         */
        this.handleUpdate = (update, origin) => {
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
        this.handleSubdocs = (event) => {
            const ctx = {
                firebaseApp: this.firebaseApp,
                parentPath: this.path,
                depth: this.depth,
                maxUpdatesThreshold: this.maxUpdatesThreshold,
                maxWaitTime: this.maxWaitTime,
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
        this.handleBeforeUnload = () => {
            if (!this.updateCache || this._isDestroyed)
                return;
            // Cancel any pending debounce - we're saving now
            if (this._debounceTimerId) {
                clearTimeout(this._debounceTimerId);
                this._debounceTimerId = null;
            }
            // Attempt synchronous save via sendBeacon
            // Note: Firestore SDK doesn't support sendBeacon, so we send to a minimal endpoint
            // that Firestore rules can process. In practice, you'd need a Cloud Function endpoint.
            // 
            // For now, we trigger saveToFirestore() which starts the async operation.
            // The browser may or may not complete it depending on timing.
            // This is still better than not trying at all.
            // Start the save operation - browser gives us a small window
            this.saveToFirestore().catch(err => {
                console.warn('Best-effort save on unload failed:', err);
            });
            // Note: For guaranteed delivery, implement a Cloud Function endpoint
            // that accepts navigator.sendBeacon data and writes to Firestore.
        };
        // Initialize from config
        const { firebaseApp, ydoc, path, maxUpdatesThreshold = DEFAULTS.MAX_UPDATES_THRESHOLD, maxWaitTime = DEFAULTS.MAX_WAIT_TIME, depth = DEFAULTS.DEPTH, lockTTL = DEFAULTS.LOCK_TTL, compactionLimit = DEFAULTS.COMPACTION_LIMIT, testHooks, } = config;
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
        this.storage = getStorage(firebaseApp);
        // Check if offline persistence is enabled
        if ((_a = config.persistence) === null || _a === void 0 ? void 0 : _a.enabled) {
            try {
                this.db = initializeFirestore(firebaseApp, {
                    localCache: persistentLocalCache({})
                });
            }
            catch (err) {
                if (err.code === 'failed-precondition') {
                    // Firestore has already been initialized in another tab/instance
                    this.db = getFirestore(firebaseApp);
                }
                else {
                    throw err;
                }
            }
        }
        else {
            this.db = getFirestore(firebaseApp);
        }
        this.path = path;
        this.doc = ydoc;
        this.uid = generateSessionId();
        this.depth = depth;
        this.maxUpdatesThreshold = maxUpdatesThreshold;
        this.maxWaitTime = maxWaitTime;
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
    get isCompacting() {
        return this._isCompacting;
    }
    /**
     * Manually trigger compaction.
     * Normally handled automatically when update threshold is exceeded.
     *
     * @param attempt - Internal retry counter (do not set manually)
     * @throws {Error} If locking fails or Firestore operations error
     */
    compact(attempt = 1) {
        return __awaiter(this, void 0, void 0, function* () {
            // Prevent concurrent compaction from same instance
            if (this._isCompacting && attempt === 1)
                return;
            this._isCompacting = true;
            const ctx = {
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
            };
            // FIX: Pause history listener during compaction to avoid contention/deadlock in emulator
            if (this._unsubscribeHistory) {
                this._unsubscribeHistory();
                this._unsubscribeHistory = null;
            }
            try {
                yield performTieredCompaction(ctx, attempt);
            }
            finally {
                this._isCompacting = false;
                // FIX: Resume history listener
                if (!this._isDestroyed && !this._unsubscribeHistory) {
                    // Use SyncContext to recreate listener
                    // We need to re-construct SyncContext or store it.
                    // Re-constructing is cheap.
                    const syncCtx = {
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
        });
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
    destroy() {
        const _super = Object.create(null, {
            destroy: { get: () => super.destroy }
        });
        return __awaiter(this, void 0, void 0, function* () {
            this._isDestroyed = true;
            // P1.5 FIX: Cancel pending debounce timer
            if (this._debounceTimerId) {
                clearTimeout(this._debounceTimerId);
                this._debounceTimerId = null;
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
            yield destroyAllSubdocs(this.subProviders);
            // Flush pending updates
            if (this.updateCache) {
                yield this.saveToFirestore();
            }
            _super.destroy.call(this);
        });
    }
    // --- Private Methods ---
    /**
     * Performs initial synchronization and sets up real-time listener.
     *
     * P0.7 NOTE: The sync algorithm uses eventual consistency.
     * Read order (Updates → History → Snapshot) ensures we never miss data,
     * though we may occasionally apply duplicates (Yjs handles this safely).
     */
    sync() {
        return __awaiter(this, void 0, void 0, function* () {
            // P0.3 FIX: Measure clock offset once per session and cache it
            // This avoids 3 Firestore ops on every lock attempt
            if (this._cachedClockOffset === undefined) {
                try {
                    this._cachedClockOffset = yield measureClockSkew(this.db, this.path, this.uid);
                    console.log(`Clock offset measured: ${this._cachedClockOffset}ms`);
                }
                catch (e) {
                    console.warn("Failed to measure clock skew, using 0:", e);
                    this._cachedClockOffset = 0;
                }
            }
            const syncCtx = {
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
                const result = yield performInitialSync(syncCtx);
                if (this._isDestroyed)
                    return;
                // Reset retry count on successful sync
                this._syncRetryCount = 0;
                // Cleanup any previous listener
                this._unsubscribers.forEach(unsub => unsub());
                this._unsubscribers = [];
                // Setup real-time listeners
                // P1.9 FIX: Pass cursor to prevent gap
                this._unsubscribers.push(createUpdateListener(syncCtx, result.lastSyncedDoc));
                // FIX: Add Snapshot and History listeners for full synchronization
                this._unsubscribers.push(createSnapshotListener(syncCtx));
                // FIX: Store history listener separately
                this._lastHistoryDoc = result.lastHistoryDoc;
                this._unsubscribeHistory = createHistoryListener(syncCtx, result.lastHistoryDoc);
            }
            catch (err) {
                console.error("Sync failed", err);
                // FIX: Circuit breaker - stop retrying after MAX_RETRIES
                if (!this._isDestroyed) {
                    this._syncRetryCount++;
                    if (this._syncRetryCount >= DEFAULTS.MAX_RETRIES) {
                        console.error(`Sync failed after ${DEFAULTS.MAX_RETRIES} attempts, giving up.`);
                        this.emit('sync-failure', [new Error(`Sync failed after ${DEFAULTS.MAX_RETRIES} attempts`)]);
                        return;
                    }
                    const backoffMs = calculateBackoff(this._syncRetryCount);
                    console.log(`Retrying sync in ${backoffMs}ms (attempt ${this._syncRetryCount}/${DEFAULTS.MAX_RETRIES})...`);
                    setTimeout(() => {
                        if (!this._isDestroyed)
                            this.sync();
                    }, backoffMs);
                }
            }
        });
    }
    /**
     * Saves the cached update to Firestore.
     * P0.5 FIX: Uses _isSaving flag to prevent race condition where
     * updates arriving during save could be duplicated or lost.
     *
     * Circuit breaker: Detects oversized documents and generic persistent
     * failures. Emits 'save-rejected' event instead of retrying forever.
     */
    saveToFirestore() {
        var _a, _b;
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.updateCache || this._isSaving)
                return;
            this._isSaving = true;
            // Take the current cache for this save operation
            const update = this.updateCache;
            this.updateCache = null;
            // Proactive size check: reject before even attempting the write
            if (update.byteLength > DEFAULTS.FIRESTORE_DOC_LIMIT) {
                this._isSaving = false;
                console.error(`Update rejected: ${update.byteLength} bytes exceeds Firestore limit of ${DEFAULTS.FIRESTORE_DOC_LIMIT} bytes`);
                this.emit('save-rejected', [{
                        code: 'document-too-large',
                        sizeBytes: update.byteLength,
                        limitBytes: DEFAULTS.FIRESTORE_DOC_LIMIT,
                        error: new Error(`Update size (${update.byteLength} bytes) exceeds Firestore document limit (${DEFAULTS.FIRESTORE_DOC_LIMIT} bytes)`),
                        update,
                    }]);
                return;
            }
            const metas = extractAllMetadata(update);
            const docData = Object.assign({ update: Bytes.fromUint8Array(update), createdAt: serverTimestamp(), createdBy: this.uid }, aggregateMetadata(metas));
            try {
                yield addDoc(collection(this.db, this.path, FIRESTORE_PATHS.UPDATES), docData);
                // Reset retry counter on success
                this._saveRetryCount = 0;
                // P0.5 FIX: Check if new updates arrived during save
                // If so, schedule another save
                if (this.updateCache) {
                    this._debouncedSave();
                }
            }
            catch (err) {
                console.error("Failed to save update to Firestore", err);
                // Detect Firestore size-limit error (server-side rejection)
                const isDocTooLarge = (err === null || err === void 0 ? void 0 : err.code) === 'invalid-argument' ||
                    ((_a = err === null || err === void 0 ? void 0 : err.message) === null || _a === void 0 ? void 0 : _a.includes('exceeds the maximum')) ||
                    ((_b = err === null || err === void 0 ? void 0 : err.message) === null || _b === void 0 ? void 0 : _b.includes('too large'));
                if (isDocTooLarge) {
                    // Terminal: the data will never fit, do not retry
                    this.emit('save-rejected', [{
                            code: 'document-too-large',
                            sizeBytes: update.byteLength,
                            limitBytes: DEFAULTS.FIRESTORE_DOC_LIMIT,
                            error: err instanceof Error ? err : new Error(String(err)),
                            update,
                        }]);
                    if (this.updateCache) {
                        this._debouncedSave();
                    }
                    return;
                }
                // Generic failure: apply retry cap
                this._saveRetryCount++;
                if (this._saveRetryCount >= DEFAULTS.MAX_SAVE_RETRIES) {
                    console.error(`Save failed after ${this._saveRetryCount} consecutive attempts, giving up.`);
                    this.emit('save-rejected', [{
                            code: 'max-retries-exceeded',
                            retries: this._saveRetryCount,
                            error: err instanceof Error ? err : new Error(String(err)),
                            update,
                        }]);
                    this._saveRetryCount = 0;
                    if (this.updateCache) {
                        this._debouncedSave();
                    }
                    return;
                }
                // Recovery: Merge back the update we failed to save
                // with any new updates that arrived during the attempt
                if (this.updateCache) {
                    this.updateCache = Y.mergeUpdates([update, this.updateCache]);
                }
                else {
                    this.updateCache = update;
                }
                // Retry
                this._debouncedSave();
            }
            finally {
                this._isSaving = false;
            }
        });
    }
}
