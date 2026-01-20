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
import { getFirestore, collection, addDoc, Bytes, serverTimestamp, } from "@firebase/firestore";
import * as Y from "yjs";
import { ObservableV2 } from "lib0/observable";
// Module imports
import { DEFAULTS, FIREBASE_ORIGINS, FIRESTORE_PATHS, } from "./types";
import { generateSessionId, calculateBackoff } from "./utils";
import { extractAllMetadata, aggregateMetadata } from "./update-metadata";
import { performInitialSync, createUpdateListener } from "./sync";
import { compact } from "./compaction";
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
        super();
        /** Map of subdocument providers */
        this.subProviders = new Map();
        /** Whether compaction is currently in progress */
        this._isCompacting = false;
        /** Pending update cache for debouncing */
        this.updateCache = null;
        // State
        this._unsubscribeUpdates = null;
        this._isDestroyed = false;
        /** P0.3 FIX: Cached clock offset to avoid measuring on every lock attempt */
        this._cachedClockOffset = undefined;
        /** P0.5 FIX: Flag to prevent race condition during save */
        this._isSaving = false;
        /** P1.4 FIX: Sync retry counter for exponential backoff */
        this._syncRetryCount = 0;
        /** P1.5 FIX: Debounce timer ID for cancellation on destroy */
        this._debounceTimerId = null;
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
        // Initialize from config
        const { firebaseApp, ydoc, path, maxUpdatesThreshold = DEFAULTS.MAX_UPDATES_THRESHOLD, maxWaitTime = DEFAULTS.MAX_WAIT_TIME, compactionProbability = DEFAULTS.COMPACTION_PROBABILITY, depth = DEFAULTS.DEPTH, lockTTL = DEFAULTS.LOCK_TTL, compactionLimit = DEFAULTS.COMPACTION_LIMIT, testHooks, } = config;
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
            const ctx = {
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
            yield compact(ctx, attempt);
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
            // Stop listening
            if (this._unsubscribeUpdates) {
                this._unsubscribeUpdates();
                this._unsubscribeUpdates = null;
            }
            // Remove document event handlers
            this.doc.off('update', this.handleUpdate);
            this.doc.off('subdocs', this.handleSubdocs);
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
                compactionProbability: this.compactionProbability,
                onCompactionNeeded: () => this.compact(),
                isDestroyed: () => this._isDestroyed,
            };
            try {
                // Perform initial sync
                yield performInitialSync(syncCtx);
                if (this._isDestroyed)
                    return;
                // Cleanup any previous listener
                if (this._unsubscribeUpdates) {
                    this._unsubscribeUpdates();
                }
                // Setup real-time listener
                this._unsubscribeUpdates = createUpdateListener(syncCtx);
            }
            catch (err) {
                console.error("Sync failed", err);
                // P1.4 FIX: Exponential backoff instead of fixed retry
                if (!this._isDestroyed) {
                    this._syncRetryCount++;
                    const backoffMs = calculateBackoff(this._syncRetryCount);
                    console.log(`Retrying sync in ${backoffMs}ms (attempt ${this._syncRetryCount})...`);
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
     */
    saveToFirestore() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.updateCache || this._isSaving)
                return;
            this._isSaving = true;
            // Take the current cache for this save operation
            const update = this.updateCache;
            this.updateCache = null;
            const metas = extractAllMetadata(update);
            const docData = Object.assign({ update: Bytes.fromUint8Array(update), createdAt: serverTimestamp(), createdBy: this.uid }, aggregateMetadata(metas));
            try {
                yield addDoc(collection(this.db, this.path, FIRESTORE_PATHS.UPDATES), docData);
                // P0.5 FIX: Check if new updates arrived during save
                // If so, schedule another save
                if (this.updateCache) {
                    this._debouncedSave();
                }
            }
            catch (err) {
                console.error("Failed to save update to Firestore", err);
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
