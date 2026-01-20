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
import { debounce, generateSessionId } from "./utils";
import { extractAllMetadata, aggregateMetadata } from "./update-metadata";
import { performInitialSync, createUpdateListener } from "./sync";
import { compact } from "./compaction";
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
    get isCompacting() {
        return this._isCompacting;
    }
    /**
     * Manually trigger compaction.
     * Normally handled automatically when update threshold is exceeded.
     *
     * @param attempt - Internal retry counter (do not set manually)
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
     */
    destroy() {
        const _super = Object.create(null, {
            destroy: { get: () => super.destroy }
        });
        return __awaiter(this, void 0, void 0, function* () {
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
     */
    sync() {
        return __awaiter(this, void 0, void 0, function* () {
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
                // Retry after delay
                if (!this._isDestroyed) {
                    console.log("Retrying sync in 5 seconds...");
                    setTimeout(() => {
                        if (!this._isDestroyed)
                            this.sync();
                    }, 5000);
                }
            }
        });
    }
    /**
     * Saves the cached update to Firestore.
     */
    saveToFirestore() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.updateCache)
                return;
            const update = this.updateCache;
            this.updateCache = null;
            const metas = extractAllMetadata(update);
            const docData = Object.assign({ update: Bytes.fromUint8Array(update), createdAt: serverTimestamp(), createdBy: this.uid }, aggregateMetadata(metas));
            try {
                yield addDoc(collection(this.db, this.path, FIRESTORE_PATHS.UPDATES), docData);
            }
            catch (err) {
                console.error("Failed to save update to Firestore", err);
                // Recovery: Put back the updates we failed to save
                if (this.updateCache) {
                    this.updateCache = Y.mergeUpdates([update, this.updateCache]);
                }
                else {
                    this.updateCache = update;
                }
                // Retry
                this._debouncedSave();
            }
        });
    }
}
