var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
import { getFirestore, onSnapshot, doc, collection, addDoc, Bytes, runTransaction, query, orderBy, getDocs, getDoc, serverTimestamp } from "@firebase/firestore";
import * as Y from "yjs";
import { ObservableV2 } from "lib0/observable";
export class FireProvider extends ObservableV2 {
    constructor({ firebaseApp, ydoc, path, maxUpdatesThreshold = 50, maxWaitTime = 500, compactionProbability = 0.01, depth = 0, }) {
        super();
        // Recursion
        this.subProviders = new Map();
        // Compaction State
        this.isCompacting = false;
        // Debounce Cache
        this.updateCache = null;
        // Configuration
        this.maxUpdatesThreshold = 50;
        this.maxWaitTime = 500;
        this.compactionProbability = 0.01;
        this._unsubscribeUpdates = null;
        this._isDestroyed = false;
        this.handleUpdate = (update, origin) => {
            // Prevent loops
            if (origin === 'origin:firebase/snapshot' ||
                origin === 'origin:firebase/history' ||
                origin === 'origin:firebase/update') {
                return;
            }
            // Merge into cache
            this.updateCache = this.updateCache ? Y.mergeUpdates([this.updateCache, update]) : update;
            // Trigger Debounced Write
            this._debouncedSave();
        };
        this.handleSubdocs = ({ added, removed, loaded }) => {
            added.forEach(subdoc => {
                this.startSubdocProvider(subdoc);
            });
            loaded.forEach(subdoc => {
                this.startSubdocProvider(subdoc);
            });
            removed.forEach(subdoc => {
                const guid = subdoc.guid;
                const provider = this.subProviders.get(guid);
                if (provider) {
                    provider.destroy();
                    this.subProviders.delete(guid);
                }
            });
        };
        this.firebaseApp = firebaseApp;
        this.db = getFirestore(firebaseApp);
        this.doc = ydoc;
        this.path = path;
        this.maxUpdatesThreshold = maxUpdatesThreshold;
        this.maxWaitTime = maxWaitTime;
        this.compactionProbability = compactionProbability;
        this.depth = depth;
        // Generate a unique ID for this session/provider instance
        this.uid = Math.random().toString(36).substring(2) + Date.now().toString(36);
        // Setup Debounced Save
        this._debouncedSave = this.debounce(this.saveToFirestore.bind(this), this.maxWaitTime);
        // Start Lifecycle
        this.doc.on('update', this.handleUpdate);
        this.doc.on('subdocs', this.handleSubdocs);
        // Start Sync
        this.sync();
    }
    // Simple debounce implementation
    debounce(func, wait) {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func(...args), wait);
        };
    }
    // helper for delay
    wait(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    /**
     * Sync Mechanism
     * 1. Load Base Snapshot
     * 2. Load History Segments
     * 3. Subscribe to Live Updates
     */
    sync() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                // SHADOW SYNC STRATEGY
                // We reconstruct the server state in a temporary "shadow" doc to determine
                // exactly what local changes are missing from the server.
                const remoteShadow = new Y.Doc();
                try {
                    // 1. Fetch Updates (Tier 3)
                    // We fetch updates FIRST to avoid the "staggered read" race condition.
                    // If compaction happens while reading, we might miss updates if we read them last.
                    // By reading them first, we either get them here, OR if they are compacted into the snapshot,
                    // we will get them in step 3 when we read the new snapshot.
                    const updatesQ = query(collection(this.db, this.path, 'updates'), orderBy('createdAt', 'asc'));
                    const updatesSnap = yield getDocs(updatesQ);
                    if (this._isDestroyed)
                        return;
                    updatesSnap.forEach(snap => {
                        const data = snap.data();
                        if (data && data.update) {
                            try {
                                const update = data.update.toUint8Array();
                                // We DO NOT apply to this.doc here, we let the onSnapshot listener handle it 
                                // (or we could, but standardizing on onSnapshot is cleaner for the live path).
                                // However, for the shadow doc, we MUST apply it.
                                Y.applyUpdate(remoteShadow, update);
                            }
                            catch (e) {
                                console.error("Failed to apply update to shadow", e);
                            }
                        }
                    });
                    // 2. Fetch History Segments (Tier 2)
                    const historyQ = query(collection(this.db, this.path, 'history'), orderBy('startTime', 'asc'));
                    const historySnaps = yield getDocs(historyQ);
                    if (this._isDestroyed)
                        return;
                    historySnaps.forEach(snap => {
                        const data = snap.data();
                        if (data && data.segment) {
                            try {
                                const segment = data.segment.toUint8Array();
                                Y.applyUpdate(this.doc, segment, 'origin:firebase/history');
                                Y.applyUpdate(remoteShadow, segment);
                            }
                            catch (e) {
                                console.error("Failed to apply history segment", e);
                            }
                        }
                    });
                    // 3. Fetch Base Snapshot (Tier 1)
                    const mainRef = doc(this.db, this.path);
                    const mainSnap = yield getDoc(mainRef);
                    if (this._isDestroyed)
                        return;
                    if (mainSnap.exists()) {
                        const data = mainSnap.data();
                        if (data && data.content) {
                            try {
                                const content = data.content.toUint8Array();
                                Y.applyUpdate(this.doc, content, 'origin:firebase/snapshot');
                                Y.applyUpdate(remoteShadow, content);
                            }
                            catch (e) {
                                console.error("Failed to apply snapshot", e);
                            }
                        }
                    }
                    // 4. Calculate Missing Local Updates
                    const shadowSv = Y.encodeStateVector(remoteShadow);
                    const localDiff = Y.encodeStateAsUpdate(this.doc, shadowSv);
                    // 5. Push if needed
                    // Yjs empty update is 2 bytes.
                    if (localDiff.byteLength > 2) {
                        console.log("Pushing missing local updates to Firestore.");
                        const pkg = {
                            update: Bytes.fromUint8Array(localDiff),
                            createdAt: serverTimestamp(),
                            createdBy: this.uid
                        };
                        yield addDoc(collection(this.db, this.path, 'updates'), pkg);
                        if (this._isDestroyed)
                            return;
                    }
                }
                finally {
                    remoteShadow.destroy();
                }
                if (this._unsubscribeUpdates)
                    this._unsubscribeUpdates();
                const listenerFn = (snapshot) => {
                    // Check for compaction trigger
                    if (snapshot.size > this.maxUpdatesThreshold && !this.isCompacting) {
                        if (Math.random() < this.compactionProbability) {
                            this.compact();
                        }
                    }
                    snapshot.docChanges().forEach((change) => {
                        if (change.type === 'added') {
                            const data = change.doc.data();
                            // Check if this update was created by us
                            if (data.createdBy === this.uid) {
                                return;
                            }
                            if (data.update) {
                                try {
                                    const update = data.update.toUint8Array();
                                    Y.applyUpdate(this.doc, update, 'origin:firebase/update');
                                }
                                catch (e) {
                                    console.error("Failed to apply update", e);
                                }
                            }
                        }
                    });
                };
                // Re-query for listener to be safe / clean
                const liveUpdatesQ = query(collection(this.db, this.path, 'updates'), orderBy('createdAt', 'asc'));
                if (this._isDestroyed)
                    return;
                this._unsubscribeUpdates = onSnapshot(liveUpdatesQ, listenerFn, (error) => {
                    console.error("onSnapshot listener failed", error);
                    // If the listener fails (e.g. Grpc error in emulator), we might want to trigger a re-sync
                    // but for now we at least log it so it's visible in tests.
                });
            }
            catch (err) {
                console.error("Sync failed", err);
            }
        });
    }
    saveToFirestore() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.updateCache)
                return;
            const update = this.updateCache;
            this.updateCache = null;
            try {
                yield addDoc(collection(this.db, this.path, 'updates'), {
                    update: Bytes.fromUint8Array(update),
                    createdAt: serverTimestamp(),
                    createdBy: this.uid
                });
            }
            catch (err) {
                console.error("Failed to save update", err);
                // If failed, we might want to preserve the cache, but strictly we could lose data here on network fail.
                // For this implementation, we log error.
            }
        });
    }
    /**
     * Compaction Logic (Tiered)
     * Merges updates into History Segments or Base Snapshot
     */
    compact(attempt = 1) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.isCompacting && attempt === 1)
                return;
            this.isCompacting = true;
            try {
                console.log(`Starting compaction (attempt ${attempt})...`);
                // 1. Get all updates to compact
                // We query them first to get references. 
                // Note: In highly concurrent env, we should verify existence in transaction.
                const updatesQ = query(collection(this.db, this.path, 'updates'), orderBy('createdAt', 'asc'));
                const updatesSnap = yield getDocs(updatesQ);
                // We also check for History segments to merge them back if possible
                const historyQ = query(collection(this.db, this.path, 'history'), orderBy('startTime', 'asc'));
                const historySnaps = yield getDocs(historyQ);
                if (updatesSnap.empty && historySnaps.empty) {
                    this.isCompacting = false;
                    return;
                }
                const updateDocs = updatesSnap.docs;
                const historyDocs = historySnaps.docs;
                yield runTransaction(this.db, (transaction) => __awaiter(this, void 0, void 0, function* () {
                    // 2. Read Base Snapshot (Tier 1) inside transaction
                    const mainRef = doc(this.db, this.path);
                    const mainSnap = yield transaction.get(mainRef);
                    let baseSnapshot = null;
                    if (mainSnap.exists()) {
                        const data = mainSnap.data();
                        if (data && data.content) {
                            baseSnapshot = data.content.toUint8Array();
                        }
                    }
                    // 3. Read specific updates to ensure they exist
                    // (Firestore transactions need read-before-write)
                    const updatesToMerge = [];
                    for (const uDoc of updateDocs) {
                        const freshSnap = yield transaction.get(uDoc.ref);
                        if (freshSnap.exists()) {
                            const data = freshSnap.data();
                            if (data && data.update) {
                                updatesToMerge.push(data.update.toUint8Array());
                            }
                        }
                    }
                    // 3b. Read History Segments to attempt full merge
                    const historyToMerge = [];
                    for (const hDoc of historyDocs) {
                        const freshSnap = yield transaction.get(hDoc.ref);
                        if (freshSnap.exists()) {
                            const data = freshSnap.data();
                            if (data && data.segment) {
                                historyToMerge.push({
                                    ref: hDoc.ref,
                                    val: data.segment.toUint8Array()
                                });
                            }
                        }
                    }
                    if (updatesToMerge.length === 0 && historyToMerge.length === 0)
                        return;
                    // 4. Merge Logic
                    // Strategy: Try Level 1 Merge (Base + History + Updates)
                    const allContent = [];
                    if (baseSnapshot)
                        allContent.push(baseSnapshot);
                    historyToMerge.forEach(h => allContent.push(h.val));
                    updatesToMerge.forEach(u => allContent.push(u));
                    const candidate = Y.mergeUpdates(allContent);
                    const sizeInBytes = candidate.byteLength;
                    const TARGET_LIMIT = 900000; // 900KB
                    // Decide Level 1 (Snapshot) vs Level 2 (History)
                    if (sizeInBytes < TARGET_LIMIT) {
                        // Success: Everything fits in Base Snapshot
                        console.log(`Compacted to Snapshot (Size: ${sizeInBytes})`);
                        transaction.set(mainRef, { content: Bytes.fromUint8Array(candidate) }, { merge: true });
                        // Delete all utilized segments
                        for (const uDoc of updateDocs) {
                            transaction.delete(uDoc.ref);
                        }
                        for (const hItem of historyToMerge) {
                            transaction.delete(hItem.ref);
                        }
                    }
                    else {
                        // Level 2: Write to History Segment (Fallback)
                        // We can't fit everything into Base.
                        // Goal: Merge updates into History Segments.
                        if (updatesToMerge.length > 0) {
                            // FIX: Handle updates that are too large for a single History Segment
                            const MAX_SEGMENT_SIZE = 900000; // Safe limit (900KB)
                            // 1. Try merging all first (Optimistic)
                            let pendingMerge = Y.mergeUpdates(updatesToMerge);
                            if (pendingMerge.byteLength < MAX_SEGMENT_SIZE) {
                                // Fast path: It fits in one segment
                                const segmentId = Math.random().toString(36).substring(2);
                                const historyRef = doc(collection(this.db, this.path, 'history'), segmentId);
                                transaction.set(historyRef, {
                                    segment: Bytes.fromUint8Array(pendingMerge),
                                    startTime: updateDocs[0].data().createdAt,
                                    endTime: updateDocs[updateDocs.length - 1].data().createdAt
                                });
                                // Delete all utilized updates
                                for (const uDoc of updateDocs) {
                                    transaction.delete(uDoc.ref);
                                }
                            }
                            else {
                                // Slow path: The updates are too big. We must split them.
                                // We iterate through updates and create multiple History Segments.
                                let currentBatch = [];
                                let currentBatchSize = 0;
                                let batchStartIndex = 0;
                                for (let i = 0; i < updatesToMerge.length; i++) {
                                    const update = updatesToMerge[i];
                                    const updateSize = update.byteLength;
                                    // If a SINGLE update is > 1MB, we might still have issues, 
                                    // but usually Yjs updates are granular.
                                    // Check if adding this update would likely exceed limit
                                    if (currentBatchSize + updateSize > MAX_SEGMENT_SIZE && currentBatch.length > 0) {
                                        // Flush current batch to a new Segment
                                        const mergedBatch = Y.mergeUpdates(currentBatch);
                                        const segmentId = Math.random().toString(36).substring(2);
                                        const historyRef = doc(collection(this.db, this.path, 'history'), segmentId);
                                        transaction.set(historyRef, {
                                            segment: Bytes.fromUint8Array(mergedBatch),
                                            startTime: updateDocs[batchStartIndex].data().createdAt,
                                            endTime: updateDocs[i - 1].data().createdAt
                                        });
                                        // Delete updates in this batch
                                        for (let j = batchStartIndex; j < i; j++) {
                                            transaction.delete(updateDocs[j].ref);
                                        }
                                        // Reset
                                        currentBatch = [];
                                        currentBatchSize = 0;
                                        batchStartIndex = i;
                                    }
                                    currentBatch.push(update);
                                    currentBatchSize += updateSize;
                                }
                                // Flush remaining batch
                                if (currentBatch.length > 0) {
                                    const mergedBatch = Y.mergeUpdates(currentBatch);
                                    const segmentId = Math.random().toString(36).substring(2);
                                    const historyRef = doc(collection(this.db, this.path, 'history'), segmentId);
                                    transaction.set(historyRef, {
                                        segment: Bytes.fromUint8Array(mergedBatch),
                                        startTime: updateDocs[batchStartIndex].data().createdAt,
                                        endTime: updateDocs[updatesToMerge.length - 1].data().createdAt
                                    });
                                    for (let j = batchStartIndex; j < updatesToMerge.length; j++) {
                                        transaction.delete(updateDocs[j].ref);
                                    }
                                }
                            }
                        }
                    }
                }));
                // On Success:
                this.isCompacting = false;
            }
            catch (e) {
                const MAX_RETRIES = 5;
                // Filter for retryable errors (Contention, Unavailable, Deadline Exceeded)
                // Firestore code 'aborted' is commonly used for transaction contention
                const isRetryable = e.code === 'aborted' || e.code === 'unavailable' || e.code === 'deadline-exceeded';
                if (attempt <= MAX_RETRIES && isRetryable) {
                    // Exponential Backoff: 2^attempt * 100ms
                    // Jitter: Randomize to prevent "thundering herd" if multiple clients fail simultaneously
                    const backoff = (Math.pow(2, attempt) * 100) + (Math.random() * 100);
                    console.warn(`Compaction failed (attempt ${attempt}). Retrying in ${Math.floor(backoff)}ms...`, e);
                    yield this.wait(backoff);
                    yield this.compact(attempt + 1); // Recursive retry
                }
                else {
                    console.error("Compaction failed permanently or reached max retries.", e);
                    this.isCompacting = false; // Give up
                }
            }
        });
    }
    startSubdocProvider(subdoc) {
        const guid = subdoc.guid;
        if (this.subProviders.has(guid))
            return;
        // Firestore path limit is 100. Safety limit at 50.
        if (this.depth >= 50) {
            console.warn(`Max subdocument depth exceeded at ${this.path}`);
            return;
        }
        const subPath = `${this.path}/subdocs/${guid}`;
        const provider = new FireProvider({
            firebaseApp: this.firebaseApp,
            ydoc: subdoc,
            path: subPath,
            maxUpdatesThreshold: this.maxUpdatesThreshold,
            maxWaitTime: this.maxWaitTime,
            depth: this.depth + 1
        });
        this.subProviders.set(guid, provider);
    }
    destroy() {
        this._isDestroyed = true;
        if (this._unsubscribeUpdates)
            this._unsubscribeUpdates();
        this.doc.off('update', this.handleUpdate);
        this.doc.off('subdocs', this.handleSubdocs);
        // Destroy children
        this.subProviders.forEach(p => p.destroy());
        this.subProviders.clear();
        // Flush any pending updates
        if (this.updateCache) {
            this.saveToFirestore();
        }
        super.destroy();
    }
}
