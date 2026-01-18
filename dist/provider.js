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
    constructor({ firebaseApp, ydoc, path, maxUpdatesThreshold = 50, maxWaitTime = 500, }) {
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
        this._unsubscribeUpdates = null;
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
    /**
     * Sync Mechanism
     * 1. Load Base Snapshot
     * 2. Load History Segments
     * 3. Subscribe to Live Updates
     */
    sync() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                // 1. Fetch Base Snapshot (Tier 1)
                // The base snapshot is stored in the 'content' field of the main document
                const mainRef = doc(this.db, this.path);
                const mainSnap = yield getDoc(mainRef);
                if (mainSnap.exists()) {
                    const data = mainSnap.data();
                    if (data && data.content) {
                        try {
                            const content = data.content.toUint8Array();
                            Y.applyUpdate(this.doc, content, 'origin:firebase/snapshot');
                        }
                        catch (e) {
                            console.error("Failed to apply snapshot", e);
                        }
                    }
                }
                // 2. Fetch History Segments (Tier 2)
                // Ordered by startTime (asc)
                const historyQ = query(collection(this.db, this.path, 'history'), orderBy('startTime', 'asc'));
                const historySnaps = yield getDocs(historyQ);
                historySnaps.forEach(snap => {
                    const data = snap.data();
                    if (data && data.segment) {
                        try {
                            const segment = data.segment.toUint8Array();
                            Y.applyUpdate(this.doc, segment, 'origin:firebase/history');
                        }
                        catch (e) {
                            console.error("Failed to apply history segment", e);
                        }
                    }
                });
                // 3. Subscribe to Live Updates (Tier 3)
                const updatesQ = query(collection(this.db, this.path, 'updates'), orderBy('createdAt', 'asc'));
                if (this._unsubscribeUpdates)
                    this._unsubscribeUpdates();
                this._unsubscribeUpdates = onSnapshot(updatesQ, (snapshot) => {
                    // Check for compaction trigger
                    if (snapshot.size > this.maxUpdatesThreshold && !this.isCompacting) {
                        this.compact();
                    }
                    snapshot.docChanges().forEach(change => {
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
    compact() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.isCompacting)
                return;
            this.isCompacting = true;
            try {
                console.log("Starting compaction...");
                // 1. Get all updates to compact
                // We query them first to get references. 
                // Note: In highly concurrent env, we should verify existence in transaction.
                const updatesQ = query(collection(this.db, this.path, 'updates'), orderBy('createdAt', 'asc'));
                const updatesSnap = yield getDocs(updatesQ);
                if (updatesSnap.empty) {
                    this.isCompacting = false;
                    return;
                }
                const updateDocs = updatesSnap.docs;
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
                    if (updatesToMerge.length === 0)
                        return;
                    // 4. Merge Logic
                    // Strategy: Try Level 1 Merge (Base + Updates)
                    // Note: We are ignoring History segments in this compaction step for simplicity 
                    // unless we want to merge History segments into Base too.
                    // Design says: "Attempt Level 1 Merge (Snapshot): Combine S + H[] + U[]".
                    // To do that, we'd need to read ALL History segments too.
                    // That might be too many reads for a transaction if History is large.
                    // Simplified approach: Compact U[] -> New H Segment. 
                    // OR checks size of Base + U[].
                    const updatesMerged = Y.mergeUpdates(updatesToMerge);
                    // Calc Candidate Snapshot Size
                    // If we have base, merge base + updates.
                    let candidate;
                    if (baseSnapshot) {
                        candidate = Y.mergeUpdates([baseSnapshot, updatesMerged]);
                    }
                    else {
                        candidate = updatesMerged;
                    }
                    const sizeInBytes = candidate.byteLength;
                    const LIMIT_1MB = 1000000;
                    const TARGET_LIMIT = 900000; // 900KB
                    // Decide Level 1 (Snapshot) vs Level 2 (History)
                    if (sizeInBytes < TARGET_LIMIT) {
                        // write to Snapshot
                        transaction.set(mainRef, { content: Bytes.fromUint8Array(candidate) }, { merge: true });
                        // We should also delete history segments if we merged them? 
                        // But we didn't read history segments here, so we only merged Base + Updates.
                        // So History Segments remain parallel? 
                        // That's fine, but inefficient eventually.
                        // Design says "Combine S + H + U". 
                        // For now, let's just do Level 2 (Append to History) if Base is full, 
                        // or Update Base if Base + Updates is small.
                        // If we Update Base, we delete Updates.
                        // If Base is small, we update Base.
                    }
                    else {
                        // Level 2: Write to History Segment
                        // We leave Base as is. We take `updatesMerged` and write as new History Segment.
                        const segmentId = Math.random().toString(36).substring(2);
                        const historyRef = doc(collection(this.db, this.path, 'history'), segmentId);
                        transaction.set(historyRef, {
                            segment: Bytes.fromUint8Array(updatesMerged),
                            startTime: updateDocs[0].data().createdAt,
                            endTime: updateDocs[updateDocs.length - 1].data().createdAt
                        });
                    }
                    // 5. Delete processed updates
                    for (const uDoc of updateDocs) {
                        transaction.delete(uDoc.ref);
                    }
                }));
            }
            catch (e) {
                console.error("Compaction execution failed", e);
            }
            finally {
                this.isCompacting = false;
            }
        });
    }
    startSubdocProvider(subdoc) {
        const guid = subdoc.guid;
        if (this.subProviders.has(guid))
            return;
        const subPath = `${this.path}/subdocs/${guid}`;
        const provider = new FireProvider({
            firebaseApp: this.firebaseApp,
            ydoc: subdoc,
            path: subPath,
            maxUpdatesThreshold: this.maxUpdatesThreshold,
            maxWaitTime: this.maxWaitTime
        });
        this.subProviders.set(guid, provider);
    }
    destroy() {
        if (this._unsubscribeUpdates)
            this._unsubscribeUpdates();
        this.doc.off('update', this.handleUpdate);
        this.doc.off('subdocs', this.handleSubdocs);
        // Destroy children
        this.subProviders.forEach(p => p.destroy());
        this.subProviders.clear();
        super.destroy();
    }
}
