/**
 * Synchronization Module
 *
 * Implements the core sync algorithm for bidirectional synchronization between
 * local Yjs documents and Firestore. Uses metadata-based comparison to minimize
 * data transfer and avoid re-applying already-seen updates.
 *
 * ## Sync Algorithm
 *
 * ### Initial Sync (performInitialSync)
 * 1. Fetch all server data (updates, history, snapshot)
 * 2. Extract metadata (client IDs and clock values) from each item
 * 3. Build a server state vector from the metadata
 * 4. Compare with local state vector
 * 5. Apply only items that contain data missing locally
 * 6. Push any local data that's missing on the server
 *
 * ### Real-time Sync (createUpdateListener)
 * - Listens to the updates collection via onSnapshot
 * - Applies new updates from other clients
 * - Skips our own updates (using createdBy)
 * - Skips redundant updates (using clientID/clockEnd metadata)
 * - Probabilistically triggers compaction when threshold exceeded
 *
 * ## Priority Order
 *
 * Updates are applied in this order to ensure correct CRDT merge:
 * 1. Base Snapshot (Tier 1) - oldest, most compacted data
 * 2. History Segments (Tier 2) - intermediate merges
 * 3. Individual Updates (Tier 3) - newest data
 *
 * @module sync
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
import { onSnapshot, doc, collection, addDoc, Bytes, query, orderBy, getDocs, getDoc, serverTimestamp, limit, startAfter, } from "@firebase/firestore";
import { getBytes, ref, uploadBytes } from "@firebase/storage";
import * as Y from "yjs";
import { fromBase64 } from "lib0/buffer";
import { FIREBASE_ORIGINS, FIRESTORE_PATHS, DEFAULTS, } from "./types";
import { writeStateVector } from "./utils";
import { extractAllMetadata, aggregateMetadata, isUpdateRedundant } from "./update-metadata";
/**
 * Performs the initial sync operation.
 *
 * This is the core sync algorithm using metadata-only comparison:
 * 1. Fetch all data (updates, history, snapshot) and extract metadata
 * 2. Build a server state vector from metadata
 * 3. Compare with local state vector
 * 4. Apply only missing data
 * 5. Push local updates not on server
 *
 * ## P0.7: Eventual Consistency
 *
 * This function uses separate, non-transactional reads which means
 * compaction can race with our reads. The read order (Updates → History →
 * Snapshot) is deliberately chosen to be safe:
 *
 * - **Worst case**: We read Updates, compaction moves Update A to History,
 *   we read History (includes A). Result: We see A in both - duplicate, but safe.
 * - **Data loss scenario (avoided)**: If we read History first and Updates second,
 *   compaction could move data between reads causing us to miss it.
 *
 * Yjs handles duplicate updates gracefully (they're idempotent), so the
 * "duplicate" worst case has no data integrity impact.
 *
 * @param ctx - Sync context
 * @returns Sync result with statistics
 *
 * @example
 * ```typescript
 * const result = await performInitialSync({
 *   db, path, doc: ydoc, uid,
 *   maxUpdatesThreshold: 50,
 *   isDestroyed: () => false
 * });
 * ```
 */
export function performInitialSync(ctx) {
    return __awaiter(this, void 0, void 0, function* () {
        const { db, path, doc: ydoc, uid, isDestroyed } = ctx;
        const BATCH_SIZE = DEFAULTS.SYNC_BATCH_SIZE;
        try {
            const serverSVMap = new Map();
            const pendingUpdates = [];
            let updatesApplied = 0;
            // 1. Fetch Updates (Tier 3) with pagination (P0.1 fix)
            let lastUpdateDoc = null;
            let hasMoreUpdates = true;
            while (hasMoreUpdates) {
                const updatesQ = lastUpdateDoc
                    ? query(collection(db, path, FIRESTORE_PATHS.UPDATES), orderBy('createdAt', 'asc'), startAfter(lastUpdateDoc), limit(BATCH_SIZE))
                    : query(collection(db, path, FIRESTORE_PATHS.UPDATES), orderBy('createdAt', 'asc'), limit(BATCH_SIZE));
                const updatesSnap = yield getDocs(updatesQ);
                if (isDestroyed())
                    return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, lastHistoryDoc: null };
                if (updatesSnap.empty) {
                    hasMoreUpdates = false;
                }
                else {
                    for (const snap of updatesSnap.docs) {
                        const data = snap.data();
                        if (data) {
                            // Download storage-backed update if present
                            if (data.updateStoragePath && !data.update) {
                                try {
                                    const storageRef = ref(ctx.storage, data.updateStoragePath);
                                    const buffer = yield getBytes(storageRef);
                                    data.update = Bytes.fromUint8Array(new Uint8Array(buffer));
                                }
                                catch (storageErr) {
                                    console.error(`Failed to download storage-backed update: ${data.updateStoragePath}`, storageErr);
                                    continue; // Skip this update — cannot apply without data
                                }
                            }
                            processUpdateMetadata(data, serverSVMap);
                            pendingUpdates.push({ type: 'update', data, priority: 3 });
                        }
                    }
                    // FIX: Verify cursor is committed to avoid "Invalid query" with pending serverTimestamp
                    let candidateDoc = updatesSnap.docs[updatesSnap.docs.length - 1];
                    while (candidateDoc && candidateDoc.metadata.hasPendingWrites) {
                        const idx = updatesSnap.docs.indexOf(candidateDoc);
                        candidateDoc = idx > 0 ? updatesSnap.docs[idx - 1] : null;
                    }
                    // Only update lastUpdateDoc if we found a later committed doc, or if we haven't set one yet
                    if (candidateDoc) {
                        lastUpdateDoc = candidateDoc;
                    }
                    hasMoreUpdates = updatesSnap.docs.length === BATCH_SIZE;
                }
            }
            // 2. Fetch History Segments (Tier 2) with pagination (P0.1 fix)
            let lastHistoryDoc = null;
            let hasMoreHistory = true;
            while (hasMoreHistory) {
                const historyQ = lastHistoryDoc
                    ? query(collection(db, path, FIRESTORE_PATHS.HISTORY), orderBy('startTime', 'asc'), startAfter(lastHistoryDoc), limit(BATCH_SIZE))
                    : query(collection(db, path, FIRESTORE_PATHS.HISTORY), orderBy('startTime', 'asc'), limit(BATCH_SIZE));
                const historySnap = yield getDocs(historyQ);
                if (isDestroyed())
                    return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, lastHistoryDoc: null };
                if (historySnap.empty) {
                    hasMoreHistory = false;
                }
                else {
                    historySnap.forEach(snap => {
                        const data = snap.data();
                        if (data && data.segment) {
                            processHistoryMetadata(data, serverSVMap);
                            pendingUpdates.push({ type: 'history', data, priority: 2 });
                        }
                    });
                    // FIX: Verify cursor is committed
                    let candidateDoc = historySnap.docs[historySnap.docs.length - 1];
                    while (candidateDoc && candidateDoc.metadata.hasPendingWrites) {
                        const idx = historySnap.docs.indexOf(candidateDoc);
                        candidateDoc = idx > 0 ? historySnap.docs[idx - 1] : null;
                    }
                    if (candidateDoc) {
                        lastHistoryDoc = candidateDoc;
                    }
                    hasMoreHistory = historySnap.docs.length === BATCH_SIZE;
                }
            }
            // 3. Fetch Base Snapshot (Tier 1) - single document, no pagination needed
            const mainRef = doc(db, path);
            const mainSnap = yield getDoc(mainRef);
            if (isDestroyed())
                return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, lastHistoryDoc: null };
            if (mainSnap.exists()) {
                const data = mainSnap.data();
                if (data) {
                    processSnapshotMetadata(data, serverSVMap);
                    // Fetch snapshot from Cloud Storage if available
                    if (data.snapshotStoragePath) {
                        try {
                            const storageRef = ref(ctx.storage, data.snapshotStoragePath);
                            const buffer = yield getBytes(storageRef);
                            // Convert ArrayBuffer to Uint8Array and inject it into data.content
                            data.content = Bytes.fromUint8Array(new Uint8Array(buffer));
                            pendingUpdates.push({ type: 'snapshot', data, priority: 1 });
                        }
                        catch (storageErr) {
                            console.error("Failed to download snapshot from Cloud Storage", storageErr);
                            // Cannot safely sync without the base snapshot — propagate so
                            // the sync retry logic in provider.ts handles backoff/retry.
                            throw storageErr;
                        }
                    }
                    else if (data.stateVector || data.content) {
                        // Fallback for older documents that haven't been compacted into Cloud Storage yet
                        pendingUpdates.push({ type: 'snapshot', data, priority: 1 });
                    }
                }
            }
            // 4. Apply missing data with state vector refresh (P0.4 fix)
            let localSVMap = Y.decodeStateVector(Y.encodeStateVector(ydoc));
            // Sort by priority (Snapshot first, then History, then Updates)
            pendingUpdates.sort((a, b) => a.priority - b.priority);
            for (const item of pendingUpdates) {
                if (isDestroyed())
                    break;
                if (!isItemRedundant(item, localSVMap)) {
                    const applied = applyItem(item, ydoc);
                    if (applied) {
                        updatesApplied++;
                        // P0.4 FIX: Refresh localSVMap after applying snapshot or history
                        // This prevents redundant processing of history/updates already in snapshot/previous segments
                        if (item.type === 'snapshot' || item.type === 'history') {
                            localSVMap = Y.decodeStateVector(Y.encodeStateVector(ydoc));
                        }
                    }
                }
            }
            // 5. Push Missing Local Updates
            const serverSV = writeStateVector(serverSVMap);
            const localDiff = Y.encodeStateAsUpdate(ydoc, serverSV);
            let localUpdatesPushed = false;
            if (localDiff.byteLength > 2) {
                console.log("Pushing missing local updates to Firestore.");
                const metas = extractAllMetadata(localDiff);
                if (localDiff.byteLength > DEFAULTS.FIRESTORE_DOC_LIMIT) {
                    // Storage-backed update: upload binary to Cloud Storage
                    const storagePath = `${path}/large_updates/${uid}_${Date.now()}.bin`;
                    const storageRef = ref(ctx.storage, storagePath);
                    yield uploadBytes(storageRef, localDiff);
                    // Write lightweight pointer document to updates collection
                    const pkg = Object.assign({ updateStoragePath: storagePath, createdAt: serverTimestamp(), createdBy: uid }, aggregateMetadata(metas));
                    yield addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), pkg);
                    console.log(`Oversized initial sync diff (${localDiff.byteLength} bytes) offloaded to Cloud Storage: ${storagePath}`);
                }
                else {
                    // Standard inline update
                    const pkg = Object.assign({ update: Bytes.fromUint8Array(localDiff), createdAt: serverTimestamp(), createdBy: uid }, aggregateMetadata(metas));
                    yield addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), pkg);
                }
                localUpdatesPushed = true;
            }
            return {
                success: true,
                updatesApplied,
                localUpdatesPushed,
                lastSyncedDoc: lastUpdateDoc,
                lastHistoryDoc
            };
        }
        catch (err) {
            console.error("Sync failed", err);
            return {
                success: false,
                error: err instanceof Error ? err : new Error(String(err)),
                updatesApplied: 0,
                localUpdatesPushed: false,
                lastSyncedDoc: null,
                lastHistoryDoc: null
            };
        }
    });
}
/**
 * Creates a real-time listener for new updates.
 *
 * P0.2 FIX: Uses limitToLast() to prevent memory explosion when connecting
 * to documents with many pending updates. Only the most recent updates are
 * tracked; older updates were already processed during initial sync.
 *
 * @param ctx - Sync context
 * @param startAfterDoc - Optional cursor to start listening from (prevents gaps)
 * @returns Unsubscribe function
 */
export function createUpdateListener(ctx, startAfterDoc = null) {
    const { db, path, doc: ydoc, uid, maxUpdatesThreshold, onCompactionNeeded, onListenerError, isDestroyed } = ctx;
    let liveUpdatesQ;
    if (startAfterDoc) {
        // P1.9 FIX: Continue exactly where sync left off to prevent "Sync Gap"
        liveUpdatesQ = query(collection(db, path, FIRESTORE_PATHS.UPDATES), orderBy('createdAt', 'asc'), startAfter(startAfterDoc));
    }
    else {
        // Fallback for fresh docs (or rely on sync to have found nothing)
        // If sync found nothing, we start from the beginning.
        // P0.2 NOTE: Removed limitToLast because we assume initial sync caught everything up to "now"
        // or there was nothing. If there was nothing, we want everything new.
        liveUpdatesQ = query(collection(db, path, FIRESTORE_PATHS.UPDATES), orderBy('createdAt', 'asc'));
    }
    return onSnapshot(liveUpdatesQ, (snapshot) => {
        // Check for compaction trigger based on actual size
        // Note: snapshot.size may be capped by limitToLast, so we check docChanges for additions
        if (snapshot.size >= DEFAULTS.REALTIME_LIMIT && onCompactionNeeded) {
            // At capacity - definitely need compaction
            onCompactionNeeded();
        }
        else if (snapshot.size > maxUpdatesThreshold && onCompactionNeeded) {
            onCompactionNeeded();
        }
        snapshot.docChanges().forEach((change) => {
            var _a, _b, _c, _d, _e;
            if (change.type === 'added') {
                const data = change.doc.data();
                // Skip our own updates
                if (data.createdBy === uid) {
                    return;
                }
                // Check if we already have this update
                if (((_a = data.clientIDs) === null || _a === void 0 ? void 0 : _a.length) > 0 && ((_b = data.clientClocks) === null || _b === void 0 ? void 0 : _b.length) > 0) {
                    const freshSV = Y.encodeStateVector(ydoc);
                    const freshMap = Y.decodeStateVector(freshSV);
                    if (isUpdateRedundant(freshMap, data.clientIDs, data.clientClocks)) {
                        return; // Skip - we have all the data
                    }
                }
                const docId = change.doc.id;
                // Skip quarantined poison pills
                if ((_c = ctx.corruptedDocIds) === null || _c === void 0 ? void 0 : _c.has(docId)) {
                    return;
                }
                // Handle storage-backed update (oversized diff offloaded to Cloud Storage)
                if (data.updateStoragePath && !data.update) {
                    (() => __awaiter(this, void 0, void 0, function* () {
                        var _f, _g;
                        try {
                            const storageRef = ref(ctx.storage, data.updateStoragePath);
                            const buffer = yield getBytes(storageRef);
                            const update = new Uint8Array(buffer);
                            Y.applyUpdate(ydoc, update, FIREBASE_ORIGINS.UPDATE);
                        }
                        catch (e) {
                            console.error(`Failed to apply storage-backed update ${docId} (quarantined)`, e);
                            (_f = ctx.corruptedDocIds) === null || _f === void 0 ? void 0 : _f.add(docId);
                            (_g = ctx.onCorruptedDocument) === null || _g === void 0 ? void 0 : _g.call(ctx, docId, e instanceof Error ? e : new Error(String(e)));
                        }
                    }))();
                    return;
                }
                if (data.update) {
                    try {
                        const update = data.update.toUint8Array();
                        Y.applyUpdate(ydoc, update, FIREBASE_ORIGINS.UPDATE);
                    }
                    catch (e) {
                        console.error(`Failed to apply update ${docId} (quarantined)`, e);
                        (_d = ctx.corruptedDocIds) === null || _d === void 0 ? void 0 : _d.add(docId);
                        (_e = ctx.onCorruptedDocument) === null || _e === void 0 ? void 0 : _e.call(ctx, docId, e instanceof Error ? e : new Error(String(e)));
                    }
                }
            }
        });
    }, (error) => {
        console.error("onSnapshot listener failed", error);
        // P1.7 FIX: Emit error event so caller can handle disconnect
        if (onListenerError) {
            onListenerError(error);
        }
    });
}
/**
 * Creates a real-time listener for the root snapshot.
 *
 * Ensures that if compaction replaces updates with a snapshot, this client
 * receives the new reference state.
 */
export function createSnapshotListener(ctx) {
    const { db, path, doc: ydoc, onListenerError, storage } = ctx;
    // Track the last quarantined snapshot path so we can clear quarantine
    // when compaction produces a new snapshot at a different path.
    let lastQuarantinedPath = null;
    return onSnapshot(doc(db, path), (snapshot) => __awaiter(this, void 0, void 0, function* () {
        var _a, _b, _c, _d, _e, _f, _g;
        if (!snapshot.exists())
            return;
        const data = snapshot.data();
        // Handle Cloud Storage Snapshot
        if ((data === null || data === void 0 ? void 0 : data.snapshotStoragePath) && (data.origin !== undefined ? data.origin !== ctx.uid : true)) {
            const snapshotKey = `snapshot:${data.snapshotStoragePath}`;
            // Clear quarantine if snapshot path changed (new compaction)
            if (lastQuarantinedPath && lastQuarantinedPath !== snapshotKey) {
                (_a = ctx.corruptedDocIds) === null || _a === void 0 ? void 0 : _a.delete(lastQuarantinedPath);
                lastQuarantinedPath = null;
            }
            // Skip quarantined snapshot
            if ((_b = ctx.corruptedDocIds) === null || _b === void 0 ? void 0 : _b.has(snapshotKey)) {
                return;
            }
            try {
                const storageRef = ref(storage, data.snapshotStoragePath);
                const buffer = yield getBytes(storageRef);
                const content = new Uint8Array(buffer);
                Y.applyUpdate(ydoc, content, FIREBASE_ORIGINS.SNAPSHOT);
            }
            catch (storageErr) {
                console.error(`Failed to apply snapshot ${snapshotKey} (quarantined)`, storageErr);
                (_c = ctx.corruptedDocIds) === null || _c === void 0 ? void 0 : _c.add(snapshotKey);
                lastQuarantinedPath = snapshotKey;
                (_d = ctx.onCorruptedDocument) === null || _d === void 0 ? void 0 : _d.call(ctx, snapshotKey, storageErr instanceof Error ? storageErr : new Error(String(storageErr)));
            }
        }
        // Handle Firestore Document Snapshot (legacy/small documents)
        else if ((data === null || data === void 0 ? void 0 : data.content) && (data.origin !== undefined ? data.origin !== ctx.uid : true)) {
            const snapshotKey = 'snapshot:inline';
            if ((_e = ctx.corruptedDocIds) === null || _e === void 0 ? void 0 : _e.has(snapshotKey)) {
                return;
            }
            try {
                const content = data.content.toUint8Array();
                Y.applyUpdate(ydoc, content, FIREBASE_ORIGINS.SNAPSHOT);
            }
            catch (err) {
                console.error(`Failed to apply inline snapshot (quarantined)`, err);
                (_f = ctx.corruptedDocIds) === null || _f === void 0 ? void 0 : _f.add(snapshotKey);
                lastQuarantinedPath = snapshotKey;
                (_g = ctx.onCorruptedDocument) === null || _g === void 0 ? void 0 : _g.call(ctx, snapshotKey, err instanceof Error ? err : new Error(String(err)));
            }
        }
    }), (error) => {
        console.error("Snapshot listener failed", error);
        if (onListenerError)
            onListenerError(error);
    });
}
/**
 * Creates a real-time listener for new history segments.
 *
 * Uses the last known history document as a cursor to only fetch NEW segments.
 */
export function createHistoryListener(ctx, startAfterDoc) {
    const { db, path, doc: ydoc, onListenerError } = ctx;
    let q;
    if (startAfterDoc) {
        q = query(collection(db, path, FIRESTORE_PATHS.HISTORY), orderBy('startTime', 'asc'), startAfter(startAfterDoc));
    }
    else {
        // If no cursor is available (history was empty during initial sync), 
        // we listen to the entire history collection.
        // This ensures we catch any segments that might have been created 
        // between the initial sync check and the listener registration.
        // Redundant segments will be filtered out by isItemRedundant() in the callback.
        q = query(collection(db, path, FIRESTORE_PATHS.HISTORY), orderBy('startTime', 'asc'));
    }
    return onSnapshot(q, (snapshot) => {
        snapshot.docChanges().forEach((change) => {
            var _a, _b, _c;
            if (change.type === 'added') {
                const data = change.doc.data();
                const docId = change.doc.id;
                // Skip quarantined poison pills
                if ((_a = ctx.corruptedDocIds) === null || _a === void 0 ? void 0 : _a.has(docId)) {
                    return;
                }
                if (data && data.segment) {
                    try {
                        // Check redundancy using local state vector
                        const localSVMap = Y.decodeStateVector(Y.encodeStateVector(ydoc));
                        const item = {
                            type: 'history',
                            data: data,
                            priority: 2
                        };
                        if (!isItemRedundant(item, localSVMap)) {
                            // Apply it
                            Y.applyUpdate(ydoc, data.segment.toUint8Array(), FIREBASE_ORIGINS.HISTORY);
                        }
                    }
                    catch (err) {
                        console.error(`Failed to apply history segment ${docId} (quarantined)`, err);
                        (_b = ctx.corruptedDocIds) === null || _b === void 0 ? void 0 : _b.add(docId);
                        (_c = ctx.onCorruptedDocument) === null || _c === void 0 ? void 0 : _c.call(ctx, docId, err instanceof Error ? err : new Error(String(err)));
                    }
                }
            }
        });
    }, (error) => {
        console.error("History listener failed", error);
        if (onListenerError)
            onListenerError(error);
    });
}
// --- Helper Functions ---
/**
 * Extracts and aggregates clock values from an update document into the server state vector.
 * Tries stored metadata first, falls back to parsing the update blob.
 *
 * @param data - Firestore document data containing update and/or metadata
 * @param serverSVMap - Map to populate with client -> clock mappings
 */
function processUpdateMetadata(data, serverSVMap) {
    var _a, _b;
    if (((_a = data.clientIDs) === null || _a === void 0 ? void 0 : _a.length) > 0 && ((_b = data.clientClocks) === null || _b === void 0 ? void 0 : _b.length) > 0) {
        data.clientIDs.forEach((cid, i) => {
            const clock = data.clientClocks[i];
            const current = serverSVMap.get(cid) || 0;
            if (clock > current) {
                serverSVMap.set(cid, clock);
            }
        });
    }
    else if (data.update) {
        try {
            const updateBlob = data.update.toUint8Array();
            const metas = extractAllMetadata(updateBlob);
            metas.forEach(meta => {
                const current = serverSVMap.get(meta.clientID) || 0;
                if (meta.clockEnd > current) {
                    serverSVMap.set(meta.clientID, meta.clockEnd);
                }
            });
        }
        catch (e) {
            console.warn("Failed to parse fallback metadata", e);
        }
    }
}
/**
 * Extracts clock values from a history segment into the server state vector.
 * Uses stateVector field if present, otherwise parses the segment blob.
 *
 * @param data - Firestore document data containing history segment
 * @param serverSVMap - Map to populate with client -> clock mappings
 */
function processHistoryMetadata(data, serverSVMap) {
    if (data.stateVector) {
        const vector = fromBase64(data.stateVector);
        const map = Y.decodeStateVector(vector);
        for (const [client, clock] of map.entries()) {
            const current = serverSVMap.get(client) || 0;
            if (clock > current) {
                serverSVMap.set(client, clock);
            }
        }
    }
    else if (data.segment) {
        try {
            const segmentBlob = data.segment.toUint8Array();
            const metas = extractAllMetadata(segmentBlob);
            metas.forEach(meta => {
                const current = serverSVMap.get(meta.clientID) || 0;
                if (meta.clockEnd > current) {
                    serverSVMap.set(meta.clientID, meta.clockEnd);
                }
            });
        }
        catch (e) {
            console.warn("Failed to parse fallback history segment", e);
        }
    }
}
/**
 * Extracts clock values from the base snapshot into the server state vector.
 * Only uses the stateVector field (snapshots always have this).
 *
 * @param data - Firestore document data from the main document
 * @param serverSVMap - Map to populate with client -> clock mappings
 */
function processSnapshotMetadata(data, serverSVMap) {
    if (data.stateVector) {
        const vector = fromBase64(data.stateVector);
        const map = Y.decodeStateVector(vector);
        for (const [client, clock] of map.entries()) {
            const current = serverSVMap.get(client) || 0;
            if (clock > current) {
                serverSVMap.set(client, clock);
            }
        }
    }
}
/**
 * Determines if a pending update is already contained in the local document.
 * Uses clock comparison to avoid re-applying known data.
 *
 * P1.3 FIX: Now handles history segments with stateVector field.
 *
 * @param item - The pending update to check
 * @param localSVMap - Local document's state vector
 * @returns true if local document already has all data from this item
 */
function isItemRedundant(item, localSVMap) {
    var _a, _b;
    if (item.type === 'snapshot' && item.data.stateVector) {
        const sv = fromBase64(item.data.stateVector);
        const map = Y.decodeStateVector(sv);
        for (const [client, clock] of map) {
            const localClock = localSVMap.get(client) || 0;
            if (clock > localClock)
                return false;
        }
        return true;
    }
    // P1.3 FIX: Handle history segments with stateVector
    if (item.type === 'history' && item.data.stateVector) {
        try {
            const sv = fromBase64(item.data.stateVector);
            const map = Y.decodeStateVector(sv);
            for (const [client, clock] of map) {
                const localClock = localSVMap.get(client) || 0;
                if (clock > localClock)
                    return false;
            }
            return true;
        }
        catch (e) {
            // If stateVector parsing fails, treat as not redundant
            return false;
        }
    }
    if (item.type === 'update') {
        const data = item.data;
        if (((_a = data.clientIDs) === null || _a === void 0 ? void 0 : _a.length) > 0 && ((_b = data.clientClocks) === null || _b === void 0 ? void 0 : _b.length) > 0) {
            return isUpdateRedundant(localSVMap, data.clientIDs, data.clientClocks);
        }
    }
    return false;
}
/**
 * Applies a pending update to the Yjs document.
 * Handles different update types (snapshot, history, update) appropriately.
 *
 * @param item - The pending update to apply
 * @param ydoc - Target Yjs document
 * @returns true if update was successfully applied
 */
function applyItem(item, ydoc) {
    try {
        if (item.type === 'snapshot' && item.data.content) {
            Y.applyUpdate(ydoc, item.data.content.toUint8Array(), FIREBASE_ORIGINS.SNAPSHOT);
            return true;
        }
        else if (item.type === 'history' && item.data.segment) {
            Y.applyUpdate(ydoc, item.data.segment.toUint8Array(), FIREBASE_ORIGINS.HISTORY);
            return true;
        }
        else if (item.type === 'update' && item.data.update) {
            Y.applyUpdate(ydoc, item.data.update.toUint8Array(), FIREBASE_ORIGINS.UPDATE);
            return true;
        }
    }
    catch (e) {
        console.error(`Failed to apply ${item.type}`, e);
    }
    return false;
}
