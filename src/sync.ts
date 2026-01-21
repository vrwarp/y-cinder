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

import {
    Firestore,
    Unsubscribe,
    onSnapshot,
    doc,
    collection,
    addDoc,
    Bytes,
    query,
    orderBy,
    getDocs,
    getDoc,
    serverTimestamp,
    limit,
    startAfter,
    limitToLast,
    QueryDocumentSnapshot,
} from "@firebase/firestore";
import * as Y from "yjs";
import { fromBase64 } from "lib0/buffer";
import {
    UpdateMetadata,
    FIREBASE_ORIGINS,
    FIRESTORE_PATHS,
    DEFAULTS,
} from "./types";
import { writeStateVector } from "./utils";
import { extractAllMetadata, aggregateMetadata, isUpdateRedundant } from "./update-metadata";

/**
 * Context required for sync operations.
 */
export interface SyncContext {
    /** Firestore instance */
    db: Firestore;
    /** Base document path */
    path: string;
    /** The Yjs document to sync */
    doc: Y.Doc;
    /** Unique client ID */
    uid: string;
    /** Maximum updates before triggering compaction consideration */
    maxUpdatesThreshold: number;
    /** Probability of attempting compaction */
    compactionProbability: number;
    /** Callback to trigger compaction */
    onCompactionNeeded?: () => void;
    /** P1.7 FIX: Callback when listener encounters an error */
    onListenerError?: (error: Error) => void;
    /** Flag to check if provider is destroyed */
    isDestroyed: () => boolean;
}

/**
 * Result of initial sync operation.
 */
export interface SyncResult {
    /** Whether sync completed successfully */
    success: boolean;
    /** Error if sync failed */
    error?: Error;
    /** Number of updates applied */
    updatesApplied: number;
    /** Whether local updates were pushed */
    localUpdatesPushed: boolean;
    /** The last document observed during sync, used as a cursor for the listener */
    lastSyncedDoc: QueryDocumentSnapshot | null;
}

/**
 * Pending update item during sync.
 */
interface PendingUpdate {
    type: 'snapshot' | 'history' | 'update';
    data: any;
    priority: number;
}

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
 *   compactionProbability: 0.01,
 *   isDestroyed: () => false
 * });
 * ```
 */
export async function performInitialSync(ctx: SyncContext): Promise<SyncResult> {
    const { db, path, doc: ydoc, uid, isDestroyed } = ctx;
    const BATCH_SIZE = DEFAULTS.SYNC_BATCH_SIZE;

    try {
        const serverSVMap = new Map<number, number>();
        const pendingUpdates: PendingUpdate[] = [];
        let updatesApplied = 0;

        // 1. Fetch Updates (Tier 3) with pagination (P0.1 fix)
        let lastUpdateDoc: QueryDocumentSnapshot | null = null;
        let hasMoreUpdates = true;

        while (hasMoreUpdates) {
            const updatesQ = lastUpdateDoc
                ? query(
                    collection(db, path, FIRESTORE_PATHS.UPDATES),
                    orderBy('createdAt', 'asc'),
                    startAfter(lastUpdateDoc),
                    limit(BATCH_SIZE)
                )
                : query(
                    collection(db, path, FIRESTORE_PATHS.UPDATES),
                    orderBy('createdAt', 'asc'),
                    limit(BATCH_SIZE)
                );

            const updatesSnap = await getDocs(updatesQ);
            if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null };

            if (updatesSnap.empty) {
                hasMoreUpdates = false;
            } else {
                updatesSnap.forEach(snap => {
                    const data = snap.data();
                    if (data) {
                        processUpdateMetadata(data, serverSVMap);
                        pendingUpdates.push({ type: 'update', data, priority: 3 });
                    }
                });

                // FIX: Verify cursor is committed to avoid "Invalid query" with pending serverTimestamp
                let candidateDoc: QueryDocumentSnapshot | null = updatesSnap.docs[updatesSnap.docs.length - 1];
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
        let lastHistoryDoc: QueryDocumentSnapshot | null = null;
        let hasMoreHistory = true;

        while (hasMoreHistory) {
            const historyQ = lastHistoryDoc
                ? query(
                    collection(db, path, FIRESTORE_PATHS.HISTORY),
                    orderBy('startTime', 'asc'),
                    startAfter(lastHistoryDoc),
                    limit(BATCH_SIZE)
                )
                : query(
                    collection(db, path, FIRESTORE_PATHS.HISTORY),
                    orderBy('startTime', 'asc'),
                    limit(BATCH_SIZE)
                );

            const historySnap = await getDocs(historyQ);
            if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, lastHistoryDoc: null };

            if (historySnap.empty) {
                hasMoreHistory = false;
            } else {
                historySnap.forEach(snap => {
                    const data = snap.data();
                    if (data && data.segment) {
                        processHistoryMetadata(data, serverSVMap);
                        pendingUpdates.push({ type: 'history', data, priority: 2 });
                    }
                });

                // FIX: Verify cursor is committed
                let candidateDoc: QueryDocumentSnapshot | null = historySnap.docs[historySnap.docs.length - 1];
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
        const mainSnap = await getDoc(mainRef);
        if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null };

        if (mainSnap.exists()) {
            const data = mainSnap.data();
            if (data) {
                processSnapshotMetadata(data, serverSVMap);
                if (data.stateVector || data.content) {
                    pendingUpdates.push({ type: 'snapshot', data, priority: 1 });
                }
            }
        }

        // 4. Apply missing data with state vector refresh (P0.4 fix)
        let localSVMap = Y.decodeStateVector(Y.encodeStateVector(ydoc));

        // Sort by priority (Snapshot first, then History, then Updates)
        pendingUpdates.sort((a, b) => a.priority - b.priority);

        for (const item of pendingUpdates) {
            if (isDestroyed()) break;

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
            const pkg: any = {
                update: Bytes.fromUint8Array(localDiff),
                createdAt: serverTimestamp(),
                createdBy: uid,
                ...aggregateMetadata(metas)
            };
            await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), pkg);
            localUpdatesPushed = true;
        }

        return {
            success: true,
            updatesApplied,
            localUpdatesPushed,
            lastSyncedDoc: lastUpdateDoc,
            lastHistoryDoc
        };
    } catch (err) {
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
export function createUpdateListener(ctx: SyncContext, startAfterDoc: QueryDocumentSnapshot | null = null): Unsubscribe {
    const { db, path, doc: ydoc, uid, maxUpdatesThreshold, compactionProbability, onCompactionNeeded, onListenerError, isDestroyed } = ctx;

    let liveUpdatesQ;

    if (startAfterDoc) {
        // P1.9 FIX: Continue exactly where sync left off to prevent "Sync Gap"
        liveUpdatesQ = query(
            collection(db, path, FIRESTORE_PATHS.UPDATES),
            orderBy('createdAt', 'asc'),
            startAfter(startAfterDoc)
        );
    } else {
        // Fallback for fresh docs (or rely on sync to have found nothing)
        // If sync found nothing, we start from the beginning.
        // P0.2 NOTE: Removed limitToLast because we assume initial sync caught everything up to "now"
        // or there was nothing. If there was nothing, we want everything new.
        liveUpdatesQ = query(
            collection(db, path, FIRESTORE_PATHS.UPDATES),
            orderBy('createdAt', 'asc')
        );
    }

    return onSnapshot(liveUpdatesQ, (snapshot) => {
        // Check for compaction trigger based on actual size
        // Note: snapshot.size may be capped by limitToLast, so we check docChanges for additions
        if (snapshot.size >= DEFAULTS.REALTIME_LIMIT && onCompactionNeeded) {
            // At capacity - definitely need compaction
            if (Math.random() < compactionProbability) {
                onCompactionNeeded();
            }
        } else if (snapshot.size > maxUpdatesThreshold && onCompactionNeeded) {
            if (Math.random() < compactionProbability) {
                onCompactionNeeded();
            }
        }

        snapshot.docChanges().forEach((change) => {
            if (change.type === 'added') {
                const data = change.doc.data();

                // Skip our own updates
                if (data.createdBy === uid) {
                    return;
                }

                // Check if we already have this update
                const clientIDs = data.clientIDs || (typeof data.clientID === 'number' ? [data.clientID] : []);
                if (clientIDs.length > 0 && typeof data.clockEnd === 'number') {
                    const freshSV = Y.encodeStateVector(ydoc);
                    const freshMap = Y.decodeStateVector(freshSV);

                    if (isUpdateRedundant(freshMap, clientIDs, data.clockEnd)) {
                        return; // Skip - we have all the data
                    }
                }

                if (data.update) {
                    try {
                        const update = (data.update as Bytes).toUint8Array();
                        Y.applyUpdate(ydoc, update, FIREBASE_ORIGINS.UPDATE);
                    } catch (e) {
                        console.error("Failed to apply update", e);
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
export function createSnapshotListener(ctx: SyncContext): Unsubscribe {
    const { db, path, doc: ydoc, onListenerError } = ctx;

    return onSnapshot(doc(db, path), (snapshot) => {
        if (!snapshot.exists()) return;

        const data = snapshot.data();
        if (data?.content && (data.origin !== undefined ? data.origin !== ctx.uid : true)) {
            // Apply snapshot if it's from a remote origin or if we need to sync up
            try {
                // We should check redundancy but applying a snapshot is generally safe/idempotent in Yjs
                // as long as we don't overwrite local pending changes that aren't in the snapshot yet.
                // However, a snapshot represents valid state at a specific point.
                // A safer check: if stateVector of snapshot is "ahead" or strictly different?
                // For now, simpler approach: Apply it. Yjs handles deduplication.
                const content = (data.content as Bytes).toUint8Array();
                Y.applyUpdate(ydoc, content, FIREBASE_ORIGINS.SNAPSHOT);
            } catch (err) {
                console.error("Failed to apply snapshot from listener", err);
            }
        }
    }, (error) => {
        console.error("Snapshot listener failed", error);
        if (onListenerError) onListenerError(error);
    });
}

/**
 * Creates a real-time listener for new history segments.
 * 
 * Uses the last known history document as a cursor to only fetch NEW segments.
 */
export function createHistoryListener(ctx: SyncContext, startAfterDoc: QueryDocumentSnapshot | null): Unsubscribe {
    const { db, path, doc: ydoc, onListenerError } = ctx;

    let q;
    if (startAfterDoc) {
        q = query(
            collection(db, path, FIRESTORE_PATHS.HISTORY),
            orderBy('startTime', 'asc'),
            startAfter(startAfterDoc)
        );
    } else {
        // If no cursor is available (history was empty during initial sync), 
        // we listen to the entire history collection.
        // This ensures we catch any segments that might have been created 
        // between the initial sync check and the listener registration.
        // Redundant segments will be filtered out by isItemRedundant() in the callback.
        q = query(
            collection(db, path, FIRESTORE_PATHS.HISTORY),
            orderBy('startTime', 'asc')
        );
    }

    return onSnapshot(q, (snapshot) => {
        snapshot.docChanges().forEach((change) => {
            if (change.type === 'added') {
                const data = change.doc.data();
                if (data && data.segment) {
                    try {
                        // Check redundancy using local state vector
                        const localSVMap = Y.decodeStateVector(Y.encodeStateVector(ydoc));
                        const item: PendingUpdate = {
                            type: 'history',
                            data: data as any,
                            priority: 2
                        };

                        if (!isItemRedundant(item, localSVMap)) {
                            // Apply it
                            Y.applyUpdate(ydoc, (data.segment as Bytes).toUint8Array(), FIREBASE_ORIGINS.HISTORY);
                        }
                    } catch (err) {
                        console.error("Failed to apply history segment from listener", err);
                    }
                }
            }
        });
    }, (error) => {
        console.error("History listener failed", error);
        if (onListenerError) onListenerError(error);
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
function processUpdateMetadata(data: any, serverSVMap: Map<number, number>): void {
    // P1.9 FIX: Use clientIDs array if available
    if (data.clientIDs && data.clientIDs.length > 0 && typeof data.clockEnd === 'number') {
        data.clientIDs.forEach((cid: number) => {
            const current = serverSVMap.get(cid) || 0;
            if (data.clockEnd > current) {
                serverSVMap.set(cid, data.clockEnd);
            }
        });
    } else if (typeof data.clientID === 'number' && typeof data.clockEnd === 'number') {
        const current = serverSVMap.get(data.clientID) || 0;
        if (data.clockEnd > current) {
            serverSVMap.set(data.clientID, data.clockEnd);
        }
    } else if (data.update) {
        try {
            const updateBlob = (data.update as Bytes).toUint8Array();
            const metas = extractAllMetadata(updateBlob);
            metas.forEach(meta => {
                const current = serverSVMap.get(meta.clientID) || 0;
                if (meta.clockEnd > current) {
                    serverSVMap.set(meta.clientID, meta.clockEnd);
                }
            });
        } catch (e) {
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
function processHistoryMetadata(data: any, serverSVMap: Map<number, number>): void {
    if (data.stateVector) {
        const vector = fromBase64(data.stateVector);
        const map = Y.decodeStateVector(vector);
        for (const [client, clock] of map.entries()) {
            const current = serverSVMap.get(client) || 0;
            if (clock > current) {
                serverSVMap.set(client, clock);
            }
        }
    } else if (data.segment) {
        try {
            const segmentBlob = (data.segment as Bytes).toUint8Array();
            const metas = extractAllMetadata(segmentBlob);
            metas.forEach(meta => {
                const current = serverSVMap.get(meta.clientID) || 0;
                if (meta.clockEnd > current) {
                    serverSVMap.set(meta.clientID, meta.clockEnd);
                }
            });
        } catch (e) {
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
function processSnapshotMetadata(data: any, serverSVMap: Map<number, number>): void {
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
function isItemRedundant(item: PendingUpdate, localSVMap: Map<number, number>): boolean {
    if (item.type === 'snapshot' && item.data.stateVector) {
        const sv = fromBase64(item.data.stateVector);
        const map = Y.decodeStateVector(sv);
        for (const [client, clock] of map) {
            const localClock = localSVMap.get(client) || 0;
            if (clock > localClock) return false;
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
                if (clock > localClock) return false;
            }
            return true;
        } catch (e) {
            // If stateVector parsing fails, treat as not redundant
            return false;
        }
    }

    if (item.type === 'update') {
        const data = item.data;
        // P1.9 FIX: Check all client IDs if available
        if (data.clientIDs && data.clientIDs.length > 0 && typeof data.clockEnd === 'number') {
            return isUpdateRedundant(localSVMap, data.clientIDs, data.clockEnd);
        }

        // Fallback to single client ID (backwards compat)
        if (data.clientID !== undefined && data.clockEnd !== undefined) {
            const localClock = localSVMap.get(data.clientID) || 0;
            return localClock >= data.clockEnd;
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
function applyItem(item: PendingUpdate, ydoc: Y.Doc): boolean {
    try {
        if (item.type === 'snapshot' && item.data.content) {
            Y.applyUpdate(ydoc, (item.data.content as Bytes).toUint8Array(), FIREBASE_ORIGINS.SNAPSHOT);
            return true;
        } else if (item.type === 'history' && item.data.segment) {
            Y.applyUpdate(ydoc, (item.data.segment as Bytes).toUint8Array(), FIREBASE_ORIGINS.HISTORY);
            return true;
        } else if (item.type === 'update' && item.data.update) {
            Y.applyUpdate(ydoc, (item.data.update as Bytes).toUint8Array(), FIREBASE_ORIGINS.UPDATE);
            return true;
        }
    } catch (e) {
        console.error(`Failed to apply ${item.type}`, e);
    }
    return false;
}
