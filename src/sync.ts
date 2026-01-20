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
} from "@firebase/firestore";
import * as Y from "yjs";
import { fromBase64 } from "lib0/buffer";
import {
    UpdateMetadata,
    FIREBASE_ORIGINS,
    FIRESTORE_PATHS
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

    try {
        const serverSVMap = new Map<number, number>();
        const pendingUpdates: PendingUpdate[] = [];
        let updatesApplied = 0;

        // 1. Fetch Updates (Tier 3)
        const updatesQ = query(
            collection(db, path, FIRESTORE_PATHS.UPDATES),
            orderBy('createdAt', 'asc')
        );
        const updatesSnap = await getDocs(updatesQ);
        if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false };

        updatesSnap.forEach(snap => {
            const data = snap.data();
            if (data) {
                processUpdateMetadata(data, serverSVMap);
                pendingUpdates.push({ type: 'update', data, priority: 3 });
            }
        });

        // 2. Fetch History Segments (Tier 2)
        const historyQ = query(
            collection(db, path, FIRESTORE_PATHS.HISTORY),
            orderBy('startTime', 'asc')
        );
        const historySnaps = await getDocs(historyQ);
        if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false };

        historySnaps.forEach(snap => {
            const data = snap.data();
            if (data) {
                processHistoryMetadata(data, serverSVMap);
                pendingUpdates.push({ type: 'history', data, priority: 2 });
            }
        });

        // 3. Fetch Base Snapshot (Tier 1)
        const mainRef = doc(db, path);
        const mainSnap = await getDoc(mainRef);
        if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false };

        if (mainSnap.exists()) {
            const data = mainSnap.data();
            if (data) {
                processSnapshotMetadata(data, serverSVMap);
                if (data.stateVector || data.content) {
                    pendingUpdates.push({ type: 'snapshot', data, priority: 1 });
                }
            }
        }

        // 4. Apply missing data
        const localSV = Y.encodeStateVector(ydoc);
        const localSVMap = Y.decodeStateVector(localSV);

        // Sort by priority (Snapshot first, then History, then Updates)
        pendingUpdates.sort((a, b) => a.priority - b.priority);

        for (const item of pendingUpdates) {
            if (isDestroyed()) break;

            if (!isItemRedundant(item, localSVMap)) {
                const applied = applyItem(item, ydoc);
                if (applied) updatesApplied++;
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

        return { success: true, updatesApplied, localUpdatesPushed };
    } catch (err) {
        console.error("Sync failed", err);
        return {
            success: false,
            error: err instanceof Error ? err : new Error(String(err)),
            updatesApplied: 0,
            localUpdatesPushed: false
        };
    }
}

/**
 * Creates a real-time listener for new updates.
 * 
 * @param ctx - Sync context
 * @returns Unsubscribe function
 */
export function createUpdateListener(ctx: SyncContext): Unsubscribe {
    const { db, path, doc: ydoc, uid, maxUpdatesThreshold, compactionProbability, onCompactionNeeded, isDestroyed } = ctx;

    const liveUpdatesQ = query(
        collection(db, path, FIRESTORE_PATHS.UPDATES),
        orderBy('createdAt', 'asc')
    );

    return onSnapshot(liveUpdatesQ, (snapshot) => {
        // Check for compaction trigger
        if (snapshot.size > maxUpdatesThreshold && onCompactionNeeded) {
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
        // Retry logic handled by caller
    });
}

// --- Helper Functions ---

function processUpdateMetadata(data: any, serverSVMap: Map<number, number>): void {
    if (typeof data.clientID === 'number' && typeof data.clockEnd === 'number') {
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

    if (item.type === 'update') {
        if (item.data.clientID !== undefined && item.data.clockEnd !== undefined) {
            const localClock = localSVMap.get(item.data.clientID) || 0;
            return localClock >= item.data.clockEnd;
        }
    }

    return false;
}

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
