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
 * - Triggers compaction when threshold exceeded (rate-limited per client)
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
import { getBytes, ref, uploadBytes, FirebaseStorage } from "@firebase/storage";
import * as Y from "yjs";
import { fromBase64 } from "lib0/buffer";
import {
    FIREBASE_ORIGINS,
    FIRESTORE_PATHS,
    DEFAULTS,
} from "./types";
import { writeStateVector } from "./utils";
import { extractClockEnds, aggregateClockEnds, isUpdateRedundant, diffCarriesNewData, deleteSetCoveredByBlobs } from "./update-metadata";
import { readDocEpoch, docHasContent } from "./squash";

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
    /** Callback to trigger compaction */
    onCompactionNeeded?: () => void;
    /** P1.7 FIX: Callback when listener encounters an error */
    onListenerError?: (error: Error) => void;
    /** Flag to check if provider is destroyed */
    isDestroyed: () => boolean;
    /** Firebase Storage instance */
    storage: FirebaseStorage;
    /**
     * Per-session quarantine set of Firestore document IDs / storage paths
     * that have failed Y.applyUpdate due to structural corruption.
     * Prevents infinite retry loops on "poison pill" documents.
     */
    corruptedDocIds?: Set<string>;
    /**
     * Callback when a corrupted document is quarantined.
     * Allows the application layer to log, alert, or take action.
     */
    onCorruptedDocument?: (docId: string, error: Error) => void;
    /**
     * Current epoch of the local document (see squash.ts). Listeners drop
     * update/history documents from foreign epochs — data written before a
     * squash must never merge into a post-squash document (the id spaces
     * are unrelated; Yjs would park it as missing dependencies forever).
     * Absent callback = epoch 0 (documents that never squashed).
     */
    getEpoch?: () => number;
    /**
     * Fired when the server's main document is at a NEWER epoch than the
     * local document: someone squashed. The local doc must not receive the
     * new snapshot (its content would duplicate); the provider surfaces
     * this so the application can rebuild from the new epoch.
     */
    onEpochChanged?: (serverEpoch: number) => void;
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
    /** The last history document observed during sync, used as a cursor for history listener */
    lastHistoryDoc: QueryDocumentSnapshot | null;
    /**
     * The main document's compaction version at sync time (null when the
     * main document does not exist / predates versioning). Passed to the
     * snapshot listener so its initial delivery — the state initial sync
     * just processed — is skipped instead of re-applying the delete-set
     * fingerprint (an O(delete-set) cost that grows with document age).
     */
    snapshotVersion: number | null;
    /** Server epoch at sync time (0 for documents that never squashed) */
    epoch: number;
    /**
     * Set when the server is at a newer epoch than the non-empty local
     * document. Nothing was applied or pushed; the provider must surface
     * an epoch-changed event instead of retrying.
     */
    epochConflict?: { serverEpoch: number; localEpoch: number };
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
            if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, lastHistoryDoc: null, snapshotVersion: null, epoch: 0 };

            if (updatesSnap.empty) {
                hasMoreUpdates = false;
            } else {
                for (const snap of updatesSnap.docs) {
                    const data = snap.data();
                    if (data) {
                        // Download storage-backed update if present
                        if (data.updateStoragePath && !data.update) {
                            try {
                                const storageRef = ref(ctx.storage, data.updateStoragePath);
                                const buffer = await getBytes(storageRef);
                                data.update = Bytes.fromUint8Array(new Uint8Array(buffer));
                            } catch (storageErr) {
                                console.error(`Failed to download storage-backed update: ${data.updateStoragePath}`, storageErr);
                                continue; // Skip this update — cannot apply without data
                            }
                        }
                        // Metadata is folded into the server state vector
                        // only after the epoch is known (main doc read) —
                        // foreign-epoch documents must not contribute.
                        pendingUpdates.push({ type: 'update', data, priority: 3 });
                    }
                }

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
            if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, lastHistoryDoc: null, snapshotVersion: null, epoch: 0 };

            if (historySnap.empty) {
                hasMoreHistory = false;
            } else {
                historySnap.forEach(snap => {
                    const data = snap.data();
                    if (data && data.segment) {
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
        if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, lastHistoryDoc: null, snapshotVersion: null, epoch: 0 };

        let snapshotVersion: number | null = null;
        let serverEpoch = 0;
        if (mainSnap.exists()) {
            const data = mainSnap.data();
            if (data) {
                if (typeof data.version === 'number') {
                    snapshotVersion = data.version;
                }
                if (typeof data.epoch === 'number') {
                    serverEpoch = data.epoch;
                }

                // Epoch fence: the server was squashed past this document's
                // history. Applying the new snapshot here would DUPLICATE
                // content (unrelated id spaces), so nothing is applied or
                // pushed — the provider surfaces epoch-changed and the app
                // rebuilds its local doc from the new epoch.
                const localEpoch = readDocEpoch(ydoc);
                if (serverEpoch > localEpoch && docHasContent(ydoc)) {
                    return {
                        success: false,
                        updatesApplied: 0,
                        localUpdatesPushed: false,
                        lastSyncedDoc: null,
                        lastHistoryDoc: null,
                        snapshotVersion,
                        epoch: serverEpoch,
                        epochConflict: { serverEpoch, localEpoch },
                    };
                }

                processSnapshotMetadata(data, serverSVMap);

                // The delete-set fingerprint written by compaction is a
                // structs-empty update. Treating it as a regular update both
                // proves delete-set coverage to the push guard below and
                // applies any deletions the local doc may have missed.
                if (data.deleteSet) {
                    pendingUpdates.push({ type: 'update', data: { update: data.deleteSet, epoch: serverEpoch }, priority: 2 });
                } else if (data.deleteSetStoragePath) {
                    // Fingerprint outgrew the inline cap and was offloaded to
                    // Cloud Storage. Download it: it is O(delete-set) and its
                    // absence would cost far more — the push guard could not
                    // prove coverage, so EVERY reconnect would write a
                    // spurious O(delete-set) update document.
                    try {
                        const buffer = await getBytes(ref(ctx.storage, data.deleteSetStoragePath));
                        pendingUpdates.push({
                            type: 'update',
                            data: { update: Bytes.fromUint8Array(new Uint8Array(buffer)), epoch: serverEpoch },
                            priority: 2,
                        });
                    } catch (dsErr) {
                        // Coverage falls back to the other server blobs; worst
                        // case is a redundant (idempotent) push.
                        console.warn("Failed to download delete-set fingerprint", dsErr);
                    }
                }

                // Fetch snapshot from Cloud Storage if available
                if (data.snapshotStoragePath) {
                    // Skip the (potentially large) blob download when the
                    // local doc already covers the snapshot's state vector —
                    // typical for reconnecting clients.
                    if (!localCoversSnapshot(data, ydoc)) {
                        try {
                            const storageRef = ref(ctx.storage, data.snapshotStoragePath);
                            const buffer = await getBytes(storageRef);
                            // Convert ArrayBuffer to Uint8Array and inject it into data.content
                            data.content = Bytes.fromUint8Array(new Uint8Array(buffer));
                            pendingUpdates.push({ type: 'snapshot', data, priority: 1 });
                        } catch (storageErr) {
                            console.error("Failed to download snapshot from Cloud Storage", storageErr);
                            // Cannot safely sync without the base snapshot — propagate so
                            // the sync retry logic in provider.ts handles backoff/retry.
                            throw storageErr;
                        }
                    }
                } else if (data.stateVector || data.content) {
                    // Fallback for older documents that haven't been compacted into Cloud Storage yet
                    pendingUpdates.push({ type: 'snapshot', data, priority: 1 });
                }
            }
        }

        // 3b. Epoch filter + server state vector. Update/history documents
        // from foreign epochs are dropped: their structs belong to an
        // unrelated id space (pre-squash) and would sit in pendingStructs
        // forever, poisoning GC compaction. Metadata therefore only enters
        // the server state vector for same-epoch items — and the fence
        // must run BEFORE metadata processing so a stale update can never
        // suppress the initial-sync push.
        for (let i = pendingUpdates.length - 1; i >= 0; i--) {
            const item = pendingUpdates[i];
            if (item.type === 'snapshot') continue; // main doc = current epoch
            const itemEpoch = typeof item.data.epoch === 'number' ? item.data.epoch : 0;
            if (itemEpoch !== serverEpoch) {
                pendingUpdates.splice(i, 1);
            }
        }
        for (const item of pendingUpdates) {
            if (item.type === 'history') {
                processHistoryMetadata(item.data, serverSVMap);
            } else if (item.type === 'update') {
                processUpdateMetadata(item.data, serverSVMap);
            }
        }

        // 4. Apply missing data with state vector refresh (P0.4 fix)
        let localSVMap = Y.decodeStateVector(Y.encodeStateVector(ydoc));

        // Sort by priority (Snapshot first, then History, then Updates)
        pendingUpdates.sort((a, b) => a.priority - b.priority);

        // Apply everything inside a single Yjs transaction. Each top-level
        // applyItem call would otherwise commit its own transaction, firing
        // doc observers and encoding an 'update' event per blob — on a
        // long-lived document with hundreds of pending items that means
        // hundreds of editor re-renders during initial load instead of one.
        ydoc.transact(() => {
            for (const item of pendingUpdates) {
                if (isDestroyed()) break;

                if (!isItemRedundant(item, localSVMap)) {
                    const applied = applyItem(item, ydoc);
                    if (applied) {
                        updatesApplied++;
                        // Incremental update of localSVMap instead of expensive re-encode/decode (P3.0 Optimization)
                        // This prevents redundant processing of history/updates already in snapshot/previous segments
                        if (item.type === 'snapshot') {
                            processSnapshotMetadata(item.data, localSVMap);
                        } else if (item.type === 'history') {
                            processHistoryMetadata(item.data, localSVMap);
                        } else if (item.type === 'update') {
                            processUpdateMetadata(item.data, localSVMap);
                        }
                    }
                }
            }
        }, FIREBASE_ORIGINS.UPDATE);

        // 5. Push Missing Local Updates
        //
        // Fast path (aged-document fix): Y.encodeStateAsUpdate(ydoc, sv)
        // embeds the document's FULL delete-set, so its cost grows with
        // total historical churn — paying it on every reconnect of every
        // client makes startup linearly slower with age. When the server
        // state vector already covers every local struct, the diff would
        // be structs-empty anyway, and pushability is decided purely by
        // delete-set coverage — provable straight from the struct store
        // and the snapshot's fingerprint without encoding anything.
        const serverSV = writeStateVector(serverSVMap);
        let localUpdatesPushed = false;

        const exactLocalSV = Y.decodeStateVector(Y.encodeStateVector(ydoc));
        let serverCoversStructs = true;
        for (const [client, clock] of exactLocalSV) {
            if ((serverSVMap.get(client) || 0) < clock) {
                serverCoversStructs = false;
                break;
            }
        }

        let shouldPush: boolean;
        let localDiff: Uint8Array | null = null;
        if (serverCoversStructs) {
            const localDs = Y.createDeleteSetFromStructStore((ydoc as any).store);
            shouldPush = !deleteSetCoveredByBlobs(localDs, () => collectServerBlobs(pendingUpdates));
            if (shouldPush) {
                // Rare: local deletion-only changes the server lacks.
                localDiff = Y.encodeStateAsUpdate(ydoc, serverSV);
            }
        } else {
            localDiff = Y.encodeStateAsUpdate(ydoc, serverSV);
            // The diff has structs by construction (a local clock exceeds
            // the server's), so it always carries new data.
            shouldPush = localDiff.byteLength > 2 &&
                diffCarriesNewData(localDiff, () => collectServerBlobs(pendingUpdates));
        }

        if (shouldPush && localDiff !== null) {
            console.log("Pushing missing local updates to Firestore.");
            const clockEnds = extractClockEnds(localDiff);
            const epochTag = serverEpoch > 0 ? { epoch: serverEpoch } : {};

            if (localDiff.byteLength > DEFAULTS.INLINE_UPDATE_LIMIT) {
                // Storage-backed update: upload binary to Cloud Storage
                const storagePath = `${path}/large_updates/${uid}_${Date.now()}.bin`;
                const storageRef = ref(ctx.storage, storagePath);
                await uploadBytes(storageRef, localDiff);

                // Write lightweight pointer document to updates collection
                const pkg: any = {
                    updateStoragePath: storagePath,
                    createdAt: serverTimestamp(),
                    createdBy: uid,
                    ...epochTag,
                    ...aggregateClockEnds(clockEnds)
                };
                await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), pkg);
                console.log(`Oversized initial sync diff (${localDiff.byteLength} bytes) offloaded to Cloud Storage: ${storagePath}`);
            } else {
                // Standard inline update
                const pkg: any = {
                    update: Bytes.fromUint8Array(localDiff),
                    createdAt: serverTimestamp(),
                    createdBy: uid,
                    ...epochTag,
                    ...aggregateClockEnds(clockEnds)
                };
                await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), pkg);
            }
            localUpdatesPushed = true;
        }

        return {
            success: true,
            updatesApplied,
            localUpdatesPushed,
            lastSyncedDoc: lastUpdateDoc,
            lastHistoryDoc,
            snapshotVersion,
            epoch: serverEpoch
        };
    } catch (err) {
        console.error("Sync failed", err);
        return {
            success: false,
            error: err instanceof Error ? err : new Error(String(err)),
            updatesApplied: 0,
            localUpdatesPushed: false,
            lastSyncedDoc: null,
            lastHistoryDoc: null,
            snapshotVersion: null,
            epoch: 0
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
    const { db, path, doc: ydoc, uid, maxUpdatesThreshold, onCompactionNeeded, onListenerError, isDestroyed } = ctx;

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

    // Cache local state vector for redundancy checks (P3.0 Optimization)
    let localSVMap = Y.decodeStateVector(Y.encodeStateVector(ydoc));

    // Rate-limit compaction triggers: above the threshold, every snapshot
    // delivery on every client would otherwise fire a (mostly futile) lock
    // transaction. The first trigger fires immediately; subsequent triggers
    // are suppressed for a cooldown window unless the hard cap is reached.
    let lastCompactionTrigger = 0;

    return onSnapshot(liveUpdatesQ, (snapshot) => {
        if (onCompactionNeeded && snapshot.size > maxUpdatesThreshold) {
            const now = Date.now();
            const atHardCap = snapshot.size >= DEFAULTS.REALTIME_LIMIT;
            if (atHardCap || now - lastCompactionTrigger >= DEFAULTS.COMPACTION_TRIGGER_COOLDOWN_MS) {
                lastCompactionTrigger = now;
                onCompactionNeeded();
            }
        }

        // Collect inline-appliable updates from this delivery first, then
        // apply them in ONE Yjs transaction. Object-heavy workloads (e.g.
        // another client dragging shapes) arrive as bursts of small update
        // documents; applying each in its own top-level transaction fires
        // an observer flush + 'update' event (editor re-render) per doc.
        const inlineBatch: { docId: string; data: any }[] = [];

        snapshot.docChanges().forEach((change) => {
            if (change.type === 'added') {
                const data = change.doc.data();

                // Epoch fence: an update written before a squash belongs to
                // an unrelated id space — applying it would park garbage in
                // pendingStructs forever. Compaction deletes such documents.
                const docEpoch = typeof data.epoch === 'number' ? data.epoch : 0;
                if (docEpoch !== (ctx.getEpoch?.() ?? 0)) {
                    return;
                }

                // Skip our own updates
                if (data.createdBy === uid) {
                    // Update our cache even for our own updates so subsequent checks are accurate
                    processUpdateMetadata(data, localSVMap);
                    return;
                }

                // Check if we already have this update
                if (data.clientIDs?.length > 0 && data.clientClocks?.length > 0) {
                    if (isUpdateRedundant(localSVMap, data.clientIDs, data.clientClocks)) {
                        return; // Skip - we have all the data
                    }
                }

                const docId = change.doc.id;

                // Skip quarantined poison pills
                if (ctx.corruptedDocIds?.has(docId)) {
                    return;
                }

                // Handle storage-backed update (oversized diff offloaded to Cloud Storage)
                if (data.updateStoragePath && !data.update) {
                    (async () => {
                        try {
                            const storageRef = ref(ctx.storage, data.updateStoragePath);
                            const buffer = await getBytes(storageRef);
                            // Provider may have been destroyed while downloading
                            if (isDestroyed()) return;
                            const update = new Uint8Array(buffer);
                            Y.applyUpdate(ydoc, update, FIREBASE_ORIGINS.UPDATE);
                            // Incremental update of cached state vector (P3.0 Optimization)
                            processUpdateMetadata(data, localSVMap);
                        } catch (e) {
                            console.error(`Failed to apply storage-backed update ${docId} (quarantined)`, e);
                            ctx.corruptedDocIds?.add(docId);
                            ctx.onCorruptedDocument?.(docId, e instanceof Error ? e : new Error(String(e)));
                        }
                    })();
                    return;
                }

                if (data.update) {
                    inlineBatch.push({ docId, data });
                }
            }
        });

        if (inlineBatch.length > 0) {
            const applyBatch = () => {
                for (const { docId, data } of inlineBatch) {
                    // Re-check redundancy as the state vector evolves within
                    // the batch (preserves the sequential semantics)
                    if (data.clientIDs?.length > 0 && data.clientClocks?.length > 0 &&
                        isUpdateRedundant(localSVMap, data.clientIDs, data.clientClocks)) {
                        continue;
                    }
                    try {
                        const update = (data.update as Bytes).toUint8Array();
                        Y.applyUpdate(ydoc, update, FIREBASE_ORIGINS.UPDATE);
                        // Incremental update of cached state vector (P3.0 Optimization)
                        processUpdateMetadata(data, localSVMap);
                    } catch (e) {
                        console.error(`Failed to apply update ${docId} (quarantined)`, e);
                        ctx.corruptedDocIds?.add(docId);
                        ctx.onCorruptedDocument?.(docId, e instanceof Error ? e : new Error(String(e)));
                    }
                }
            };
            if (inlineBatch.length === 1) {
                applyBatch();
            } else {
                ydoc.transact(applyBatch, FIREBASE_ORIGINS.UPDATE);
            }
        }
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
 *
 * Before downloading the (potentially large) snapshot blob, the listener
 * compares the snapshot's stored state vector against the local document.
 * Snapshots produced by compacting data we already hold are skipped,
 * avoiding a full-document download per compaction per client (and a
 * duplicate download right after initial sync).
 *
 * @param initialVersion - The main document's compaction version already
 * processed by initial sync. Deliveries carrying the same version are
 * skipped wholesale — in particular the immediate delivery on listener
 * attach, which used to re-apply the delete-set fingerprint (O(delete-set),
 * a cost that grows with document age) on every reconnect.
 */
export function createSnapshotListener(ctx: SyncContext, initialVersion: number | null = null): Unsubscribe {
    const { db, path, doc: ydoc, isDestroyed, onListenerError, storage } = ctx;

    // Track the last quarantined snapshot path so we can clear quarantine
    // when compaction produces a new snapshot at a different path.
    let lastQuarantinedPath: string | null = null;
    let lastProcessedVersion: number | null = initialVersion;

    return onSnapshot(doc(db, path), async (snapshot) => {
        if (!snapshot.exists()) return;

        const data = snapshot.data();
        if (!data) return;

        // Skip snapshots produced by our own compaction
        if (data.origin === ctx.uid) {
            if (typeof data.version === 'number') {
                lastProcessedVersion = data.version;
            }
            return;
        }

        // Epoch fence: someone squashed the document into a new epoch.
        // The new snapshot must NOT be applied onto the old-epoch local
        // doc (content would duplicate — the id spaces are unrelated);
        // surface it so the application rebuilds instead.
        const snapEpoch = typeof data.epoch === 'number' ? data.epoch : 0;
        const curEpoch = ctx.getEpoch?.() ?? 0;
        if (snapEpoch > curEpoch) {
            ctx.onEpochChanged?.(snapEpoch);
            return;
        }
        if (snapEpoch < curEpoch) {
            // Stale delivery from before our epoch — ignore
            return;
        }

        // Version gate: a delivery carrying a version we already processed
        // (typically the immediate delivery on listener attach) has nothing
        // new — skip before touching the fingerprint or the blob.
        if (typeof data.version === 'number' && data.version === lastProcessedVersion) {
            return;
        }

        // Redundancy check: if the local doc already covers the snapshot's
        // state vector, downloading it would be a no-op. The delete-set
        // fingerprint is still applied first to pick up any deletions that
        // travelled in structs we already cover.
        if (data.deleteSet) {
            try {
                Y.applyUpdate(ydoc, (data.deleteSet as Bytes).toUint8Array(), FIREBASE_ORIGINS.SNAPSHOT);
            } catch (e) {
                console.warn("Failed to apply snapshot delete-set fingerprint", e);
            }
        } else if (data.deleteSetStoragePath) {
            // Oversized fingerprint offloaded to Cloud Storage
            try {
                const buffer = await getBytes(ref(storage, data.deleteSetStoragePath));
                if (isDestroyed()) return;
                Y.applyUpdate(ydoc, new Uint8Array(buffer), FIREBASE_ORIGINS.SNAPSHOT);
            } catch (e) {
                console.warn("Failed to apply storage-backed delete-set fingerprint", e);
            }
        }
        if (localCoversSnapshot(data, ydoc)) {
            if (typeof data.version === 'number') {
                lastProcessedVersion = data.version;
            }
            return;
        }

        // Handle Cloud Storage Snapshot
        if (data.snapshotStoragePath) {
            const snapshotKey = `snapshot:${data.snapshotStoragePath}`;

            // Clear quarantine if snapshot path changed (new compaction)
            if (lastQuarantinedPath && lastQuarantinedPath !== snapshotKey) {
                ctx.corruptedDocIds?.delete(lastQuarantinedPath);
                lastQuarantinedPath = null;
            }

            // Skip quarantined snapshot
            if (ctx.corruptedDocIds?.has(snapshotKey)) {
                return;
            }

            try {
                const storageRef = ref(storage, data.snapshotStoragePath);
                const buffer = await getBytes(storageRef);
                // Provider may have been destroyed while downloading
                if (isDestroyed()) return;
                const content = new Uint8Array(buffer);
                Y.applyUpdate(ydoc, content, FIREBASE_ORIGINS.SNAPSHOT);
                if (typeof data.version === 'number') {
                    lastProcessedVersion = data.version;
                }
            } catch (storageErr) {
                console.error(`Failed to apply snapshot ${snapshotKey} (quarantined)`, storageErr);
                ctx.corruptedDocIds?.add(snapshotKey);
                lastQuarantinedPath = snapshotKey;
                ctx.onCorruptedDocument?.(snapshotKey, storageErr instanceof Error ? storageErr : new Error(String(storageErr)));
            }
        }
        // Handle Firestore Document Snapshot (legacy/small documents)
        else if (data.content) {
            const snapshotKey = 'snapshot:inline';

            if (ctx.corruptedDocIds?.has(snapshotKey)) {
                return;
            }

            try {
                const content = (data.content as Bytes).toUint8Array();
                Y.applyUpdate(ydoc, content, FIREBASE_ORIGINS.SNAPSHOT);
                if (typeof data.version === 'number') {
                    lastProcessedVersion = data.version;
                }
            } catch (err) {
                console.error(`Failed to apply inline snapshot (quarantined)`, err);
                ctx.corruptedDocIds?.add(snapshotKey);
                lastQuarantinedPath = snapshotKey;
                ctx.onCorruptedDocument?.(snapshotKey, err instanceof Error ? err : new Error(String(err)));
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

    // Cache local state vector for redundancy checks (P3.0 Optimization)
    let localSVMap = Y.decodeStateVector(Y.encodeStateVector(ydoc));

    return onSnapshot(q, (snapshot) => {
        // Collect first, then apply in one transaction (see
        // createUpdateListener): several segments can land in a single
        // delivery, e.g. when the listener resumes after compaction.
        const batch: { docId: string; data: any }[] = [];

        snapshot.docChanges().forEach((change) => {
            if (change.type === 'added') {
                const data = change.doc.data();
                const docId = change.doc.id;

                // Epoch fence (see createUpdateListener)
                const docEpoch = typeof data?.epoch === 'number' ? data.epoch : 0;
                if (docEpoch !== (ctx.getEpoch?.() ?? 0)) {
                    return;
                }

                // Skip quarantined poison pills
                if (ctx.corruptedDocIds?.has(docId)) {
                    return;
                }

                if (data && data.segment) {
                    batch.push({ docId, data });
                }
            }
        });

        if (batch.length > 0) {
            const applyBatch = () => {
                for (const { docId, data } of batch) {
                    try {
                        // Check redundancy using local state vector
                        const item: PendingUpdate = {
                            type: 'history',
                            data: data as any,
                            priority: 2
                        };

                        if (!isItemRedundant(item, localSVMap)) {
                            // Apply it
                            Y.applyUpdate(ydoc, (data.segment as Bytes).toUint8Array(), FIREBASE_ORIGINS.HISTORY);
                            // Incremental update of cached state vector (P3.0 Optimization)
                            processHistoryMetadata(data, localSVMap);
                        }
                    } catch (err) {
                        console.error(`Failed to apply history segment ${docId} (quarantined)`, err);
                        ctx.corruptedDocIds?.add(docId);
                        ctx.onCorruptedDocument?.(docId, err instanceof Error ? err : new Error(String(err)));
                    }
                }
            };
            if (batch.length === 1) {
                applyBatch();
            } else {
                ydoc.transact(applyBatch, FIREBASE_ORIGINS.HISTORY);
            }
        }
    }, (error) => {
        console.error("History listener failed", error);
        if (onListenerError) onListenerError(error);
    });
}

// --- Helper Functions ---

/**
 * Extracts the raw binary blobs from all fetched server items.
 * Used for delete-set coverage checks before pushing a local diff.
 *
 * Items without an inline blob (e.g., a legacy main document carrying only
 * a stateVector) are skipped — missing coverage can only cause a redundant
 * (idempotent) push, never data loss.
 *
 * @param items - Pending updates collected during initial sync
 * @returns Array of update/segment/content blobs
 */
function collectServerBlobs(items: PendingUpdate[]): Uint8Array[] {
    const blobs: Uint8Array[] = [];
    for (const item of items) {
        const raw = item.type === 'snapshot' ? item.data.content
            : item.type === 'history' ? item.data.segment
                : item.data.update;
        if (raw) {
            blobs.push((raw as Bytes).toUint8Array());
        }
    }
    return blobs;
}

/**
 * Ensures that the document data has a decoded state vector map cached.
 * P3.1 OPTIMIZATION: Cache decoded state vector to avoid repeated parsing.
 *
 * @param data - Firestore document data containing stateVector
 * @returns The decoded state vector map
 */
function ensureDecodedSV(data: any): Map<number, number> {
    if (!data._decodedSV) {
        const vector = fromBase64(data.stateVector);
        data._decodedSV = Y.decodeStateVector(vector);
    }
    return data._decodedSV;
}

/**
 * Checks whether the local document already covers a snapshot's state
 * vector, i.e. downloading/applying the snapshot blob would be a no-op.
 *
 * Returns false when the stateVector is missing or malformed, so callers
 * fall back to fetching and applying the content (the safe direction).
 *
 * @param data - Main document data containing a stateVector field
 * @param ydoc - Local Yjs document
 */
function localCoversSnapshot(data: any, ydoc: Y.Doc): boolean {
    if (!data.stateVector) return false;
    try {
        const remoteSV = ensureDecodedSV(data);
        const localSV = Y.decodeStateVector(Y.encodeStateVector(ydoc));
        for (const [client, clock] of remoteSV) {
            if ((localSV.get(client) || 0) < clock) {
                return false;
            }
        }
        return true;
    } catch (e) {
        console.warn("Failed to decode snapshot stateVector", e);
        return false;
    }
}

/**
 * Extracts and aggregates clock values from an update document into the server state vector.
 * Tries stored metadata first, falls back to parsing the update blob.
 * 
 * @param data - Firestore document data containing update and/or metadata
 * @param serverSVMap - Map to populate with client -> clock mappings
 */
function processUpdateMetadata(data: any, serverSVMap: Map<number, number>): void {
    if (data.clientIDs?.length > 0 && data.clientClocks?.length > 0) {
        data.clientIDs.forEach((cid: number, i: number) => {
            const clock = data.clientClocks[i];
            const current = serverSVMap.get(cid) || 0;
            if (clock > current) {
                serverSVMap.set(cid, clock);
            }
        });
    } else if (data.update) {
        // Lazy clock extraction — avoids materializing the struct tree of
        // potentially large update blobs (extractClockEnds handles parse
        // errors internally by returning an empty map).
        const clockEnds = extractClockEnds((data.update as Bytes).toUint8Array());
        for (const [clientID, clock] of clockEnds) {
            const current = serverSVMap.get(clientID) || 0;
            if (clock > current) {
                serverSVMap.set(clientID, clock);
            }
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
        const map = ensureDecodedSV(data);
        for (const [client, clock] of map.entries()) {
            const current = serverSVMap.get(client) || 0;
            if (clock > current) {
                serverSVMap.set(client, clock);
            }
        }
    } else if (data.segment) {
        // Lazy clock extraction for history segments, which are large by
        // construction (merged batches of updates).
        const clockEnds = extractClockEnds((data.segment as Bytes).toUint8Array());
        for (const [clientID, clock] of clockEnds) {
            const current = serverSVMap.get(clientID) || 0;
            if (clock > current) {
                serverSVMap.set(clientID, clock);
            }
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
        const map = ensureDecodedSV(data);
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
        const map = ensureDecodedSV(item.data);
        for (const [client, clock] of map) {
            const localClock = localSVMap.get(client) || 0;
            if (clock > localClock) return false;
        }
        return true;
    }

    // P1.3 FIX: Handle history segments with stateVector
    if (item.type === 'history' && item.data.stateVector) {
        try {
            const map = ensureDecodedSV(item.data);
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
        if (data.clientIDs?.length > 0 && data.clientClocks?.length > 0) {
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
