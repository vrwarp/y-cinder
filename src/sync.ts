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
 * 3. Build a server state vector: the clocks the server holds contiguously
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
    getDocFromServer,
    serverTimestamp,
    limit,
    startAfter,
    limitToLast,
    QueryDocumentSnapshot,
    DocumentReference,
} from "@firebase/firestore";
import { getBytes, ref, deleteObject, FirebaseStorage } from "@firebase/storage";
import * as Y from "yjs";
import { fromBase64 } from "lib0/buffer";
import {
    FIREBASE_ORIGINS,
    FIRESTORE_PATHS,
    DEFAULTS,
} from "./types";
import { writeStateVector, wait, calculateBackoff, generateSessionId } from "./utils";
import { extractClockEnds, aggregateClockEnds, updateEndsWithDeletions, isUpdateRedundant, deleteSetCoveredByBlobs, withoutServerDeletions } from "./update-metadata";
import {
    PendingUpdate,
    collectServerBlobs,
    ensureDecodedSV,
    foldTailMayCatchUp,
    localCoversSnapshot,
    processSnapshotMetadata,
    buildServerCoverage,
    refreshLocalClocks,
    rebaseIfPending,
    isItemRedundant,
    fingerprintIsRedundant,
    transactionChangedDoc,
    applyItem,
    blobOf,
    diffSnapshotForLocal,
} from "./sync-helpers";
import {
    CLIENT_OFFLINE,
    diffHasPayload,
    diffNeedsStorage,
    epochTag as buildEpochTag,
    hasMorePages,
    isClientOfflineError,
    isLostAckCommit,
    isPermanentDownloadError,
    isServedFromCache,
    largeUpdatePath,
    orderByApplyPriority,
    serverCoversLocalStructs,
    pickPaginationCursorIndex,
    planIncomingUpdate,
    shouldTriggerCompaction,
    survivesEpochFence,
} from "./sync-policy";
import { readDocEpoch, docHasContent } from "./squash";
import { uploadBlob, restoreMissingBlob } from "./storage-blobs";

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
    /**
     * When automatic compaction triggers may resume (epoch ms, 0 = now).
     * The provider backs off after a compaction failure that retrying
     * cannot fix; until then the update listener triggers nothing, not
     * even at the realtime hard cap.
     */
    getCompactionBackoffUntil?: () => number;
    /**
     * Fired when this client sees compaction progress made by any client:
     * update documents leaving the collection, or a new snapshot version.
     * The provider ends its failure backoff, so recovery after another
     * client succeeds (or an operator fixes the cause) is not delayed by up
     * to the backoff cap.
     */
    onCompactionProgress?: () => void;
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
    /**
     * Fired during initial sync once the server's epoch is known and does
     * not conflict with the local document, before any local data is
     * pushed. The provider must tag its saves with this epoch from then
     * on: a save that starts while the initial-sync push is in flight
     * carries edits made after the diff, and peers drop it unless it
     * belongs to the same epoch as the push.
     */
    onEpochAdopted?: (serverEpoch: number) => void;
    /**
     * Fired synchronously right before the push decision reads the local
     * doc. Once performInitialSync then reports success, the server holds
     * everything the doc held at this point (already, or inside the push),
     * so the provider retires the local updates it had buffered by then
     * instead of saving them a second time.
     */
    onLocalStateCaptured?: () => void;
    /**
     * Resolves once local persistence has loaded into the doc (bounded by
     * the provider; never rejects — see FireProviderConfig.localReady).
     * Initial sync awaits it after its server reads, before anything
     * compares the local doc with the server.
     */
    localReady?: Promise<void>;
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
    /**
     * Number of update documents up to and including lastSyncedDoc. The
     * listener's query starts after that cursor, so these are passed to it
     * separately: they still count toward the compaction threshold.
     */
    syncedUpdateCount: number;
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
 * Fails initial sync when a read was answered from the local cache (see
 * isServedFromCache): the client is offline.
 *
 * @param snapshot - The query or document snapshot just read.
 * @throws An error isClientOfflineError recognizes.
 */
function requireServerRead(snapshot: { metadata?: { fromCache?: boolean } }): void {
    if (isServedFromCache(snapshot)) {
        throw Object.assign(
            new Error('Initial sync read was served from the local cache: client is offline'),
            { code: CLIENT_OFFLINE }
        );
    }
}

/**
 * Pending update item during sync.
 */

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
        const snapshotSVMap = new Map<number, number>();
        const pendingUpdates: PendingUpdate[] = [];
        let updatesApplied = 0;

        // 1. Fetch Updates (Tier 3) with pagination (P0.1 fix)
        let lastUpdateDoc: QueryDocumentSnapshot | null = null;
        let syncedUpdateCount = 0;
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
            requireServerRead(updatesSnap);
            if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, syncedUpdateCount: 0, lastHistoryDoc: null, snapshotVersion: null, epoch: 0 };

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
                                data.update = new Uint8Array(buffer);
                            } catch (storageErr) {
                                console.error(`Failed to download storage-backed update: ${data.updateStoragePath}`, storageErr);
                                // Skipping would still report success and move the listener
                                // cursor past this update, so nothing would ever fetch it
                                // again — propagate so the sync retry logic in provider.ts
                                // handles backoff/retry. Only a blob that is gone is skipped.
                                if (!isPermanentDownloadError(storageErr)) {
                                    throw storageErr;
                                }
                                continue; // Skip this update — cannot apply without data
                            }
                        }
                        // Metadata is folded into the server state vector
                        // only after the epoch is known (main doc read) —
                        // foreign-epoch documents must not contribute.
                        pendingUpdates.push({ type: 'update', data, priority: 3, unacknowledged: snap.metadata.hasPendingWrites });
                    }
                }

                // FIX: Verify cursor is committed to avoid "Invalid query" with pending serverTimestamp
                const cursorIndex = pickPaginationCursorIndex(updatesSnap.docs);

                if (cursorIndex >= 0) {
                    lastUpdateDoc = updatesSnap.docs[cursorIndex];
                    syncedUpdateCount += cursorIndex + 1;
                }

                hasMoreUpdates = hasMorePages(updatesSnap.docs.length, BATCH_SIZE);
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
            requireServerRead(historySnap);
            if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, syncedUpdateCount: 0, lastHistoryDoc: null, snapshotVersion: null, epoch: 0 };

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
        requireServerRead(mainSnap);
        if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, syncedUpdateCount: 0, lastHistoryDoc: null, snapshotVersion: null, epoch: 0 };

        // Local persistence may still be loading into the doc (provider
        // constructed first); the reads above overlapped with it. Wait for
        // it before anything below compares the doc with the server: a
        // still-empty doc would slip past the epoch fence and download the
        // Storage snapshot that local persistence is about to load.
        if (ctx.localReady) {
            await ctx.localReady;
            if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, syncedUpdateCount: 0, lastHistoryDoc: null, snapshotVersion: null, epoch: 0 };
        }

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
                        syncedUpdateCount: 0,
                        lastHistoryDoc: null,
                        snapshotVersion,
                        epoch: serverEpoch,
                        epochConflict: { serverEpoch, localEpoch },
                    };
                }

                processSnapshotMetadata(data, snapshotSVMap);

                // The delete-set fingerprint written by compaction is a
                // structs-empty update. Treating it as a regular update both
                // proves delete-set coverage to the push guard below and
                // applies any deletions the local doc may have missed (only
                // when there are some: see the apply loop).
                if (data.deleteSet) {
                    pendingUpdates.push({ type: 'update', data: { update: data.deleteSet, epoch: serverEpoch }, priority: 2, fingerprint: true });
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
                            data: { update: new Uint8Array(buffer), epoch: serverEpoch },
                            priority: 2,
                            fingerprint: true,
                        });
                    } catch (dsErr) {
                        // Coverage falls back to the other server blobs; worst
                        // case is a redundant (idempotent) push.
                        console.warn("Failed to download delete-set fingerprint", dsErr);
                    }
                }

                // Fetch snapshot from Cloud Storage if available
                if (data.snapshotStoragePath) {
                    // A client that missed only the last fold (a device
                    // returning while another one kept editing) lacks just
                    // that fold's tail: catch up from it first. Applied on
                    // its own, ahead of the transaction below, because only
                    // the re-check after it can tell whether the snapshot
                    // is still needed.
                    if (!localCoversSnapshot(data, ydoc) && foldTailMayCatchUp(data, ydoc)) {
                        await applyFoldTail(ctx.storage, data, ydoc, isDestroyed);
                        if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, syncedUpdateCount: 0, lastHistoryDoc: null, snapshotVersion: null, epoch: 0 };
                    }
                    // Skip the (potentially large) blob download when the
                    // local doc already covers the snapshot's state vector —
                    // typical for reconnecting clients.
                    if (!localCoversSnapshot(data, ydoc)) {
                        try {
                            const storageRef = ref(ctx.storage, data.snapshotStoragePath);
                            const buffer = await getBytes(storageRef);
                            // A local doc behind the snapshot gets only what
                            // it lacks (diffed off the main thread when
                            // possible); a fresh one gets the whole blob.
                            // Injected into data.content as a Uint8Array, not
                            // Bytes: the readers take either (blobOf), and
                            // wrapping an O(document) blob costs a string
                            // concat per byte.
                            data.content = await diffSnapshotForLocal(new Uint8Array(buffer), data, ydoc);
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
        // The Storage downloads above can take seconds: a provider destroyed
        // meanwhile must not apply anything nor push its local diff.
        if (isDestroyed()) return { success: false, updatesApplied: 0, localUpdatesPushed: false, lastSyncedDoc: null, syncedUpdateCount: 0, lastHistoryDoc: null, snapshotVersion: null, epoch: 0 };

        // The server epoch is final for this sync: adopt it before the
        // push below computes its diff (see SyncContext.onEpochAdopted).
        ctx.onEpochAdopted?.(serverEpoch);

        // 3b. Epoch filter + server state vector. Update/history documents
        // from foreign epochs are dropped: their structs belong to an
        // unrelated id space (pre-squash) and would sit in pendingStructs
        // forever, poisoning GC compaction. Blobs therefore only enter
        // the server state vector for same-epoch items — and the fence
        // must run BEFORE it is built so a stale update can never
        // suppress the initial-sync push.
        for (let i = pendingUpdates.length - 1; i >= 0; i--) {
            const item = pendingUpdates[i];
            if (!survivesEpochFence(item, serverEpoch)) {
                pendingUpdates.splice(i, 1);
            }
        }
        // Contiguous coverage, not the metadata's clock ENDs: an update
        // written without what precedes it (e.g. a save that committed
        // before initial sync pushed the doc's pre-existing content) ends
        // at the client's full clock although the server lacks its start.
        //
        // Only committed documents count as server evidence. This client's
        // unacknowledged saves are applied like the rest, but the server
        // may reject one after the push left its content out, and the
        // provider retires the batch it put back along with the push's.
        const serverItems = pendingUpdates.filter(item => !item.unacknowledged);
        const serverSVMap = buildServerCoverage(snapshotSVMap, serverItems);

        // 4. Apply missing data with state vector refresh (P0.4 fix)
        let localSVMap = Y.decodeStateVector(Y.encodeStateVector(ydoc));

        // Sort by priority (Snapshot first, then History, then Updates)
        const orderedUpdates = orderByApplyPriority(pendingUpdates);

        // Apply everything inside a single Yjs transaction. Each top-level
        // applyItem call would otherwise commit its own transaction, firing
        // doc observers and encoding an 'update' event per blob — on a
        // long-lived document with hundreds of pending items that means
        // hundreds of editor re-renders during initial load instead of one.
        //
        // The delete-set fingerprint is applied only when it deletes
        // something: on a client that already holds every deletion (a
        // synced warm start) the apply is an O(delete-set) no-op. The local
        // delete-set it is checked against is kept for the push guard.
        let fingerprintLocalDs: ReturnType<typeof Y.createDeleteSetFromStructStore> | null = null;
        const applyTransaction = ydoc.transact((tr) => {
            for (const item of orderedUpdates) {
                if (isDestroyed()) break;

                if (item.fingerprint) {
                    fingerprintLocalDs = Y.createDeleteSetFromStructStore((ydoc as any).store);
                    // blobOf: an offloaded fingerprint is the downloaded
                    // Uint8Array, an inline one shares the push guard's copy
                    if (fingerprintIsRedundant(blobOf(item.data.update), fingerprintLocalDs)) {
                        // Skipped, not dropped: it stays in pendingUpdates
                        // as server evidence for the push guard below.
                        continue;
                    }
                }

                if (!isItemRedundant(item, localSVMap)) {
                    const applied = applyItem(item, ydoc);
                    if (applied) {
                        updatesApplied++;
                        // Incremental update of localSVMap instead of expensive re-encode/decode (P3.0 Optimization)
                        // This prevents redundant processing of history/updates already in snapshot/previous segments
                        refreshLocalClocks(item, ydoc, localSVMap);
                        // Structs parked behind a gap did not advance the doc;
                        // the item that fills it must not look redundant
                        rebaseIfPending(ydoc, localSVMap);
                    }
                }
            }
            return tr;
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
        //
        // When a diff is pushed, it carries only the deletions the server
        // blobs do not already prove (withoutServerDeletions): otherwise a
        // one-character offline edit is written as an O(delete-set) update
        // that every peer integrates and the next segment inherits.
        const serverSV = writeStateVector(serverSVMap);
        let localUpdatesPushed = false;

        // No await from here to the diff: the push covers exactly what the
        // doc holds now (see SyncContext.onLocalStateCaptured).
        ctx.onLocalStateCaptured?.();
        const exactLocalSV = Y.decodeStateVector(Y.encodeStateVector(ydoc));
        const serverCoversStructs = serverCoversLocalStructs(exactLocalSV, serverSVMap);
        // The fingerprint stands in for the snapshot, whose content is
        // O(document) and never decoded just to trim the push.
        const deletionProofBlobs = () =>
            collectServerBlobs(serverItems.filter(item => item.type !== 'snapshot'));

        let shouldPush: boolean;
        let localDiff: Uint8Array | null = null;
        if (serverCoversStructs) {
            // The fingerprint check's delete-set is still exact unless the
            // apply transaction changed the doc (an item applied after the
            // check, or an observer reacting to one applied before it).
            const localDs = fingerprintLocalDs !== null && !transactionChangedDoc(applyTransaction)
                ? fingerprintLocalDs
                : Y.createDeleteSetFromStructStore((ydoc as any).store);
            shouldPush = !deleteSetCoveredByBlobs(localDs, () => collectServerBlobs(serverItems));
            if (shouldPush) {
                // Rare: local deletion-only changes the server lacks.
                localDiff = withoutServerDeletions(Y.encodeStateAsUpdate(ydoc, serverSV), deletionProofBlobs);
            }
        } else {
            localDiff = withoutServerDeletions(Y.encodeStateAsUpdate(ydoc, serverSV), deletionProofBlobs);
            // The diff has structs by construction (a local clock exceeds
            // the server's), so it always carries new data. It is not
            // decoded again to check (diffCarriesNewData): stripping
            // already decoded it, and a second pass doubles the cost of a
            // large first push. A diff left with neither structs nor
            // missing deletions would be the bare two-byte header anyway.
            shouldPush = diffHasPayload(localDiff.byteLength);
        }

        if (shouldPush && localDiff !== null) {
            console.log("Pushing missing local updates to Firestore.");
            const clockEnds = extractClockEnds(localDiff);
            const epochTag = buildEpochTag(serverEpoch);
            // Readers may already hold every struct (from a save that
            // committed after the reads above); the flag keeps them from
            // skipping its deletions as redundant.
            const deletionTag = updateEndsWithDeletions(localDiff) ? { hasDeletions: true } : {};

            if (diffNeedsStorage(localDiff.byteLength, DEFAULTS.INLINE_UPDATE_LIMIT)) {
                // Storage-backed update: upload binary to Cloud Storage
                const storagePath = largeUpdatePath(path, uid, Date.now(), generateSessionId());
                const storageRef = ref(ctx.storage, storagePath);
                await uploadBlob(ctx.storage, storagePath, localDiff);

                // Write lightweight pointer document to updates collection
                const pkg: any = {
                    updateStoragePath: storagePath,
                    createdAt: serverTimestamp(),
                    createdBy: uid,
                    ...epochTag,
                    ...aggregateClockEnds(clockEnds),
                    ...deletionTag
                };
                try {
                    await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), pkg);
                } catch (pointerErr) {
                    // Committed by an earlier send whose ack was lost: the
                    // pointer is live (see isLostAckCommit).
                    if (!isLostAckCommit(pointerErr)) {
                        // Any other rejected write never commits (the SDK
                        // retries transient errors itself), and the sync
                        // retry pushes again under a new path: delete this
                        // copy instead of orphaning it, in the background.
                        deleteObject(storageRef).catch(err => {
                            console.warn(`Failed to delete unreferenced update blob ${storagePath}`, err);
                        });
                        throw pointerErr;
                    }
                }
                // A re-send may have re-created a pointer whose blob a
                // compaction already reclaimed
                await restoreMissingBlob(ctx.storage, storagePath, localDiff);
                console.log(`Oversized initial sync diff (${localDiff.byteLength} bytes) offloaded to Cloud Storage: ${storagePath}`);
            } else {
                // Standard inline update
                const pkg: any = {
                    update: Bytes.fromUint8Array(localDiff),
                    createdAt: serverTimestamp(),
                    createdBy: uid,
                    ...epochTag,
                    ...aggregateClockEnds(clockEnds),
                    ...deletionTag
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
            syncedUpdateCount,
            lastHistoryDoc,
            snapshotVersion,
            epoch: serverEpoch
        };
    } catch (err) {
        if (!isClientOfflineError(err)) {
            console.error("Sync failed", err);
        }
        return {
            success: false,
            error: err instanceof Error ? err : new Error(String(err)),
            updatesApplied: 0,
            localUpdatesPushed: false,
            lastSyncedDoc: null,
            syncedUpdateCount: 0,
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
 * @param syncedUpdateCount - Update documents up to and including the
 * cursor (see SyncResult.syncedUpdateCount), counted toward the compaction
 * threshold alongside the documents this listener delivers
 * @returns Unsubscribe function
 */
export function createUpdateListener(ctx: SyncContext, startAfterDoc: QueryDocumentSnapshot | null = null, syncedUpdateCount: number = 0): Unsubscribe {
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

    // Storage-backed downloads are retried beyond the delivery that started
    // them, so they must stop with the listener: on destroy, and when the
    // provider unsubscribes it (e.g. at an epoch fence).
    let stopped = false;
    const isStopped = () => stopped || isDestroyed();

    // Update documents initial sync read up to the cursor are outside this
    // query but still in the updates collection, so they count toward the
    // threshold too; otherwise only sessions that write more than the
    // threshold themselves would ever compact. They leave the count once a
    // trigger hands them to compaction, which drains oldest-first.
    let backlog = syncedUpdateCount;

    const unsubscribe = onSnapshot(liveUpdatesQ, (snapshot) => {
        const changes = snapshot.docChanges();

        // Committed update documents leave the collection only when a
        // compaction (or a squash) deletes them; a document that still had
        // pending writes was this client's own write, rejected by the
        // server. Reported before the trigger decision so a backoff it ends
        // no longer suppresses this delivery.
        if (changes.some(change => change.type === 'removed' && !change.doc.metadata.hasPendingWrites)) {
            ctx.onCompactionProgress?.();
        }

        if (onCompactionNeeded) {
            const now = Date.now();

            if (shouldTriggerCompaction({
                size: backlog + snapshot.size,
                maxUpdatesThreshold,
                now,
                lastTriggerAt: lastCompactionTrigger,
                cooldownMs: DEFAULTS.COMPACTION_TRIGGER_COOLDOWN_MS,
                hardCap: DEFAULTS.REALTIME_LIMIT,
                backoffUntil: ctx.getCompactionBackoffUntil?.() ?? 0,
            })) {
                lastCompactionTrigger = now;
                backlog = 0;
                onCompactionNeeded();
            }
        }

        // Collect inline-appliable updates from this delivery first, then
        // apply them in ONE Yjs transaction. Object-heavy workloads (e.g.
        // another client dragging shapes) arrive as bursts of small update
        // documents; applying each in its own top-level transaction fires
        // an observer flush + 'update' event (editor re-render) per doc.
        const inlineBatch: { docId: string; data: any }[] = [];

        changes.forEach((change) => {
            if (change.type === 'added') {
                const data = change.doc.data();
                const docId = change.doc.id;
                const plan = planIncomingUpdate(data, {
                    uid,
                    currentEpoch: ctx.getEpoch?.() ?? 0,
                    docId,
                    corruptedDocIds: ctx.corruptedDocIds,
                    localSVMap,
                });

                if (plan.kind === 'skip-own') {
                    // Refresh the clocks it touches anyway so later checks stay accurate.
                    refreshLocalClocks({ type: 'update', data }, ydoc, localSVMap);
                    // Our initial-sync push also carries any structs parked in
                    // pendingStructs; re-base while anything is still parked.
                    rebaseIfPending(ydoc, localSVMap);
                    return;
                }
                if (plan.kind !== 'download' && plan.kind !== 'apply-inline') {
                    return;
                }

                // Handle storage-backed update (oversized diff offloaded to Cloud Storage)
                if (plan.kind === 'download') {
                    (async () => {
                        try {
                            const buffer = await downloadUpdateWithRetry(ctx.storage, data.updateStoragePath, isStopped);
                            // Listener may have stopped (or the provider been destroyed) while downloading
                            if (buffer === null || isStopped()) return;
                            const update = new Uint8Array(buffer);
                            Y.applyUpdate(ydoc, update, FIREBASE_ORIGINS.UPDATE);
                            // Incremental update of cached state vector (P3.0 Optimization)
                            refreshLocalClocks({ type: 'update', data }, ydoc, localSVMap);
                            rebaseIfPending(ydoc, localSVMap);
                        } catch (e) {
                            // Compacted away mid-download: the blob is
                            // deleted once the pointer's deletion commits,
                            // and the fold or segment holding the update
                            // arrives through the other listeners.
                            if (isPermanentDownloadError(e) && !(await updateDocExists(change.doc.ref))) {
                                return;
                            }
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
                    if (!data.hasDeletions && data.clientIDs?.length > 0 && data.clientClocks?.length > 0 &&
                        isUpdateRedundant(localSVMap, data.clientIDs, data.clientClocks)) {
                        continue;
                    }
                    try {
                        const update = (data.update as Bytes).toUint8Array();
                        Y.applyUpdate(ydoc, update, FIREBASE_ORIGINS.UPDATE);
                        // Incremental update of cached state vector (P3.0 Optimization)
                        refreshLocalClocks({ type: 'update', data }, ydoc, localSVMap);
                        rebaseIfPending(ydoc, localSVMap);
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

    return () => {
        stopped = true;
        unsubscribe();
    };
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

    // A delivery that awaited its snapshot diff must not apply once the
    // provider unsubscribed the listener (e.g. at an epoch fence).
    let stopped = false;

    // Each new fold carries the whole delete-set again; when the local doc
    // already holds every deletion in it, applying it is an O(delete-set)
    // no-op (see fingerprintIsRedundant).
    const applyFingerprint = (fingerprint: Uint8Array): void => {
        if (!fingerprintIsRedundant(fingerprint, Y.createDeleteSetFromStructStore((ydoc as any).store))) {
            Y.applyUpdate(ydoc, fingerprint, FIREBASE_ORIGINS.SNAPSHOT);
        }
    };

    const unsubscribe = onSnapshot(doc(db, path), async (snapshot) => {
        if (!snapshot.exists()) return;

        const data = snapshot.data();
        if (!data) return;

        // Epoch fence: someone squashed the document into a new epoch.
        // The new snapshot must NOT be applied onto the old-epoch local
        // doc (content would duplicate — the id spaces are unrelated);
        // surface it so the application rebuilds instead. Our own squash
        // fences this provider itself (it emits 'squashed').
        const snapEpoch = typeof data.epoch === 'number' ? data.epoch : 0;
        const curEpoch = ctx.getEpoch?.() ?? 0;
        if (snapEpoch > curEpoch) {
            if (data.origin !== ctx.uid) {
                ctx.onEpochChanged?.(snapEpoch);
            }
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

        // A new version means a fold committed (ours or another client's).
        ctx.onCompactionProgress?.();

        // Redundancy check: if the local doc already covers the snapshot's
        // state vector, downloading it would be a no-op. The delete-set
        // fingerprint is still applied first to pick up any deletions that
        // travelled in structs we already cover. Our own compactions go
        // through this too: a fold merges what the SERVER holds, which can
        // include data the local doc lacks (e.g. a quarantined update) —
        // and it deletes the source documents, so this is the only path
        // left for that data.
        if (data.deleteSet) {
            try {
                applyFingerprint((data.deleteSet as Bytes).toUint8Array());
            } catch (e) {
                console.warn("Failed to apply snapshot delete-set fingerprint", e);
            }
        } else if (data.deleteSetStoragePath) {
            // Oversized fingerprint offloaded to Cloud Storage
            try {
                const buffer = await getBytes(ref(storage, data.deleteSetStoragePath));
                if (isDestroyed()) return;
                applyFingerprint(new Uint8Array(buffer));
            } catch (e) {
                console.warn("Failed to apply storage-backed delete-set fingerprint", e);
            }
        }
        // A client that missed only this fold (e.g. offline across it)
        // lacks just its tail; the check below decides whether that sufficed.
        if (!localCoversSnapshot(data, ydoc) && foldTailMayCatchUp(data, ydoc)) {
            const tailStopped = () => stopped || isDestroyed() || (ctx.getEpoch?.() ?? 0) !== snapEpoch;
            await applyFoldTail(storage, data, ydoc, tailStopped);
            if (tailStopped()) return;
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
                // Only what the local doc lacks (see diffSnapshotForLocal)
                const content = await diffSnapshotForLocal(new Uint8Array(buffer), data, ydoc);
                if (stopped || isDestroyed() || (ctx.getEpoch?.() ?? 0) !== snapEpoch) return;
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

    return () => {
        stopped = true;
        unsubscribe();
    };
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
                            refreshLocalClocks(item, ydoc, localSVMap);
                            rebaseIfPending(ydoc, localSVMap);
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
 * Downloads the current fold's tail and applies it to the local document.
 *
 * Only a shortcut around the snapshot download (see foldTailMayCatchUp),
 * so it never throws and quarantines nothing: on any failure — including
 * a tail the next fold has already garbage-collected — the caller's
 * coverage re-check still fails and it downloads the snapshot as before.
 *
 * @param storage - Firebase Storage instance
 * @param data - Main document data carrying the tail fields
 * @param ydoc - Local Yjs document
 * @param isStopped - Whether the caller has stopped (checked after the download)
 */
async function applyFoldTail(
    storage: FirebaseStorage,
    data: any,
    ydoc: Y.Doc,
    isStopped: () => boolean
): Promise<void> {
    try {
        const buffer = await getBytes(ref(storage, data.foldTailStoragePath));
        if (isStopped()) return;
        Y.applyUpdate(ydoc, new Uint8Array(buffer), FIREBASE_ORIGINS.SNAPSHOT);
    } catch (e) {
        console.warn(`Failed to apply fold tail ${data.foldTailStoragePath}; downloading the snapshot instead`, e);
    }
}

/**
 * Whether an update document still exists on the server.
 *
 * Asked once its Storage blob turned out to be missing. Compaction and
 * squash delete a pointer's blob right after the transaction deleting the
 * pointer commits, so a listener download racing that cycle finds the
 * object gone while the update itself lives on in the fold, segment or
 * new epoch the other listeners deliver. Only a blob missing behind a
 * pointer that still exists is lost. When the check itself fails, the
 * document is assumed to exist.
 *
 * @param docRef - The update (pointer) document.
 * @returns false when the document is gone.
 */
async function updateDocExists(docRef: DocumentReference): Promise<boolean> {
    try {
        return (await getDocFromServer(docRef)).exists();
    } catch {
        return true;
    }
}

/**
 * Downloads a storage-backed update for the update listener, retrying
 * failures that may be transient.
 *
 * The update document is never delivered again, so giving up on a network
 * error would drop the update for the rest of the session. Failures are
 * retried for as long as the listener runs, with a backoff that stops
 * growing after DEFAULTS.MAX_RETRIES attempts; a permanent failure is
 * rethrown so the caller quarantines the document.
 *
 * @param storage - Firebase Storage instance
 * @param storagePath - The update's blob path
 * @param isStopped - Whether the listener has stopped
 * @returns The blob, or null when the listener stopped first
 */
async function downloadUpdateWithRetry(
    storage: FirebaseStorage,
    storagePath: string,
    isStopped: () => boolean
): Promise<ArrayBuffer | null> {
    for (let attempt = 1; !isStopped(); attempt++) {
        try {
            return await getBytes(ref(storage, storagePath));
        } catch (e) {
            if (isPermanentDownloadError(e)) throw e;
            const backoff = calculateBackoff(Math.min(attempt, DEFAULTS.MAX_RETRIES));
            console.warn(`Failed to download storage-backed update ${storagePath} (attempt ${attempt}). Retrying in ${Math.floor(backoff)}ms...`, e);
            await wait(backoff);
        }
    }
    return null;
}
