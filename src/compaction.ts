/**
 * Compaction Module
 *
 * Implements the tiered compaction strategy for managing Yjs updates in Firestore.
 * The compaction system reduces storage costs and sync times by periodically
 * merging small updates into larger, more efficient structures.
 *
 * ## Architecture
 *
 * The storage hierarchy (from most to least compact):
 * ```
 * ┌─────────────────────────────────────────────────┐
 * │  Base Snapshot (Tier 1)                         │
 * │  - Single document with full state              │
 * │  - Target: < 900KB                              │
 * ├─────────────────────────────────────────────────┤
 * │  History Segments (Tier 2)                      │
 * │  - Merged batches of updates                    │
 * │  - Created when snapshot would exceed limit     │
 * ├─────────────────────────────────────────────────┤
 * │  Updates (Tier 3)                               │
 * │  - Individual client updates                    │
 * │  - Compacted when count exceeds threshold       │
 * └─────────────────────────────────────────────────┘
 * ```
 *
 * ## Safety Guarantees
 *
 * - **Atomicity**: All operations happen within Firestore transactions
 * - **Locking**: Distributed lock prevents concurrent compaction
 * - **Retry**: Exponential backoff handles transient failures
 * - **Chunking**: Large data is split to stay under Firestore limits
 *
 * @module compaction
 */

import {
    Firestore,
    doc,
    collection,
    Bytes,
    runTransaction,
    query,
    orderBy,
    getDocs,
    getDoc,
    serverTimestamp,
    deleteField,
    limit,
    DocumentReference,
    Timestamp,
} from "@firebase/firestore";
import { ref, uploadBytes, deleteObject, getBytes, FirebaseStorage } from "@firebase/storage";
import { toBase64 } from "lib0/buffer";
import {
    buildDeltaSegmentDoc,
    buildSnapshotResult,
    deltaSegmentFitsInline,
    epochOf,
    nextSnapshotVersion,
    planHistoryDoc,
    planUpdateDoc,
    readMainDocState,
    shouldRetryCompaction,
    shouldUseDelta,
} from './compaction-policy';
import { DEFAULTS, FIRESTORE_PATHS, TestHooks } from "./types";
import { wait, calculateBackoff } from "./utils";
import { acquireLock, releaseLock } from "./locking";
import { mergeUpdatesWithMetaAsync } from "./merge-utils";

/**
 * Context required for compaction operations.
 */
export interface CompactionContext {
    /** Firestore instance */
    db: Firestore;
    /** Base document path */
    path: string;
    /** Unique client ID */
    uid: string;
    /** Lock time-to-live in milliseconds */
    lockTTL: number;
    /** Maximum updates to process per compaction */
    compactionLimit: number;
    /** Flag to check if provider is destroyed */
    isDestroyed: () => boolean;
    /** Test hooks for dependency injection */
    testHooks?: TestHooks;
    /** P0.3 FIX: Cached clock offset to pass to locking */
    cachedClockOffset?: number;
    /** Firebase Storage instance */
    storage: FirebaseStorage;
    /**
     * Whether to garbage-collect deleted content when building the snapshot.
     * Defaults to true; see FireProviderConfig.gcCompaction.
     */
    gc?: boolean;
    /**
     * History segments allowed before compaction folds everything into the
     * base snapshot. Below the threshold, compaction runs in DELTA mode
     * (updates -> one history segment, O(new data)); at the threshold it
     * folds (snapshot + history + updates -> new snapshot, O(document)).
     * Defaults to DEFAULTS.HISTORY_FOLD_THRESHOLD; 1 = always fold.
     */
    historyFoldThreshold?: number;
    /**
     * Test seam: inline cap for the delete-set fingerprint field.
     * Defaults to DEFAULTS.MAX_DELETE_SET_FIELD_BYTES.
     * @internal
     */
    maxDeleteSetFieldBytes?: number;
}

/**
 * Result of a compaction operation.
 */
export interface CompactionResult {
    /** Whether compaction completed successfully */
    success: boolean;
    /** Type of compaction performed */
    type?: 'snapshot' | 'history' | 'none';
    /** Number of updates compacted */
    updatesCompacted: number;
    /** Number of history segments merged */
    historySegmentsMerged: number;
    /** Error if compaction failed */
    error?: Error;
    /** Version number of the snapshot that was replaced (for garbage collection) */
    previousVersion?: number;
}

/**
 * Performs tiered compaction of updates.
 * 
 * The compaction strategy is:
 * 1. Acquire distributed lock (only one client compacts at a time)
 * 2. Fetch current state (base snapshot, history, updates)
 * 3. Try Level 1: Merge everything into base snapshot (if under size limit)
 * 4. Fallback Level 2: Merge updates into history segment
 * 5. Handle oversized updates by chunking into multiple history segments
 * 
 * Uses exponential backoff with jitter for retry on transient failures.
 * 
 * @param ctx - Compaction context
 * @param attempt - Current retry attempt (1-based)
 * @returns Compaction result
 * 
 * @example
 * ```typescript
 * const result = await compact({
 *   db, path, uid,
 *   lockTTL: 60000,
 *   compactionLimit: 500,
 *   isDestroyed: () => false
 * });
 * ```
 */
export async function compact(
    ctx: CompactionContext,
    attempt: number = 1
): Promise<CompactionResult> {
    const { db, path, uid, lockTTL, compactionLimit, isDestroyed, testHooks, cachedClockOffset, storage } = ctx;
    const historyFoldThreshold = ctx.historyFoldThreshold ?? DEFAULTS.HISTORY_FOLD_THRESHOLD;

    // 1. Distributed Gate: Try to become the Leader
    // P0.3 FIX: Pass cached clock offset to avoid re-measuring (saves 3 Firestore ops)
    const hasLock = await acquireLock({ db, path, uid, lockTTL, cachedClockOffset });
    if (!hasLock) {
        return { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };
    }

    try {
        // Fetch work items.
        // Limits are clamped so deletes (updates + history) plus the snapshot
        // write stay within Firestore's 500-op transaction budget. Anything
        // left over is picked up by the next compaction cycle.
        const updatesQ = query(
            collection(db, path, FIRESTORE_PATHS.UPDATES),
            orderBy('createdAt', 'asc'),
            limit(Math.min(compactionLimit, DEFAULTS.MAX_COMPACTION_UPDATES))
        );
        const updatesSnap = await getDocs(updatesQ);

        const historyQ = query(
            collection(db, path, FIRESTORE_PATHS.HISTORY),
            orderBy('startTime', 'asc'),
            limit(DEFAULTS.MAX_COMPACTION_HISTORY)
        );
        const historySnaps = await getDocs(historyQ);

        if (updatesSnap.empty && historySnaps.empty) {
            return { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };
        }

        const updateDocs = updatesSnap.docs;
        const historyDocs = historySnaps.docs;

        // Test hook for simulating concurrent modifications
        if (testHooks?.beforeTransaction) {
            await testHooks.beforeTransaction();
        }

        // === STEP 2: Read main-document metadata (base presence + version).
        // The base blob itself is only downloaded on the fold path — delta
        // compaction must not pay O(snapshot) transfer.
        const mainRef = doc(db, path);
        const mainSnap = await getDoc(mainRef);

        const mainState = readMainDocState(mainSnap.exists() ? mainSnap.data() : null);
        const { hasBase, baseStoragePath, currentVersion, currentEpoch } = mainState;
        const baseInline = mainState.baseInline as Bytes | null;

        // Epoch fence: documents written before a squash belong to an
        // unrelated id space. Merging them would permanently poison the
        // snapshot (their structs can never integrate — Yjs parks them as
        // missing dependencies, which also disables GC compaction), so
        // they are deleted without merging.
        const staleRefs: DocumentReference[] = [];

        // Use the data already returned by the queries above. Update and
        // history documents are immutable (only ever created or deleted),
        // and the transaction below re-verifies existence before deleting,
        // so re-fetching each document individually would only double the
        // read cost. Storage-backed payloads are downloaded in parallel.
        const updateResults = await Promise.all(updateDocs.map(async (uDoc) => {
            const data = uDoc.data() as Record<string, any>;
            const plan = planUpdateDoc(data, currentEpoch);

            if (plan.kind === 'stale') {
                staleRefs.push(uDoc.ref);
                return null;
            }
            if (plan.kind === 'storage') {
                try {
                    const storageRef = ref(storage, plan.storagePath);
                    const buffer = await getBytes(storageRef);
                    return {
                        ref: uDoc.ref,
                        data: new Uint8Array(buffer),
                        createdAt: data.createdAt,
                    };
                } catch (e) {
                    console.error(`Compaction skipped storage-backed update ${uDoc.id} due to download failure`, e);
                    return null;
                }
            }
            if (plan.kind === 'inline') {
                return {
                    ref: uDoc.ref,
                    data: (data.update as Bytes).toUint8Array(),
                    createdAt: data.createdAt,
                };
            }
            return null;
        }));
        const updatesToProcess = updateResults.filter((u): u is { ref: DocumentReference; data: Uint8Array; createdAt: Timestamp } => u !== null);

        const historyToMerge = historyDocs
            .map((hDoc) => {
                const data = hDoc.data() as Record<string, any>;
                const plan = planHistoryDoc(data, currentEpoch);

                if (plan.kind === 'stale') {
                    staleRefs.push(hDoc.ref);
                    return null;
                }
                if (plan.kind === 'merge') {
                    return {
                        ref: hDoc.ref,
                        val: (data.segment as Bytes).toUint8Array(),
                    };
                }
                return null;
            })
            .filter((h): h is { ref: DocumentReference; val: Uint8Array } => h !== null);

        if (updatesToProcess.length === 0 && historyToMerge.length === 0) {
            if (staleRefs.length > 0) {
                await deleteStaleEpochDocs(db, path, uid, staleRefs);
                return { success: true, type: 'none' as const, updatesCompacted: staleRefs.length, historySegmentsMerged: 0 };
            }
            return { success: true, type: 'none' as const, updatesCompacted: 0, historySegmentsMerged: 0 };
        }

        // === STEP 3: Choose compaction mode ===
        //
        // DELTA (the steady-state cycle on aged documents): merge ONLY the
        // pending update documents into one history segment. O(new data)
        // CPU and bandwidth — the multi-MB base snapshot is neither
        // downloaded nor re-uploaded.
        //
        // FOLD (amortized): everything (base + history + updates) merges
        // into a fresh GC'd snapshot. Runs when history has accumulated to
        // the fold threshold, when there is no base yet, or when a delta
        // segment would not fit inline in a Firestore document.
        const wantDelta = shouldUseDelta({
            hasBase,
            updateCount: updatesToProcess.length,
            historyCount: historyToMerge.length,
            historyFoldThreshold,
        });

        if (wantDelta) {
            const deltaResult = await tryDeltaCompaction({
                db,
                path,
                uid,
                updatesToProcess,
                staleRefs,
                epoch: currentEpoch,
            });
            if (deltaResult !== null) {
                return deltaResult;
            }
            // Segment would not fit inline — fall through to a full fold.
        }

        // === FOLD: download base, merge all, upload new snapshot ===
        let baseSnapshot: Uint8Array | null = null;
        if (baseStoragePath) {
            try {
                const storageRef = ref(storage, baseStoragePath);
                const buffer = await getBytes(storageRef);
                baseSnapshot = new Uint8Array(buffer);
            } catch (e) {
                console.error("Compaction failed to download base snapshot from storage", e);
                throw e; // Cannot safely compact without base state
            }
        } else if (baseInline) {
            baseSnapshot = baseInline.toUint8Array();
        }

        // GC (default on) rewrites the merged result so deleted-item content
        // is dropped: without it the snapshot grows with total historical
        // churn instead of live content.
        //
        // The merge also validates the candidate and derives the snapshot
        // metadata (state vector + delete-set fingerprint) — all inside the
        // merge Web Worker when available. At multi-megabyte snapshot sizes
        // those walks cost hundreds of milliseconds; doing them worker-side
        // keeps compaction's main-thread cost near zero. A validation
        // failure rejects, and a corrupted merge must never overwrite the
        // canonical snapshot.
        const allContent = [...(baseSnapshot ? [baseSnapshot] : []), ...historyToMerge.map(h => h.val), ...updatesToProcess.map(u => u.data)];
        let candidate: Uint8Array;
        let stateVectorB64: string;
        let deleteSetUpdate: Uint8Array | null = null;
        let oversizedDeleteSet: Uint8Array | null = null;
        try {
            const merged = await mergeUpdatesWithMetaAsync(allContent, { gc: ctx.gc !== false });
            candidate = merged.result;
            stateVectorB64 = toBase64(merged.stateVector);

            // The structs-empty delete-set fingerprint is stored inline on
            // the main document: it lets clients that already cover the
            // snapshot's state vector skip downloading the blob while still
            // proving their deletions are on the server.
            if (merged.dsUpdate.byteLength <= (ctx.maxDeleteSetFieldBytes ?? DEFAULTS.MAX_DELETE_SET_FIELD_BYTES)) {
                deleteSetUpdate = merged.dsUpdate;
            } else {
                // Too large to inline (very old, deletion-heavy document).
                // Offload to Cloud Storage instead of dropping it: without a
                // fingerprint every reconnecting client fails the push-guard
                // coverage proof and writes a spurious O(delete-set) update
                // document on every boot, forever.
                oversizedDeleteSet = merged.dsUpdate;
            }
        } catch (decodeErr) {
            throw new Error(
                `Compaction candidate failed validation: ${decodeErr}`
            );
        }

        const nextVersion = currentVersion + 1;
        const snapshotFilename = `snapshot_v${nextVersion}.bin`;
        const storagePath = `${path}/${snapshotFilename}`;
        const storageRef = ref(storage, storagePath);

        // Upload candidate blob to Cloud Storage first
        // It is safe to upload first because if transaction fails, it just leaves an orphaned file that we ignore.
        await uploadBytes(storageRef, candidate);

        let deleteSetStoragePath: string | null = null;
        if (oversizedDeleteSet) {
            deleteSetStoragePath = `${path}/ds_v${nextVersion}.bin`;
            await uploadBytes(ref(storage, deleteSetStoragePath), oversizedDeleteSet);
        }

        // === STEP 4: Transaction ===
        const result = await performCompactionTransaction({
            db,
            path,
            uid,
            verifiedUpdateRefs: updatesToProcess.map(u => u.ref),
            verifiedHistoryRefs: historyToMerge.map(h => h.ref),
            staleRefs,
            storagePath,
            candidate,
            stateVectorB64,
            deleteSetUpdate,
            deleteSetStoragePath,
            expectedVersion: currentVersion,
        });

        // Garbage Collect Old Storage Snapshot (and its delete-set blob)
        if (result.success && result.type === 'snapshot' && result.previousVersion !== undefined && result.previousVersion > 0) {
            try {
                const oldSnapshotPath = `${path}/snapshot_v${result.previousVersion}.bin`;
                const oldStorageRef = ref(storage, oldSnapshotPath);
                await deleteObject(oldStorageRef);
                console.log(`Garbage collected old snapshot: ${oldSnapshotPath}`);
            } catch (err) {
                console.warn(`Failed to garbage collect old snapshot for ${path}`, err);
            }
            try {
                await deleteObject(ref(storage, `${path}/ds_v${result.previousVersion}.bin`));
            } catch (err) {
                // Normal case: no offloaded delete-set existed for that version
            }
        }

        return result;

    } catch (e: any) {
        return await handleCompactionError(ctx, e, attempt);
    } finally {
        await releaseLock({ db, path, uid });
    }
}

/**
 * DELTA compaction: merge the pending update documents into ONE history
 * segment, leaving the base snapshot untouched.
 *
 * Returns null when the merged segment cannot be stored inline in a
 * Firestore document (caller falls back to a full fold, whose snapshot
 * lives in Cloud Storage and has no such limit).
 */
async function tryDeltaCompaction(params: {
    db: Firestore;
    path: string;
    uid: string;
    updatesToProcess: { ref: DocumentReference; data: Uint8Array }[];
    staleRefs: DocumentReference[];
    epoch: number;
}): Promise<CompactionResult | null> {
    const { db, path, uid, updatesToProcess, staleRefs, epoch } = params;

    // Merge + validate + derive the segment's state vector (clock ends per
    // client — what the sync layer's redundancy checks consume). gc is
    // intentionally off: a partial merge references structs that live in
    // the base snapshot, so a GC rebuild would find missing dependencies
    // and fall back to the plain merge anyway — no point paying for the
    // attempt.
    const merged = await mergeUpdatesWithMetaAsync(updatesToProcess.map(u => u.data), { gc: false });

    if (!deltaSegmentFitsInline(merged.result.byteLength, DEFAULTS.INLINE_UPDATE_LIMIT)) {
        return null;
    }

    const segmentB64Sv = toBase64(merged.stateVector);

    return await runTransaction(db, async (transaction) => {
        // Kill switch: bail if the lock was lost (another client may be
        // mid-fold and about to delete the same update documents).
        const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);
        const lockSnap = await transaction.get(lockRef);
        if (!lockSnap.exists() || lockSnap.data().owner !== uid) {
            throw new Error("Lock lost or expired during compaction phase - Aborting write.");
        }

        // Verify updates still exist before deleting (zombie protection)
        const updateSnaps = await Promise.all(updatesToProcess.map(u => transaction.get(u.ref)));
        const survivors = updateSnaps.filter(snap => snap.exists());
        if (survivors.length === 0) {
            return { success: true, type: 'none' as const, updatesCompacted: 0, historySegmentsMerged: 0 };
        }

        const segmentRef = doc(collection(db, path, FIRESTORE_PATHS.HISTORY));
        transaction.set(segmentRef, {
            ...buildDeltaSegmentDoc({ stateVectorB64: segmentB64Sv, uid, epoch }),
            segment: Bytes.fromUint8Array(merged.result),
            startTime: serverTimestamp(),
        });
        survivors.forEach(snap => transaction.delete(snap.ref));
        staleRefs.forEach(ref => transaction.delete(ref));

        console.log(`Delta-compacted ${survivors.length} updates into history segment (${merged.result.byteLength} bytes)`);

        return {
            success: true,
            type: 'history' as const,
            updatesCompacted: survivors.length,
            historySegmentsMerged: 0,
        };
    });
}

/**
 * Performs the actual compaction within a Firestore transaction.
 *
 * Verifies the version and deletes processed documents.
 */
async function performCompactionTransaction(params: {
    db: Firestore;
    path: string;
    uid: string;
    verifiedUpdateRefs: DocumentReference[];
    verifiedHistoryRefs: DocumentReference[];
    staleRefs: DocumentReference[];
    storagePath: string;
    candidate: Uint8Array;
    stateVectorB64: string;
    deleteSetUpdate: Uint8Array | null;
    deleteSetStoragePath: string | null;
    expectedVersion: number;
}): Promise<CompactionResult> {
    const { db, path, uid, verifiedUpdateRefs, verifiedHistoryRefs, staleRefs, storagePath, candidate, stateVectorB64, deleteSetUpdate, deleteSetStoragePath, expectedVersion } = params;

    return await runTransaction(db, async (transaction) => {
        // === STEP A: THE KILL SWITCH ===
        const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);
        const lockSnap = await transaction.get(lockRef);

        if (!lockSnap.exists() || lockSnap.data().owner !== uid) {
            throw new Error("Lock lost or expired during compaction phase - Aborting write.");
        }

        // === STEP B: Read current state & verify version ===
        const mainRef = doc(db, path);
        const mainSnap = await transaction.get(mainRef);

        let currentVersion = 0;
        if (mainSnap.exists()) {
            const data = mainSnap.data();
            if (typeof data?.version === 'number') {
                currentVersion = data.version;
            }
        }

        if (currentVersion !== expectedVersion) {
            throw new Error("Document version changed during compaction upload. Aborting to retry.");
        }

        // Verify updates still exist (avoid zombie bugs) before deleting
        // P1.1 Optimization: Use parallel transaction.get to eliminate N+1 queries
        const [updateSnaps, historySnaps] = await Promise.all([
            Promise.all(verifiedUpdateRefs.map(ref => transaction.get(ref))),
            Promise.all(verifiedHistoryRefs.map(ref => transaction.get(ref)))
        ]);

        const updatesToProcess = updateSnaps
            .filter(snap => snap.exists())
            .map(snap => ({ ref: snap.ref }));

        const historyToMerge = historySnaps
            .filter(snap => snap.exists())
            .map(snap => ({ ref: snap.ref }));

        if (updatesToProcess.length === 0 && historyToMerge.length === 0) {
            return { success: true, type: 'none' as const, updatesCompacted: 0, historySegmentsMerged: 0 };
        }

        // === STEP C: Commit Pointers ===
        const result = compactToSnapshot({
            transaction,
            mainRef,
            uid,
            storagePath,
            candidate,
            stateVectorB64,
            deleteSetUpdate,
            deleteSetStoragePath,
            currentVersion,
            updatesToProcess,
            historyToMerge,
        });
        // Old-epoch documents ride along in the same transaction: they are
        // never merged, only removed.
        staleRefs.forEach(ref => transaction.delete(ref));
        return result;
    });
}

/**
 * Deletes stale-epoch update/history documents when there is nothing else
 * to compact. Existence is re-verified inside the transaction.
 */
async function deleteStaleEpochDocs(
    db: Firestore,
    path: string,
    uid: string,
    staleRefs: DocumentReference[]
): Promise<void> {
    await runTransaction(db, async (transaction) => {
        const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);
        const lockSnap = await transaction.get(lockRef);
        if (!lockSnap.exists() || lockSnap.data().owner !== uid) {
            throw new Error("Lock lost or expired during compaction phase - Aborting write.");
        }
        const snaps = await Promise.all(staleRefs.map(r => transaction.get(r)));
        snaps.forEach(s => { if (s.exists()) transaction.delete(s.ref); });
    });
}

/**
 * Compacts everything into the base snapshot.
 */
function compactToSnapshot(params: {
    transaction: any;
    mainRef: DocumentReference;
    uid: string;
    storagePath: string;
    candidate: Uint8Array;
    stateVectorB64: string;
    deleteSetUpdate: Uint8Array | null;
    deleteSetStoragePath: string | null;
    currentVersion: number;
    updatesToProcess: { ref: DocumentReference }[];
    historyToMerge: { ref: DocumentReference }[];
}): CompactionResult {
    const { transaction, mainRef, uid, storagePath, candidate, stateVectorB64, deleteSetUpdate, deleteSetStoragePath, currentVersion, updatesToProcess, historyToMerge } = params;

    console.log(`Compacted to Snapshot (Size: ${candidate.byteLength})`);

    transaction.set(mainRef, {
        snapshotStoragePath: storagePath,
        // Drop any legacy inline snapshot now that content lives in Storage.
        // Without this, a merge:true write over an ancient inline-`content`
        // doc keeps `content` alongside snapshotStoragePath + a <=700KB
        // deleteSet, which can exceed Firestore's 1MB doc limit and abort
        // every future compaction on that doc.
        content: deleteField(),
        // Precomputed outside the transaction: the transaction body can
        // re-run on contention, and re-walking a large candidate each
        // attempt is wasted CPU.
        stateVector: stateVectorB64,
        // A stale fingerprint would hide newer deletions. Exactly one of
        // the two fingerprint fields survives: inline for the normal case,
        // a Cloud Storage pointer once the delete-set outgrows the inline
        // cap (dropping it entirely would send every future reconnect down
        // the spurious-push slow path).
        deleteSet: deleteSetUpdate ? Bytes.fromUint8Array(deleteSetUpdate) : deleteField(),
        deleteSetStoragePath: deleteSetStoragePath ?? deleteField(),
        version: nextSnapshotVersion(currentVersion),
        updatedAt: serverTimestamp(),
        // Lets the compacting client's own snapshot listener skip this write
        origin: uid,
    }, { merge: true });

    updatesToProcess.forEach(u => transaction.delete(u.ref));
    historyToMerge.forEach(h => transaction.delete(h.ref));

    return buildSnapshotResult({
        updatesCompacted: updatesToProcess.length,
        historySegmentsMerged: historyToMerge.length,
        currentVersion,
    });
}

/**
 * Handles compaction errors with exponential backoff retry.
 */
async function handleCompactionError(
    ctx: CompactionContext,
    error: any,
    attempt: number
): Promise<CompactionResult> {
    const { isDestroyed } = ctx;

    if (shouldRetryCompaction({ error, attempt, isDestroyed: isDestroyed() })) {
        const backoff = calculateBackoff(attempt);
        console.warn(`Compaction failed (attempt ${attempt}). Retrying in ${Math.floor(backoff)}ms...`, error);

        await wait(backoff);

        if (!isDestroyed()) {
            return compact(ctx, attempt + 1);
        }
    }

    console.error("Compaction failed permanently.", error);
    return {
        success: false,
        type: 'none',
        updatesCompacted: 0,
        historySegmentsMerged: 0,
        error: error instanceof Error ? error : new Error(String(error)),
    };
}
