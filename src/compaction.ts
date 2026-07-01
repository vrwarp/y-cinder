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

        // === STEP 2: Read current state outside transaction to prepare upload ===
        // This avoids uploading files inside a potentially repeating transaction block
        const mainRef = doc(db, path);
        const mainSnap = await getDoc(mainRef);

        let baseSnapshot: Uint8Array | null = null;
        let currentVersion = 0;

        if (mainSnap.exists()) {
            const data = mainSnap.data();
            // Fetch from Cloud Storage if configured
            if (data?.snapshotStoragePath) {
                try {
                    const storageRef = ref(storage, data.snapshotStoragePath);
                    const buffer = await getBytes(storageRef);
                    baseSnapshot = new Uint8Array(buffer);
                } catch (e) {
                    console.error("Compaction failed to download base snapshot from storage", e);
                    throw e; // Cannot safely compact without base state
                }
            } else if (data?.content) {
                baseSnapshot = (data.content as Bytes).toUint8Array();
            }
            if (typeof data?.version === 'number') {
                currentVersion = data.version;
            }
        }

        // Use the data already returned by the queries above. Update and
        // history documents are immutable (only ever created or deleted),
        // and the transaction below re-verifies existence before deleting,
        // so re-fetching each document individually would only double the
        // read cost. Storage-backed payloads are downloaded in parallel.
        const updateResults = await Promise.all(updateDocs.map(async (uDoc) => {
            const data = uDoc.data() as Record<string, any>;
            if (data?.updateStoragePath && !data?.update) {
                try {
                    const storageRef = ref(storage, data.updateStoragePath);
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
            } else if (data?.update) {
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
                if (data?.segment) {
                    return {
                        ref: hDoc.ref,
                        val: (data.segment as Bytes).toUint8Array(),
                    };
                }
                return null;
            })
            .filter((h): h is { ref: DocumentReference; val: Uint8Array } => h !== null);

        if (updatesToProcess.length === 0 && historyToMerge.length === 0) {
            return { success: true, type: 'none' as const, updatesCompacted: 0, historySegmentsMerged: 0 };
        }

        // === STEP 3: Merge and Upload (Outside Transaction) ===
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
        try {
            const merged = await mergeUpdatesWithMetaAsync(allContent, { gc: ctx.gc !== false });
            candidate = merged.result;
            stateVectorB64 = toBase64(merged.stateVector);

            // The structs-empty delete-set fingerprint is stored inline on
            // the main document: it lets clients that already cover the
            // snapshot's state vector skip downloading the blob while still
            // proving their deletions are on the server.
            if (merged.dsUpdate.byteLength <= DEFAULTS.MAX_DELETE_SET_FIELD_BYTES) {
                deleteSetUpdate = merged.dsUpdate;
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

        // === STEP 4: Transaction ===
        const result = await performCompactionTransaction({
            db,
            path,
            uid,
            verifiedUpdateRefs: updatesToProcess.map(u => u.ref),
            verifiedHistoryRefs: historyToMerge.map(h => h.ref),
            storagePath,
            candidate,
            stateVectorB64,
            deleteSetUpdate,
            expectedVersion: currentVersion,
        });

        // Garbage Collect Old Storage Snapshot
        if (result.success && result.type === 'snapshot' && result.previousVersion !== undefined && result.previousVersion > 0) {
            try {
                const oldSnapshotPath = `${path}/snapshot_v${result.previousVersion}.bin`;
                const oldStorageRef = ref(storage, oldSnapshotPath);
                await deleteObject(oldStorageRef);
                console.log(`Garbage collected old snapshot: ${oldSnapshotPath}`);
            } catch (err) {
                console.warn(`Failed to garbage collect old snapshot for ${path}`, err);
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
    storagePath: string;
    candidate: Uint8Array;
    stateVectorB64: string;
    deleteSetUpdate: Uint8Array | null;
    expectedVersion: number;
}): Promise<CompactionResult> {
    const { db, path, uid, verifiedUpdateRefs, verifiedHistoryRefs, storagePath, candidate, stateVectorB64, deleteSetUpdate, expectedVersion } = params;

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
        return compactToSnapshot({
            transaction,
            mainRef,
            uid,
            storagePath,
            candidate,
            stateVectorB64,
            deleteSetUpdate,
            currentVersion,
            updatesToProcess,
            historyToMerge,
        });
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
    currentVersion: number;
    updatesToProcess: { ref: DocumentReference }[];
    historyToMerge: { ref: DocumentReference }[];
}): CompactionResult {
    const { transaction, mainRef, uid, storagePath, candidate, stateVectorB64, deleteSetUpdate, currentVersion, updatesToProcess, historyToMerge } = params;

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
        // A stale fingerprint would hide newer deletions, so when the
        // delete-set is too large to store we remove the field entirely
        deleteSet: deleteSetUpdate ? Bytes.fromUint8Array(deleteSetUpdate) : deleteField(),
        version: currentVersion + 1,
        updatedAt: serverTimestamp(),
        // Lets the compacting client's own snapshot listener skip this write
        origin: uid,
    }, { merge: true });

    updatesToProcess.forEach(u => transaction.delete(u.ref));
    historyToMerge.forEach(h => transaction.delete(h.ref));

    return {
        success: true,
        type: 'snapshot',
        updatesCompacted: updatesToProcess.length,
        historySegmentsMerged: historyToMerge.length,
        previousVersion: currentVersion > 0 ? currentVersion : undefined,
    };
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

    const isRetryable = error.code === 'aborted' || error.code === 'unavailable' || error.code === 'deadline-exceeded';
    const isLockLostError = error.message?.includes('Lock lost');

    if (attempt < DEFAULTS.MAX_RETRIES && isRetryable && !isLockLostError && !isDestroyed()) {
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
