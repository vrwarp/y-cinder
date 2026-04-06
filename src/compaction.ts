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
    limit,
    DocumentReference,
    Timestamp,
} from "@firebase/firestore";
import { ref, uploadBytes, deleteObject, getBytes, FirebaseStorage } from "@firebase/storage";
import * as Y from "yjs";
import { toBase64 } from "lib0/buffer";
import { DEFAULTS, FIRESTORE_PATHS, TestHooks } from "./types";
import { calculateStateVector, wait, calculateBackoff } from "./utils";
import { acquireLock, releaseLock } from "./locking";
import { mergeUpdatesAsync } from "./merge-utils";

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
        console.log(`Starting compaction (attempt ${attempt})...`);

        // Fetch work items
        const updatesQ = query(
            collection(db, path, FIRESTORE_PATHS.UPDATES),
            orderBy('createdAt', 'asc'),
            limit(compactionLimit)
        );
        const updatesSnap = await getDocs(updatesQ);

        const historyQ = query(
            collection(db, path, FIRESTORE_PATHS.HISTORY),
            orderBy('startTime', 'asc')
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

        // Read updates
        const updatesToProcess: { ref: DocumentReference; data: Uint8Array; createdAt: Timestamp }[] = [];
        for (const uDoc of updateDocs) {
            const freshSnap = await getDoc(uDoc.ref);
            if (freshSnap.exists()) {
                const data = freshSnap.data() as Record<string, any>;
                if (data?.updateStoragePath && !data?.update) {
                    try {
                        const storageRef = ref(storage, data.updateStoragePath);
                        const buffer = await getBytes(storageRef);
                        updatesToProcess.push({
                            ref: uDoc.ref,
                            data: new Uint8Array(buffer),
                            createdAt: data.createdAt,
                        });
                    } catch (e) {
                        console.error(`Compaction skipped storage-backed update ${uDoc.id} due to download failure`, e);
                        // Skip this update - do not process or delete it, but continue compacting the rest
                    }
                } else if (data?.update) {
                    updatesToProcess.push({
                        ref: uDoc.ref,
                        data: (data.update as Bytes).toUint8Array(),
                        createdAt: data.createdAt,
                    });
                }
            }
        }

        // Read history
        const historyToMerge: { ref: DocumentReference; val: Uint8Array }[] = [];
        for (const hDoc of historyDocs) {
            const freshSnap = await getDoc(hDoc.ref);
            if (freshSnap.exists()) {
                const data = freshSnap.data() as Record<string, any>;
                if (data?.segment) {
                    historyToMerge.push({
                        ref: hDoc.ref,
                        val: (data.segment as Bytes).toUint8Array(),
                    });
                }
            }
        }

        if (updatesToProcess.length === 0 && historyToMerge.length === 0) {
            return { success: true, type: 'none' as const, updatesCompacted: 0, historySegmentsMerged: 0 };
        }

        // === STEP 3: Merge and Upload (Outside Transaction) ===
        const allContent = [...(baseSnapshot ? [baseSnapshot] : []), ...historyToMerge.map(h => h.val), ...updatesToProcess.map(u => u.data)];
        const candidate = await mergeUpdatesAsync(allContent);

        // Validate candidate before committing — a corrupted merge must never
        // overwrite the canonical snapshot.
        try {
            Y.decodeUpdate(candidate);
        } catch (decodeErr) {
            throw new Error(
                `Compaction candidate failed validation (${candidate.byteLength} bytes): ${decodeErr}`
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
    expectedVersion: number;
}): Promise<CompactionResult> {
    const { db, path, uid, verifiedUpdateRefs, verifiedHistoryRefs, storagePath, candidate, expectedVersion } = params;

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
        const updatesToProcess: { ref: DocumentReference }[] = [];
        for (const ref of verifiedUpdateRefs) {
            const freshSnap = await transaction.get(ref);
            if (freshSnap.exists()) {
                updatesToProcess.push({ ref });
            }
        }

        const historyToMerge: { ref: DocumentReference }[] = [];
        for (const ref of verifiedHistoryRefs) {
            const freshSnap = await transaction.get(ref);
            if (freshSnap.exists()) {
                historyToMerge.push({ ref });
            }
        }

        if (updatesToProcess.length === 0 && historyToMerge.length === 0) {
            return { success: true, type: 'none' as const, updatesCompacted: 0, historySegmentsMerged: 0 };
        }

        // === STEP C: Commit Pointers ===
        return compactToSnapshot({
            transaction,
            mainRef,
            storagePath,
            candidate,
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
    storagePath: string;
    candidate: Uint8Array;
    currentVersion: number;
    updatesToProcess: { ref: DocumentReference }[];
    historyToMerge: { ref: DocumentReference }[];
}): CompactionResult {
    const { transaction, mainRef, storagePath, candidate, currentVersion, updatesToProcess, historyToMerge } = params;

    console.log(`Compacted to Snapshot (Size: ${candidate.byteLength})`);

    transaction.set(mainRef, {
        snapshotStoragePath: storagePath,
        stateVector: calculateStateVector(candidate),
        version: currentVersion + 1,
        updatedAt: serverTimestamp(),
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
