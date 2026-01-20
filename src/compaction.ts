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
    serverTimestamp,
    limit,
    DocumentReference,
    Timestamp,
} from "@firebase/firestore";
import * as Y from "yjs";
import { toBase64 } from "lib0/buffer";
import { DEFAULTS, FIRESTORE_PATHS, TestHooks } from "./types";
import { calculateStateVector, wait, calculateBackoff } from "./utils";
import { acquireLock, releaseLock } from "./locking";

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
    /** Callback when compaction state changes */
    onCompactionStateChange?: (isCompacting: boolean) => void;
    /** P0.3 FIX: Cached clock offset to pass to locking */
    cachedClockOffset?: number;
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
    const { db, path, uid, lockTTL, compactionLimit, isDestroyed, testHooks, onCompactionStateChange, cachedClockOffset } = ctx;

    // 1. Distributed Gate: Try to become the Leader
    // P0.3 FIX: Pass cached clock offset to avoid re-measuring (saves 3 Firestore ops)
    const hasLock = await acquireLock({ db, path, uid, lockTTL, cachedClockOffset });
    if (!hasLock) {
        return { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };
    }

    onCompactionStateChange?.(true);

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

        const result = await performCompactionTransaction({
            db,
            path,
            uid,
            updateDocs,
            historyDocs,
        });

        return result;

    } catch (e: any) {
        return await handleCompactionError(ctx, e, attempt);
    } finally {
        onCompactionStateChange?.(false);
        await releaseLock({ db, path, uid });
    }
}

/**
 * Performs the actual compaction within a Firestore transaction.
 */
async function performCompactionTransaction(params: {
    db: Firestore;
    path: string;
    uid: string;
    updateDocs: any[];
    historyDocs: any[];
}): Promise<CompactionResult> {
    const { db, path, uid, updateDocs, historyDocs } = params;

    return await runTransaction(db, async (transaction) => {
        // === STEP A: THE KILL SWITCH ===
        // Re-read the lock to ensure we still own it
        const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);
        const lockSnap = await transaction.get(lockRef);

        if (!lockSnap.exists() || lockSnap.data().owner !== uid) {
            throw new Error("Lock lost or expired during compaction phase - Aborting write.");
        }

        // === STEP B: Read current state ===
        const mainRef = doc(db, path);
        const mainSnap = await transaction.get(mainRef);

        let baseSnapshot: Uint8Array | null = null;
        let currentVersion = 0;

        if (mainSnap.exists()) {
            const data = mainSnap.data();
            if (data?.content) {
                baseSnapshot = (data.content as Bytes).toUint8Array();
            }
            if (typeof data?.version === 'number') {
                currentVersion = data.version;
            }
        }

        // Read updates to merge (re-read in transaction for consistency)
        const updatesToProcess: { ref: DocumentReference; data: Uint8Array; createdAt: Timestamp }[] = [];
        for (const uDoc of updateDocs) {
            const freshSnap = await transaction.get(uDoc.ref);
            if (freshSnap.exists()) {
                const data = freshSnap.data() as Record<string, any>;
                if (data?.update) {
                    updatesToProcess.push({
                        ref: uDoc.ref,
                        data: (data.update as Bytes).toUint8Array(),
                        createdAt: data.createdAt,
                    });
                }
            }
        }

        // Read history segments
        const historyToMerge: { ref: DocumentReference; val: Uint8Array }[] = [];
        for (const hDoc of historyDocs) {
            const freshSnap = await transaction.get(hDoc.ref);
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

        // === STEP C: Perform Merge ===
        const allContent: Uint8Array[] = [];
        if (baseSnapshot) allContent.push(baseSnapshot);
        historyToMerge.forEach(h => allContent.push(h.val));
        updatesToProcess.forEach(u => allContent.push(u.data));

        const candidate = Y.mergeUpdates(allContent);
        const sizeInBytes = candidate.byteLength;

        if (sizeInBytes < DEFAULTS.TARGET_SNAPSHOT_SIZE) {
            // Path 1: Compact to Snapshot
            return compactToSnapshot({
                transaction,
                mainRef,
                candidate,
                currentVersion,
                updatesToProcess,
                historyToMerge,
            });
        } else {
            // Path 2: Compact to History Segments
            return compactToHistory({
                transaction,
                db,
                path,
                updatesToProcess,
            });
        }
    });
}

/**
 * Compacts everything into the base snapshot.
 */
function compactToSnapshot(params: {
    transaction: any;
    mainRef: DocumentReference;
    candidate: Uint8Array;
    currentVersion: number;
    updatesToProcess: { ref: DocumentReference }[];
    historyToMerge: { ref: DocumentReference }[];
}): CompactionResult {
    const { transaction, mainRef, candidate, currentVersion, updatesToProcess, historyToMerge } = params;

    console.log(`Compacted to Snapshot (Size: ${candidate.byteLength})`);

    transaction.set(mainRef, {
        content: Bytes.fromUint8Array(candidate),
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
    };
}

/**
 * Compacts updates into history segments (with chunking for large updates).
 */
function compactToHistory(params: {
    transaction: any;
    db: Firestore;
    path: string;
    updatesToProcess: { ref: DocumentReference; data: Uint8Array; createdAt: Timestamp }[];
}): CompactionResult {
    const { transaction, db, path, updatesToProcess } = params;

    if (updatesToProcess.length === 0) {
        return { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };
    }

    const MAX_SEGMENT_SIZE = DEFAULTS.TARGET_SNAPSHOT_SIZE;

    // Try merging all first (optimistic)
    const allUpdates = updatesToProcess.map(u => u.data);
    const pendingMerge = Y.mergeUpdates(allUpdates);

    if (pendingMerge.byteLength < MAX_SEGMENT_SIZE) {
        // Fast path: It fits in one segment
        const segmentId = Math.random().toString(36).substring(2);
        const historyRef = doc(collection(db, path, FIRESTORE_PATHS.HISTORY), segmentId);

        // P1.2 FIX: Calculate and store stateVector for efficient sync redundancy checks
        const tempDoc = new Y.Doc();
        Y.applyUpdate(tempDoc, pendingMerge);
        const stateVector = toBase64(Y.encodeStateVector(tempDoc));

        transaction.set(historyRef, {
            segment: Bytes.fromUint8Array(pendingMerge),
            startTime: updatesToProcess[0].createdAt,
            endTime: updatesToProcess[updatesToProcess.length - 1].createdAt,
            stateVector, // P1.2 FIX: Pre-computed stateVector
        });

        for (const item of updatesToProcess) {
            transaction.delete(item.ref);
        }

        return {
            success: true,
            type: 'history',
            updatesCompacted: updatesToProcess.length,
            historySegmentsMerged: 0,
        };
    }

    // Slow path: Chunk into multiple segments
    return chunkIntoHistorySegments({ transaction, db, path, updatesToProcess, maxSegmentSize: MAX_SEGMENT_SIZE });
}

/**
 * Chunks updates into multiple history segments when they exceed size limits.
 */
function chunkIntoHistorySegments(params: {
    transaction: any;
    db: Firestore;
    path: string;
    updatesToProcess: { ref: DocumentReference; data: Uint8Array; createdAt: Timestamp }[];
    maxSegmentSize: number;
}): CompactionResult {
    const { transaction, db, path, updatesToProcess, maxSegmentSize } = params;

    let currentBatch: Uint8Array[] = [];
    let currentBatchSize = 0;
    let batchStartIndex = 0;
    let segmentsCreated = 0;

    for (let i = 0; i < updatesToProcess.length; i++) {
        const item = updatesToProcess[i];
        const updateSize = item.data.byteLength;

        if (currentBatchSize + updateSize > maxSegmentSize && currentBatch.length > 0) {
            // Flush current batch
            const mergedBatch = Y.mergeUpdates(currentBatch);
            const segmentId = Math.random().toString(36).substring(2);
            const historyRef = doc(collection(db, path, FIRESTORE_PATHS.HISTORY), segmentId);

            transaction.set(historyRef, {
                segment: Bytes.fromUint8Array(mergedBatch),
                startTime: updatesToProcess[batchStartIndex].createdAt,
                endTime: updatesToProcess[i - 1].createdAt,
            });
            segmentsCreated++;

            for (let j = batchStartIndex; j < i; j++) {
                transaction.delete(updatesToProcess[j].ref);
            }

            currentBatch = [];
            currentBatchSize = 0;
            batchStartIndex = i;
        }

        currentBatch.push(item.data);
        currentBatchSize += updateSize;
    }

    // Flush remaining batch
    if (currentBatch.length > 0) {
        const mergedBatch = Y.mergeUpdates(currentBatch);
        const segmentId = Math.random().toString(36).substring(2);
        const historyRef = doc(collection(db, path, FIRESTORE_PATHS.HISTORY), segmentId);

        transaction.set(historyRef, {
            segment: Bytes.fromUint8Array(mergedBatch),
            startTime: updatesToProcess[batchStartIndex].createdAt,
            endTime: updatesToProcess[updatesToProcess.length - 1].createdAt,
        });
        segmentsCreated++;

        for (let j = batchStartIndex; j < updatesToProcess.length; j++) {
            transaction.delete(updatesToProcess[j].ref);
        }
    }

    return {
        success: true,
        type: 'history',
        updatesCompacted: updatesToProcess.length,
        historySegmentsMerged: segmentsCreated,
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
