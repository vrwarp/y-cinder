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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
import { doc, collection, Bytes, runTransaction, query, orderBy, getDocs, serverTimestamp, limit, } from "@firebase/firestore";
import * as Y from "yjs";
import { toBase64 } from "lib0/buffer";
import { DEFAULTS, FIRESTORE_PATHS } from "./types";
import { calculateStateVector, wait, calculateBackoff } from "./utils";
import { acquireLock, releaseLock } from "./locking";
import { mergeUpdatesAsync } from "./merge-utils";
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
export function compact(ctx, attempt = 1) {
    return __awaiter(this, void 0, void 0, function* () {
        const { db, path, uid, lockTTL, compactionLimit, isDestroyed, testHooks, onCompactionStateChange, cachedClockOffset } = ctx;
        // 1. Distributed Gate: Try to become the Leader
        // P0.3 FIX: Pass cached clock offset to avoid re-measuring (saves 3 Firestore ops)
        const hasLock = yield acquireLock({ db, path, uid, lockTTL, cachedClockOffset });
        if (!hasLock) {
            return { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };
        }
        onCompactionStateChange === null || onCompactionStateChange === void 0 ? void 0 : onCompactionStateChange(true);
        try {
            console.log(`Starting compaction (attempt ${attempt})...`);
            // Fetch work items
            const updatesQ = query(collection(db, path, FIRESTORE_PATHS.UPDATES), orderBy('createdAt', 'asc'), limit(compactionLimit));
            const updatesSnap = yield getDocs(updatesQ);
            const historyQ = query(collection(db, path, FIRESTORE_PATHS.HISTORY), orderBy('startTime', 'asc'));
            const historySnaps = yield getDocs(historyQ);
            if (updatesSnap.empty && historySnaps.empty) {
                return { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };
            }
            const updateDocs = updatesSnap.docs;
            const historyDocs = historySnaps.docs;
            // Test hook for simulating concurrent modifications
            if (testHooks === null || testHooks === void 0 ? void 0 : testHooks.beforeTransaction) {
                yield testHooks.beforeTransaction();
            }
            const result = yield performCompactionTransaction({
                db,
                path,
                uid,
                updateDocs,
                historyDocs,
            });
            return result;
        }
        catch (e) {
            return yield handleCompactionError(ctx, e, attempt);
        }
        finally {
            onCompactionStateChange === null || onCompactionStateChange === void 0 ? void 0 : onCompactionStateChange(false);
            yield releaseLock({ db, path, uid });
        }
    });
}
/**
 * Performs the actual compaction within a Firestore transaction.
 */
function performCompactionTransaction(params) {
    return __awaiter(this, void 0, void 0, function* () {
        const { db, path, uid, updateDocs, historyDocs } = params;
        return yield runTransaction(db, (transaction) => __awaiter(this, void 0, void 0, function* () {
            // === STEP A: THE KILL SWITCH ===
            // Re-read the lock to ensure we still own it
            const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);
            const lockSnap = yield transaction.get(lockRef);
            if (!lockSnap.exists() || lockSnap.data().owner !== uid) {
                throw new Error("Lock lost or expired during compaction phase - Aborting write.");
            }
            // === STEP B: Read current state ===
            const mainRef = doc(db, path);
            const mainSnap = yield transaction.get(mainRef);
            let baseSnapshot = null;
            let currentVersion = 0;
            if (mainSnap.exists()) {
                const data = mainSnap.data();
                if (data === null || data === void 0 ? void 0 : data.content) {
                    baseSnapshot = data.content.toUint8Array();
                }
                if (typeof (data === null || data === void 0 ? void 0 : data.version) === 'number') {
                    currentVersion = data.version;
                }
            }
            // Read updates to merge (re-read in transaction for consistency)
            const updatesToProcess = [];
            for (const uDoc of updateDocs) {
                const freshSnap = yield transaction.get(uDoc.ref);
                if (freshSnap.exists()) {
                    const data = freshSnap.data();
                    if (data === null || data === void 0 ? void 0 : data.update) {
                        updatesToProcess.push({
                            ref: uDoc.ref,
                            data: data.update.toUint8Array(),
                            createdAt: data.createdAt,
                        });
                    }
                }
            }
            // Read history segments
            const historyToMerge = [];
            for (const hDoc of historyDocs) {
                const freshSnap = yield transaction.get(hDoc.ref);
                if (freshSnap.exists()) {
                    const data = freshSnap.data();
                    if (data === null || data === void 0 ? void 0 : data.segment) {
                        historyToMerge.push({
                            ref: hDoc.ref,
                            val: data.segment.toUint8Array(),
                        });
                    }
                }
            }
            if (updatesToProcess.length === 0 && historyToMerge.length === 0) {
                return { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };
            }
            // === STEP C: Perform Merge ===
            // CRITICAL FIX: Use async merge to avoid blocking main thread
            // Note: Inside transaction, this may fall back to sync if worker unavailable
            // Try to merge everything
            const allContent = [...(baseSnapshot ? [baseSnapshot] : []), ...historyToMerge.map(h => h.val), ...updatesToProcess.map(u => u.data)];
            const candidate = yield mergeUpdatesAsync(allContent);
            const sizeInBytes = candidate.byteLength;
            if (candidate.byteLength <= DEFAULTS.TARGET_SNAPSHOT_SIZE) {
                // Path 1: Compact to Snapshot
                return yield compactToSnapshot({
                    transaction,
                    mainRef,
                    candidate,
                    currentVersion,
                    updatesToProcess,
                    historyToMerge,
                });
            }
            else {
                // Path 2: Compact to History Segments
                return yield compactToHistory({
                    transaction,
                    db,
                    path,
                    updatesToProcess,
                });
            }
        }));
    });
}
/**
 * Compacts everything into the base snapshot.
 */
function compactToSnapshot(params) {
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
 * CRITICAL FIX: Now async to support off-main-thread merge.
 */
function compactToHistory(params) {
    return __awaiter(this, void 0, void 0, function* () {
        const { transaction, db, path, updatesToProcess } = params;
        if (updatesToProcess.length === 0) {
            return { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };
        }
        const MAX_SEGMENT_SIZE = DEFAULTS.TARGET_SNAPSHOT_SIZE;
        // Try merging all first (optimistic) - CRITICAL FIX: async merge
        const allUpdates = updatesToProcess.map(u => u.data);
        const pendingMerge = yield mergeUpdatesAsync(allUpdates);
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
        return yield chunkIntoHistorySegments({ transaction, db, path, updatesToProcess, maxSegmentSize: MAX_SEGMENT_SIZE });
    });
}
/**
 * Chunks updates into multiple history segments when they exceed size limits.
 *
 * STRATEGY: We keep adding updates to a batch until the ESTIMATED merged size
 * would exceed the limit. Then we flush that batch as a segment.
 *
 * IMPORTANT: Each segment is independently mergeable because Y.mergeUpdates
 * combines updates with shared history correctly. The sync will apply segments
 * in order, building up the state progressively.
 */
function chunkIntoHistorySegments(params) {
    return __awaiter(this, void 0, void 0, function* () {
        const { transaction, db, path, updatesToProcess, maxSegmentSize } = params;
        let currentBatch = [];
        let batchStartIndex = 0;
        let segmentsCreated = 0;
        for (let i = 0; i < updatesToProcess.length; i++) {
            const item = updatesToProcess[i];
            // Add to current batch
            currentBatch.push(item.data);
            // Check if we should flush (estimate by trying merge)
            // This is expensive but accurate
            const merged = yield mergeUpdatesAsync(currentBatch);
            const mergedSize = merged.byteLength;
            // If we exceed limit and have more than 1 item, flush all but the last
            if (mergedSize >= maxSegmentSize && currentBatch.length > 1) {
                // Remove the last item (the one that pushed us over)
                currentBatch.pop();
                // Merge the remaining items
                const segmentData = yield mergeUpdatesAsync(currentBatch);
                const segmentId = Math.random().toString(36).substring(2);
                const historyRef = doc(collection(db, path, FIRESTORE_PATHS.HISTORY), segmentId);
                // FIX: Calculate and store stateVector for efficient sync redundancy checks
                const tempDoc = new Y.Doc();
                Y.applyUpdate(tempDoc, segmentData);
                const encodedSV = Y.encodeStateVector(tempDoc);
                // Only store SV if it's not empty (length > 1 or non-zero). 
                // An empty SV ([0]) implies the updates were not applied (e.g. pending dependencies),
                // so we shouldn't claim to "know" the state is empty.
                let stateVector;
                if (encodedSV.length > 1 || encodedSV[0] !== 0) {
                    stateVector = toBase64(encodedSV);
                }
                const historyData = {
                    segment: Bytes.fromUint8Array(segmentData),
                    startTime: updatesToProcess[batchStartIndex].createdAt,
                    endTime: updatesToProcess[i - 1].createdAt,
                };
                if (stateVector) {
                    historyData.stateVector = stateVector;
                }
                transaction.set(historyRef, historyData);
                segmentsCreated++;
                // Start new batch with just the current item
                currentBatch = [item.data];
                batchStartIndex = i;
            }
        }
        // Flush remaining batch
        if (currentBatch.length > 0) {
            const segmentData = yield mergeUpdatesAsync(currentBatch);
            const segmentId = Math.random().toString(36).substring(2);
            const historyRef = doc(collection(db, path, FIRESTORE_PATHS.HISTORY), segmentId);
            const tempDoc = new Y.Doc();
            Y.applyUpdate(tempDoc, segmentData);
            const encodedSV = Y.encodeStateVector(tempDoc);
            let stateVector;
            if (encodedSV.length > 1 || encodedSV[0] !== 0) {
                stateVector = toBase64(encodedSV);
            }
            const historyData = {
                segment: Bytes.fromUint8Array(segmentData),
                startTime: updatesToProcess[batchStartIndex].createdAt,
                endTime: updatesToProcess[updatesToProcess.length - 1].createdAt,
            };
            if (stateVector) {
                historyData.stateVector = stateVector;
            }
            transaction.set(historyRef, historyData);
            segmentsCreated++;
        }
        // Delete all processed updates
        for (const item of updatesToProcess) {
            transaction.delete(item.ref);
        }
        return {
            success: true,
            type: 'history',
            updatesCompacted: updatesToProcess.length,
            historySegmentsMerged: segmentsCreated,
        };
    });
}
/**
 * Handles compaction errors with exponential backoff retry.
 */
function handleCompactionError(ctx, error, attempt) {
    var _a;
    return __awaiter(this, void 0, void 0, function* () {
        const { isDestroyed } = ctx;
        const isRetryable = error.code === 'aborted' || error.code === 'unavailable' || error.code === 'deadline-exceeded';
        const isLockLostError = (_a = error.message) === null || _a === void 0 ? void 0 : _a.includes('Lock lost');
        if (attempt < DEFAULTS.MAX_RETRIES && isRetryable && !isLockLostError && !isDestroyed()) {
            const backoff = calculateBackoff(attempt);
            console.warn(`Compaction failed (attempt ${attempt}). Retrying in ${Math.floor(backoff)}ms...`, error);
            yield wait(backoff);
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
    });
}
