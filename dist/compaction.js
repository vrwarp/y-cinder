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
import { doc, collection, Bytes, runTransaction, query, orderBy, getDocs, getDoc, serverTimestamp, deleteField, limit, } from "@firebase/firestore";
import { ref, uploadBytes, deleteObject, getBytes } from "@firebase/storage";
import * as Y from "yjs";
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
        const { db, path, uid, lockTTL, compactionLimit, isDestroyed, testHooks, cachedClockOffset, storage } = ctx;
        // 1. Distributed Gate: Try to become the Leader
        // P0.3 FIX: Pass cached clock offset to avoid re-measuring (saves 3 Firestore ops)
        const hasLock = yield acquireLock({ db, path, uid, lockTTL, cachedClockOffset });
        if (!hasLock) {
            return { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };
        }
        try {
            // Fetch work items.
            // Limits are clamped so deletes (updates + history) plus the snapshot
            // write stay within Firestore's 500-op transaction budget. Anything
            // left over is picked up by the next compaction cycle.
            const updatesQ = query(collection(db, path, FIRESTORE_PATHS.UPDATES), orderBy('createdAt', 'asc'), limit(Math.min(compactionLimit, DEFAULTS.MAX_COMPACTION_UPDATES)));
            const updatesSnap = yield getDocs(updatesQ);
            const historyQ = query(collection(db, path, FIRESTORE_PATHS.HISTORY), orderBy('startTime', 'asc'), limit(DEFAULTS.MAX_COMPACTION_HISTORY));
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
            // === STEP 2: Read current state outside transaction to prepare upload ===
            // This avoids uploading files inside a potentially repeating transaction block
            const mainRef = doc(db, path);
            const mainSnap = yield getDoc(mainRef);
            let baseSnapshot = null;
            let currentVersion = 0;
            if (mainSnap.exists()) {
                const data = mainSnap.data();
                // Fetch from Cloud Storage if configured
                if (data === null || data === void 0 ? void 0 : data.snapshotStoragePath) {
                    try {
                        const storageRef = ref(storage, data.snapshotStoragePath);
                        const buffer = yield getBytes(storageRef);
                        baseSnapshot = new Uint8Array(buffer);
                    }
                    catch (e) {
                        console.error("Compaction failed to download base snapshot from storage", e);
                        throw e; // Cannot safely compact without base state
                    }
                }
                else if (data === null || data === void 0 ? void 0 : data.content) {
                    baseSnapshot = data.content.toUint8Array();
                }
                if (typeof (data === null || data === void 0 ? void 0 : data.version) === 'number') {
                    currentVersion = data.version;
                }
            }
            // Use the data already returned by the queries above. Update and
            // history documents are immutable (only ever created or deleted),
            // and the transaction below re-verifies existence before deleting,
            // so re-fetching each document individually would only double the
            // read cost. Storage-backed payloads are downloaded in parallel.
            const updateResults = yield Promise.all(updateDocs.map((uDoc) => __awaiter(this, void 0, void 0, function* () {
                const data = uDoc.data();
                if ((data === null || data === void 0 ? void 0 : data.updateStoragePath) && !(data === null || data === void 0 ? void 0 : data.update)) {
                    try {
                        const storageRef = ref(storage, data.updateStoragePath);
                        const buffer = yield getBytes(storageRef);
                        return {
                            ref: uDoc.ref,
                            data: new Uint8Array(buffer),
                            createdAt: data.createdAt,
                        };
                    }
                    catch (e) {
                        console.error(`Compaction skipped storage-backed update ${uDoc.id} due to download failure`, e);
                        return null;
                    }
                }
                else if (data === null || data === void 0 ? void 0 : data.update) {
                    return {
                        ref: uDoc.ref,
                        data: data.update.toUint8Array(),
                        createdAt: data.createdAt,
                    };
                }
                return null;
            })));
            const updatesToProcess = updateResults.filter((u) => u !== null);
            const historyToMerge = historyDocs
                .map((hDoc) => {
                const data = hDoc.data();
                if (data === null || data === void 0 ? void 0 : data.segment) {
                    return {
                        ref: hDoc.ref,
                        val: data.segment.toUint8Array(),
                    };
                }
                return null;
            })
                .filter((h) => h !== null);
            if (updatesToProcess.length === 0 && historyToMerge.length === 0) {
                return { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };
            }
            // === STEP 3: Merge and Upload (Outside Transaction) ===
            const allContent = [...(baseSnapshot ? [baseSnapshot] : []), ...historyToMerge.map(h => h.val), ...updatesToProcess.map(u => u.data)];
            const candidate = yield mergeUpdatesAsync(allContent);
            // Validate candidate before committing — a corrupted merge must never
            // overwrite the canonical snapshot.
            try {
                Y.decodeUpdate(candidate);
            }
            catch (decodeErr) {
                throw new Error(`Compaction candidate failed validation (${candidate.byteLength} bytes): ${decodeErr}`);
            }
            // Extract a structs-empty update carrying the candidate's full
            // delete-set. Stored inline on the main document, it lets clients
            // that already cover the snapshot's state vector skip downloading
            // the blob while still proving their deletions are on the server.
            let deleteSetUpdate = null;
            try {
                const candidateSV = Y.encodeStateVectorFromUpdate(candidate);
                const dsUpdate = Y.diffUpdate(candidate, candidateSV);
                if (dsUpdate.byteLength <= DEFAULTS.MAX_DELETE_SET_FIELD_BYTES) {
                    deleteSetUpdate = dsUpdate;
                }
            }
            catch (e) {
                console.warn("Failed to extract delete-set fingerprint (optional optimization)", e);
            }
            const nextVersion = currentVersion + 1;
            const snapshotFilename = `snapshot_v${nextVersion}.bin`;
            const storagePath = `${path}/${snapshotFilename}`;
            const storageRef = ref(storage, storagePath);
            // Upload candidate blob to Cloud Storage first
            // It is safe to upload first because if transaction fails, it just leaves an orphaned file that we ignore.
            yield uploadBytes(storageRef, candidate);
            // === STEP 4: Transaction ===
            const result = yield performCompactionTransaction({
                db,
                path,
                uid,
                verifiedUpdateRefs: updatesToProcess.map(u => u.ref),
                verifiedHistoryRefs: historyToMerge.map(h => h.ref),
                storagePath,
                candidate,
                deleteSetUpdate,
                expectedVersion: currentVersion,
            });
            // Garbage Collect Old Storage Snapshot
            if (result.success && result.type === 'snapshot' && result.previousVersion !== undefined && result.previousVersion > 0) {
                try {
                    const oldSnapshotPath = `${path}/snapshot_v${result.previousVersion}.bin`;
                    const oldStorageRef = ref(storage, oldSnapshotPath);
                    yield deleteObject(oldStorageRef);
                    console.log(`Garbage collected old snapshot: ${oldSnapshotPath}`);
                }
                catch (err) {
                    console.warn(`Failed to garbage collect old snapshot for ${path}`, err);
                }
            }
            return result;
        }
        catch (e) {
            return yield handleCompactionError(ctx, e, attempt);
        }
        finally {
            yield releaseLock({ db, path, uid });
        }
    });
}
/**
 * Performs the actual compaction within a Firestore transaction.
 *
 * Verifies the version and deletes processed documents.
 */
function performCompactionTransaction(params) {
    return __awaiter(this, void 0, void 0, function* () {
        const { db, path, uid, verifiedUpdateRefs, verifiedHistoryRefs, storagePath, candidate, deleteSetUpdate, expectedVersion } = params;
        return yield runTransaction(db, (transaction) => __awaiter(this, void 0, void 0, function* () {
            // === STEP A: THE KILL SWITCH ===
            const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);
            const lockSnap = yield transaction.get(lockRef);
            if (!lockSnap.exists() || lockSnap.data().owner !== uid) {
                throw new Error("Lock lost or expired during compaction phase - Aborting write.");
            }
            // === STEP B: Read current state & verify version ===
            const mainRef = doc(db, path);
            const mainSnap = yield transaction.get(mainRef);
            let currentVersion = 0;
            if (mainSnap.exists()) {
                const data = mainSnap.data();
                if (typeof (data === null || data === void 0 ? void 0 : data.version) === 'number') {
                    currentVersion = data.version;
                }
            }
            if (currentVersion !== expectedVersion) {
                throw new Error("Document version changed during compaction upload. Aborting to retry.");
            }
            // Verify updates still exist (avoid zombie bugs) before deleting
            // P1.1 Optimization: Use parallel transaction.get to eliminate N+1 queries
            const [updateSnaps, historySnaps] = yield Promise.all([
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
                return { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };
            }
            // === STEP C: Commit Pointers ===
            return compactToSnapshot({
                transaction,
                mainRef,
                uid,
                storagePath,
                candidate,
                deleteSetUpdate,
                currentVersion,
                updatesToProcess,
                historyToMerge,
            });
        }));
    });
}
/**
 * Compacts everything into the base snapshot.
 */
function compactToSnapshot(params) {
    const { transaction, mainRef, uid, storagePath, candidate, deleteSetUpdate, currentVersion, updatesToProcess, historyToMerge } = params;
    console.log(`Compacted to Snapshot (Size: ${candidate.byteLength})`);
    transaction.set(mainRef, {
        snapshotStoragePath: storagePath,
        stateVector: calculateStateVector(candidate),
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
