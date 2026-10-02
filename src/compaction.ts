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
 * - **Deletion**: Update and history documents are immutable and created
 *   under unique auto IDs, and only a transaction that reads
 *   LOCK_COMPACTION and finds itself the owner deletes them (delta, fold
 *   and stale-epoch cleanup here, the squash in squash.ts). These
 *   transactions delete the refs their queries returned WITHOUT re-reading
 *   them: each deleted document's payload is already in what the
 *   transaction commits (or is stale-epoch data), deleting a missing
 *   document is a no-op, and another deleter committing in between would
 *   have changed the lock and failed our commit. A re-read would cost a
 *   billed read of the full payload and one RPC per document, on every
 *   transaction attempt. A lockless deleter, or IDs that are reused or
 *   rewritten in place, would break this (an existence re-read would not
 *   catch those either).
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
    getCountFromServer,
    serverTimestamp,
    deleteField,
    limit,
    DocumentReference,
    Timestamp,
} from "@firebase/firestore";
import { ref, deleteObject, getBytes, FirebaseStorage } from "@firebase/storage";
import * as Y from "yjs";
import { fromBase64, toBase64 } from "lib0/buffer";
import {
    blobsReplacedByFold,
    buildDeltaSegmentDoc,
    buildSnapshotResult,
    deleteSetFitsInline,
    deltaSegmentFitsInline,
    effectiveFoldThreshold,
    epochOf,
    foldDeleteSetPath,
    foldSnapshotPath,
    foldTailBaseClocks,
    foldTailPath,
    nextSnapshotVersion,
    planHistoryDoc,
    planUpdateDoc,
    readMainDocState,
    shouldPublishFoldTail,
    shouldRetryCompaction,
    shouldUseDelta,
    updateBlobPath,
} from './compaction-policy';
import { DEFAULTS, FIRESTORE_PATHS, TestHooks } from "./types";
import { wait, calculateBackoff, generateSessionId, writeStateVector } from "./utils";
import { acquireLock, releaseLock } from "./locking";
import { mergeUpdatesWithMetaAsync } from "./merge-utils";
import { uploadBlob } from "./storage-blobs";
import { updateHasDeletions } from "./update-metadata";

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
     * Capped at DEFAULTS.MAX_COMPACTION_HISTORY + 1: one fold merges at
     * most MAX_COMPACTION_HISTORY segments.
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
 * What a committed compaction transaction did: its result, and the
 * documents it deleted (whose Storage blobs the cycle then reclaims).
 */
interface CommittedCompaction {
    result: CompactionResult;
    deletedRefs: DocumentReference[];
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

        // One row past the cap: it is never merged, it only reveals that
        // history extends beyond what this cycle can fold.
        const historyQ = query(
            collection(db, path, FIRESTORE_PATHS.HISTORY),
            orderBy('startTime', 'asc'),
            limit(DEFAULTS.MAX_COMPACTION_HISTORY + 1)
        );
        // Segment payloads are only merged by a fold, so the fold path
        // fetches them; a delta cycle just counts the segments (STEP 3).
        // With no update documents there is nothing to delta: fetch them
        // now, which also detects a cycle with nothing to do.
        const prefetchedHistory = updatesSnap.empty ? await getDocs(historyQ) : null;

        if (updatesSnap.empty && prefetchedHistory.empty) {
            return { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };
        }

        const updateDocs = updatesSnap.docs;

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
        // The Storage blob of every pointer document read above, by
        // document path: reclaimed once a committed transaction deletes
        // the pointer (see reclaimUpdateBlobs).
        const updateBlobs = new Map<string, string>();

        // Use the data already returned by the queries. Update and
        // history documents are immutable (only ever created or deleted),
        // so neither this step nor the commit transaction re-fetches them
        // (see "Deletion" in the module header): that would only double
        // the read cost. Storage-backed payloads are downloaded in parallel.
        const updateResults = await Promise.all(updateDocs.map(async (uDoc) => {
            const data = uDoc.data() as Record<string, any>;
            const plan = planUpdateDoc(data, currentEpoch);
            const blobPath = updateBlobPath(data);
            if (blobPath !== null) {
                updateBlobs.set(uDoc.ref.path, blobPath);
            }

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
                    // Cannot safely compact around a missing payload: the
                    // same client's later updates (and anything built on
                    // them) would merge past a clock gap, and the result's
                    // state vector (clock ends) would claim the skipped
                    // range — clients would then drop the update document
                    // still holding it as redundant. Abort; a later cycle
                    // retries once the download succeeds.
                    console.error(`Compaction failed to download storage-backed update ${uDoc.id}`, e);
                    throw e;
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
        //
        // The choice needs only how many history segments exist, so they
        // are counted (countHistory), not downloaded: the k-th delta after
        // a fold would otherwise re-download the k segments every client
        // already holds. shouldUseDelta only grows stricter as history
        // grows, so when even an empty history would fold (no base, nothing
        // new, a threshold of 1) the fold is certain and the count skipped.
        const modeParams = { hasBase, updateCount: updatesToProcess.length, historyFoldThreshold };
        const wantDelta = shouldUseDelta({ ...modeParams, historyCount: 0 })
            && shouldUseDelta({ ...modeParams, historyCount: await countHistory(db, path, historyFoldThreshold) });

        if (wantDelta) {
            const delta = await tryDeltaCompaction({
                db,
                path,
                uid,
                updatesToProcess,
                staleRefs,
                epoch: currentEpoch,
            });
            if (delta !== null) {
                await reclaimUpdateBlobs(storage, updateBlobs, delta.deletedRefs);
                return delta.result;
            }
            // Segment would not fit inline — fall through to a full fold.
        }

        // A fold merges the segments themselves, so it needs their payloads.
        const historySnaps = prefetchedHistory ?? await getDocs(historyQ);
        const historyTruncated = historySnaps.docs.length > DEFAULTS.MAX_COMPACTION_HISTORY;
        const historyDocs = historySnaps.docs.slice(0, DEFAULTS.MAX_COMPACTION_HISTORY);

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
                const deletedRefs = await deleteStaleEpochDocs(db, path, uid, staleRefs);
                await reclaimUpdateBlobs(storage, updateBlobs, deletedRefs);
                return { success: true, type: 'none' as const, updatesCompacted: staleRefs.length, historySegmentsMerged: 0 };
            }
            return { success: true, type: 'none' as const, updatesCompacted: 0, historySegmentsMerged: 0 };
        }

        // A fold must merge a per-client prefix of the document: the
        // snapshot's state vector holds clock ends, and the sync layer skips
        // every segment that vector covers. When history extends past what
        // one fold can merge, the pending updates are newer than the
        // segments left behind, so folding them in would make the snapshot
        // claim clocks it does not hold and fresh clients would skip those
        // segments for good. Such a fold takes base + the oldest history
        // only; the updates wait for a later cycle.
        const updatesToFold = historyTruncated ? [] : updatesToProcess;

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
        const tailSources = [...historyToMerge.map(h => h.val), ...updatesToFold.map(u => u.data)];
        const allContent = [...(baseSnapshot ? [baseSnapshot] : []), ...tailSources];
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
            if (deleteSetFitsInline({
                deleteSetBytes: merged.dsUpdate.byteLength,
                stateVectorB64Length: stateVectorB64.length,
                maxFieldBytes: ctx.maxDeleteSetFieldBytes ?? DEFAULTS.MAX_DELETE_SET_FIELD_BYTES,
                inlineLimit: DEFAULTS.INLINE_UPDATE_LIMIT,
            })) {
                deleteSetUpdate = merged.dsUpdate;
            } else {
                // Too large to inline beside the state vector (very old,
                // deletion-heavy or many-client document). Offload to Cloud
                // Storage instead of dropping it: without a fingerprint
                // every reconnecting client fails the push-guard coverage
                // proof and writes a spurious O(delete-set) update document
                // on every boot, forever.
                oversizedDeleteSet = merged.dsUpdate;
            }
        } catch (decodeErr) {
            throw new Error(
                `Compaction candidate failed validation: ${decodeErr}`
            );
        }

        const nextVersion = currentVersion + 1;
        const attemptId = generateSessionId();
        const storagePath = foldSnapshotPath(path, nextVersion, attemptId);

        // Upload candidate blob to Cloud Storage first
        // It is safe to upload first because if transaction fails, it just leaves an orphaned file that we ignore.
        // The path is unique to this attempt: if we lose the lock while
        // uploading, another client may commit the same version, and a
        // version-derived name would let our late upload replace its blob.
        // The blob is gzipped in the merge worker (about 3x smaller); the
        // metadata above was derived from the raw candidate.
        await uploadBlob(storage, storagePath, candidate);

        let deleteSetStoragePath: string | null = null;
        if (oversizedDeleteSet) {
            deleteSetStoragePath = foldDeleteSetPath(path, nextVersion, attemptId);
            await uploadBlob(storage, deleteSetStoragePath, oversizedDeleteSet);
        }

        // A client that held the replaced snapshot lacks only what this
        // fold merges on top of it. Publish that tail beside the snapshot
        // so such a client can catch up without downloading the snapshot.
        const foldTail = baseSnapshot && mainState.baseStateVector
            ? await publishFoldTail({
                storage,
                storagePath: foldTailPath(path, nextVersion, attemptId),
                sources: tailSources,
                baseStateVector: mainState.baseStateVector,
                snapshotBytes: candidate.byteLength,
                stateVectorB64Length: stateVectorB64.length,
                inlineDeleteSetBytes: deleteSetUpdate?.byteLength ?? 0,
            })
            : null;

        // === STEP 4: Transaction ===
        const { result, deletedRefs } = await performCompactionTransaction({
            db,
            path,
            uid,
            verifiedUpdateRefs: updatesToFold.map(u => u.ref),
            verifiedHistoryRefs: historyToMerge.map(h => h.ref),
            staleRefs,
            storagePath,
            candidate,
            stateVectorB64,
            deleteSetUpdate,
            deleteSetStoragePath,
            foldTail,
            expectedVersion: currentVersion,
        });
        await reclaimUpdateBlobs(storage, updateBlobs, deletedRefs);

        // Garbage Collect Old Storage Snapshot (and its delete-set and tail blobs).
        // Delete the paths the replaced main document stored: blob names are
        // attempt-unique (and a squash snapshot is named
        // snapshot_e{E}_v{V}_{id}.bin), so they cannot be rebuilt from the
        // version. The transaction verified the version is unchanged, and
        // every writer of these fields bumps it, so they still describe what
        // we replaced.
        if (result.success && result.type === 'snapshot') {
            for (const oldPath of blobsReplacedByFold(mainState, [storagePath, deleteSetStoragePath, foldTail?.storagePath ?? null])) {
                try {
                    await deleteObject(ref(storage, oldPath));
                    console.log(`Garbage collected old blob: ${oldPath}`);
                } catch (err) {
                    console.warn(`Failed to garbage collect old blob ${oldPath} for ${path}`, err);
                }
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
 * Counts history segments for the DELTA/FOLD choice without downloading
 * them. The count stops at the effective fold threshold (every count from
 * there on folds), so it bills one read however long history is.
 *
 * It counts what the fold's history query reads, stale-epoch segments
 * included: a fold that comes sooner for them is still correct, and it
 * deletes them. Only the lock holder creates or deletes segments, so the
 * count holds until this cycle's transaction.
 */
async function countHistory(db: Firestore, path: string, historyFoldThreshold: number): Promise<number> {
    const snap = await getCountFromServer(query(
        collection(db, path, FIRESTORE_PATHS.HISTORY),
        orderBy('startTime', 'asc'),
        limit(effectiveFoldThreshold(historyFoldThreshold))
    ));
    return snap.data().count;
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
}): Promise<CommittedCompaction | null> {
    const { db, path, uid, updatesToProcess, staleRefs, epoch } = params;

    // Merge + validate + derive the segment's state vector (clock ends per
    // client — what the sync layer's redundancy checks consume). gc is
    // intentionally off: a partial merge references structs that live in
    // the base snapshot, so a GC rebuild would find missing dependencies
    // and fall back to the plain merge anyway — no point paying for the
    // attempt.
    const merged = await mergeUpdatesWithMetaAsync(updatesToProcess.map(u => u.data), { gc: false });

    const segmentB64Sv = toBase64(merged.stateVector);

    if (!deltaSegmentFitsInline(merged.result.byteLength, segmentB64Sv.length, DEFAULTS.INLINE_UPDATE_LIMIT)) {
        return null;
    }

    // The state vector cannot show deletions; flag them so readers never
    // skip this segment as covered (see buildDeltaSegmentDoc).
    const hasDeletions = updateHasDeletions(merged.dsUpdate);

    return await runTransaction(db, async (transaction) => {
        // Kill switch: bail if the lock was lost (another client may be
        // mid-fold and about to delete the same update documents).
        const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);
        const lockSnap = await transaction.get(lockRef);
        if (!lockSnap.exists() || lockSnap.data().owner !== uid) {
            throw new Error("Lock lost or expired during compaction phase - Aborting write.");
        }

        const segmentRef = doc(collection(db, path, FIRESTORE_PATHS.HISTORY));
        transaction.set(segmentRef, {
            ...buildDeltaSegmentDoc({ stateVectorB64: segmentB64Sv, hasDeletions, uid, epoch }),
            segment: Bytes.fromUint8Array(merged.result),
            startTime: serverTimestamp(),
        });
        // Deleted without re-reading: the segment holds every one of them,
        // and only lock holders delete update documents (see "Deletion" in
        // the module header).
        updatesToProcess.forEach(u => transaction.delete(u.ref));
        staleRefs.forEach(ref => transaction.delete(ref));

        console.log(`Delta-compacted ${updatesToProcess.length} updates into history segment (${merged.result.byteLength} bytes)`);

        return {
            result: {
                success: true,
                type: 'history' as const,
                updatesCompacted: updatesToProcess.length,
                historySegmentsMerged: 0,
            },
            deletedRefs: [...updatesToProcess.map(u => u.ref), ...staleRefs],
        };
    });
}

/** A fold's published tail, as stored on the main document. */
interface FoldTail {
    /** Cloud Storage path of the tail blob */
    storagePath: string;
    /** Base64 base clocks of the tail (see foldTailBaseClocks) */
    baseClocks: string;
}

/**
 * Publishes a fold's tail: everything the fold merges on top of the base
 * (its history segments and update documents, un-GC'd) as one blob.
 *
 * A fold deletes its sources, so afterwards the data a lagging client
 * lacks exists only inside the new snapshot — multi-MB on an aged
 * document, and growing with its age. A client that held the replaced
 * snapshot (typically a device that was away while another one crossed a
 * fold) lacks only this tail, a few percent of the snapshot, and the sync
 * layer downloads it instead (see foldTailMayCatchUp).
 *
 * Uploaded before the commit, like the snapshot, so the pointer never
 * names a missing blob. Never throws: the tail is a shortcut, and a fold
 * without one is complete — readers then download the snapshot.
 *
 * @returns The tail's main-document fields, or null when none is published.
 */
async function publishFoldTail(params: {
    storage: FirebaseStorage;
    storagePath: string;
    sources: Uint8Array[];
    baseStateVector: string;
    snapshotBytes: number;
    stateVectorB64Length: number;
    inlineDeleteSetBytes: number;
}): Promise<FoldTail | null> {
    const { storage, storagePath, sources, baseStateVector, snapshotBytes, stateVectorB64Length, inlineDeleteSetBytes } = params;

    if (sources.length === 0) {
        return null;
    }
    try {
        // gc off, as for a delta segment: the tail references structs that
        // live in the base. The merge also validates it and yields the
        // clients it touches, worker-side.
        const tail = await mergeUpdatesWithMetaAsync(sources, { gc: false });
        const baseClocks = toBase64(writeStateVector(foldTailBaseClocks(
            Y.decodeStateVector(fromBase64(baseStateVector)),
            Y.decodeStateVector(tail.stateVector),
        )));
        if (!shouldPublishFoldTail({
            tailBytes: tail.result.byteLength,
            snapshotBytes,
            tailFieldsLength: baseClocks.length + storagePath.length,
            stateVectorB64Length,
            inlineDeleteSetBytes,
            inlineLimit: DEFAULTS.INLINE_UPDATE_LIMIT,
        })) {
            return null;
        }
        await uploadBlob(storage, storagePath, tail.result);
        return { storagePath, baseClocks };
    } catch (e) {
        console.warn(`Failed to publish fold tail ${storagePath}; readers will download the snapshot`, e);
        return null;
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
    staleRefs: DocumentReference[];
    storagePath: string;
    candidate: Uint8Array;
    stateVectorB64: string;
    deleteSetUpdate: Uint8Array | null;
    deleteSetStoragePath: string | null;
    foldTail: FoldTail | null;
    expectedVersion: number;
}): Promise<CommittedCompaction> {
    const { db, path, uid, verifiedUpdateRefs, verifiedHistoryRefs, staleRefs, storagePath, candidate, stateVectorB64, deleteSetUpdate, deleteSetStoragePath, foldTail, expectedVersion } = params;

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

        // Deleted without re-reading: the candidate holds every one of
        // them, and only lock holders delete update/history documents (see
        // "Deletion" in the module header).
        const updatesToProcess = verifiedUpdateRefs.map(ref => ({ ref }));
        const historyToMerge = verifiedHistoryRefs.map(ref => ({ ref }));

        if (updatesToProcess.length === 0 && historyToMerge.length === 0) {
            return {
                result: { success: true, type: 'none' as const, updatesCompacted: 0, historySegmentsMerged: 0 },
                deletedRefs: [],
            };
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
            foldTail,
            currentVersion,
            updatesToProcess,
            historyToMerge,
        });
        // Old-epoch documents ride along in the same transaction: they are
        // never merged, only removed.
        staleRefs.forEach(ref => transaction.delete(ref));
        return {
            result,
            deletedRefs: [...updatesToProcess.map(u => u.ref), ...historyToMerge.map(h => h.ref), ...staleRefs],
        };
    });
}

/**
 * Deletes stale-epoch update/history documents when there is nothing else
 * to compact. They are deleted without re-reading (see "Deletion" in the
 * module header).
 *
 * @returns The documents the committed transaction deleted.
 */
async function deleteStaleEpochDocs(
    db: Firestore,
    path: string,
    uid: string,
    staleRefs: DocumentReference[]
): Promise<DocumentReference[]> {
    return await runTransaction(db, async (transaction) => {
        const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);
        const lockSnap = await transaction.get(lockRef);
        if (!lockSnap.exists() || lockSnap.data().owner !== uid) {
            throw new Error("Lock lost or expired during compaction phase - Aborting write.");
        }
        staleRefs.forEach(ref => transaction.delete(ref));
        return staleRefs;
    });
}

/**
 * Deletes the Storage blobs of the pointer documents a committed
 * transaction deleted. With its pointer gone nothing can read a blob
 * again; left alone, every oversized save or push stayed in billed
 * Storage forever.
 *
 * Must run only after the commit, and only for documents that commit
 * deleted (the transaction body's return value, so a re-run on
 * contention reports its own deletes). The deletes are blind (see
 * "Deletion" in the module header), but once the commit lands each
 * pointer is gone either way, and its payload is in what the commit
 * wrote. Readers racing the delete are
 * safe: initial sync skips a missing blob and reads the snapshot after
 * the updates, and the update listener skips one whose pointer is gone,
 * receiving the data through the fold or segment instead.
 *
 * Best effort, like snapshot garbage collection: a failure (including a
 * 404 from a double delete) only leaves an orphan, never fails the cycle.
 */
async function reclaimUpdateBlobs(
    storage: FirebaseStorage,
    updateBlobs: Map<string, string>,
    deletedRefs: DocumentReference[]
): Promise<void> {
    await Promise.all(deletedRefs.map(async (docRef) => {
        const blobPath = updateBlobs.get(docRef.path);
        if (blobPath === undefined) return;
        try {
            await deleteObject(ref(storage, blobPath));
        } catch (err) {
            console.warn(`Failed to delete update blob ${blobPath}`, err);
        }
    }));
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
    foldTail: FoldTail | null;
    currentVersion: number;
    updatesToProcess: { ref: DocumentReference }[];
    historyToMerge: { ref: DocumentReference }[];
}): CompactionResult {
    const { transaction, mainRef, uid, storagePath, candidate, stateVectorB64, deleteSetUpdate, deleteSetStoragePath, foldTail, currentVersion, updatesToProcess, historyToMerge } = params;

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
        // The tail is bound to the version it was folded into: a writer
        // that predates tails bumps the version without touching these
        // fields, and readers must then ignore them. No tail clears them.
        foldTailStoragePath: foldTail?.storagePath ?? deleteField(),
        foldTailBaseClocks: foldTail?.baseClocks ?? deleteField(),
        foldTailVersion: foldTail ? nextSnapshotVersion(currentVersion) : deleteField(),
        version: nextSnapshotVersion(currentVersion),
        updatedAt: serverTimestamp(),
        // Identifies the compacting client. Its own snapshot listener still
        // runs the coverage check: the fold may hold data it lacks.
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
