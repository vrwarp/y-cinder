/**
 * Merge Core Module
 *
 * Shared merge logic used by both the main thread (merge-utils fallback
 * paths) and the Web Worker (merge-worker). This module is bundled into the
 * worker blob by scripts/bundle-worker.js, so it must stay dependency-free
 * apart from yjs.
 *
 * ## Why garbage collection matters here
 *
 * `Y.mergeUpdates` is fast but never garbage-collects: the content of every
 * deleted item is carried forward verbatim. For a document that has been
 * edited for a long time (high churn), the compacted snapshot therefore
 * grows linearly with *total historical churn*, not with live content —
 * every compaction, download, and initial sync pays for text deleted months
 * ago.
 *
 * Rewriting the merged update through a temporary Y.Doc with GC enabled
 * replaces deleted item content with tiny GC id-ranges. This matches what
 * every live client already does locally (Y.Doc defaults to gc: true), so
 * it does not change convergence semantics — state vectors and delete-sets
 * are fully preserved.
 *
 * @module merge-core
 */

import * as Y from 'yjs';

/**
 * Options controlling merge behavior.
 */
export interface MergeOptions {
    /**
     * When true, the merged result is rewritten through a temporary Y.Doc
     * with garbage collection enabled, dropping deleted-item content.
     */
    gc?: boolean;
}

/**
 * Merges Yjs updates, optionally garbage-collecting the result.
 *
 * @param updates - Array of update blobs to merge
 * @param options - Merge options (gc)
 * @returns The merged (and possibly GC-rewritten) update
 */
export function mergeUpdatesCore(updates: Uint8Array[], options?: MergeOptions): Uint8Array {
    const merged = Y.mergeUpdates(updates);
    if (!options?.gc) {
        return merged;
    }
    return gcMergedUpdate(merged);
}

/**
 * Result of a merge that also validates the output and derives the
 * snapshot metadata compaction needs.
 */
export interface MergeWithMetaResult {
    /** The merged (and possibly GC-rewritten) update */
    result: Uint8Array;
    /** Encoded state vector of the result */
    stateVector: Uint8Array;
    /**
     * Structs-empty update carrying the result's full delete-set (the
     * snapshot's inline "fingerprint" used by reconnect fast paths).
     */
    dsUpdate: Uint8Array;
}

/**
 * Merges updates and derives the metadata compaction stores alongside the
 * snapshot: the state vector and the delete-set fingerprint. Throwing here
 * means the merged candidate failed structural validation and must not be
 * committed.
 *
 * Why one combined operation: at multi-megabyte snapshot sizes the lazy
 * walks alone (`encodeStateVectorFromUpdate` + `diffUpdate`) cost hundreds
 * of milliseconds — profiled at ~670 ms for a 10 MB / 300k-struct
 * snapshot. Run inside the merge Web Worker they cost the main thread
 * nothing, and on the GC path they are nearly free because the rebuilt
 * Y.Doc is already in hand (the state vector is O(clients) and the
 * delete-set encodes straight from the store instead of re-walking the
 * binary).
 */
export function mergeUpdatesWithMeta(updates: Uint8Array[], options?: MergeOptions): MergeWithMetaResult {
    const merged = Y.mergeUpdates(updates);

    if (options?.gc) {
        const doc = new Y.Doc({ gc: true });
        try {
            Y.applyUpdate(doc, merged);
            const store = doc.store as unknown as { pendingStructs: unknown; pendingDs: unknown };
            if (store.pendingStructs === null && store.pendingDs === null) {
                // Applying cleanly IS structural validation; derive
                // everything from the live doc.
                const rebuilt = Y.encodeStateAsUpdate(doc);
                const result = rebuilt.byteLength <= merged.byteLength ? rebuilt : merged;
                const stateVector = Y.encodeStateVector(doc);
                // Diff against the doc's own state: no structs, full DS
                const dsUpdate = Y.encodeStateAsUpdate(doc, stateVector);
                return { result, stateVector, dsUpdate };
            }
            // Missing dependencies: fall through to lazy walks on the
            // gap-preserving plain merge.
        } catch (e) {
            console.warn('GC rewrite of merged update failed; using plain merge result', e);
        } finally {
            doc.destroy();
        }
    }

    // Plain merge (or GC fallback): validate + derive via lazy walks.
    // parseUpdateMeta walks every struct and diffUpdate additionally
    // parses the delete-set, so together they reject the same corruption
    // Y.decodeUpdate would.
    //
    // parseUpdateMeta (not encodeStateVectorFromUpdate) is load-bearing:
    // the latter returns an EMPTY vector for updates whose structs do not
    // start at clock 0 — true of every PARTIAL merge (delta-compaction
    // segments, gap-preserving fallbacks). An empty state vector both
    // breaks the sync layer's redundancy checks (empty = "covers nothing"
    // = segment skipped as vacuously redundant) and makes the dsUpdate
    // diff below degenerate to the whole update.
    const stateVector = Y.encodeStateVector(Y.parseUpdateMeta(merged).to);
    const dsUpdate = Y.diffUpdate(merged, stateVector);
    return { result: merged, stateVector, dsUpdate };
}

/**
 * Rewrites a merged update through a temporary Y.Doc with garbage
 * collection enabled.
 *
 * Safety rules:
 * - If the update has missing dependencies (Yjs queues them as
 *   pendingStructs/pendingDs), rebuilding would silently drop the queued
 *   data, so the input is returned unchanged.
 * - If applying fails for any reason, the input is returned unchanged and
 *   downstream validation decides what to do with it.
 * - The rebuilt update is only preferred when it is not larger than the
 *   input (re-encoding a churn-free document can add slight overhead).
 *
 * @param merged - A merged update (output of Y.mergeUpdates)
 * @returns GC-rewritten update, or the input when rewriting is unsafe
 */
export function gcMergedUpdate(merged: Uint8Array): Uint8Array {
    const doc = new Y.Doc({ gc: true });
    try {
        Y.applyUpdate(doc, merged);

        // Missing dependencies stay queued on the store and would be
        // silently dropped by encodeStateAsUpdate — fall back to the
        // gap-preserving plain merge.
        const store = doc.store as unknown as { pendingStructs: unknown; pendingDs: unknown };
        if (store.pendingStructs !== null || store.pendingDs !== null) {
            return merged;
        }

        const rebuilt = Y.encodeStateAsUpdate(doc);
        return rebuilt.byteLength <= merged.byteLength ? rebuilt : merged;
    } catch (e) {
        console.warn('GC rewrite of merged update failed; using plain merge result', e);
        return merged;
    } finally {
        doc.destroy();
    }
}
