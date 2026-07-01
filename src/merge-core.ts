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
