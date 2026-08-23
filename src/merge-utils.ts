/**
 * Merge Utilities Module
 * 
 * Provides async merge operations that can run either on the main thread
 * or in a Web Worker (when available) to prevent UI blocking.
 * 
 * ## Architecture
 * 
 * 1. On first call, attempts to create a Web Worker from a pre-bundled blob
 * 2. If Worker is available, merges happen off main thread
 * 3. If Worker fails (e.g., Node.js, strict CSP), falls back to sync merge
 * 
 * ## Bundling
 * 
 * The worker code (including Yjs) is pre-bundled at build time by
 * `scripts/bundle-worker.js` into `generated/merge-worker-blob.ts`.
 * No external CDN or network requests are needed at runtime.
 * 
 * ## Fallback Strategy
 * 
 * The async merge always works - it just uses sync merge on the main thread
 * when workers aren't available. This ensures compatibility across environments.
 * 
 * @module merge-utils
 */

import { mergeUpdatesCore, mergeUpdatesWithMeta, MergeOptions, MergeWithMetaResult } from './merge-core';
import { MERGE_WORKER_CODE } from './generated/merge-worker-blob';

export type { MergeOptions, MergeWithMetaResult } from './merge-core';

/** Response payload from the worker (plain or meta request) */
interface WorkerResponse {
    result: Uint8Array;
    stateVector?: Uint8Array;
    dsUpdate?: Uint8Array;
}

// Worker instance (lazily initialized, singleton)
let mergeWorker: Worker | null = null;
let workerInitialized = false;
let workerSupported = true; // Assume supported until proven otherwise

// Pending merge requests
const pendingRequests = new Map<string, {
    resolve: (response: WorkerResponse) => void;
    reject: (error: Error) => void;
    /** Fallback timer — cleared as soon as the worker responds */
    timer: ReturnType<typeof setTimeout>;
}>();

// Request ID counter
let requestIdCounter = 0;

/**
 * Generate a unique request ID.
 */
function generateRequestId(): string {
    return `merge-${requestIdCounter++}-${Date.now()}`;
}

/**
 * Initialize the merge worker (lazy, singleton pattern).
 * Returns true if worker is available, false otherwise.
 */
function initWorker(): boolean {
    if (workerInitialized) {
        return workerSupported;
    }

    workerInitialized = true;

    // Check if we're in a browser environment with Worker support
    if (typeof Worker === 'undefined') {
        workerSupported = false;
        console.debug('Web Workers not available - using main thread merge');
        return false;
    }

    try {
        // Create worker from pre-bundled code (no external network requests)
        const blob = new Blob([MERGE_WORKER_CODE], { type: 'application/javascript' });
        const workerUrl = URL.createObjectURL(blob);
        mergeWorker = new Worker(workerUrl);

        // Handle worker messages
        mergeWorker.onmessage = (event) => {
            const { id, result, stateVector, dsUpdate, error, type } = event.data;

            if (type === 'ready') {
                console.debug('Merge worker ready');
                return;
            }

            const pending = pendingRequests.get(id);
            if (!pending) {
                console.warn('Received response for unknown request:', id);
                return;
            }

            pendingRequests.delete(id);
            clearTimeout(pending.timer);

            if (error) {
                pending.reject(new Error(error));
            } else if (result) {
                pending.resolve({ result, stateVector, dsUpdate });
            }
        };

        // Handle worker errors
        mergeWorker.onerror = (event) => {
            console.error('Worker error:', event);
            // Reject all pending requests
            pendingRequests.forEach((pending) => {
                clearTimeout(pending.timer);
                pending.reject(new Error('Worker crashed'));
            });
            pendingRequests.clear();

            // Disable worker and fall back to main thread
            workerSupported = false;
            mergeWorker = null;
        };

        return true;
    } catch (err) {
        console.debug('Failed to create merge worker, using main thread:', err);
        workerSupported = false;
        return false;
    }
}

/**
 * Merge Yjs updates asynchronously, using Web Worker when available.
 * 
 * This function:
 * 1. Attempts to use a Web Worker if available (non-blocking)
 * 2. Falls back to main thread sync merge if Worker fails
 * 
 * @param updates - Array of Uint8Array updates to merge
 * @param options - Merge options; `gc: true` garbage-collects deleted
 *                  content from the result (used for snapshot compaction)
 * @returns Promise resolving to merged Uint8Array
 *
 * @example
 * ```typescript
 * const merged = await mergeUpdatesAsync([update1, update2, update3]);
 * const snapshot = await mergeUpdatesAsync(blobs, { gc: true });
 * ```
 */
export async function mergeUpdatesAsync(updates: Uint8Array[], options?: MergeOptions): Promise<Uint8Array> {
    // Edge case: empty array
    if (updates.length === 0) {
        return new Uint8Array(0);
    }
    // NOTE: We intentionally do NOT short-circuit for length === 1, so the
    // single-update case still takes the same code path as any other.
    //
    // Be aware of what that does and does not buy: Y.mergeUpdates itself
    // short-circuits on a one-element array and returns the blob unparsed,
    // so a corrupt single update is NOT rejected here (two or more updates
    // are parsed, and a corrupt one throws). Callers that must not commit an
    // invalid blob use mergeUpdatesWithMetaAsync, which derives the state
    // vector from the result and therefore does parse it — that is the path
    // compaction takes before writing a snapshot.

    // Try to use worker
    if (initWorker() && mergeWorker) {
        const response = await postToWorker(
            { updates, gc: !!options?.gc, meta: false },
            () => ({ result: mergeUpdatesCore(updates, options) })
        );
        return response.result;
    }

    // Fallback: sync merge on main thread
    // Wrap in Promise.resolve to keep API consistent
    return Promise.resolve(mergeUpdatesCore(updates, options));
}

/**
 * Merge updates AND derive compaction metadata (state vector + delete-set
 * fingerprint), validating the result — all inside the Web Worker when
 * available.
 *
 * At multi-megabyte snapshot sizes the metadata walks alone cost hundreds
 * of main-thread milliseconds (~670 ms at 10 MB / 300k structs), so
 * compaction uses this instead of merging in the worker and then walking
 * the result on the main thread. Rejects if the merged candidate fails
 * structural validation.
 *
 * @param updates - Array of Uint8Array updates to merge
 * @param options - Merge options (gc)
 * @returns Promise resolving to the merged update plus its metadata
 */
export async function mergeUpdatesWithMetaAsync(
    updates: Uint8Array[],
    options?: MergeOptions
): Promise<MergeWithMetaResult> {
    if (initWorker() && mergeWorker) {
        const response = await postToWorker(
            { updates, gc: !!options?.gc, meta: true },
            () => mergeUpdatesWithMeta(updates, options)
        );
        if (response.stateVector && response.dsUpdate) {
            return { result: response.result, stateVector: response.stateVector, dsUpdate: response.dsUpdate };
        }
        // Worker dropped the meta fields — derive on the main thread from
        // its merge result (defensive; should not happen since worker and
        // client are bundled together). gc already happened in the worker.
        return mergeUpdatesWithMeta([response.result], { gc: false });
    }

    return Promise.resolve(mergeUpdatesWithMeta(updates, options));
}

/**
 * Posts a request to the merge worker with a 30s main-thread fallback.
 *
 * @param message - Request fields (id is added here)
 * @param fallback - Synchronous main-thread computation used if the worker
 *                   does not respond in time
 */
function postToWorker(
    message: { updates: Uint8Array[]; gc: boolean; meta: boolean },
    fallback: () => WorkerResponse
): Promise<WorkerResponse> {
    return new Promise((resolve, reject) => {
        const id = generateRequestId();

        // Fallback timeout to prevent hanging; cleared when the worker responds
        const timer = setTimeout(() => {
            if (pendingRequests.has(id)) {
                pendingRequests.delete(id);
                console.warn('Worker merge timed out, falling back to main thread');
                try {
                    resolve(fallback());
                } catch (err) {
                    reject(err instanceof Error ? err : new Error(String(err)));
                }
            }
        }, 30000); // 30 second timeout
        // Don't keep Node.js processes alive just for this fallback timer
        (timer as any).unref?.();

        pendingRequests.set(id, { resolve, reject, timer });

        try {
            // Send updates to worker
            // Note: We don't transfer buffers here as we may need them for fallback
            mergeWorker!.postMessage({ id, ...message });
        } catch (err) {
            pendingRequests.delete(id);
            clearTimeout(timer);
            reject(err instanceof Error ? err : new Error(String(err)));
        }
    });
}

/**
 * Check if Web Worker merge is available.
 * 
 * @returns true if merges will happen off main thread
 */
export function isWorkerMergeAvailable(): boolean {
    initWorker();
    return workerSupported && mergeWorker !== null;
}

/**
 * Terminate the merge worker (cleanup).
 * Call this when you're done with merging.
 */
export function terminateMergeWorker(): void {
    if (mergeWorker) {
        mergeWorker.terminate();
        mergeWorker = null;
    }
    pendingRequests.forEach((pending) => {
        clearTimeout(pending.timer);
        pending.reject(new Error('Merge worker terminated'));
    });
    pendingRequests.clear();
    workerInitialized = false;
    workerSupported = true; // Reset for potential restart
}
