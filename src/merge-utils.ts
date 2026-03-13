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

import * as Y from 'yjs';
import { MERGE_WORKER_CODE } from './generated/merge-worker-blob';

// Worker instance (lazily initialized, singleton)
let mergeWorker: Worker | null = null;
let workerInitialized = false;
let workerSupported = true; // Assume supported until proven otherwise

// Pending merge requests
const pendingRequests = new Map<string, {
    resolve: (result: Uint8Array) => void;
    reject: (error: Error) => void;
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
            const { id, result, error, type } = event.data;

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

            if (error) {
                pending.reject(new Error(error));
            } else if (result) {
                pending.resolve(result);
            }
        };

        // Handle worker errors
        mergeWorker.onerror = (event) => {
            console.error('Worker error:', event);
            // Reject all pending requests
            pendingRequests.forEach((pending) => {
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
 * @returns Promise resolving to merged Uint8Array
 * 
 * @example
 * ```typescript
 * const merged = await mergeUpdatesAsync([update1, update2, update3]);
 * ```
 */
export async function mergeUpdatesAsync(updates: Uint8Array[]): Promise<Uint8Array> {
    // Edge case: empty array
    if (updates.length === 0) {
        return new Uint8Array(0);
    }
    // NOTE: We intentionally do NOT short-circuit for length === 1.
    // Passing a single update through Y.mergeUpdates validates its
    // internal structure. Without this, a corrupted or zero-byte
    // payload bypasses Yjs validation and can overwrite canonical state.

    // Try to use worker
    if (initWorker() && mergeWorker) {
        return new Promise((resolve, reject) => {
            const id = generateRequestId();
            pendingRequests.set(id, { resolve, reject });

            try {
                // Send updates to worker
                // Note: We don't transfer buffers here as we may need them for fallback
                mergeWorker!.postMessage({ id, updates });

                // Add timeout to prevent hanging
                setTimeout(() => {
                    if (pendingRequests.has(id)) {
                        pendingRequests.delete(id);
                        console.warn('Worker merge timed out, falling back to main thread');
                        // Fall back to sync merge
                        try {
                            const result = Y.mergeUpdates(updates);
                            resolve(result);
                        } catch (err) {
                            reject(err);
                        }
                    }
                }, 30000); // 30 second timeout
            } catch (err) {
                pendingRequests.delete(id);
                reject(err);
            }
        });
    }

    // Fallback: sync merge on main thread
    // Wrap in Promise.resolve to keep API consistent
    return Promise.resolve(Y.mergeUpdates(updates));
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
    pendingRequests.clear();
    workerInitialized = false;
    workerSupported = true; // Reset for potential restart
}
