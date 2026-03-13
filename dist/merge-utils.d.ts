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
export declare function mergeUpdatesAsync(updates: Uint8Array[]): Promise<Uint8Array>;
/**
 * Check if Web Worker merge is available.
 *
 * @returns true if merges will happen off main thread
 */
export declare function isWorkerMergeAvailable(): boolean;
/**
 * Terminate the merge worker (cleanup).
 * Call this when you're done with merging.
 */
export declare function terminateMergeWorker(): void;
//# sourceMappingURL=merge-utils.d.ts.map