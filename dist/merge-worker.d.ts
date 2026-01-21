/**
 * Merge Worker Module
 *
 * Web Worker for offloading CPU-intensive Y.mergeUpdates operations
 * from the main thread. This prevents UI freezes during compaction.
 *
 * ## Usage
 *
 * The worker receives an array of Uint8Array updates and returns
 * the merged result. Communication is via postMessage.
 *
 * ## Message Format
 *
 * Request: { id: string, updates: Uint8Array[] }
 * Response: { id: string, result?: Uint8Array, error?: string }
 *
 * @module merge-worker
 */
export {};
//# sourceMappingURL=merge-worker.d.ts.map