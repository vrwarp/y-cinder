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

import * as Y from 'yjs';

// Type definitions for worker messages
interface MergeRequest {
    id: string;
    updates: Uint8Array[];
}

interface MergeResponse {
    id: string;
    result?: Uint8Array;
    error?: string;
}

// Worker context (self in worker scope)
const ctx: Worker = self as any;

/**
 * Handle incoming merge requests from the main thread.
 */
ctx.onmessage = (event: MessageEvent<MergeRequest>) => {
    const { id, updates } = event.data;

    try {
        // Perform the CPU-intensive merge operation
        const result = Y.mergeUpdates(updates);

        // Send result back to main thread
        const response: MergeResponse = { id, result };
        ctx.postMessage(response, [result.buffer]); // Transfer buffer for efficiency
    } catch (err) {
        // Send error back to main thread
        const response: MergeResponse = {
            id,
            error: err instanceof Error ? err.message : String(err)
        };
        ctx.postMessage(response);
    }
};

// Signal that worker is ready
ctx.postMessage({ type: 'ready' });
