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
 * Request: { id: string, updates: Uint8Array[], diffAgainst?: Uint8Array }
 * Response: { id: string, result?: Uint8Array, error?: string }
 *
 * The worker also gzips Cloud Storage uploads (`gzip: true`, one blob in
 * `updates`): the result is the gzip stream, and an error means the blob
 * is uploaded raw.
 * 
 * @module merge-worker
 */

import * as Y from 'yjs';
import { mergeUpdatesCore, mergeUpdatesWithMeta } from './merge-core';
import { gzipBlob } from './gzip';

// Type definitions for worker messages
interface MergeRequest {
    id: string;
    updates: Uint8Array[];
    /** When true, garbage-collect deleted content from the merged result */
    gc?: boolean;
    /** When true, the updates make up a whole document (see MergeOptions.snapshot) */
    snapshot?: boolean;
    /**
     * When true, also validate the result and return its state vector and
     * delete-set fingerprint (compaction metadata). Keeps multi-hundred-ms
     * lazy walks over large snapshots off the main thread.
     */
    meta?: boolean;
    /**
     * When set, return `Y.diffUpdate(updates[0], diffAgainst)` instead of
     * merging: the part of the update a document at this state vector
     * lacks. Lets a client that is behind a multi-MB snapshot integrate
     * only what it is missing on the main thread.
     */
    diffAgainst?: Uint8Array;
    /**
     * When true, gzip the single blob in `updates` for a Cloud Storage
     * upload instead of merging (see gzipBlob).
     */
    gzip?: boolean;
}

interface MergeResponse {
    id: string;
    result?: Uint8Array;
    stateVector?: Uint8Array;
    dsUpdate?: Uint8Array;
    error?: string;
}

// Worker context (self in worker scope)
const ctx: Worker = self as any;

/**
 * Handle incoming merge requests from the main thread.
 */
ctx.onmessage = (event: MessageEvent<MergeRequest>) => {
    const { id, updates, gc, snapshot, meta, diffAgainst, gzip } = event.data;

    if (gzip) {
        // Compression Streams are async; they run on this thread, so a
        // multi-MB snapshot costs the main thread nothing.
        gzipBlob(updates[0]).then(
            (result) => {
                const response: MergeResponse = { id, result };
                ctx.postMessage(response, [result.buffer]);
            },
            (err) => {
                const response: MergeResponse = { id, error: err instanceof Error ? err.message : String(err) };
                ctx.postMessage(response);
            }
        );
        return;
    }

    try {
        if (diffAgainst) {
            const result = Y.diffUpdate(updates[0], diffAgainst);
            const response: MergeResponse = { id, result };
            ctx.postMessage(response, [result.buffer]);
            return;
        }

        if (meta) {
            const { result, stateVector, dsUpdate } = mergeUpdatesWithMeta(updates, { gc, snapshot });
            const response: MergeResponse = { id, result, stateVector, dsUpdate };
            ctx.postMessage(response, [result.buffer, stateVector.buffer, dsUpdate.buffer]);
            return;
        }

        // Perform the CPU-intensive merge operation
        const result = mergeUpdatesCore(updates, { gc });

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
