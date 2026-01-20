/**
 * Utility Functions
 *
 * General-purpose utility functions used throughout the y-fire library.
 * These are pure functions with no side effects (except for timing).
 *
 * ## Functions
 *
 * - **debounce**: Rate-limits function calls (used for batching updates)
 * - **wait**: Promise-based delay (used for retry backoff)
 * - **writeStateVector**: Encodes state vectors for efficient comparison
 * - **calculateStateVector**: Extracts state vector from Yjs update blobs
 * - **generateSessionId**: Creates unique client identifiers
 * - **calculateBackoff**: Computes exponential backoff delays with jitter
 *
 * @module utils
 */

import * as Y from "yjs";
import { toBase64 } from "lib0/buffer";
import * as encoding from "lib0/encoding";

/**
 * Creates a debounced version of a function that delays invocation
 * until after `wait` milliseconds have elapsed since the last call.
 * 
 * @param func - The function to debounce
 * @param wait - The number of milliseconds to delay
 * @returns A debounced version of the function
 * 
 * @example
 * ```typescript
 * const debouncedSave = debounce(save, 500);
 * debouncedSave(); // Called
 * debouncedSave(); // Ignored (within 500ms)
 * // ... 500ms later, save() is invoked once
 * ```
 */
export function debounce<T extends (...args: any[]) => any>(
    func: T,
    wait: number
): (...args: Parameters<T>) => void {
    let timeout: ReturnType<typeof setTimeout> | null = null;

    return (...args: Parameters<T>) => {
        if (timeout !== null) {
            clearTimeout(timeout);
        }
        timeout = setTimeout(() => func(...args), wait);
    };
}

/**
 * Returns a promise that resolves after the specified delay.
 * 
 * @param ms - The number of milliseconds to wait
 * @returns A promise that resolves after the delay
 * 
 * @example
 * ```typescript
 * await wait(1000); // Pauses for 1 second
 * ```
 */
export function wait(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Encodes a state vector map into a Uint8Array.
 * 
 * The format is:
 * - VarUint: number of entries
 * - For each entry:
 *   - VarUint: client ID
 *   - VarUint: clock value
 * 
 * @param sv - Map of client IDs to clock values
 * @returns Encoded state vector as Uint8Array
 * 
 * @example
 * ```typescript
 * const sv = new Map([[1, 10], [2, 20]]);
 * const encoded = writeStateVector(sv);
 * ```
 */
export function writeStateVector(sv: Map<number, number>): Uint8Array {
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, sv.size);
    for (const [client, clock] of sv) {
        encoding.writeVarUint(encoder, client);
        encoding.writeVarUint(encoder, clock);
    }
    return encoding.toUint8Array(encoder);
}

/**
 * Calculates the state vector of a Yjs update and returns it as a Base64 string.
 * 
 * Creates a temporary Y.Doc, applies the update, extracts the state vector,
 * and encodes it to Base64. The temporary document is destroyed after use.
 * 
 * @param update - The Yjs update blob
 * @returns Base64-encoded state vector string
 * 
 * @example
 * ```typescript
 * const update = Y.encodeStateAsUpdate(doc);
 * const svBase64 = calculateStateVector(update);
 * ```
 */
export function calculateStateVector(update: Uint8Array): string {
    const tempDoc = new Y.Doc();
    Y.applyUpdate(tempDoc, update);
    const sv = Y.encodeStateVector(tempDoc);
    const svBase64 = toBase64(sv);
    tempDoc.destroy();
    return svBase64;
}

/**
 * Generates a unique session ID combining random characters and timestamp.
 * 
 * @returns A unique session identifier string
 * 
 * @example
 * ```typescript
 * const uid = generateSessionId(); // e.g., "abc123def456xyz789"
 * ```
 */
export function generateSessionId(): string {
    // P2.17: Use crypto.randomUUID() if available for better entropy
    const g = globalThis as any;
    if (g.crypto && g.crypto.randomUUID) {
        return g.crypto.randomUUID();
    }
    // Fallback for legacy environments
    return Math.random().toString(36).substring(2) + Date.now().toString(36);
}

/**
 * Calculates exponential backoff with jitter for retry logic.
 * 
 * @param attempt - The current attempt number (1-based)
 * @param baseMs - Base delay in milliseconds (default: 100)
 * @param jitterMs - Maximum random jitter in milliseconds (default: 100)
 * @returns The calculated backoff delay in milliseconds
 * 
 * @example
 * ```typescript
 * const delay = calculateBackoff(3); // ~800ms + random jitter
 * await wait(delay);
 * ```
 */
export function calculateBackoff(
    attempt: number,
    baseMs: number = 100,
    jitterMs: number = 100
): number {
    return (Math.pow(2, attempt) * baseMs) + (Math.random() * jitterMs);
}
