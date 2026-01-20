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
export declare function debounce<T extends (...args: any[]) => any>(func: T, wait: number): (...args: Parameters<T>) => void;
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
export declare function wait(ms: number): Promise<void>;
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
export declare function writeStateVector(sv: Map<number, number>): Uint8Array;
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
export declare function calculateStateVector(update: Uint8Array): string;
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
export declare function generateSessionId(): string;
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
export declare function calculateBackoff(attempt: number, baseMs?: number, jitterMs?: number): number;
//# sourceMappingURL=utils.d.ts.map