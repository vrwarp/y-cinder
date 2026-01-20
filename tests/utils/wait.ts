/**
 * Async Polling Utilities
 *
 * Provides utilities for waiting on asynchronous conditions in tests.
 * Essential for integration tests where timing is non-deterministic.
 *
 * @module tests/utils/wait
 */

/**
 * Waits for a condition to become true, polling at regular intervals.
 *
 * This utility is essential for integration tests where the timing of
 * asynchronous operations (like Firestore sync) is non-deterministic.
 * It polls the predicate function until it returns true or times out.
 *
 * @param predicate - Function that returns true when the condition is met.
 *                    Can be sync or async.
 * @param timeout - Maximum time to wait in milliseconds (default: 2000ms)
 * @param interval - Polling interval in milliseconds (default: 50ms)
 * @param message - Error message to show on timeout (default: 'Condition not met')
 * @returns Promise that resolves when condition is met
 * @throws Error if timeout is reached before condition is met
 *
 * @example
 * ```typescript
 * // Wait for document sync
 * await waitForCondition(
 *   () => doc2.getText('content').toString() === 'Hello',
 *   5000,
 *   100,
 *   'Doc2 should receive content'
 * );
 *
 * // With async predicate
 * await waitForCondition(
 *   async () => (await getDoc(ref)).exists(),
 *   3000
 * );
 * ```
 */
export async function waitForCondition(
    predicate: () => boolean | Promise<boolean>,
    timeout: number = 2000,
    interval: number = 50,
    message: string = 'Condition not met'
): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeout) {
        if (await predicate()) {
            return;
        }
        await new Promise(r => setTimeout(r, interval));
    }
    throw new Error(`${message} (timed out after ${timeout}ms)`);
}
