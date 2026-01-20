/**
 * Async Polling Utilities (Improved)
 *
 * Provides utilities for waiting on asynchronous conditions in tests with
 * meaningful error messages that include the last seen value on timeout.
 *
 * @module tests/utils/wait
 */

export interface WaitOptions {
    /** Maximum time to wait in milliseconds (default: 2000ms) */
    timeout?: number;
    /** Polling interval in milliseconds (default: 50ms) */
    interval?: number;
    /** Error message to show on timeout */
    message?: string;
    /** Callback to generate additional debug info on failure */
    onFailure?: () => Promise<string> | string;
}

const DEFAULT_TIMEOUT = 2000;
const DEFAULT_INTERVAL = 50;

/**
 * Generic wait function that polls a getter until a predicate returns true.
 * On timeout, throws an error including the last retrieved value.
 *
 * @param getter - Function to retrieve the current value (can be async)
 * @param predicate - Function that returns true when the value is valid
 * @param options - Wait configuration
 * @returns Promise resolving to the final value that satisfied the predicate
 */
export async function waitFor<T>(
    getter: () => T | Promise<T>,
    predicate: (value: T) => boolean,
    options: WaitOptions = {}
): Promise<T> {
    const timeout = options.timeout ?? DEFAULT_TIMEOUT;
    const interval = options.interval ?? DEFAULT_INTERVAL;
    const start = Date.now();
    let lastValue: T | undefined;
    let lastError: any;

    while (Date.now() - start < timeout) {
        try {
            lastValue = await getter();
            if (predicate(lastValue)) {
                return lastValue;
            }
        } catch (e) {
            lastError = e;
        }
        await new Promise(r => setTimeout(r, interval));
    }

    // Prepare detailed error message
    const msg = options.message ? `${options.message} (timed out after ${timeout}ms)` : `Wait timed out after ${timeout}ms`;

    let details = '';
    if (lastValue !== undefined) {
        try {
            const strVal = typeof lastValue === 'object' ? JSON.stringify(lastValue) : String(lastValue);
            // Truncate long values
            const truncated = strVal.length > 200 ? strVal.substring(0, 200) + '...' : strVal;
            details = `\nLast value: ${truncated}`;
        } catch (e) {
            details = `\nLast value: [Unable to stringify]`;
        }
    } else if (lastError) {
        details = `\nLast error: ${lastError.message}`;
    }

    if (options.onFailure) {
        try {
            const extraDebug = await options.onFailure();
            details += `\nDebug Info: ${extraDebug}`;
        } catch (e) {
            details += `\nDebug Info: [Failed to generate: ${e}]`;
        }
    }

    throw new Error(`${msg}${details}`);
}

/**
 * Waits for a value to strictly equal an expected value.
 */
export async function waitForConditionEquals<T>(
    getter: () => T | Promise<T>,
    expected: T,
    optionsOrTimeout?: WaitOptions | number,
    interval?: number,
    message?: string,
    onFailure?: () => Promise<string> | string
): Promise<T> {
    const options = normalizeOptions(optionsOrTimeout, interval, message, onFailure);
    if (!options.message) {
        options.message = `Expected value to equal ${JSON.stringify(expected)}`;
    }
    return waitFor(getter, val => val === expected, options);
}

/**
 * Waits for a value to NOT equal an expected value.
 */
export async function waitForConditionNotEquals<T>(
    getter: () => T | Promise<T>,
    notExpected: T,
    optionsOrTimeout?: WaitOptions | number,
    interval?: number,
    message?: string,
    onFailure?: () => Promise<string> | string
): Promise<T> {
    const options = normalizeOptions(optionsOrTimeout, interval, message, onFailure);
    if (!options.message) {
        options.message = `Expected value to NOT equal ${JSON.stringify(notExpected)}`;
    }
    return waitFor(getter, val => val !== notExpected, options);
}

/**
 * Waits for a numeric value to be greater than a limit.
 */
export async function waitForConditionGreaterThan(
    getter: () => number | Promise<number>,
    limit: number,
    optionsOrTimeout?: WaitOptions | number,
    interval?: number,
    message?: string,
    onFailure?: () => Promise<string> | string
): Promise<number> {
    const options = normalizeOptions(optionsOrTimeout, interval, message, onFailure);
    if (!options.message) {
        options.message = `Expected value > ${limit}`;
    }
    return waitFor(getter, val => val > limit, options);
}

/**
 * Waits for a numeric value to be less than a limit.
 */
export async function waitForConditionLessThan(
    getter: () => number | Promise<number>,
    limit: number,
    optionsOrTimeout?: WaitOptions | number,
    interval?: number,
    message?: string,
    onFailure?: () => Promise<string> | string
): Promise<number> {
    const options = normalizeOptions(optionsOrTimeout, interval, message, onFailure);
    if (!options.message) {
        options.message = `Expected value < ${limit}`;
    }
    return waitFor(getter, val => val < limit, options);
}

/**
 * Waits for a value to be truthy (!!value === true).
 */
export async function waitForConditionTruthy<T>(
    getter: () => T | Promise<T>,
    optionsOrTimeout?: WaitOptions | number,
    interval?: number,
    message?: string,
    onFailure?: () => Promise<string> | string
): Promise<T> {
    const options = normalizeOptions(optionsOrTimeout, interval, message, onFailure);
    if (!options.message) {
        options.message = `Expected value to be truthy`;
    }
    return waitFor(getter, val => !!val, options);
}

/**
 * Generic predicate wait (similar to old waitForCondition but with options object support)
 * DEPRECATED: Prefer specific helpers for better error messaging.
 */
export async function waitForCondition(
    predicate: () => boolean | Promise<boolean>,
    timeout?: number,
    interval?: number,
    message?: string
): Promise<void> {
    await waitFor(predicate, val => !!val, { timeout, interval, message });
}

// Helper to handle mixed argument styles for backward compatibility ease
function normalizeOptions(
    optionsOrTimeout?: WaitOptions | number,
    interval?: number,
    message?: string,
    onFailure?: () => Promise<string> | string
): WaitOptions {
    if (typeof optionsOrTimeout === 'number') {
        return {
            timeout: optionsOrTimeout,
            interval,
            message,
            onFailure
        };
    }
    return optionsOrTimeout || {};
}
