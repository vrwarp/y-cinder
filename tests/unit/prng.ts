/**
 * Seedable Pseudo-Random Number Generator
 * 
 * Implements a Park-Miller LCG (Linear Congruential Generator) with
 * deterministic output for reproducible testing.
 * 
 * @module prng
 */

/**
 * A seedable PRNG for deterministic fuzz testing.
 * 
 * Uses the Park-Miller LCG algorithm with the following parameters:
 * - Multiplier: 16807 (7^5)
 * - Modulus: 2147483647 (2^31 - 1)
 * 
 * @example
 * ```typescript
 * const rng = new SeededRandom(12345);
 * console.log(rng.next());     // Always the same value for seed 12345
 * console.log(rng.int(1, 10)); // Random int between 1 and 10
 * console.log(rng.choice(['a', 'b', 'c'])); // Random element
 * ```
 */
export class SeededRandom {
    private seed: number;

    /** Park-Miller multiplier */
    private static readonly A = 16807;

    /** Park-Miller modulus (2^31 - 1) */
    private static readonly M = 2147483647;

    /**
     * Creates a new seeded PRNG.
     * 
     * @param seed - The seed value (will be normalized to valid range)
     */
    constructor(seed: number) {
        // Ensure seed is within valid range
        this.seed = Math.abs(seed % SeededRandom.M) || 1;
    }

    /**
     * Returns the next random number in [0, 1).
     * 
     * @returns A deterministic pseudo-random number
     */
    next(): number {
        this.seed = (this.seed * SeededRandom.A) % SeededRandom.M;
        return (this.seed - 1) / (SeededRandom.M - 1);
    }

    /**
     * Returns a random integer in [min, max] (inclusive).
     * 
     * @param min - Minimum value (inclusive)
     * @param max - Maximum value (inclusive)
     * @returns A random integer
     */
    int(min: number, max: number): number {
        return Math.floor(this.next() * (max - min + 1)) + min;
    }

    /**
     * Returns a random element from an array.
     * 
     * @param arr - The array to choose from
     * @returns A random element
     * @throws Error if array is empty
     */
    choice<T>(arr: T[]): T {
        if (arr.length === 0) {
            throw new Error('Cannot choose from empty array');
        }
        return arr[this.int(0, arr.length - 1)];
    }

    /**
     * Returns a random boolean with the given probability of being true.
     * 
     * @param probability - Probability of true (0-1), default 0.5
     * @returns A random boolean
     */
    bool(probability: number = 0.5): boolean {
        return this.next() < probability;
    }

    /**
     * Shuffles an array in place using Fisher-Yates algorithm.
     * 
     * @param arr - The array to shuffle
     * @returns The shuffled array (same reference)
     */
    shuffle<T>(arr: T[]): T[] {
        for (let i = arr.length - 1; i > 0; i--) {
            const j = this.int(0, i);
            [arr[i], arr[j]] = [arr[j], arr[i]];
        }
        return arr;
    }

    /**
     * Returns a random string of the given length.
     * 
     * @param length - Length of the string
     * @param chars - Characters to use (default: alphanumeric)
     * @returns A random string
     */
    string(length: number, chars: string = 'abcdefghijklmnopqrstuvwxyz0123456789'): string {
        let result = '';
        for (let i = 0; i < length; i++) {
            result += chars.charAt(this.int(0, chars.length - 1));
        }
        return result;
    }

    /**
     * Returns a random Uint8Array of the given length.
     * 
     * @param length - Length of the array
     * @returns A random Uint8Array
     */
    bytes(length: number): Uint8Array {
        const arr = new Uint8Array(length);
        for (let i = 0; i < length; i++) {
            arr[i] = this.int(0, 255);
        }
        return arr;
    }
}

/**
 * Creates a SeededRandom instance from a string seed.
 * Useful for creating reproducible tests with descriptive names.
 * 
 * @param str - String to convert to seed
 * @returns A SeededRandom instance
 * 
 * @example
 * ```typescript
 * const rng = seedFromString('test-case-1');
 * ```
 */
export function seedFromString(str: string): SeededRandom {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32-bit integer
    }
    return new SeededRandom(Math.abs(hash) || 1);
}

/**
 * Returns a stable date string (YYYY-MM-DD) for use in deterministic seeds.
 * 
 * @returns Date string in ISO format (e.g., "2024-01-20")
 */
export function getStableDate(): string {
    return new Date().toISOString().split('T')[0];
}
