/**
 * Utility Function Unit Tests
 *
 * Tests for the core utility functions used throughout the provider:
 * - debounce: Coalesces rapid function calls
 * - wait: Promise-based delay
 * - writeStateVector: State vector encoding
 * - calculateStateVector: State vector extraction from updates
 * - generateSessionId: Unique ID generation
 * - calculateBackoff: Exponential backoff with jitter
 *
 * @file utils.test.ts
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
    debounce,
    wait,
    writeStateVector,
    calculateStateVector,
    generateSessionId,
    calculateBackoff
} from '../../src/utils';
import * as Y from 'yjs';

describe('utils', () => {
    describe('debounce', () => {
        it('should coalesce rapid calls', async () => {
            let callCount = 0;
            const fn = debounce(() => callCount++, 50);

            fn();
            fn();
            fn();

            expect(callCount).toBe(0);

            await wait(100);

            expect(callCount).toBe(1);
        });

        it('should pass arguments to the debounced function', async () => {
            let receivedArgs: any[] = [];
            const fn = debounce((...args: any[]) => { receivedArgs = args; }, 50);

            fn('a', 'b', 'c');

            await wait(100);

            expect(receivedArgs).toEqual(['a', 'b', 'c']);
        });

        it('should use the last call arguments', async () => {
            let receivedValue = '';
            const fn = debounce((val: string) => { receivedValue = val; }, 50);

            fn('first');
            fn('second');
            fn('third');

            await wait(100);

            expect(receivedValue).toBe('third');
        });
    });

    describe('wait', () => {
        it('should resolve after the specified delay', async () => {
            const start = Date.now();
            await wait(100);
            const elapsed = Date.now() - start;

            expect(elapsed).toBeGreaterThanOrEqual(95); // Allow some tolerance
            expect(elapsed).toBeLessThan(200);
        });

        it('should resolve immediately for 0ms', async () => {
            const start = Date.now();
            await wait(0);
            const elapsed = Date.now() - start;

            expect(elapsed).toBeLessThan(50);
        });
    });

    describe('writeStateVector', () => {
        it('should encode an empty state vector', () => {
            const sv = new Map<number, number>();
            const encoded = writeStateVector(sv);

            expect(encoded).toBeInstanceOf(Uint8Array);
            expect(encoded.length).toBeGreaterThan(0);
        });

        it('should encode a single-entry state vector', () => {
            const sv = new Map<number, number>([[1, 10]]);
            const encoded = writeStateVector(sv);

            expect(encoded).toBeInstanceOf(Uint8Array);
            expect(encoded.length).toBeGreaterThan(0);
        });

        it('should encode a multi-entry state vector', () => {
            const sv = new Map<number, number>([
                [1, 10],
                [2, 20],
                [3, 30],
            ]);
            const encoded = writeStateVector(sv);

            expect(encoded).toBeInstanceOf(Uint8Array);
            // Should contain the count (1 byte) + entries
            expect(encoded.length).toBeGreaterThan(3);
        });

        it('should produce different output for different state vectors', () => {
            const sv1 = new Map<number, number>([[1, 10]]);
            const sv2 = new Map<number, number>([[1, 20]]);

            const encoded1 = writeStateVector(sv1);
            const encoded2 = writeStateVector(sv2);

            // At least one byte should differ
            expect(encoded1).not.toEqual(encoded2);
        });
    });

    describe('calculateStateVector', () => {
        it('should return a Base64 string', () => {
            const doc = new Y.Doc();
            doc.getText('test').insert(0, 'hello');
            const update = Y.encodeStateAsUpdate(doc);

            const svBase64 = calculateStateVector(update);

            expect(typeof svBase64).toBe('string');
            expect(svBase64.length).toBeGreaterThan(0);
            // Should be valid Base64
            expect(() => atob(svBase64)).not.toThrow();

            doc.destroy();
        });

        it('should produce consistent output for the same update', () => {
            const doc = new Y.Doc();
            doc.getText('test').insert(0, 'hello');
            const update = Y.encodeStateAsUpdate(doc);

            const sv1 = calculateStateVector(update);
            const sv2 = calculateStateVector(update);

            expect(sv1).toBe(sv2);

            doc.destroy();
        });
    });

    describe('generateSessionId', () => {
        it('should return a non-empty string', () => {
            const id = generateSessionId();

            expect(typeof id).toBe('string');
            expect(id.length).toBeGreaterThan(0);
        });

        it('should generate unique IDs', () => {
            const ids = new Set<string>();
            for (let i = 0; i < 100; i++) {
                ids.add(generateSessionId());
            }

            expect(ids.size).toBe(100);
        });
    });

    describe('calculateBackoff', () => {
        it('should increase exponentially', () => {
            // Use fixed seed for deterministic test
            const backoff1 = calculateBackoff(1, 100, 0);
            const backoff2 = calculateBackoff(2, 100, 0);
            const backoff3 = calculateBackoff(3, 100, 0);

            expect(backoff1).toBe(200);  // 2^1 * 100
            expect(backoff2).toBe(400);  // 2^2 * 100
            expect(backoff3).toBe(800);  // 2^3 * 100
        });

        it('should add jitter within expected range', () => {
            const samples: number[] = [];
            for (let i = 0; i < 100; i++) {
                samples.push(calculateBackoff(1, 100, 100));
            }

            const min = Math.min(...samples);
            const max = Math.max(...samples);

            expect(min).toBeGreaterThanOrEqual(200);
            expect(max).toBeLessThan(400);
        });
    });
});

/*
 * The crypto fallbacks in generateSessionId never run in Node 22, where
 * crypto.randomUUID exists — but they are exactly what an older browser or
 * a locked-down runtime would take, and a broken fallback there means
 * colliding session ids, which is a correctness problem across clients
 * rather than a cosmetic one.
 */
describe('generateSessionId environment fallbacks', () => {
    const g = globalThis as any;
    let originalCrypto: unknown;

    beforeEach(() => {
        originalCrypto = Object.getOwnPropertyDescriptor(g, 'crypto');
    });

    afterEach(() => {
        if (originalCrypto) {
            Object.defineProperty(g, 'crypto', originalCrypto as PropertyDescriptor);
        } else {
            delete g.crypto;
        }
    });

    const setCrypto = (value: unknown) => {
        Object.defineProperty(g, 'crypto', { value, configurable: true, writable: true });
    };

    it('prefers randomUUID when available', () => {
        setCrypto({ randomUUID: () => 'uuid-from-crypto' });

        expect(generateSessionId()).toBe('uuid-from-crypto');
    });

    it('falls back to getRandomValues and returns hex plus a timestamp suffix', () => {
        setCrypto({
            getRandomValues: (array: Uint8Array) => {
                array.fill(0xab);
                return array;
            },
        });

        const id = generateSessionId();

        // 16 bytes -> 32 hex chars, then a base36 timestamp.
        expect(id.startsWith('ab'.repeat(16))).toBe(true);
        expect(id.length).toBeGreaterThan(32);
        expect(id.slice(0, 32)).toMatch(/^[0-9a-f]{32}$/);
        // Suffix is a base36 timestamp, not hex.
        expect(id.slice(32)).toMatch(/^[0-9a-z]+$/);
    });

    it('zero-pads single-digit bytes in the getRandomValues fallback', () => {
        setCrypto({
            getRandomValues: (array: Uint8Array) => {
                array.fill(0x05);
                return array;
            },
        });

        // Without padding this would be "5" repeated and the id would be short.
        expect(generateSessionId().startsWith('05'.repeat(16))).toBe(true);
    });

    it('falls back to Math.random when there is no crypto at all', () => {
        delete g.crypto;

        const first = generateSessionId();
        const second = generateSessionId();

        expect(first.length).toBeGreaterThan(0);
        expect(first).not.toBe(second);
    });

    it('falls back to Math.random when crypto exists but offers neither API', () => {
        setCrypto({});

        expect(generateSessionId().length).toBeGreaterThan(0);
    });
});

describe('calculateBackoff', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('doubles the base delay with each attempt', () => {
        vi.spyOn(Math, 'random').mockReturnValue(0);

        expect(calculateBackoff(1, 100, 100)).toBe(200);
        expect(calculateBackoff(2, 100, 100)).toBe(400);
        expect(calculateBackoff(3, 100, 100)).toBe(800);
    });

    it('adds jitter scaled by the jitter ceiling', () => {
        vi.spyOn(Math, 'random').mockReturnValue(0.5);

        expect(calculateBackoff(1, 100, 100)).toBe(250);
        expect(calculateBackoff(1, 100, 0)).toBe(200);
    });

    it('honours a custom base', () => {
        vi.spyOn(Math, 'random').mockReturnValue(0);

        expect(calculateBackoff(1, 50, 0)).toBe(100);
        expect(calculateBackoff(4, 10, 0)).toBe(160);
    });

    it('uses the documented defaults', () => {
        vi.spyOn(Math, 'random').mockReturnValue(0);

        expect(calculateBackoff(1)).toBe(200);
    });
});
