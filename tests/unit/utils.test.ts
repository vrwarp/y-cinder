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

import { describe, it, expect } from 'vitest';
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
