import { describe, it, expect } from 'vitest';
import { SeededRandom, seedFromString } from './prng';
import { writeStateVector } from '../../src/utils';
import * as Y from 'yjs';

/**
 * Fuzz tests for state vector operations using seeded PRNG.
 * 
 * These tests verify:
 * 1. State vector encoding is deterministic
 * 2. Round-trip encode/decode preserves data
 * 3. Edge cases are handled correctly
 */
describe('Fuzz: State Vector Operations', () => {
    const FUZZ_ITERATIONS = 100;
    const SEED = 42;

    describe('writeStateVector determinism', () => {
        it('should produce identical output for same input', () => {
            const rng = new SeededRandom(SEED);

            for (let i = 0; i < FUZZ_ITERATIONS; i++) {
                // Generate random state vector
                const numClients = rng.int(0, 10);
                const sv = new Map<number, number>();
                for (let j = 0; j < numClients; j++) {
                    sv.set(rng.int(0, 1000000), rng.int(0, 100000));
                }

                // Encode twice
                const encoded1 = writeStateVector(sv);
                const encoded2 = writeStateVector(sv);

                // Should be identical
                expect(encoded1).toEqual(encoded2);
            }
        });
    });

    describe('state vector round-trip', () => {
        it('should preserve data through Yjs encode/decode cycle', () => {
            const rng = new SeededRandom(SEED);

            for (let i = 0; i < FUZZ_ITERATIONS; i++) {
                // Create a doc with random content
                const doc = new Y.Doc();
                doc.clientID = rng.int(1, 1000000);

                const text = doc.getText('test');
                const numOps = rng.int(0, 20);
                for (let j = 0; j < numOps; j++) {
                    const pos = Math.min(j, text.length);
                    text.insert(pos, rng.string(rng.int(1, 5)));
                }

                // Get state vector
                const sv = Y.encodeStateVector(doc);
                const decoded = Y.decodeStateVector(sv);

                // Re-encode
                const reEncoded = Y.encodeStateVector(doc);
                const reDecoded = Y.decodeStateVector(reEncoded);

                // Should be equivalent
                expect(decoded.size).toBe(reDecoded.size);
                for (const [client, clock] of decoded) {
                    expect(reDecoded.get(client)).toBe(clock);
                }

                doc.destroy();
            }
        });
    });

    describe('edge cases', () => {
        it('should handle empty state vector', () => {
            const sv = new Map<number, number>();
            const encoded = writeStateVector(sv);

            expect(encoded).toBeInstanceOf(Uint8Array);
            expect(encoded.length).toBeGreaterThan(0);
        });

        it('should handle large client IDs', () => {
            const sv = new Map<number, number>([
                [Number.MAX_SAFE_INTEGER - 1000, 100],
            ]);
            const encoded = writeStateVector(sv);

            expect(encoded).toBeInstanceOf(Uint8Array);
            expect(encoded.length).toBeGreaterThan(0);
        });

        it('should handle large clock values', () => {
            const sv = new Map<number, number>([
                [100, Number.MAX_SAFE_INTEGER - 1000],
            ]);
            const encoded = writeStateVector(sv);

            expect(encoded).toBeInstanceOf(Uint8Array);
            expect(encoded.length).toBeGreaterThan(0);
        });

        it('should handle many clients', () => {
            const rng = new SeededRandom(SEED);
            const sv = new Map<number, number>();

            for (let i = 0; i < 1000; i++) {
                sv.set(rng.int(0, 10000000), rng.int(0, 100000));
            }

            const encoded = writeStateVector(sv);

            expect(encoded).toBeInstanceOf(Uint8Array);
            expect(encoded.length).toBeGreaterThan(1000); // Should be substantial
        });
    });

    describe('reproducibility', () => {
        it('should produce same results for same seed', () => {
            const run = (seed: number) => {
                const rng = new SeededRandom(seed);
                const results: string[] = [];

                for (let i = 0; i < 10; i++) {
                    const sv = new Map<number, number>([
                        [rng.int(0, 1000), rng.int(0, 1000)],
                    ]);
                    const encoded = writeStateVector(sv);
                    results.push(Array.from(encoded).join(','));
                }

                return results;
            };

            const results1 = run(12345);
            const results2 = run(12345);
            const results3 = run(54321);

            expect(results1).toEqual(results2);
            expect(results1).not.toEqual(results3);
        });
    });
});

describe('Fuzz: SeededRandom', () => {
    describe('determinism', () => {
        it('should produce same sequence for same seed', () => {
            const rng1 = new SeededRandom(42);
            const rng2 = new SeededRandom(42);

            for (let i = 0; i < 1000; i++) {
                expect(rng1.next()).toBe(rng2.next());
            }
        });

        it('should produce different sequences for different seeds', () => {
            const rng1 = new SeededRandom(42);
            const rng2 = new SeededRandom(43);

            const seq1 = Array.from({ length: 10 }, () => rng1.next());
            const seq2 = Array.from({ length: 10 }, () => rng2.next());

            expect(seq1).not.toEqual(seq2);
        });
    });

    describe('distribution', () => {
        it('should produce values in [0, 1)', () => {
            const rng = new SeededRandom(42);

            for (let i = 0; i < 10000; i++) {
                const val = rng.next();
                expect(val).toBeGreaterThanOrEqual(0);
                expect(val).toBeLessThan(1);
            }
        });

        it('should produce roughly uniform distribution', () => {
            const rng = new SeededRandom(42);
            const buckets = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

            for (let i = 0; i < 100000; i++) {
                const bucket = Math.floor(rng.next() * 10);
                buckets[bucket]++;
            }

            // Each bucket should have roughly 10000 values (±10%)
            for (const count of buckets) {
                expect(count).toBeGreaterThan(9000);
                expect(count).toBeLessThan(11000);
            }
        });
    });

    describe('seedFromString', () => {
        it('should produce same PRNG for same string', () => {
            const rng1 = seedFromString('test-seed');
            const rng2 = seedFromString('test-seed');

            for (let i = 0; i < 100; i++) {
                expect(rng1.next()).toBe(rng2.next());
            }
        });

        it('should produce different PRNGs for different strings', () => {
            const rng1 = seedFromString('test-seed-1');
            const rng2 = seedFromString('test-seed-2');

            const seq1 = Array.from({ length: 10 }, () => rng1.next());
            const seq2 = Array.from({ length: 10 }, () => rng2.next());

            expect(seq1).not.toEqual(seq2);
        });
    });
});
