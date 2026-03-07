/**
 * Update Metadata Unit Tests
 *
 * Tests for metadata extraction and comparison functions:
 * - extractAllMetadata: Parses Yjs update internals to get clock ranges
 * - aggregateMetadata: Combines metadata for Firestore storage
 * - isUpdateRedundant: Determines if an update is already applied locally
 *
 * These functions enable efficient sync by comparing clocks instead of content.
 *
 * @file update-metadata.test.ts
 */

import { describe, it, expect } from 'vitest';
import {
    extractAllMetadata,
    aggregateMetadata,
    isUpdateRedundant
} from '../../src/update-metadata';
import * as Y from 'yjs';

describe('update-metadata', () => {
    describe('extractAllMetadata', () => {
        it('should extract metadata from a single-client update', () => {
            const doc = new Y.Doc();
            doc.clientID = 12345;
            doc.getText('test').insert(0, 'hello');
            const update = Y.encodeStateAsUpdate(doc);

            const metas = extractAllMetadata(update);

            expect(metas.length).toBe(1);
            expect(metas[0].clientID).toBe(12345);
            expect(metas[0].clockStart).toBe(0);
            expect(metas[0].clockEnd).toBeGreaterThan(0);

            doc.destroy();
        });

        it('should extract metadata from a merged multi-client update', () => {
            const doc1 = new Y.Doc();
            doc1.clientID = 100;
            doc1.getText('test').insert(0, 'hello');
            const update1 = Y.encodeStateAsUpdate(doc1);

            const doc2 = new Y.Doc();
            doc2.clientID = 200;
            Y.applyUpdate(doc2, update1);
            doc2.getText('test').insert(5, ' world');
            const update2 = Y.encodeStateAsUpdate(doc2);

            const merged = Y.mergeUpdates([update1, update2]);
            const metas = extractAllMetadata(merged);

            expect(metas.length).toBeGreaterThanOrEqual(2);

            const clientIDs = metas.map(m => m.clientID);
            expect(clientIDs).toContain(100);
            expect(clientIDs).toContain(200);

            doc1.destroy();
            doc2.destroy();
        });

        it('should return empty array for empty update', () => {
            const doc = new Y.Doc();
            const update = Y.encodeStateAsUpdate(doc);

            const metas = extractAllMetadata(update);

            expect(Array.isArray(metas)).toBe(true);
            expect(metas.length).toBe(0);

            doc.destroy();
        });

        it('should return empty array for malformed update', () => {
            const malformed = new Uint8Array([1, 2, 3, 4, 5]);

            const metas = extractAllMetadata(malformed);

            expect(Array.isArray(metas)).toBe(true);
            expect(metas.length).toBe(0);
        });

        it('should correctly compute clock ranges for multiple operations', () => {
            const doc = new Y.Doc();
            doc.clientID = 100;
            const text = doc.getText('test');

            // Multiple operations
            text.insert(0, 'a');
            text.insert(1, 'b');
            text.insert(2, 'c');

            const update = Y.encodeStateAsUpdate(doc);
            const metas = extractAllMetadata(update);

            expect(metas.length).toBe(1);
            expect(metas[0].clockStart).toBe(0);
            expect(metas[0].clockEnd).toBe(3);

            doc.destroy();
        });
    });

    describe('aggregateMetadata', () => {
        it('should return empty object for empty array', () => {
            const result = aggregateMetadata([]);

            expect(Object.keys(result).length).toBe(0);
        });

        it('should aggregate single metadata entry', () => {
            const metas = [{ clientID: 100, clockStart: 0, clockEnd: 5 }];

            const result = aggregateMetadata(metas);

            expect(result.clientIDs).toEqual([100]);
            expect(result.clientClocks).toEqual([5]);
        });

        it('should aggregate multiple metadata entries', () => {
            const metas = [
                { clientID: 100, clockStart: 0, clockEnd: 5 },
                { clientID: 200, clockStart: 10, clockEnd: 20 },
                { clientID: 300, clockStart: 5, clockEnd: 15 },
            ];

            const result = aggregateMetadata(metas);

            expect(result.clientIDs).toEqual([100, 200, 300]);
            expect(result.clientClocks).toEqual([5, 20, 15]);
        });

        it('should return empty object when client count exceeds cap', () => {
            const metas = Array.from({ length: 51 }, (_, i) => ({
                clientID: i + 1,
                clockStart: 0,
                clockEnd: i + 10,
            }));

            const result = aggregateMetadata(metas);

            expect(Object.keys(result).length).toBe(0);
        });

        it('should include clientClocks at exactly the cap limit', () => {
            const metas = Array.from({ length: 50 }, (_, i) => ({
                clientID: i + 1,
                clockStart: 0,
                clockEnd: i + 10,
            }));

            const result = aggregateMetadata(metas);

            expect(result.clientIDs).toHaveLength(50);
            expect(result.clientClocks).toHaveLength(50);
        });
    });

    describe('isUpdateRedundant', () => {
        it('should return true if local has all clocks >= update clockEnd', () => {
            const localSV = new Map<number, number>([
                [100, 20],  // >= 15
                [200, 20],  // >= 15
            ]);

            const result = isUpdateRedundant(localSV, [100, 200], [15, 15]);

            expect(result).toBe(true);
        });

        it('should return false if any client clock is behind', () => {
            const localSV = new Map<number, number>([
                [100, 10],
                [200, 5],  // Behind
            ]);

            const result = isUpdateRedundant(localSV, [100, 200], [10, 10]);

            expect(result).toBe(false);
        });

        it('should return false if client is missing from local', () => {
            const localSV = new Map<number, number>([
                [100, 10],
            ]);

            const result = isUpdateRedundant(localSV, [100, 200], [5, 5]);

            expect(result).toBe(false);
        });

        it('should handle empty clientIDs array', () => {
            const localSV = new Map<number, number>([[100, 10]]);

            const result = isUpdateRedundant(localSV, [], []);

            expect(result).toBe(true);
        });

        it('should use per-client clocks', () => {
            // Client A clock 10, Client B clock 5000
            // Local state: A=15, B=5000
            const localSV = new Map<number, number>([
                [100, 15],
                [200, 5000],
            ]);

            const result = isUpdateRedundant(
                localSV, [100, 200], [10, 5000]
            );

            expect(result).toBe(true);
        });

        it('should detect missing data with per-client clocks', () => {
            const localSV = new Map<number, number>([
                [100, 5],   // Behind client A's clock of 10
                [200, 5000],
            ]);

            const result = isUpdateRedundant(
                localSV, [100, 200], [10, 5000]
            );

            expect(result).toBe(false);
        });
    });
});
