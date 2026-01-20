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
            expect(result.clientID).toBe(100);
            expect(result.clockStart).toBe(0);
            expect(result.clockEnd).toBe(5);
        });

        it('should aggregate multiple metadata entries', () => {
            const metas = [
                { clientID: 100, clockStart: 0, clockEnd: 5 },
                { clientID: 200, clockStart: 10, clockEnd: 20 },
                { clientID: 300, clockStart: 5, clockEnd: 15 },
            ];

            const result = aggregateMetadata(metas);

            expect(result.clientIDs).toEqual([100, 200, 300]);
            expect(result.clientID).toBe(100); // First for backwards compat
            expect(result.clockStart).toBe(0); // Min
            expect(result.clockEnd).toBe(20); // Max
        });
    });

    describe('isUpdateRedundant', () => {
        it('should return true if local has all clocks >= update clockEnd', () => {
            const localSV = new Map<number, number>([
                [100, 20],  // >= 15
                [200, 20],  // >= 15
            ]);

            const result = isUpdateRedundant(localSV, [100, 200], 15);

            expect(result).toBe(true);
        });

        it('should return false if any client clock is behind', () => {
            const localSV = new Map<number, number>([
                [100, 10],
                [200, 5],  // Behind
            ]);

            const result = isUpdateRedundant(localSV, [100, 200], 10);

            expect(result).toBe(false);
        });

        it('should return false if client is missing from local', () => {
            const localSV = new Map<number, number>([
                [100, 10],
            ]);

            const result = isUpdateRedundant(localSV, [100, 200], 5);

            expect(result).toBe(false);
        });

        it('should handle empty clientIDs array', () => {
            const localSV = new Map<number, number>([[100, 10]]);

            const result = isUpdateRedundant(localSV, [], 10);

            expect(result).toBe(true);
        });
    });
});
