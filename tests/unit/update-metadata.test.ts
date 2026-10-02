/**
 * Update Metadata Unit Tests
 *
 * Tests for metadata extraction and comparison functions:
 * - extractAllMetadata: Parses Yjs update internals to get clock ranges
 * - aggregateMetadata: Combines metadata for Firestore storage
 * - isUpdateRedundant: Determines if an update is already applied locally
 * - updateHasDeletions: Detects deletions, which clock metadata cannot show
 * - deleteSetContains: Exact delete-set range containment
 *
 * These functions enable efficient sync by comparing clocks instead of content.
 *
 * @file update-metadata.test.ts
 */

import { describe, it, expect, vi } from 'vitest';
import {
    extractAllMetadata,
    aggregateMetadata,
    isUpdateRedundant,
    updateHasDeletions,
    deleteSetContains
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

        it('should return empty array and log warning on parse error', () => {
            // A non-empty array that is not a valid Yjs update will cause Y.decodeUpdate to throw
            const malformed = new Uint8Array([1, 2, 3]);
            const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});

            const metas = extractAllMetadata(malformed);

            expect(metas).toEqual([]);
            expect(warnSpy).toHaveBeenCalledWith(
                "Failed to parse update metadata:",
                expect.any(Error)
            );

            warnSpy.mockRestore();
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

    describe('updateHasDeletions', () => {
        it('should be false for an insert-only update', () => {
            const doc = new Y.Doc();
            doc.getMap('m').set('a', 1);

            expect(updateHasDeletions(Y.encodeStateAsUpdate(doc))).toBe(false);
            doc.destroy();
        });

        it('should be true for a delete-only update, whose clock metadata is empty', () => {
            const doc = new Y.Doc();
            const map = doc.getMap('m');
            map.set('a', 1);
            const before = Y.encodeStateVector(doc);
            map.delete('a');
            const deleteOnly = Y.encodeStateAsUpdate(doc, before);

            expect(Y.parseUpdateMeta(deleteOnly).to.size).toBe(0);
            expect(updateHasDeletions(deleteOnly)).toBe(true);
            doc.destroy();
        });

        it('should be false for a structs-empty update with an empty delete-set', () => {
            const doc = new Y.Doc();
            doc.getMap('m').set('a', 1);

            expect(updateHasDeletions(Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc)))).toBe(false);
            doc.destroy();
        });

        it('should claim deletions for an unparseable blob (never skipped)', () => {
            const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

            expect(updateHasDeletions(new Uint8Array([0xff, 0xff, 0xff]))).toBe(true);
            expect(warn).toHaveBeenCalled();
            warn.mockRestore();
        });
    });

    describe('deleteSetContains', () => {
        /** A delete-set from client -> [clock, len] ranges, in the order given. */
        const dsOf = (ranges: Record<number, [number, number][]>) => {
            const ds = Y.createDeleteSet();
            for (const [client, items] of Object.entries(ranges)) {
                ds.clients.set(Number(client), items.map(([clock, len]) => ({ clock, len })) as any);
            }
            return ds;
        };
        // Canonical: sorted, overlapping and adjacent ranges joined.
        const canonical = () => dsOf({ 1: [[0, 5], [10, 5], [20, 1]], 2: [[3, 4]] });

        it('should be true for ranges inside, or exactly matching, canonical ranges', () => {
            expect(deleteSetContains(canonical(), dsOf({ 1: [[0, 5], [11, 2], [14, 1], [20, 1]], 2: [[3, 4]] }))).toBe(true);
        });

        it('should be true for an empty delete-set', () => {
            expect(deleteSetContains(canonical(), Y.createDeleteSet())).toBe(true);
            expect(deleteSetContains(Y.createDeleteSet(), Y.createDeleteSet())).toBe(true);
        });

        it('should be false for a range one clock past either end of a canonical range', () => {
            expect(deleteSetContains(canonical(), dsOf({ 1: [[10, 6]] }))).toBe(false);
            expect(deleteSetContains(canonical(), dsOf({ 1: [[9, 2]] }))).toBe(false);
            expect(deleteSetContains(canonical(), dsOf({ 1: [[21, 1]] }))).toBe(false);
        });

        it('should be false for a range bridging the gap between two canonical ranges', () => {
            expect(deleteSetContains(canonical(), dsOf({ 1: [[3, 9]] }))).toBe(false);
        });

        it('should be false for a range before every canonical range of its client', () => {
            expect(deleteSetContains(canonical(), dsOf({ 2: [[0, 1]] }))).toBe(false);
        });

        it('should be false for a client the canonical set lacks', () => {
            expect(deleteSetContains(canonical(), dsOf({ 3: [[0, 1]] }))).toBe(false);
        });

        it('should judge every range, in any order, not just the first or last', () => {
            expect(deleteSetContains(canonical(), dsOf({ 1: [[20, 1], [5, 1], [0, 1]] }))).toBe(false);
            expect(deleteSetContains(canonical(), dsOf({ 1: [[20, 1], [12, 1], [0, 1]] }))).toBe(true);
        });

        it('should modify neither set', () => {
            const sup = canonical();
            const sub = dsOf({ 1: [[1, 2], [12, 8]] });
            const copy = (ds: ReturnType<typeof dsOf>) => [...ds.clients].map(([c, items]) => [c, items.map(i => [i.clock, i.len])]);
            const [supBefore, subBefore] = [copy(sup), copy(sub)];

            deleteSetContains(sup, sub);

            expect(copy(sup)).toEqual(supBefore);
            expect(copy(sub)).toEqual(subBefore);
        });

        it('should judge real delete-sets against one built from the struct store', () => {
            const doc = new Y.Doc();
            const map = doc.getMap('m');
            for (let i = 0; i < 30; i++) map.set(`k${i % 7}`, i);
            const local = Y.createDeleteSetFromStructStore(doc.store);
            const held = Y.decodeUpdate(Y.encodeStateAsUpdate(doc)).ds;
            map.delete('k3');
            const ahead = Y.createDeleteSetFromStructStore(doc.store);

            expect(deleteSetContains(local, held)).toBe(true);
            expect(deleteSetContains(local, ahead)).toBe(false);
            expect(deleteSetContains(ahead, local)).toBe(true);
            doc.destroy();
        });
    });
});
