/**
 * Subdocument Management Unit Tests
 *
 * Tests for the subdocument management utility functions:
 * - getSubdocStats: Collects stats about active subdoc providers
 *
 * @file subdocs.test.ts
 */

import { describe, it, expect } from 'vitest';
import { getSubdocStats, SubProviderMap } from '../../src/subdocs';

describe('subdocs', () => {
    describe('getSubdocStats', () => {
        it('should return zero count and empty guids for an empty map', () => {
            const subProviders: SubProviderMap = new Map();
            const stats = getSubdocStats(subProviders);

            expect(stats.count).toBe(0);
            expect(stats.guids).toEqual([]);
        });

        it('should return correct count and guid for a map with one entry', () => {
            const subProviders: SubProviderMap = new Map();
            const mockProvider = { destroy: () => Promise.resolve() };
            const guid = 'test-guid-1';

            subProviders.set(guid, mockProvider);

            const stats = getSubdocStats(subProviders);

            expect(stats.count).toBe(1);
            expect(stats.guids).toEqual([guid]);
        });

        it('should return correct count and guids for a map with multiple entries', () => {
            const subProviders: SubProviderMap = new Map();
            const mockProvider = { destroy: () => Promise.resolve() };
            const guids = ['guid-1', 'guid-2', 'guid-3'];

            guids.forEach(guid => {
                subProviders.set(guid, mockProvider);
            });

            const stats = getSubdocStats(subProviders);

            expect(stats.count).toBe(3);
            // Using set comparison because Map iteration order is insertion order
            // but we just want to ensure all GUIDs are present.
            expect(new Set(stats.guids)).toEqual(new Set(guids));
            expect(stats.guids.length).toBe(3);
        });
    });
});
