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

describe('startSubdocProvider config inheritance', () => {
    it('passes performance-critical settings through to child providers', async () => {
        const { startSubdocProvider } = await import('../../src/subdocs');
        const Y = await import('yjs');

        const captured: any[] = [];
        const ctx = {
            firebaseApp: {} as any,
            parentPath: 'docs/parent',
            depth: 1,
            maxUpdatesThreshold: 42,
            maxWaitTime: 250,
            maxAggregationTime: 3000,
            gcCompaction: false,
            lockTTL: 12345,
            compactionLimit: 111,
            persistence: { enabled: true },
            cachedClockOffset: -77, // parent's measured skew, shared
            createProvider: (config: any) => {
                captured.push(config);
                return { destroy: () => Promise.resolve() };
            },
        };

        const subdoc = new Y.Doc({ guid: 'child-guid' });
        const subProviders = new Map();
        startSubdocProvider(subdoc, ctx as any, subProviders);

        expect(captured).toHaveLength(1);
        const config = captured[0];
        expect(config.path).toBe('docs/parent/subdocs/child-guid');
        expect(config.depth).toBe(2);
        expect(config.maxUpdatesThreshold).toBe(42);
        expect(config.maxWaitTime).toBe(250);
        // Previously silently reset to defaults on subdocs:
        expect(config.maxAggregationTime).toBe(3000);
        expect(config.gcCompaction).toBe(false);
        // Previously re-measured per subdoc (3 Firestore ops each):
        expect(config.cachedClockOffset).toBe(-77);
        subdoc.destroy();
    });
});
