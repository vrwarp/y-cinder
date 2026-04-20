/**
 * Compaction Performance Benchmark
 *
 * Measures the execution time of the compaction process with varying numbers of updates.
 * Used to verify the impact of N+1 query optimizations.
 *
 * @file compaction_perf.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import * as Y from 'yjs';
import {
    collection,
    addDoc,
    serverTimestamp,
    Bytes,
    Firestore,
} from 'firebase/firestore';
import { FirebaseStorage } from 'firebase/storage';
import { compact, CompactionContext } from '../../src/compaction';
import { getStableDate } from '../unit/prng';

describe('Performance: Compaction N+1 Query Optimization', () => {
    let app: any;
    let db: Firestore;
    let storage: FirebaseStorage;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const seed = `compaction-perf-${getStableDate()}-${counter++}`;
        const emulator = await import("../utils/emulator").then(m => m.setupEmulator());
        app = emulator.app;
        db = emulator.db;
        storage = emulator.storage;
        path = `tests/${seed}`;
    });

    it('should measure compaction time for 100 updates', async () => {
        const numUpdates = 100;
        console.log(`Seeding ${numUpdates} updates...`);

        // Seed valid updates
        const seedPromises = [];
        for (let i = 0; i < numUpdates; i++) {
            const d = new Y.Doc();
            d.getText('content').insert(0, `Update ${i} - some reasonable amount of text to simulate real usage.`);
            seedPromises.push(addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(d)),
                createdAt: serverTimestamp()
            }));
            d.destroy();
        }
        await Promise.all(seedPromises);

        const ctx: CompactionContext = {
            db,
            path,
            uid: 'perf-test-client',
            lockTTL: 60000,
            compactionLimit: 200,
            isDestroyed: () => false,
            storage,
        };

        console.log('Starting compaction...');
        const start = performance.now();
        const result = await compact(ctx);
        const end = performance.now();

        console.log(`Compaction took ${end - start}ms`);
        expect(result.success).toBe(true);
        expect(result.updatesCompacted).toBe(numUpdates);
    }, 60000);
});
