/**
 * Reproduction test for Issue 2: Compaction retry logic doesn't actually retry
 * 
 * Bug: The catch block calculates backoff but never actually calls compact() again.
 * Updates accumulate indefinitely until a new probabilistic trigger.
 * 
 * These tests call the compact() function from compaction.ts directly,
 * bypassing the FireProvider constructor overhead (sync(), listeners, clock
 * skew measurement) that would cause emulator timeouts.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as Y from 'yjs';
import { seedFromString, getStableDate } from '../unit/prng';
import {
    collection,
    getDocs,
    addDoc,
    serverTimestamp,
    Bytes,
    Firestore,
} from 'firebase/firestore';
import { getStorage, FirebaseStorage } from 'firebase/storage';
import { compact, CompactionContext } from '../../src/compaction';

describe('Issue 2: Compaction Retry Logic', () => {
    let app: any;
    let db: Firestore;
    let storage: FirebaseStorage;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const seed = `compaction-retry-${getStableDate()}-${counter++}`;
        const rng = seedFromString(seed);
        const emulator = await import("../utils/emulator").then(m => m.setupEmulator());
        app = emulator.app;
        db = emulator.db;
        storage = emulator.storage;
        path = `tests/${seed}`;
    });

    afterEach(async () => {
        // No provider to clean up — we call compact() directly
    });

    it('should actually retry compaction after transient failure', async () => {
        let failCount = 0;
        const MAX_FAIL = 2;

        // Add some updates to compact
        for (let i = 0; i < 3; i++) {
            await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(new Y.Doc())),
                createdAt: serverTimestamp()
            });
        }

        const ctx: CompactionContext = {
            db,
            path,
            uid: 'test-client',
            lockTTL: 60000,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
            testHooks: {
                beforeTransaction: async () => {
                    if (failCount < MAX_FAIL) {
                        failCount++;
                        console.log(`[Hook] Simulating failure #${failCount}`);
                        throw { code: 'aborted', message: 'Simulated contention' };
                    }
                    console.log(`[Hook] Allowing transaction to proceed`);
                }
            },
        };

        // compact() is fully awaited including all internal retries
        const result = await compact(ctx);

        // Verify updates were compacted
        const updatesSnap = await getDocs(collection(db, path, 'updates'));

        console.log(`Fail count: ${failCount}`);
        console.log(`Remaining updates: ${updatesSnap.size}`);
        console.log(`Result: ${JSON.stringify(result)}`);

        expect(failCount).toBe(MAX_FAIL); // Confirms we triggered failures
        expect(result.success).toBe(true); // Compaction eventually succeeded
        expect(updatesSnap.size).toBe(0); // Updates compacted away
    }, 60000);

    it('should eventually give up after MAX_RETRIES', async () => {
        let failCount = 0;

        // Add a single update so compaction has work to do
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(new Y.Doc())),
            createdAt: serverTimestamp()
        });

        const ctx: CompactionContext = {
            db,
            path,
            uid: 'test-client',
            lockTTL: 60000,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
            testHooks: {
                beforeTransaction: async () => {
                    failCount++;
                    console.log(`[Hook] Failure #${failCount}`);
                    throw { code: 'aborted', message: 'Permanent failure' };
                }
            },
        };

        // compact() will exhaust all retries and return failure
        const result = await compact(ctx);

        console.log(`Total fail count: ${failCount}`);
        console.log(`Result: ${JSON.stringify({ success: result.success, error: result.error?.message })}`);

        // MAX_RETRIES = 5, so attempts go 1..5 (attempt < MAX_RETRIES means 1,2,3,4 retry)
        // Total calls = 5 (initial attempt 1 + retries at 2,3,4,5 — attempt 5 gives up)
        expect(failCount).toBeGreaterThanOrEqual(5);
        expect(result.success).toBe(false); // Should have given up
    }, 60000);
});
