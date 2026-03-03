/**
 * Reproduction test for Issue 2: Compaction retry logic doesn't actually retry
 * 
 * Bug: The catch block calculates backoff but never actually calls compact() again.
 * Updates accumulate indefinitely until a new probabilistic trigger.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { seedFromString, getStableDate } from '../unit/prng';
import { initializeApp } from '@firebase/app';
import {
    getFirestore,
    connectFirestoreEmulator,
    collection,
    doc,
    getDocs,
    addDoc,
    serverTimestamp,
    Bytes,
    terminate
} from '@firebase/firestore';

const EMULATOR_HOST = '127.0.0.1';
const FIRESTORE_PORT = 8080;
const PROJECT_ID = 'demo-test';

describe('Issue 2: Compaction Retry Logic', () => {
    let app: any;
    let db: any;
    let provider: FireProvider;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const seed = `compaction-retry-${getStableDate()}-${counter++}`;
        // console.log(`Test Seed: ${seed}`);
        const rng = seedFromString(seed);
        const { app: a, db: d } = await import("../utils/emulator").then(m => m.setupEmulator());
        app = a;
        db = d;
        db = getFirestore(app);
        connectFirestoreEmulator(db, EMULATOR_HOST, FIRESTORE_PORT);
        path = `tests/${seed}`;
    });

    afterEach(async () => {
        if (provider) provider.destroy();
        await terminate(db);
    });

    it('should actually retry compaction after transient failure', async () => {
        const ydoc = new Y.Doc();

        let failCount = 0;
        const MAX_FAIL = 2;

        provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 1000
        });

        // Add some updates to compact
        for (let i = 0; i < 5; i++) {
            await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(new Y.Doc())),
                createdAt: serverTimestamp()
            });
        }

        // Setup hook to fail first N attempts
        // Issue 16 Fix: Use DI via constructor
        provider.destroy();
        provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 1000,
            testHooks: {
                beforeTransaction: async () => {
                    if (failCount < MAX_FAIL) {
                        failCount++;
                        console.log(`[Hook] Simulating failure #${failCount}`);
                        throw { code: 'aborted', message: 'Simulated contention' };
                    }
                    console.log(`[Hook] Allowing transaction to proceed`);
                }
            }
        });

        // Trigger compaction
        await provider.compact();

        // Wait for retries to complete (backoff is up to 2^5 * 100 + 100 = ~3.3s for 5 attempts)
        await new Promise(r => setTimeout(r, 5000));

        // Verify updates were compacted
        const updatesSnap = await getDocs(collection(db, path, 'updates'));

        console.log(`Fail count: ${failCount}`);
        console.log(`Remaining updates: ${updatesSnap.size}`);

        // Bug: If retry doesn't work, updates remain
        // Expected: After retry succeeds, updates should be compacted (0 remaining)
        expect(failCount).toBe(MAX_FAIL); // Confirms we triggered failures
        expect(updatesSnap.size).toBe(0); // Should be 0 if compaction succeeded
    }, 30000);

    it('should eventually give up after MAX_RETRIES', async () => {
        const ydoc = new Y.Doc();

        let failCount = 0;

        provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 10000 // Manual only
        });

        // Add updates
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(new Y.Doc())),
            createdAt: serverTimestamp()
        });

        // Always fail
        provider.destroy();
        provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 10000,
            testHooks: {
                beforeTransaction: async () => {
                    failCount++;
                    console.log(`[Hook] Failure #${failCount}`);
                    throw { code: 'aborted', message: 'Permanent failure' };
                }
            }
        });

        // Trigger compaction
        await provider.compact();

        // Wait for all retries
        await new Promise(r => setTimeout(r, 10000));

        console.log(`Total fail count: ${failCount}`);

        // Should have tried MAX_RETRIES + 1 times (initial + 5 retries = 6)
        // Bug: if no retry, failCount = 1
        expect(failCount).toBeGreaterThanOrEqual(5);
    }, 30000);
});
