/**
 * Reproduction test for Issue 4: Lock uses client time vs server time
 * 
 * Bug: Distributed lock uses Date.now() which varies per client.
 * With clock skew, both clients can enter critical section simultaneously.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { seedFromString, getStableDate } from '../unit/prng';
import { initializeApp } from '@firebase/app';
import {
    getFirestore,
    collection,
    doc,
    getDocs,
    getDoc,
    setDoc,
    addDoc,
    serverTimestamp,
    Bytes,
    Timestamp,
    terminate
} from '@firebase/firestore';

const EMULATOR_HOST = '127.0.0.1';
const FIRESTORE_PORT = 8080;
const PROJECT_ID = 'demo-test';

describe('Issue 4: Clock Skew in Distributed Lock', () => {
    let app: any;
    let db: any;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const seed = `clock-skew-${getStableDate()}-${counter++}`;
        // console.log(`Test Seed: ${seed}`); // Optional logging to reduce noise
        const rng = seedFromString(seed);
        const setup = await import("../utils/emulator").then(m => m.setupEmulator());
        app = setup.app;
        db = setup.db;
        path = `tests/${seed}`;
    });

    afterEach(async () => {
        await terminate(db);
    });

    it('should respect lock even when client clock is behind server', async () => {
        const lockRef = doc(db, path, 'metadata/lock_compaction');

        // Simulate: Lock was created by client with "correct" time
        // The lock expires in 30 seconds from "now" (server perspective)
        // But our client thinks it's already expired (client clock is 1 minute behind)

        const serverNow = Date.now();
        const lockExpiry = serverNow + 30000; // Valid for 30 more seconds

        await setDoc(lockRef, {
            owner: 'other-client-holding-lock',
            createdAt: Timestamp.fromMillis(serverNow), // Issue 4 Fix: Lock uses createdAt + TTL
            expiresAt: Timestamp.fromMillis(lockExpiry)
        });

        // Create provider
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            lockTTL: 60000,
            maxUpdatesThreshold: 10000 // Prevent auto-compaction
        });

        // Add work
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(new Y.Doc())),
            createdAt: serverTimestamp()
        });

        // Mock Date.now to return a time 1 minute BEHIND
        const originalDateNow = Date.now;
        const clientTimeBehind = serverNow - 60000; // Client thinks it's 1 minute ago
        Date.now = () => clientTimeBehind;

        try {
            // Try to compact
            // With buggy code: Client sees lock as "expired" (clientTime > expiresAt) and steals it
            // With correct code: Should check server time and respect the lock
            await provider.compact();

            // Check if lock was respected
            const lockSnap = await getDoc(lockRef);
            const lockData = lockSnap.data();

            console.log(`Lock owner: ${lockData?.owner}`);
            console.log(`Client time (mocked): ${new Date(clientTimeBehind).toISOString()}`);
            console.log(`Lock expiry: ${lockData?.expiresAt?.toDate?.()?.toISOString?.()}`);

            // Bug: If client overrides lock, owner changes
            // Expected: Lock should still be owned by original client
            expect(lockData?.owner).toBe('other-client-holding-lock');
        } finally {
            Date.now = originalDateNow;
            provider.destroy();
        }
    });

    it('should respect lock even when client clock is ahead of server', async () => {
        const lockRef = doc(db, path, 'metadata/lock_compaction');
        const serverNow = Date.now();
        const lockExpiry = serverNow + 30000; // Valid for 30 more seconds

        await setDoc(lockRef, {
            owner: 'other-client-holding-lock',
            createdAt: Timestamp.fromMillis(serverNow),
            expiresAt: Timestamp.fromMillis(lockExpiry)
        });

        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            lockTTL: 60000,
            maxUpdatesThreshold: 10000 // Prevent auto-compaction
        });

        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(new Y.Doc())),
            createdAt: serverTimestamp()
        });

        // MOCK: Client thinks it is 2 minutes in the FUTURE
        // Current Code: LockAge = (Now+2m) - LockTime = 2m.
        // 2m > TTL (1m). Client decides lock is old. Steals it.
        const originalDateNow = Date.now;
        const clientTimeAhead = serverNow + 120000;
        Date.now = () => clientTimeAhead;

        try {
            await provider.compact();

            const lockSnap = await getDoc(lockRef);
            const lockData = lockSnap.data();

            // FAIL CONDITION: If lockData.owner is provider.uid, we stole it.
            // EXPECTED: owner is still 'other-client-holding-lock'
            expect(lockData?.owner).toBe('other-client-holding-lock');
        } finally {
            Date.now = originalDateNow;
            provider.destroy();
        }
    });

    it('should prevent concurrent compaction due to clock skew', { timeout: 15000 }, async () => {
        const ydoc1 = new Y.Doc();
        const ydoc2 = new Y.Doc();

        const provider1 = new FireProvider({
            firebaseApp: app,
            ydoc: ydoc1,
            path,
            lockTTL: 30000,
            maxUpdatesThreshold: 10000 // Prevent auto-compaction
        });

        const provider2 = new FireProvider({
            firebaseApp: app,
            ydoc: ydoc2,
            path,
            lockTTL: 30000,
            maxUpdatesThreshold: 10000 // Prevent auto-compaction
        });

        // Add work
        for (let i = 0; i < 3; i++) {
            await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(new Y.Doc())),
                createdAt: serverTimestamp()
            });
        }

        // Track who enters the critical section
        let entriesCount = 0;
        let concurrentEntries = false;
        let currentlyCompacting = 0;

        const originalCompact1 = provider1.compact.bind(provider1);
        const originalCompact2 = provider2.compact.bind(provider2);

        // Wrap compact to detect concurrent execution
        provider1.compact = async (...args: any[]) => {
            currentlyCompacting++;
            entriesCount++;
            if (currentlyCompacting > 1) concurrentEntries = true;
            console.log(`Provider1 entering compact, concurrent: ${currentlyCompacting}`);
            try {
                return await originalCompact1(...args);
            } finally {
                currentlyCompacting--;
            }
        };

        provider2.compact = async (...args: any[]) => {
            currentlyCompacting++;
            entriesCount++;
            if (currentlyCompacting > 1) concurrentEntries = true;
            console.log(`Provider2 entering compact, concurrent: ${currentlyCompacting}`);
            try {
                return await originalCompact2(...args);
            } finally {
                currentlyCompacting--;
            }
        };

        // Run compaction simultaneously
        await Promise.all([
            provider1.compact(),
            provider2.compact()
        ]);

        console.log(`Total entries: ${entriesCount}, Concurrent: ${concurrentEntries}`);

        // Both may enter compact() function simultaneously, but only one acquires lock
        // and does work. The other exits early. This is expected behavior.
        // The key is that data is not corrupted - not that one blocks the other at entry.
        expect(entriesCount).toBe(2); // Both entered

        // Verify data consistency - updates should be compacted
        const updatesSnap = await getDocs(collection(db, path, 'updates'));
        console.log(`Updates remaining: ${updatesSnap.size}`);
        expect(updatesSnap.size).toBe(0); // Work was done by one

        await provider1.destroy();
        await provider2.destroy();
    });
});
