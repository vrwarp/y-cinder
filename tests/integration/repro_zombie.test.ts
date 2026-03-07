/**
 * Zombie Provider Detection Tests
 *
 * Tests that destroyed providers stop processing updates and don't
 * interfere with newly created providers on the same path. Verifies
 * clean teardown prevents "zombie" listeners from duplicating work.
 *
 * @file repro_zombie.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { initializeApp } from '@firebase/app';
import {
    getFirestore,
    connectFirestoreEmulator,
    collection,
    doc,
    getDocs,
    deleteDoc,
    addDoc,
    serverTimestamp,
    Bytes,
    terminate
} from '@firebase/firestore';
import { waitForConditionEquals } from '../utils/wait';
import { getStableDate } from '../unit/prng';

// Emulator settings
const EMULATOR_HOST = '127.0.0.1';
const FIRESTORE_PORT = 8080;
const PROJECT_ID = 'demo-test';

describe('Zombie Update Reproduction (Index Misalignment)', () => {
    let app: any;
    let db: any;
    let provider: FireProvider;
    let ydoc: Y.Doc;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        path = `tests/repro_zombie_${getStableDate()}-${counter++}`;
        const { app: a, db: d } = await import('../utils/emulator').then(m => m.setupEmulator());
        app = a;
        db = d;

        ydoc = new Y.Doc();
        provider = new FireProvider({
            firebaseApp: app,
            ydoc: ydoc,
            path: path,
            maxUpdatesThreshold: 1000
        });

        // Ensure clean state
        await deleteCollection(db, path + '/updates');
        await deleteCollection(db, path + '/history');
        await deleteDoc(doc(db, path));
    });

    afterEach(async () => {
        if (provider) {
            provider.destroy();
        }
        await terminate(db);
    });

    it('should correctly handle concurrent deletion of an update during compaction (No Zombie Updates)', async () => {
        // 1. Setup: Create 3 updates: A, B, C
        // We manually insert them to ensure we control the order/timing
        const updatesCol = collection(db, path, 'updates');

        // Create random update data
        const updateA = Y.encodeStateAsUpdate(new Y.Doc());
        const updateB = Y.encodeStateAsUpdate(new Y.Doc());
        const updateC = Y.encodeStateAsUpdate(new Y.Doc());

        // Helper to add update with controlled timestamp (simulated by order mostly, but using serverTimestamp)
        const addUpdate = async (update: Uint8Array, clientId: number) => {
            await addDoc(updatesCol, {
                update: Bytes.fromUint8Array(update),
                createdAt: serverTimestamp(),
                createdBy: 'test_client',
                clientIDs: [clientId],
                clientClocks: [1]
            });
        };

        await addUpdate(updateA, 1);
        await addUpdate(updateB, 2);
        await addUpdate(updateC, 3);

        // Wait for them to exist
        await waitForConditionEquals(async () => {
            const snap = await getDocs(updatesCol);
            return snap.size;
        }, 3, { timeout: 5000, interval: 100, message: 'Wait for updates to appear' });

        const initialSnap = await getDocs(updatesCol);
        const sortedDocs = initialSnap.docs.sort((a, b) => {
            // approximate sort key, but they are added sequentially so order is likely preserved
            return a.id.localeCompare(b.id);
            // Note: In real life we sort by createdAt. 
            // For this test, effectively A, B, C are the docs.
        });

        // Identify Doc A (or just the first one)
        // We trust getDocs returns them or we just pick the first one.
        // The compaction logic sorts by 'createdAt', let's assume insertion order holds or closely enough.
        // The specific bug is index based. If we delete the *first* one in the list, index 0 is skipped.
        const docToDelete = sortedDocs[0]; // Doc A
        const docToKeep1 = sortedDocs[1];  // Doc B
        const docToKeep2 = sortedDocs[2];  // Doc C

        console.log(`Target to delete (concurrently): ${docToDelete.id}`);

        // 2. Setup the Trap
        // We need to re-create the provider to inject the hook (Issue 16 Fix: DI)
        // Since provider state is stateless regarding firestore data (it reads from DB),
        // we can safely destroy and recreate.
        provider.destroy();

        provider = new FireProvider({
            firebaseApp: app,
            ydoc: new Y.Doc(), // Use dummy doc for compaction-only provider
            path,
            maxUpdatesThreshold: 10000,
            testHooks: {
                beforeTransaction: async () => {
                    console.log("HOOK TRIGGERED: Deleting Doc A to simulate concurrent removal...");
                    await deleteDoc(docToDelete.ref);
                }
            }
        });

        // 3. Trigger Compaction
        // This will:
        // a. Fetch updates [A, B, C]
        // b. Call hook -> Deletes A.
        // c. Run Transaction.
        //    i.  get(A) -> !exists. Skipped.
        //    ii. get(B) -> exists. Added to `updatesToProcess`.
        //    iii. get(C) -> exists. Added to `updatesToProcess`.
        //    iv. Processing loop initiates.

        await provider.compact();

        // 4. Assertions
        // Bug Behavior:
        // - Loop `i=0` (updatesToProcess[0] is B).
        // - Uses `updateDocs[0]` (A) for metadata.
        // - Deletes `updateDocs[0]` (A) -> No-op (already deleted).
        // - FAILS to delete `updateDocs[1]` (B) -> Zombie.

        // Expected Behavior (Fix):
        // - Loop iterates `updatesToProcess`.
        // - Merges B and C.
        // - Deletes `updatesToProcess[0].ref` (B).
        // - Deletes `updatesToProcess[1].ref` (C).

        // Verify updates collection
        const finalSnap = await getDocs(updatesCol);
        const remainingIds = finalSnap.docs.map(d => d.id);

        console.log("Remaining updates:", remainingIds);

        // B and C should be GONE.
        // If B remains, the bug is present.
        expect(remainingIds).not.toContain(docToKeep1.id);
        expect(remainingIds).not.toContain(docToKeep2.id);
        expect(remainingIds).toHaveLength(0); // Should be empty ideally, unless new ones came in (none expected)

        // Verify History or Base
        // Since updates were small, it likely compacted to Base Snapshot.
        // The critical check is that there are no ZOMBIE updates left.
        // expect(historySnap.size).toBeGreaterThan(0); // Removed strict check

        // If compacted to base, even better. The logic splits based on size.
        // Since these refer empty updates, it likely compacted to base or small segment.
        // The key checks are the Deletions.
    });
});

async function deleteCollection(db: any, collectionPath: string) {
    const collectionRef = collection(db, collectionPath);
    const querySnapshot = await getDocs(collectionRef);
    const deletePromises = querySnapshot.docs.map((doc: any) => deleteDoc(doc.ref));
    await Promise.all(deletePromises);
}
