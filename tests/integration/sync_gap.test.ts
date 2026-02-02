
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { getFirestore, collection, addDoc, serverTimestamp, Bytes, QueryDocumentSnapshot } from 'firebase/firestore';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { seedFromString, getStableDate } from '../unit/prng';
import { performInitialSync, createUpdateListener, SyncContext, SyncResult } from '../../src/sync';
import { DEFAULTS, FIRESTORE_PATHS } from '../../src/types';

describe('Sync Gap Race Condition', () => {
    let app: any;
    let db: any;
    let path: string;
    let ydoc: Y.Doc;
    let uid: string;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        const seed = `sync-gap-${getStableDate()}-${counter++}`;
        // console.log(`Test Seed: ${seed}`);
        const rng = seedFromString(seed);
        path = `sync-gap-tests/${seed}-${rng.string(5)}`;
        ydoc = new Y.Doc();
        uid = 'client-B';
        await clearFirestore(db);
    });

    it('should receive updates that occur between sync and listener (The "Sync Gap")', async () => {
        // 1. Setup SyncContext
        const syncCtx: SyncContext = {
            db,
            path,
            doc: ydoc,
            uid,
            maxUpdatesThreshold: 1000, // High threshold to avoid compaction interference
            onCompactionNeeded: vi.fn(),
            isDestroyed: () => false
        };

        // 2. Client B performs initial sync (Database is empty)
        const result: SyncResult = await performInitialSync(syncCtx);

        // Assert initial state
        expect(result.success).toBe(true);
        expect(result.updatesApplied).toBe(0);
        expect(result.lastSyncedDoc).toBeNull();

        // 3. THE GAP: Simulate "Thundering Herd" of updates arriving *after* sync but *before* listen
        const updatesCount = DEFAULTS.REALTIME_LIMIT + 50; // 250 updates (exceeds default 200)
        console.log(`Simulating gap: Injecting ${updatesCount} updates (Limit is ${DEFAULTS.REALTIME_LIMIT})...`);

        // Use a separate client ID to avoid "own update" filtering (though test sets uid='client-B')
        const clientA_ID = 'client-A';

        // Batch insert updates (parallel for speed, but sequentially ordered by time implicitly)
        // Note: Firestore emulator is fast. We just need them to exist.
        // Use a persistent doc to generate sequential updates/clocks
        const sourceDoc = new Y.Doc();
        sourceDoc.clientID = 1111;
        const sourceArr = sourceDoc.getArray('data');

        const updates: Uint8Array[] = [];
        sourceDoc.on('update', (update) => {
            updates.push(update);
        });

        const updatePromises = [];
        for (let i = 0; i < updatesCount; i++) {
            // Perform op
            sourceArr.insert(0, [i]);

            // sourceDoc 'update' event fires synchronously
            const update = updates[updates.length - 1];

            updatePromises.push(addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
                update: Bytes.fromUint8Array(update),
                createdAt: serverTimestamp(),
                createdBy: clientA_ID,
                clientID: 1111,
                clockStart: i,
                clockEnd: i + 1
            }));
        }
        await Promise.all(updatePromises);

        // 4. Client B starts listening
        // CRITICAL: We pass the result from step 2 (which has lastSyncedDoc: null)
        // With the fix, this should query "everything after null" (i.e. everything)
        // Without the fix (if it used limitToLast), it would miss the first 50 updates.

        let updatesReceived = 0;
        const receivedIndices = new Set<number>();

        // Re-create ydoc to be clean (though it was empty after sync anyway)
        // We are testing if the listener applies the updates to the doc.

        return new Promise<void>((resolve, reject) => {
            // Listen for updates on the Yjs doc
            ydoc.on('update', (update: Uint8Array, origin: any) => {
                // Decode to verify which items we got (optional, but good for debug)
                // For this test, simply checking the Array length eventually constitutes success
            });

            const unsubscribe = createUpdateListener(syncCtx, result.lastSyncedDoc);

            // Poll for convergence
            const interval = setInterval(() => {
                const arr = ydoc.getArray('data');
                console.log(`Updates received: ${arr.length} / ${updatesCount}`);

                if (arr.length === updatesCount) {
                    clearInterval(interval);
                    unsubscribe();
                    resolve();
                } else if (arr.length > updatesCount) {
                    clearInterval(interval);
                    unsubscribe();
                    reject(new Error(`Received too many updates? ${arr.length}`));
                }
            }, 500);

            // Timeout if we miss updates (The failure mode)
            setTimeout(() => {
                clearInterval(interval);
                unsubscribe();
                const arr = ydoc.getArray('data');
                if (arr.length < updatesCount) {
                    reject(new Error(`Timed out! Missing updates. Received ${arr.length} of ${updatesCount}. Likely simulated the Sync Gap bug.`));
                } else {
                    resolve(); // Should have been caught by interval, but just in case
                }
            }, 10000);
        });
    }, 15000);

    it('should handle non-null cursor correctly', async () => {
        // 1. Setup SyncContext
        const syncCtx: SyncContext = {
            db,
            path,
            doc: ydoc,
            uid,
            maxUpdatesThreshold: 1000,
            onCompactionNeeded: vi.fn(),
            isDestroyed: () => false
        };

        // 2. Pre-fill DB with 10 updates
        let lastDocStub: QueryDocumentSnapshot | null = null;
        for (let i = 0; i < 10; i++) {
            const ref = await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
                createdAt: serverTimestamp(),
                val: i
            });
            if (i === 9) {
                const snap = await import('firebase/firestore').then(mod => mod.getDoc(ref));
                lastDocStub = snap as QueryDocumentSnapshot;
            }
        }

        // 3. Simulate correct "Sync" outcome
        // We manually assume sync saw these 10 docs and returned the 10th as cursor.
        const mockSyncResult = {
            lastSyncedDoc: lastDocStub
        };

        // 4. Add 5 NEW updates (The Gap Content)
        for (let i = 10; i < 15; i++) {
            const tempDoc = new Y.Doc();
            tempDoc.getArray('data').insert(0, [i]);
            await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(tempDoc)),
                createdAt: serverTimestamp(),
                createdBy: 'other',
            });
        }

        // 5. Listen with cursor
        return new Promise<void>((resolve, reject) => {
            const unsubscribe = createUpdateListener(syncCtx, mockSyncResult.lastSyncedDoc); // Pass the cursor (Doc #9)

            const interval = setInterval(() => {
                const arr = ydoc.getArray('data');
                // We expect 5 items (indices 10-14). 
                // The first 10 were "synced" (but not applied to local doc in this test setup, 
                // we ONLY care that the listener picks up the NEW ones).

                if (arr.length === 5) {
                    clearInterval(interval);
                    unsubscribe();
                    resolve();
                }
            }, 200);

            setTimeout(() => {
                clearInterval(interval);
                unsubscribe();
                const arr = ydoc.getArray('data');
                if (arr.length !== 5) {
                    reject(new Error(`Failed to pick up updates after cursor. Got ${arr.length}, expected 5.`));
                } else {
                    resolve();
                }
            }, 3000);
        });

    });
});
