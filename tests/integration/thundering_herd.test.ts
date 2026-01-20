/**
 * Thundering Herd Compaction Tests
 *
 * Tests the probabilistic compaction trigger mechanism that prevents the
 * "thundering herd" problem. When many clients are connected and updates
 * exceed the threshold, only a small percentage (controlled by
 * `compactionProbability`) should attempt compaction, preventing contention.
 *
 * @file thundering_herd.test.ts
 * @see https://en.wikipedia.org/wiki/Thundering_herd_problem
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { collection, addDoc, Bytes, serverTimestamp, getDocs, onSnapshot } from 'firebase/firestore';
import { setupEmulator, clearFirestore } from '../utils/emulator';

describe('Thundering Herd Compaction Fix', () => {
    let app: any;
    let db: any;
    let path: string;

    beforeEach(async () => {
        path = `tests/thundering-herd-${Date.now()}-${Math.random().toString(36).substring(2)}`;
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        await clearFirestore(db);
    });

    it('should NOT trigger compaction when probability is 0', { timeout: 60000 }, async () => {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 5,
            compactionProbability: 0 // Never compact
        });

        // CRITICAL: Wait for provider to complete initial sync and set up listener
        // The constructor calls sync() async, so we must wait for it
        await new Promise(r => setTimeout(r, 500));

        // Add 6 updates (above threshold 5)
        for (let i = 0; i < 6; i++) {
            await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(new Y.Doc())),
                createdAt: serverTimestamp(),
                createdBy: 'other-user'
            });
        }

        // Wait for onSnapshot to process (give time for potential compaction)
        await new Promise(r => setTimeout(r, 2000));

        // Check if compaction happened (updates should still be there since probability=0)
        const updatesSnap = await getDocs(collection(db, path, 'updates'));
        expect(updatesSnap.size).toBeGreaterThanOrEqual(6);

        await provider.destroy();
    });

    it('should trigger compaction when probability is 1', { timeout: 60000 }, async () => {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 5,
            compactionProbability: 1 // Always compact
        });

        // CRITICAL: Wait for provider to complete initial sync and set up listener
        await new Promise(r => setTimeout(r, 500));

        // Add 6 updates
        for (let i = 0; i < 6; i++) {
            await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(new Y.Doc())),
                createdAt: serverTimestamp(),
                createdBy: 'other-user'
            });
        }

        // Wait for compaction to complete
        // The onSnapshot should trigger compaction when it sees > 5 updates
        await new Promise<void>((resolve, reject) => {
            let resolved = false;

            const unsub = onSnapshot(collection(db, path, 'updates'), (snap) => {
                // Compaction either moves updates to snapshot (size=0) 
                // or to history segments (size reduced)
                if (snap.size === 0 && !resolved) {
                    resolved = true;
                    unsub();
                    resolve();
                }
            }, (err) => {
                unsub();
                reject(err);
            });

            // Safety timeout - longer to allow for lock acquisition and compaction
            setTimeout(() => {
                if (!resolved) {
                    unsub();
                    reject(new Error("Compaction wait timed out"));
                }
            }, 30000);
        });

        await provider.destroy();
    });
});
