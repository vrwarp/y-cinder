
import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { collection, addDoc, Bytes, serverTimestamp, getDocs, onSnapshot } from 'firebase/firestore';
import { setupEmulator } from '../utils/emulator';

describe('Thundering Herd Compaction Fix', () => {
    let app: any;
    let db: any;
    let path: string;

    beforeEach(async () => {
        path = `tests/thundering-herd-${Date.now()}-${Math.random().toString(36).substring(2)}`;
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
    });

    it('should NOT trigger compaction when probability is 0', { timeout: 30000 }, async () => {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 5,
            compactionProbability: 0 // Never compact
        });

        // Add 6 updates (above threshold 5)
        const promises = [];
        for (let i = 0; i < 6; i++) {
            promises.push(addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(new Y.Doc())),
                createdAt: serverTimestamp(),
                createdBy: 'other-user'
            }));
        }
        await Promise.all(promises);

        // Wait for onSnapshot (just a small buffer)
        await new Promise(r => setTimeout(r, 2000));

        // Check if compaction happened (updates should still be there)
        const updatesSnap = await getDocs(collection(db, path, 'updates'));
        expect(updatesSnap.size).toBeGreaterThanOrEqual(6);

        provider.destroy();
    });

    it('should trigger compaction when probability is 1', { timeout: 30000 }, async () => {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 5,
            compactionProbability: 1 // Always compact
        });

        // Add 6 updates
        const promises = [];
        for (let i = 0; i < 6; i++) {
            promises.push(addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(new Y.Doc())),
                createdAt: serverTimestamp(),
                createdBy: 'other-user'
            }));
        }
        await Promise.all(promises);

        // Wait for updates to be cleared (compaction finished)
        await new Promise<void>((resolve, reject) => {
            const unsub = onSnapshot(collection(db, path, 'updates'), (snap) => {
                if (snap.size === 0) {
                    unsub();
                    resolve();
                }
            }, (err) => {
                unsub();
                reject(err);
            });
            // Safety timeout
            setTimeout(() => { unsub(); reject(new Error("Compaction wait timed out")); }, 15000);
        });

        provider.destroy();
    });
});
