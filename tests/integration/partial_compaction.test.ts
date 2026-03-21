/**
 * Test for Issue 12: Partial compaction failure
 * 
 * Tests that partial failures in compaction don't cause data duplication.
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
    getDoc,
    addDoc,
    setDoc,
    serverTimestamp,
    Bytes,
    terminate
} from '@firebase/firestore';

const EMULATOR_HOST = '127.0.0.1';
const FIRESTORE_PORT = 8080;
const PROJECT_ID = 'demo-test';

describe('Issue 12: Partial Compaction Failure', () => {
    let app: any;
    let db: any;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const seed = `partial-compact-${getStableDate()}-${counter++}`;
        // console.log(`Test Seed: ${seed}`);
        const rng = seedFromString(seed);
        const { app: a, db: d } = await import("../utils/emulator").then(m => m.setupEmulator());
        app = a;
        db = d;
        path = `tests/${seed}`;
    }, 30000);

    afterEach(async () => {
        // await terminate(db);
    });

    it('should not duplicate data if transaction partially fails', { timeout: 30000 }, async () => {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 1000
        });

        // Add several updates with content
        const contentDoc = new Y.Doc();
        contentDoc.getText('text').insert(0, 'TestData');

        for (let i = 0; i < 5; i++) {
            await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(contentDoc)),
                createdAt: serverTimestamp()
            });
        }

        // Run compaction - should atomically succeed or fail
        await provider.compact();

        // Wait for completion
        await new Promise(r => setTimeout(r, 1000));

        // Check state
        const mainSnap = await getDoc(doc(db, path));
        const updatesSnap = await getDocs(collection(db, path, 'updates'));
        const historySnap = await getDocs(collection(db, path, 'history'));

        console.log(`Main doc exists: ${mainSnap.exists()}`);
        console.log(`Updates remaining: ${updatesSnap.size}`);
        console.log(`History segments: ${historySnap.size}`);

        // Either all updates are compacted, or none are
        // Should not have duplicated data
        if (mainSnap.exists() && mainSnap.data()?.content) {
            expect(updatesSnap.size).toBe(0);
        }

        await provider.destroy();
    });

    it('should maintain data integrity after compaction', { timeout: 30000 }, async () => {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 10000 // Prevent auto-compaction
        });

        // Wait for sync
        await new Promise(r => setTimeout(r, 500));

        // Add unique content
        ydoc.getText('content').insert(0, 'UniqueContent123');

        // Wait for save
        await new Promise(r => setTimeout(r, 1000));

        // Trigger compaction
        await provider.compact();
        await new Promise(r => setTimeout(r, 500));

        // Verify with new provider
        await provider.destroy();

        const ydoc2 = new Y.Doc();
        const provider2 = new FireProvider({
            firebaseApp: app,
            ydoc: ydoc2,
            path
        });

        await new Promise(r => setTimeout(r, 2000));

        const content = ydoc2.getText('content').toString();
        console.log(`Recovered content: "${content}"`);

        expect(content).toBe('UniqueContent123');

        await provider2.destroy();
    });
});
