/**
 * Reproduction test for Issue 5: destroy() flush is fire-and-forget
 * 
 * Bug: saveToFirestore() is not awaited in destroy().
 * If app exits immediately after destroy, pending data is lost.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { seedFromString, getStableDate } from '../unit/prng';
import { initializeApp } from '@firebase/app';
import {
    getFirestore,
    collection,
    getDocs
} from '@firebase/firestore';


const EMULATOR_HOST = '127.0.0.1';
const FIRESTORE_PORT = 8080;
const PROJECT_ID = 'demo-test';

describe('Issue 5: destroy() Fire-and-Forget Flush', () => {
    let app: any;
    let db: any;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const seed = `destroy-flush-${getStableDate()}-${counter++}`;
        // console.log(`Test Seed: ${seed}`);
        const rng = seedFromString(seed);
        const setup = await import("../utils/emulator").then(m => m.setupEmulator());
        app = setup.app;
        db = setup.db;
        path = `tests/${seed}`;
    });

    afterEach(async () => {
        // We no longer call terminate(db) here because it is a singleton
        // shared across tests. The emulator handles cleanup on exit.
    });

    it('should flush pending updates before destroy completes', async () => {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxWaitTime: 10000 // Long debounce to ensure update is in cache
        });

        // Wait for initial sync
        await new Promise(r => setTimeout(r, 500));

        // Make an update
        ydoc.getText('content').insert(0, 'ImportantData');

        // Immediately destroy - update should be in cache, not yet saved
        // Current bug: destroy() returns immediately, update may be lost
        const destroyPromise = provider.destroy();

        // Check if destroy returns a promise
        const isPromise = destroyPromise instanceof Promise;
        console.log(`destroy() returns promise: ${isPromise}`);

        // Wait for destroy to "complete" (or just return)
        if (isPromise) {
            await destroyPromise;
        }

        // Small grace period for fire-and-forget write to maybe complete
        await new Promise(r => setTimeout(r, 500));

        // Check if data was persisted
        const updatesSnap = await getDocs(collection(db, path, 'updates'));
        console.log(`Updates saved: ${updatesSnap.size}`);

        // Create new provider to verify data
        const ydoc2 = new Y.Doc();
        const provider2 = new FireProvider({
            firebaseApp: app,
            ydoc: ydoc2,
            path,
            maxWaitTime: 50
        });

        await new Promise(r => setTimeout(r, 2000));

        const content = ydoc2.getText('content').toString();
        console.log(`Recovered content: "${content}"`);

        // Bug: If flush wasn't awaited, content may be empty or partial
        expect(content).toBe('ImportantData');

        provider2.destroy();
    });

    it('should wait for flush even with many pending updates', async () => {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxWaitTime: 10000 // Long debounce
        });

        await new Promise(r => setTimeout(r, 500));

        // Make many updates rapidly
        for (let i = 0; i < 100; i++) {
            ydoc.getText('content').insert(i, String(i % 10));
        }

        const expectedContent = ydoc.getText('content').toString();
        console.log(`Expected content length: ${expectedContent.length}`);

        // Destroy
        const result = provider.destroy();
        if (result instanceof Promise) await result;
        await new Promise(r => setTimeout(r, 500));

        // Verify
        const ydoc2 = new Y.Doc();
        const provider2 = new FireProvider({
            firebaseApp: app,
            ydoc: ydoc2,
            path,
            maxWaitTime: 50
        });

        await new Promise(r => setTimeout(r, 2000));

        const actualContent = ydoc2.getText('content').toString();
        console.log(`Actual content length: ${actualContent.length}`);

        expect(actualContent).toBe(expectedContent);

        provider2.destroy();
    });

    it('should handle destroy called during active write', async () => {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxWaitTime: 50 // Short debounce
        });

        await new Promise(r => setTimeout(r, 500));

        // Make update
        ydoc.getText('content').insert(0, 'Data1');

        // Wait for debounce to trigger write
        await new Promise(r => setTimeout(r, 60));

        // Make another update while first write is in progress
        ydoc.getText('content').insert(5, 'Data2');

        // Destroy immediately
        const result = provider.destroy();
        if (result instanceof Promise) await result;
        await new Promise(r => setTimeout(r, 500));

        // Verify all data persisted
        const ydoc2 = new Y.Doc();
        const provider2 = new FireProvider({
            firebaseApp: app,
            ydoc: ydoc2,
            path,
            maxWaitTime: 50
        });

        await new Promise(r => setTimeout(r, 2000));

        const content = ydoc2.getText('content').toString();
        console.log(`Content after destroy during write: "${content}"`);

        expect(content).toContain('Data1');
        expect(content).toContain('Data2');

        provider2.destroy();
    });
});
