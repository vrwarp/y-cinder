/**
 * Reproduction test for Issue 3: Single-client metadata loses multi-client updates
 * 
 * Bug: extractAllMetadata() returns array but only first element is used for filtering.
 * Updates containing multiple clients' changes may be incorrectly skipped.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { initializeApp } from '@firebase/app';
import {
    getFirestore,
    connectFirestoreEmulator,
    collection,
    addDoc,
    serverTimestamp,
    Bytes,
    terminate
} from '@firebase/firestore';
import { waitForConditionTruthy } from '../utils/wait';
import { seedFromString, getStableDate } from '../unit/prng';

const EMULATOR_HOST = '127.0.0.1';
const FIRESTORE_PORT = 8080;
const PROJECT_ID = 'demo-test';

describe('Issue 3: Multi-Client Metadata Handling', () => {
    let app: any;
    let db: any;
    let path: string;
    let counter = 0;

    let seed: string;
    let rng: any;

    beforeEach(async () => {
        seed = `multiclient-meta-${getStableDate()}-${counter++}`;
        // console.log(`Test Seed: ${seed}`);
        rng = seedFromString(seed);

        const { app: a, db: d } = await import("../utils/emulator").then(m => m.setupEmulator());
        app = a;
        db = d;
        db = getFirestore(app);
        connectFirestoreEmulator(db, EMULATOR_HOST, FIRESTORE_PORT);

        path = `tests/${seed}`;
    });

    afterEach(async () => {
        await terminate(db);
    });

    it('should sync updates that contain changes from multiple clients', async () => {
        // Create two docs with different client IDs
        const doc1 = new Y.Doc();
        doc1.clientID = 1001;
        doc1.getText('content').insert(0, 'FromClient1');

        const doc2 = new Y.Doc();
        doc2.clientID = 2002;
        doc2.getText('content').insert(0, 'FromClient2');

        // Merge updates from both clients into a single update
        const update1 = Y.encodeStateAsUpdate(doc1);
        const update2 = Y.encodeStateAsUpdate(doc2);
        const mergedUpdate = Y.mergeUpdates([update1, update2]);

        // Save the merged update with FIXED metadata (clientIDs array)
        // This is what the Issue 3 fix now does - stores all client IDs
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(mergedUpdate),
            createdAt: serverTimestamp(),
            createdBy: 'test',
            clientIDs: [1001, 2002],  // Issue 3 Fix: Array of all client IDs
            clientClocks: [5, 11]     // Clocks for each client
        });

        // Create a new provider that already has client1's data
        const receiverDoc = new Y.Doc();
        receiverDoc.clientID = 3003;

        // Pre-populate with client1's data (to simulate having received it before)
        Y.applyUpdate(receiverDoc, update1);

        const provider = new FireProvider({
            firebaseApp: app,
            ydoc: receiverDoc,
            path,
            maxWaitTime: 50
        });

        // Wait for sync (poll until both clients' content is applied)
        await waitForConditionTruthy(() => {
            const c = receiverDoc.getText('content').toString();
            return c.includes('FromClient1') && c.includes('FromClient2');
        }, { timeout: 30000, interval: 100, message: 'Receiver should apply updates from both clients' });

        const content = receiverDoc.getText('content').toString();
        console.log(`Receiver content: "${content}"`);

        // With Issue 3 Fix: Filter checks ALL clientIDs, sees we DON'T have client 2002's data,
        // so it applies the update and we get both contents
        expect(content).toContain('FromClient1');
        expect(content).toContain('FromClient2');

        await provider.destroy();
    });

    it('should correctly filter updates when metadata tracks all clients', async () => {
        const path2 = `tests/multiclient-filter-${seed}`;

        // Setup: Provider A has made changes
        const docA = new Y.Doc();
        docA.clientID = 100;
        const providerA = new FireProvider({
            firebaseApp: app,
            ydoc: docA,
            path: path2,
            maxWaitTime: 50
        });

        await new Promise(r => setTimeout(r, 500));

        docA.getText('shared').insert(0, 'A');

        // Provider B joins and makes changes
        const docB = new Y.Doc();
        docB.clientID = 200;
        const providerB = new FireProvider({
            firebaseApp: app,
            ydoc: docB,
            path: path2,
            maxWaitTime: 50
        });

        await new Promise(r => setTimeout(r, 1000));

        docB.getText('shared').insert(1, 'B');

        // Wait for sync
        await waitForConditionTruthy(() => {
            return docA.getText('shared').toString().includes('B') &&
                docB.getText('shared').toString().includes('A');
        }, { timeout: 30000, interval: 100, message: 'Both clients should see each other changes' });

        // Provider C joins late
        const docC = new Y.Doc();
        docC.clientID = 300;
        const providerC = new FireProvider({
            firebaseApp: app,
            ydoc: docC,
            path: path2,
            maxWaitTime: 50
        });

        await waitForConditionTruthy(() => {
            const text = docC.getText('shared').toString();
            return text.includes('A') && text.includes('B');
        }, { timeout: 30000, interval: 100, message: 'Late joiner should get all updates' });

        const finalText = docC.getText('shared').toString();
        console.log(`Final text on C: "${finalText}"`);

        expect(finalText).toContain('A');
        expect(finalText).toContain('B');

        providerA.destroy();
        providerB.destroy();
        providerC.destroy();
    });
});
