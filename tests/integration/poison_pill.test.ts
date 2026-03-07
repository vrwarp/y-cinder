/**
 * Integration test: Poison Pill Resilience
 *
 * Verifies that a corrupted document in Firestore (a "poison pill") does not
 * permanently block sync for all clients. The provider should quarantine
 * the corrupted document after the first parse failure and continue syncing
 * all valid data.
 *
 * @file poison_pill.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import {
    doc,
    setDoc,
    collection,
    addDoc,
    serverTimestamp,
    Bytes,
} from 'firebase/firestore';
import { waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('Data Integrity: Poison Pill Resilience', () => {
    let app: any;
    let db: any;
    let counter = 0;

    const createProvider = (ydoc: Y.Doc, path: string, config: any = {}) => {
        return new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            ...config,
        });
    };

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
    });

    it('should sync valid updates even when a corrupted update exists', async () => {
        const path = `integration-tests/poison-update-${getStableDate()}-${counter++}`;

        // 1. Seed a valid update
        const validDoc = new Y.Doc();
        validDoc.getText('content').insert(0, 'ValidData');
        const validUpdate = Y.encodeStateAsUpdate(validDoc);

        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(validUpdate),
            createdAt: serverTimestamp(),
            createdBy: 'seeder-valid',
        });

        // 2. Seed a corrupted update (garbage bytes)
        const garbage = new Uint8Array([255, 254, 253, 252, 251, 250, 249, 248]);
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(garbage),
            createdAt: serverTimestamp(),
            createdBy: 'seeder-corrupt',
        });

        // 3. Connect a client — it should sync the valid data despite the poison pill
        const testDoc = new Y.Doc();
        const provider = createProvider(testDoc, path);

        await waitForConditionEquals(
            () => testDoc.getText('content').toString(),
            'ValidData',
            { timeout: 10000, interval: 100, message: 'Valid data should sync despite corrupted update' }
        );

        expect(testDoc.getText('content').toString()).toBe('ValidData');
        await provider.destroy();
        validDoc.destroy();
    }, 20000);

    it('should sync valid history while quarantining corrupted history segment', async () => {
        const path = `integration-tests/poison-history-${getStableDate()}-${counter++}`;

        // 1. Seed a valid base snapshot
        const baseDoc = new Y.Doc();
        baseDoc.getText('content').insert(0, 'Base');
        const baseUpdate = Y.encodeStateAsUpdate(baseDoc);
        await setDoc(doc(db, path), {
            content: Bytes.fromUint8Array(baseUpdate),
        });

        // 2. Seed a valid history segment (causally linked)
        const sv1 = Y.encodeStateVector(baseDoc);
        baseDoc.getText('content').insert(4, '+Valid');
        const validHistory = Y.encodeStateAsUpdate(baseDoc, sv1);
        await addDoc(collection(db, path, 'history'), {
            segment: Bytes.fromUint8Array(validHistory),
            startTime: serverTimestamp(),
            endTime: serverTimestamp(),
        });

        // 3. Seed a corrupted history segment
        const garbage = new Uint8Array([255, 254, 253, 252, 251]);
        await addDoc(collection(db, path, 'history'), {
            segment: Bytes.fromUint8Array(garbage),
            startTime: serverTimestamp(),
            endTime: serverTimestamp(),
        });

        // 4. Connect a client — it should sync valid data
        const testDoc = new Y.Doc();
        const provider = createProvider(testDoc, path);

        await waitForConditionEquals(
            () => testDoc.getText('content').toString(),
            'Base+Valid',
            { timeout: 10000, interval: 100, message: 'Valid history should sync despite corrupted segment' }
        );

        expect(testDoc.getText('content').toString()).toBe('Base+Valid');
        await provider.destroy();
        baseDoc.destroy();
    }, 20000);

    it('should emit corrupted-document event when quarantining', async () => {
        const path = `integration-tests/poison-event-${getStableDate()}-${counter++}`;

        // 1. Connect the provider FIRST (on an empty document)
        const testDoc = new Y.Doc();
        const provider = createProvider(testDoc, path);

        // Listen for the corrupted-document event
        const corruptedEvents: { docId: string; error: Error }[] = [];
        provider.on('corrupted-document', (event: any) => {
            corruptedEvents.push(event);
        });

        // Wait for initial sync to complete
        await new Promise(r => setTimeout(r, 2000));

        // 2. THEN seed a corrupted update — this arrives via the real-time listener
        const garbage = new Uint8Array([255, 254, 253, 252, 251, 250, 249, 248]);
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(garbage),
            createdAt: serverTimestamp(),
            createdBy: 'seeder-corrupt',
        });

        // Wait for the event to fire
        await waitForConditionTruthy(
            () => corruptedEvents.length > 0,
            { timeout: 10000, interval: 100, message: 'corrupted-document event should fire' }
        );

        expect(corruptedEvents.length).toBeGreaterThan(0);
        expect(corruptedEvents[0].docId).toBeDefined();
        expect(corruptedEvents[0].error).toBeInstanceOf(Error);

        await provider.destroy();
    }, 20000);

    it('should continue syncing new valid updates after quarantining a poison pill', async () => {
        const path = `integration-tests/poison-then-valid-${getStableDate()}-${counter++}`;

        // 1. Seed a corrupted update first
        const garbage = new Uint8Array([255, 254, 253, 252, 251, 250, 249, 248]);
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(garbage),
            createdAt: serverTimestamp(),
            createdBy: 'seeder-corrupt',
        });

        // 2. Connect a client
        const testDoc = new Y.Doc();
        const provider = createProvider(testDoc, path);

        // Wait briefly for sync to process the corrupted update
        await new Promise(r => setTimeout(r, 2000));

        // 3. Now seed a valid update — should still be applied
        const validDoc = new Y.Doc();
        validDoc.getText('content').insert(0, 'AfterPoison');
        const validUpdate = Y.encodeStateAsUpdate(validDoc);
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(validUpdate),
            createdAt: serverTimestamp(),
            createdBy: 'seeder-valid',
        });

        await waitForConditionEquals(
            () => testDoc.getText('content').toString(),
            'AfterPoison',
            { timeout: 10000, interval: 100, message: 'Valid updates after poison pill should still sync' }
        );

        expect(testDoc.getText('content').toString()).toBe('AfterPoison');
        await provider.destroy();
        validDoc.destroy();
    }, 20000);
});
