/**
 * Epoch Squash Integration Tests
 *
 * End-to-end verification of the long-lived-document floor reset:
 *
 *  - squash() rebuilds the document into a new epoch: content identical,
 *    state vector reset to one client, delete-set empty, updates/history
 *    collections cleared, main document carries epoch + fresh fingerprint
 *  - a fresh client bootstraps the new epoch and reads the epoch marker
 *    from inside the document
 *  - a connected old-epoch client receives 'epoch-changed' (and never
 *    applies the new snapshot onto its old doc)
 *  - stale old-epoch update documents are ignored by new-epoch clients
 *    and deleted by compaction without merging
 *
 * @file squash.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import { readDocEpoch } from '../../src/squash';
import * as Y from 'yjs';
import { collection, getDocs, getDoc, doc, addDoc, Bytes } from 'firebase/firestore';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('FireProvider Epoch Squash', () => {
    let app: any;
    let db: any;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        path = `tests/squash-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    it('squashes into a new epoch; fresh clients bootstrap it; old clients get epoch-changed', { timeout: 120000 }, async () => {
        // --- Client A builds a churned document ---
        const ydocA = new Y.Doc();
        const providerA = new FireProvider({
            firebaseApp: app,
            ydoc: ydocA,
            path,
            maxUpdatesThreshold: 1000,
            maxWaitTime: 50,
        });
        await waitForConditionTruthy(() => providerA.synced, { timeout: 30000, message: 'A synced' });

        const posA = ydocA.getMap('positions');
        const annA = ydocA.getMap('annotations');
        for (let i = 0; i < 30; i++) {
            posA.set('book' + (i % 5), { cfi: 'cfi-' + i, pct: i / 30 });
            if (i % 3 === 0) annA.set('a' + i, 'note ' + i);
            if (i % 9 === 0 && i > 0) annA.delete('a' + (i - 9));
            await new Promise(r => setTimeout(r, 90));
        }
        await waitForConditionTruthy(async () => {
            const snap = await getDocs(collection(db, path, 'updates'));
            return snap.size >= 1;
        }, { timeout: 20000, message: 'A updates persisted' });

        // --- Client B connects on the old epoch and stays connected ---
        const ydocB = new Y.Doc();
        const providerB = new FireProvider({
            firebaseApp: app,
            ydoc: ydocB,
            path,
            maxUpdatesThreshold: 1000,
        });
        await waitForConditionTruthy(() => providerB.synced, { timeout: 30000, message: 'B synced' });
        const epochChanges: any[] = [];
        providerB.on('epoch-changed', (e: any) => epochChanges.push(e));

        const contentBefore = JSON.stringify({
            positions: ydocA.getMap('positions').toJSON(),
            annotations: ydocA.getMap('annotations').toJSON(),
        });

        // --- A squashes ---
        const result = await providerA.squash();
        expect(result.error).toBeUndefined();
        expect(result.success).toBe(true);
        expect(result.epoch).toBe(1);
        expect(providerA.epoch).toBe(1);

        // Server state: epoch bumped, collections cleared, fingerprint reset
        const mainSnap = await getDoc(doc(db, path));
        expect(mainSnap.data()!.epoch).toBe(1);
        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);
        expect((await getDocs(collection(db, path, 'history'))).size).toBe(0);
        expect(mainSnap.data()!.snapshotStoragePath).toContain('snapshot_e1_');

        // --- Fresh client C bootstraps the new epoch ---
        const ydocC = new Y.Doc();
        const providerC = new FireProvider({
            firebaseApp: app,
            ydoc: ydocC,
            path,
            maxUpdatesThreshold: 1000,
            maxWaitTime: 50,
        });
        await waitForConditionTruthy(() => providerC.synced, { timeout: 30000, message: 'C synced' });
        const contentC = JSON.stringify({
            positions: ydocC.getMap('positions').toJSON(),
            annotations: ydocC.getMap('annotations').toJSON(),
        });
        expect(contentC).toBe(contentBefore);
        expect(readDocEpoch(ydocC)).toBe(1);
        expect(providerC.epoch).toBe(1);
        // The floor reset is visible to the new client: single-client SV
        expect(Y.decodeStateVector(Y.encodeStateVector(ydocC)).size).toBe(1);

        // --- Old client B is fenced, not corrupted ---
        await waitForConditionTruthy(() => epochChanges.length > 0, {
            timeout: 30000, message: 'B receives epoch-changed',
        });
        expect(epochChanges[0].epoch).toBe(1);
        expect(epochChanges[0].previousEpoch).toBe(0);
        expect(epochChanges[0].localState).toBeInstanceOf(Uint8Array);
        // B's old doc still materializes the old content — nothing doubled
        expect(readDocEpoch(ydocB)).toBe(0);
        expect(JSON.stringify({
            positions: ydocB.getMap('positions').toJSON(),
            annotations: ydocB.getMap('annotations').toJSON(),
        })).toBe(contentBefore);

        // --- New-epoch writes flow between A-successor clients ---
        // (C writes; a second fresh client D receives it)
        ydocC.getMap('positions').set('bookNew', { cfi: 'fresh', pct: 1 });
        const ydocD = new Y.Doc();
        const providerD = new FireProvider({
            firebaseApp: app, ydoc: ydocD, path, maxUpdatesThreshold: 1000,
        });
        await waitForConditionTruthy(
            () => (ydocD.getMap('positions').toJSON() as any).bookNew?.cfi === 'fresh',
            { timeout: 30000, message: 'D sees C\'s new-epoch write' }
        );

        await providerA.destroy();
        await providerB.destroy();
        await providerC.destroy();
        await providerD.destroy();
        ydocA.destroy(); ydocB.destroy(); ydocC.destroy(); ydocD.destroy();
    });

    it('ignores and garbage-collects stale old-epoch update documents', { timeout: 90000 }, async () => {
        // Build + squash a doc quickly
        const ydocA = new Y.Doc();
        const providerA = new FireProvider({
            firebaseApp: app, ydoc: ydocA, path, maxUpdatesThreshold: 1000, maxWaitTime: 50,
        });
        await waitForConditionTruthy(() => providerA.synced, { timeout: 30000, message: 'A synced' });
        ydocA.getMap('data').set('k', 'v');
        await waitForConditionTruthy(async () =>
            (await getDocs(collection(db, path, 'updates'))).size >= 1,
            { timeout: 20000, message: 'update persisted' });
        const squashRes = await providerA.squash();
        expect(squashRes.success).toBe(true);

        // A stale old-epoch update arrives late (offline client flush).
        // Its content must never reach new-epoch clients.
        const staleDoc = new Y.Doc();
        staleDoc.getMap('data').set('stale', 'poison');
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(staleDoc)),
            createdAt: new Date(),
            createdBy: 'old-epoch-client',
            // epoch field absent = epoch 0
        });
        staleDoc.destroy();

        // Fresh new-epoch client: must NOT see the stale write
        const ydocC = new Y.Doc();
        const providerC = new FireProvider({
            firebaseApp: app, ydoc: ydocC, path, maxUpdatesThreshold: 1000,
        });
        await waitForConditionTruthy(() => providerC.synced, { timeout: 30000, message: 'C synced' });
        expect(ydocC.getMap('data').get('k')).toBe('v');
        expect(ydocC.getMap('data').get('stale')).toBeUndefined();

        // Compaction deletes the stale doc without merging it
        await providerC.compact();
        await waitForConditionTruthy(async () =>
            (await getDocs(collection(db, path, 'updates'))).size === 0,
            { timeout: 20000, message: 'stale update garbage-collected' });

        // Still not visible after compaction, including for a fresh client
        const ydocE = new Y.Doc();
        const providerE = new FireProvider({
            firebaseApp: app, ydoc: ydocE, path, maxUpdatesThreshold: 1000,
        });
        await waitForConditionTruthy(() => providerE.synced, { timeout: 30000, message: 'E synced' });
        expect(ydocE.getMap('data').get('stale')).toBeUndefined();
        expect(ydocE.getMap('data').get('k')).toBe('v');

        await providerA.destroy();
        await providerC.destroy();
        await providerE.destroy();
        ydocA.destroy(); ydocC.destroy(); ydocE.destroy();
    });
});
