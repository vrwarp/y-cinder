/**
 * Regression test: an offline deletion contiguous with a deletion the server
 * already has must be pushed on the next initial sync.
 *
 * Bug: the initial-sync fast path (server covers every local struct — true
 * for any deletion-only offline edit) decides whether to push using
 * deleteSetCoveredByBlobs(). That check merges the server and local
 * delete-sets with Y.mergeDeleteSets, which extends the server's own
 * DeleteItem in place when a local range touches it, so the local-only
 * deletion was reported as already on the server. No update was written:
 * the local doc showed '' while the server and every other client kept
 * ' world' permanently.
 *
 * @file offline_contiguous_deletion.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { getDocs, collection } from '@firebase/firestore';
import { waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('Offline deletion contiguous with a server deletion', () => {
    let app: any;
    let db: any;
    let counter = 0;

    const createProvider = (ydoc: Y.Doc, path: string) => {
        return new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxWaitTime: 50,
        });
    };

    const serverText = async (path: string): Promise<string> => {
        const snap = await getDocs(collection(db, path, 'updates'));
        const view = new Y.Doc();
        snap.forEach(d => {
            const data = d.data();
            if (data.update) {
                Y.applyUpdate(view, data.update.toUint8Array());
            }
        });
        return view.getText('t').toString();
    };

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
    });

    it('pushes the offline deletion so a fresh client converges to the local state', async () => {
        const path = `integration-tests/offline-contiguous-deletion-${getStableDate()}-${counter++}`;

        // Session 1: type 'hello world', delete 'hello', let both save
        const doc = new Y.Doc();
        const provider1 = createProvider(doc, path);
        await waitForConditionTruthy(() => provider1.synced, { timeout: 30000 });

        doc.getText('t').insert(0, 'hello world');
        doc.getText('t').delete(0, 5);
        await waitForConditionEquals(
            () => serverText(path),
            ' world',
            { timeout: 30000, interval: 100, message: "Server should hold ' world' after session 1" }
        );
        await provider1.destroy();

        // Offline (no provider): delete ' world'. Deletion-only edit whose
        // clock range is contiguous with the server's 'hello' deletion.
        doc.getText('t').delete(0, 6);
        expect(doc.getText('t').toString()).toBe('');

        // Session 2: reconnect the same doc — the deletion must be pushed
        const provider2 = createProvider(doc, path);
        await waitForConditionTruthy(() => provider2.synced, { timeout: 30000 });
        await provider2.destroy(); // flushes anything pending

        expect(doc.getText('t').toString()).toBe('');

        // A fresh client must converge to the same (empty) text
        const freshDoc = new Y.Doc();
        const provider3 = createProvider(freshDoc, path);
        try {
            await waitForConditionTruthy(() => provider3.synced, { timeout: 30000 });
            await waitForConditionEquals(
                () => freshDoc.getText('t').toString(),
                '',
                { timeout: 10000, interval: 100, message: 'Fresh client should see the offline deletion' }
            );
        } finally {
            await provider3.destroy();
        }
    }, 90000);
});
