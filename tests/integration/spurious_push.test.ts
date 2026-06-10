/**
 * Regression test: reconnecting must not write spurious updates.
 *
 * Bug: Yjs embeds the document's full delete-set in every diff produced by
 * encodeStateAsUpdate(doc, serverSV). For any document whose history
 * contains a deletion, a fully-synced client therefore produced a non-empty
 * "diff" on every connect and wrote a useless update document each time —
 * accumulating toward the compaction threshold on idle reconnects.
 *
 * The fix only pushes when the diff carries structs or deletions the server
 * is missing. Genuine deletion-only offline edits must still be pushed.
 *
 * @file spurious_push.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { getDocs, collection } from '@firebase/firestore';
import { waitForConditionEquals, waitForConditionTruthy, waitForConditionGreaterThan } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('Spurious push prevention', () => {
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

    const countUpdates = async (path: string): Promise<number> => {
        const snap = await getDocs(collection(db, path, 'updates'));
        return snap.size;
    };

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
    });

    it('does not push on reconnect when fully synced (doc with deletion history)', async () => {
        const path = `integration-tests/spurious-push-${getStableDate()}-${counter++}`;

        // Session 1: create content including a deletion, let it save
        const doc = new Y.Doc();
        const provider1 = createProvider(doc, path);
        await waitForConditionTruthy(() => provider1.synced, { timeout: 30000 });

        doc.getText('t').insert(0, 'hello world');
        doc.getText('t').delete(0, 6); // deletion history is the trigger for the bug

        await waitForConditionGreaterThan(
            () => countUpdates(path),
            0,
            { timeout: 30000, interval: 100, message: 'Initial edits should be saved' }
        );
        await provider1.destroy();

        const countBefore = await countUpdates(path);

        // Session 2: reconnect with the SAME (fully synced) doc
        const provider2 = createProvider(doc, path);
        await waitForConditionTruthy(() => provider2.synced, { timeout: 30000 });
        // Give a potential (buggy) push time to land
        await new Promise(r => setTimeout(r, 1500));
        await provider2.destroy();

        const countAfter = await countUpdates(path);
        expect(countAfter).toBe(countBefore);
    }, 90000);

    it('does not push on reconnect after compaction (delete-set fingerprint)', async () => {
        const path = `integration-tests/spurious-push-compact-${getStableDate()}-${counter++}`;

        // Session 1: content with deletions, then compact everything into a
        // storage-backed snapshot (updates collection becomes empty)
        const doc = new Y.Doc();
        const provider1 = new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path,
            maxWaitTime: 50,
            maxUpdatesThreshold: 1000,
        });
        await waitForConditionTruthy(() => provider1.synced, { timeout: 30000 });

        doc.getText('t').insert(0, 'hello world');
        doc.getText('t').delete(0, 6);
        await waitForConditionGreaterThan(() => countUpdates(path), 0, { timeout: 30000 });

        await provider1.compact();
        await provider1.destroy();
        expect(await countUpdates(path)).toBe(0);

        // Session 2: reconnect fully synced — the deletions now only live in
        // the snapshot; the stored delete-set fingerprint must prove server
        // coverage without downloading the snapshot blob
        const provider2 = new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path,
            maxWaitTime: 50,
            maxUpdatesThreshold: 1000,
        });
        await waitForConditionTruthy(() => provider2.synced, { timeout: 30000 });
        await new Promise(r => setTimeout(r, 1500));
        await provider2.destroy();

        expect(await countUpdates(path)).toBe(0);
    }, 90000);

    it('still pushes genuine deletion-only offline edits', async () => {
        const path = `integration-tests/deletion-push-${getStableDate()}-${counter++}`;

        // Session 1: seed content
        const doc = new Y.Doc();
        const provider1 = createProvider(doc, path);
        await waitForConditionTruthy(() => provider1.synced, { timeout: 30000 });
        doc.getText('t').insert(0, 'hello world');
        await waitForConditionGreaterThan(() => countUpdates(path), 0, { timeout: 30000 });
        await provider1.destroy();

        // Offline: delete only (creates no new structs, only delete-set entries)
        doc.getText('t').delete(0, 6);
        expect(doc.getText('t').toString()).toBe('world');

        // Session 2: reconnect — the deletion must be pushed
        const provider2 = createProvider(doc, path);
        await waitForConditionTruthy(() => provider2.synced, { timeout: 30000 });
        await new Promise(r => setTimeout(r, 1000));
        await provider2.destroy();

        // A fresh client must observe the deletion
        const freshDoc = new Y.Doc();
        const provider3 = createProvider(freshDoc, path);
        await waitForConditionEquals(
            () => freshDoc.getText('t').toString(),
            'world',
            { timeout: 30000, interval: 100, message: 'Fresh client should see the offline deletion' }
        );
        await provider3.destroy();
    }, 90000);
});
