/**
 * Regression: fold compaction must garbage-collect the snapshot blob it
 * replaces, even when that blob was written by squash().
 *
 * squash() stores its snapshot at `snapshot_e{E}_v{V}.bin`. The first fold
 * compaction after a squash replaces the main document's snapshot pointer,
 * so the squash blob is no longer referenced and must be deleted once the
 * fold commits. Every snapshot blob left behind is a full copy of the
 * document that is paid for in Cloud Storage forever.
 *
 * Contract checked here: after squash() followed by a fold, the only
 * objects under the document's Storage prefix are the ones the main
 * document references (its snapshot and, if offloaded, its delete-set).
 *
 * @file compaction_gc_squash_snapshot.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import * as Y from 'yjs';
import { collection, getDocs, getDoc, doc } from 'firebase/firestore';
import { FirebaseStorage, listAll, ref } from 'firebase/storage';
import { FireProvider } from '../../src/provider';
import { setupEmulator } from '../utils/emulator';
import { waitFor, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('Compaction GC after squash', () => {
    let app: any;
    let db: any;
    let storage: FirebaseStorage;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
        path = `tests/compaction-gc-squash-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    /** Full paths of the Storage objects directly under the document's prefix. */
    async function listBlobs(): Promise<string[]> {
        const res = await listAll(ref(storage, path));
        return res.items.map(i => i.fullPath).sort();
    }

    /** Storage objects the main document currently points at. */
    async function referencedBlobs(): Promise<string[]> {
        const data = (await getDoc(doc(db, path))).data() ?? {};
        return [data.snapshotStoragePath, data.deleteSetStoragePath]
            .filter((p): p is string => typeof p === 'string')
            .sort();
    }

    it('deletes the squash snapshot blob (snapshot_e{E}_v{V}.bin) when a later fold replaces it', { timeout: 120000 }, async () => {
        // historyFoldThreshold: 1 => every compaction is a fold into the
        // base snapshot. maxUpdatesThreshold is high so compaction only
        // runs when the test asks for it.
        const config = { maxUpdatesThreshold: 1000, maxWaitTime: 50, historyFoldThreshold: 1 };

        // --- Client A writes, then squashes into epoch 1 ---
        const ydocA = new Y.Doc();
        const providerA = new FireProvider({ firebaseApp: app, ydoc: ydocA, path, ...config });
        await waitForConditionTruthy(() => providerA.synced, { timeout: 30000, message: 'A synced' });

        ydocA.getMap('data').set('before', 'squash');
        await waitForConditionTruthy(async () =>
            (await getDocs(collection(db, path, 'updates'))).size >= 1,
            { timeout: 20000, message: 'A update persisted' });

        const squashRes = await providerA.squash();
        expect(squashRes.error).toBeUndefined();
        expect(squashRes.success).toBe(true);
        expect(squashRes.epoch).toBe(1);

        const afterSquash = (await getDoc(doc(db, path))).data()!;
        const squashBlob: string = afterSquash.snapshotStoragePath;
        expect(squashBlob).toContain('snapshot_e1_');
        // squash() already cleaned up what it replaced: only its blob is left.
        expect(await listBlobs()).toEqual([squashBlob]);

        await providerA.destroy();
        ydocA.destroy();

        // --- Client B bootstraps the new epoch, writes, and folds ---
        const ydocB = new Y.Doc();
        const providerB = new FireProvider({ firebaseApp: app, ydoc: ydocB, path, ...config });
        await waitForConditionTruthy(() => providerB.synced, { timeout: 30000, message: 'B synced' });
        expect(ydocB.getMap('data').get('before')).toBe('squash');

        ydocB.getMap('data').set('after', 'fold');
        await waitForConditionTruthy(async () =>
            (await getDocs(collection(db, path, 'updates'))).size >= 1,
            { timeout: 20000, message: 'B update persisted' });

        await providerB.compact();

        // Precondition: the fold committed and moved the snapshot pointer
        // off the squash blob.
        const afterFold = (await getDoc(doc(db, path))).data()!;
        expect(afterFold.version).toBeGreaterThan(afterSquash.version);
        expect(afterFold.snapshotStoragePath).not.toBe(squashBlob);
        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);

        // Contract: the replaced snapshot blob is garbage-collected, so
        // Storage holds exactly what the main document references. Allow
        // a short grace period in case GC is deferred after the commit.
        const expected = await referencedBlobs();
        let blobs: string[] = [];
        try {
            blobs = await waitFor(
                listBlobs,
                (items) => JSON.stringify(items) === JSON.stringify(expected),
                { timeout: 5000, interval: 200, message: 'Storage matches main-doc references' },
            );
        } catch {
            blobs = await listBlobs();
        }
        expect(blobs, `squash blob ${squashBlob} must not outlive the fold that replaced it`)
            .toEqual(expected);
        expect(blobs).not.toContain(squashBlob);

        // GC removed only the stale blob: a fresh client still loads everything.
        const ydocC = new Y.Doc();
        const providerC = new FireProvider({ firebaseApp: app, ydoc: ydocC, path, ...config });
        await waitForConditionTruthy(() => providerC.synced, { timeout: 30000, message: 'C synced' });
        expect(ydocC.getMap('data').toJSON()).toEqual({ before: 'squash', after: 'fold' });

        await providerB.destroy();
        await providerC.destroy();
        ydocB.destroy();
        ydocC.destroy();
    });
});
