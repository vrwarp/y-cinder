/**
 * Integration test: the fold tail's Storage lifecycle and its fallbacks.
 *
 * A fold publishes its tail (what it merged on top of the base) so a client
 * that missed only that fold can catch up without the snapshot (see
 * returning_device_fold_tail.test.ts for the transfer contract). The tail
 * is a shortcut, never a source of truth:
 *
 *  - each tail is bound to the version it was folded into; the next fold
 *    garbage-collects it and squash clears it, so Storage holds exactly
 *    what the main document references;
 *  - a tail that is gone (the next fold collected it while a reader was
 *    about to download it) or does not apply makes the reader download the
 *    snapshot as before — nothing is quarantined, and the reader converges.
 *
 * @file fold_tail_lifecycle.test.ts
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

const { tailFault } = vi.hoisted(() => ({
    tailFault: {
        /** While true, downloads of fold tails come back as bytes Yjs rejects. */
        corrupt: false,
        downloads: [] as string[],
    },
}));

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getBytes: async (storageRef: any, ...rest: any[]) => {
            const fullPath: string = storageRef?.fullPath ?? String(storageRef);
            tailFault.downloads.push(fullPath.split('/').pop()!);
            if (tailFault.corrupt && fullPath.includes('/tail_v')) {
                return new Uint8Array([255, 254, 253, 252, 251, 250, 249, 248]).buffer;
            }
            return actual.getBytes(storageRef, ...rest);
        },
    };
});

import * as Y from 'yjs';
import { doc as fsDoc, getDoc } from '@firebase/firestore';
import { deleteObject, listAll, ref } from '@firebase/storage';
import { FireProvider } from '../../src/provider';
import { createSnapshotListener, SyncContext } from '../../src/sync';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** Large enough that a one-key tail is far below half the snapshot. */
const BASE_PAYLOAD = 'x'.repeat(20_000);

describe('Fold tail lifecycle', () => {
    let app: any;
    let db: any;
    let storage: any;
    let counter = 0;
    const cleanup: Array<() => Promise<void> | void> = [];

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
        tailFault.corrupt = false;
        tailFault.downloads = [];
    });

    afterEach(async () => {
        tailFault.corrupt = false;
        for (const fn of cleanup.splice(0).reverse()) {
            try { await fn(); } catch { /* best effort */ }
        }
    });

    const newPath = (name: string) => `integration-tests/fold-tail-${name}-${getStableDate()}-${Date.now()}-${counter++}`;

    const createProvider = (ydoc: Y.Doc, path: string) => {
        const p = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxWaitTime: 50,
            maxUpdatesThreshold: 1000, // compaction only via explicit compact()
            historyFoldThreshold: 1, // every compaction folds
        });
        cleanup.push(() => p.destroy());
        return p;
    };

    const mainDoc = async (path: string) => (await getDoc(fsDoc(db, path))).data()!;

    /** Full paths of the Storage objects directly under the document's prefix. */
    const listBlobs = async (path: string) => (await listAll(ref(storage, path))).items.map(i => i.fullPath).sort();

    /** Storage objects the main document currently points at. */
    const referencedBlobs = async (path: string) => {
        const data = await mainDoc(path);
        return [data.snapshotStoragePath, data.deleteSetStoragePath, data.foldTailStoragePath]
            .filter((p): p is string => typeof p === 'string')
            .sort();
    };

    /** Writer A: one saved edit per call. */
    const writer = async (path: string) => {
        const docA = new Y.Doc();
        const providerA = createProvider(docA, path);
        await waitForConditionTruthy(() => providerA.synced, { timeout: 30000, message: 'A synced' });
        const write = async (key: string, value: unknown) => {
            const saved = new Promise<void>(resolve => providerA.once('saved', () => resolve()));
            docA.getMap('m').set(key, value);
            await saved;
        };
        return { docA, providerA, write };
    };

    it('binds a tail to each fold, collects the previous one, and squash clears it', async () => {
        const path = newPath('gc');
        const { providerA, write } = await writer(path);

        // Fold 1 has no base to build on: no tail.
        await write('base', BASE_PAYLOAD);
        await providerA.compact();
        let main = await mainDoc(path);
        expect(main.version).toBe(1);
        expect(main.foldTailStoragePath).toBeUndefined();
        expect(await listBlobs(path)).toEqual(await referencedBlobs(path));

        // Fold 2 publishes the tail of what it merged on top of the base.
        await write('second', 2);
        await providerA.compact();
        main = await mainDoc(path);
        const tail2: string = main.foldTailStoragePath;
        expect(tail2).toContain('/tail_v2_');
        expect(main.foldTailVersion).toBe(2);
        expect(typeof main.foldTailBaseClocks).toBe('string');
        expect(await listBlobs(path)).toEqual(await referencedBlobs(path));
        expect(await listBlobs(path)).toContain(tail2);

        // Fold 3 replaces it and collects the previous tail.
        await write('third', 3);
        await providerA.compact();
        main = await mainDoc(path);
        expect(main.foldTailVersion).toBe(3);
        expect(main.foldTailStoragePath).toContain('/tail_v3_');
        expect(await listBlobs(path)).toEqual(await referencedBlobs(path));
        expect(await listBlobs(path)).not.toContain(tail2);

        // Squash starts a new id space: the tail no longer applies.
        const squashed = await providerA.squash();
        expect(squashed.error).toBeUndefined();
        expect(squashed.success).toBe(true);
        main = await mainDoc(path);
        expect(main.foldTailStoragePath).toBeUndefined();
        expect(main.foldTailBaseClocks).toBeUndefined();
        expect(main.foldTailVersion).toBeUndefined();
        expect(await listBlobs(path)).toEqual([main.snapshotStoragePath]);
    }, 120000);

    /**
     * A holds the base (fold 1); B gets a warm copy; A then folds again
     * (fold 2, which publishes a tail) and is destroyed. Returns B's warm
     * doc and A's final content.
     */
    const returningDevice = async (path: string) => {
        const { docA, providerA, write } = await writer(path);
        await write('base', BASE_PAYLOAD);
        await providerA.compact();
        const docB = new Y.Doc();
        Y.applyUpdate(docB, Y.encodeStateAsUpdate(docA));
        await write('missed', 'by B');
        await providerA.compact();
        const main = await mainDoc(path);
        expect(main.foldTailVersion).toBe(main.version);
        const expected = docA.getMap('m').toJSON();
        await providerA.destroy();
        return { docB, expected, main };
    };

    it('initial sync: a tail collected before the download falls back to the snapshot', async () => {
        const path = newPath('missing');
        const { docB, expected, main } = await returningDevice(path);
        // The next fold's GC got there first.
        await deleteObject(ref(storage, main.foldTailStoragePath));

        tailFault.downloads = [];
        const providerB = createProvider(docB, path);
        const quarantined: string[] = [];
        providerB.on('corrupted-document', (e: { docId: string }) => quarantined.push(e.docId));
        await waitForConditionTruthy(() => providerB.synced, { timeout: 30000, message: 'B synced' });

        expect(docB.getMap('m').toJSON()).toEqual(expected);
        expect(tailFault.downloads).toEqual([
            main.foldTailStoragePath.split('/').pop(),
            main.snapshotStoragePath.split('/').pop(),
        ]);
        expect(quarantined).toEqual([]);
    }, 120000);

    it('initial sync: a tail that does not apply falls back to the snapshot', async () => {
        const path = newPath('corrupt');
        const { docB, expected, main } = await returningDevice(path);

        tailFault.corrupt = true;
        tailFault.downloads = [];
        const providerB = createProvider(docB, path);
        const quarantined: string[] = [];
        providerB.on('corrupted-document', (e: { docId: string }) => quarantined.push(e.docId));
        await waitForConditionTruthy(() => providerB.synced, { timeout: 30000, message: 'B synced' });

        expect(docB.getMap('m').toJSON()).toEqual(expected);
        expect(tailFault.downloads).toEqual([
            main.foldTailStoragePath.split('/').pop(),
            main.snapshotStoragePath.split('/').pop(),
        ]);
        expect(quarantined).toEqual([]);
    }, 120000);

    it('snapshot listener: a tail that does not apply falls back to the snapshot, quarantining nothing', async () => {
        const path = newPath('listener');
        const { docA, providerA, write } = await writer(path);
        await write('base', BASE_PAYLOAD);
        await providerA.compact();
        const docB = new Y.Doc();
        Y.applyUpdate(docB, Y.encodeStateAsUpdate(docA));

        let destroyed = false;
        const corruptedDocIds = new Set<string>();
        const ctxB: SyncContext = {
            db,
            path,
            doc: docB,
            uid: 'offline-device-b',
            maxUpdatesThreshold: 1000,
            isDestroyed: () => destroyed,
            storage,
            corruptedDocIds,
            getEpoch: () => 0,
        };
        const unsubscribe = createSnapshotListener(ctxB, (await mainDoc(path)).version);
        cleanup.push(() => { destroyed = true; unsubscribe(); });

        tailFault.corrupt = true;
        await write('missed', 'by B');
        await providerA.compact();
        const main = await mainDoc(path);
        expect(main.foldTailVersion).toBe(main.version);

        await waitForConditionTruthy(() => docB.getMap('m').get('missed') === 'by B', { timeout: 30000, message: 'B caught up' });
        expect(docB.getMap('m').toJSON()).toEqual(docA.getMap('m').toJSON());
        expect(tailFault.downloads).toContain(main.foldTailStoragePath.split('/').pop());
        expect(tailFault.downloads).toContain(main.snapshotStoragePath.split('/').pop());
        expect([...corruptedDocIds]).toEqual([]);
    }, 120000);
});
