/**
 * Regression test (mixed deployment): a fold by a peer still running the
 * PREVIOUS release (4297ea9) over a snapshot committed by the current code
 * must not leave the replaced snapshot and delete-set blobs in Cloud
 * Storage forever.
 *
 * Current folds name their blobs per attempt
 * (`snapshot_v{V}_{attemptId}.bin`, `ds_v{V}_{attemptId}.bin`) and
 * garbage-collect the blobs a fold replaces through the paths stored on the
 * replaced main document. The previous release cannot be changed: its fold
 * rebuilds the replaced names from the version (`snapshot_v{V}.bin`,
 * `ds_v{V}.bin`), so over a new-code snapshot it deletes objects that do
 * not exist (404, logged and ignored), and its merge:true commit overwrites
 * snapshotStoragePath / deleteSetStoragePath. From then on no main document
 * points at the new-code blobs, and the next new-code fold only deletes the
 * paths the main document stores (the old fold's), so the new-code blobs
 * are never reclaimed: one full snapshot (plus its offloaded delete-set)
 * of billed Storage leaks per old-over-new fold.
 *
 * Contract asserted: after new fold -> old fold -> new fold on one
 * document, the objects under the document's Storage prefix are exactly
 * the ones the main document references (as between two clients on the
 * same release), and a fresh client still loads every edit. The same
 * holds around a current squash, whose snapshot
 * (`snapshot_e{E}_v{V}_{id}.bin`) is just as unknown to the previous
 * release: a squash after an old-release fold, and a fold after an
 * old-release fold over the squash, leave only what the main document
 * references.
 *
 * The previous release's sources are materialized from git (`git archive`)
 * into a temporary directory that links this checkout's node_modules, so
 * both compactors share one Firebase SDK and one Yjs and run against the
 * same emulator document. Both releases' `compact()` are called directly,
 * one after the other, so no timers or provider scheduling are involved.
 *
 * @file old_client_fold_blob_gc.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import * as Y from 'yjs';
import { collection, addDoc, getDoc, getDocs, doc, serverTimestamp, Bytes } from 'firebase/firestore';
import { FirebaseStorage, getBytes, listAll, ref } from 'firebase/storage';
import { compact, CompactionContext, CompactionResult } from '../../src/compaction';
import { FireProvider } from '../../src/provider';
import { squashDocument } from '../../src/squash';
import { setupEmulator } from '../utils/emulator';
import { waitFor, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** The last release before the current change set. */
const OLD_REVISION = '4297ea9';

/**
 * Extracts `src/` of OLD_REVISION into a temporary directory, links this
 * checkout's node_modules next to it and bundles its merge worker.
 */
function materializeOldSources(): string {
    const repoRoot = resolve(__dirname, '../..');
    const dir = mkdtempSync(join(tmpdir(), `y-cinder-${OLD_REVISION}-`));
    execFileSync('bash', ['-c',
        `git -C "${repoRoot}" archive ${OLD_REVISION} src scripts/bundle-worker.js | tar -x -C "${dir}"`,
    ]);
    symlinkSync(join(repoRoot, 'node_modules'), join(dir, 'node_modules'));
    execFileSync('node', [join(dir, 'scripts/bundle-worker.js')], { cwd: dir, stdio: 'ignore' });
    return dir;
}

describe('Fold blob GC across releases (old-release fold over a new-code snapshot)', () => {
    let app: any;
    let db: any;
    let storage: FirebaseStorage;
    let path: string;
    let counter = 0;
    let oldDir: string;
    let oldCompact: (ctx: CompactionContext) => Promise<CompactionResult>;

    beforeAll(async () => {
        oldDir = materializeOldSources();
        const oldModule = await import(/* @vite-ignore */ join(oldDir, 'src/compaction.ts'));
        oldCompact = oldModule.compact;
        // Guard: the old release really is a separate module.
        expect(oldCompact).toBeTypeOf('function');
        expect(oldCompact).not.toBe(compact);
    }, 60000);

    afterAll(() => {
        if (oldDir) rmSync(oldDir, { recursive: true, force: true });
    });

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
        path = `tests/old-client-fold-gc-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    /** Full paths of the Storage objects directly under the document's prefix. */
    async function listBlobs(): Promise<string[]> {
        const res = await listAll(ref(storage, path));
        return res.items.map(i => i.fullPath).sort();
    }

    async function readMain(): Promise<Record<string, any>> {
        return (await getDoc(doc(db, path))).data() ?? {};
    }

    /** Storage objects the main document currently points at. */
    async function referencedBlobs(): Promise<string[]> {
        const data = await readMain();
        return [data.snapshotStoragePath, data.deleteSetStoragePath, data.foldTailStoragePath]
            .filter((p): p is string => typeof p === 'string')
            .sort();
    }

    /**
     * Asserts that Storage holds exactly what the main document references.
     * A short grace period allows GC that is deferred after the commit.
     *
     * @returns The objects listed.
     */
    async function expectOnlyReferencedBlobs(message: string): Promise<string[]> {
        const expected = await referencedBlobs();
        let blobs: string[];
        try {
            blobs = await waitFor(
                listBlobs,
                (items) => JSON.stringify(items) === JSON.stringify(expected),
                { timeout: 5000, interval: 200, message: 'Storage matches main-doc references' },
            );
        } catch {
            blobs = await listBlobs();
        }
        expect(blobs, message).toEqual(expected);
        return blobs;
    }

    /** Pushes `ydoc`'s change from `edit` as an update document, as FireProvider writes one. */
    async function pushEdit(ydoc: Y.Doc, edit: (ydoc: Y.Doc) => void, epoch = 0): Promise<void> {
        const sv = Y.encodeStateVector(ydoc);
        edit(ydoc);
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(ydoc, sv)),
            createdAt: serverTimestamp(),
            createdBy: 'writer',
            ...(epoch > 0 ? { epoch } : {}),
        });
    }

    function ctxFor(uid: string): CompactionContext {
        return {
            db,
            path,
            uid,
            lockTTL: 60_000,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
            historyFoldThreshold: 1, // every compaction folds into the base snapshot
            maxDeleteSetFieldBytes: 0, // and offloads its delete-set to Storage
        };
    }

    /** Asserts that a fresh client loads exactly `expected` into map 'm'. */
    async function expectFreshClientLoads(expected: Record<string, string>): Promise<void> {
        const ydocC = new Y.Doc();
        const providerC = new FireProvider({ firebaseApp: app, ydoc: ydocC, path, maxUpdatesThreshold: 1000, maxWaitTime: 50 });
        try {
            await waitForConditionTruthy(() => providerC.synced, { timeout: 30000, message: 'fresh client synced' });
            expect(ydocC.getMap('m').toJSON()).toEqual(expected);
        } finally {
            await providerC.destroy();
            ydocC.destroy();
        }
    }

    it('reclaims the new-code snapshot and delete-set blobs an old-release fold replaced', { timeout: 120000 }, async () => {
        const writer = new Y.Doc();
        const pushUpdate = (key: string) => pushEdit(writer, d => d.getMap('m').set(key, key));
        const newCtx = ctxFor('new-client');
        const oldCtx = ctxFor('old-client');

        // 1. A device on the current code folds.
        await pushUpdate('a');
        expect((await compact(newCtx)).type).toBe('snapshot');
        const afterNew = await readMain();
        expect(afterNew.version).toBe(1);
        const newSnapshotBlob: string = afterNew.snapshotStoragePath;
        const newDeleteSetBlob: string = afterNew.deleteSetStoragePath;
        expect(newSnapshotBlob).toBeTruthy();
        expect(newDeleteSetBlob).toBeTruthy();

        // 2. A device still on the previous release folds over it.
        await pushUpdate('b');
        expect((await oldCompact(oldCtx)).type).toBe('snapshot');
        const afterOld = await readMain();
        expect(afterOld.version).toBe(2);
        expect(afterOld.snapshotStoragePath).not.toBe(newSnapshotBlob);
        // The previous release offloaded its delete-set too (maxDeleteSetFieldBytes 0).
        expect(afterOld.deleteSetStoragePath).toBe(`${path}/ds_v2.bin`);

        // 3. The current-code device folds again.
        await pushUpdate('c');
        expect((await compact(newCtx)).type).toBe('snapshot');
        const afterSecondNew = await readMain();
        expect(afterSecondNew.version).toBe(3);
        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);

        // Contract: every snapshot / delete-set blob a fold replaced is gone,
        // whichever release wrote or replaced it. Storage holds exactly what
        // the main document references. The orphans are expected gone after
        // the very next current-code fold, the first writer to replace the
        // old fold's snapshot: reclaiming them only at squash or later fails.
        const blobs = await expectOnlyReferencedBlobs(
            'blobs replaced by the old-release fold must not outlive the folds after it');
        expect(blobs).not.toContain(newSnapshotBlob);
        expect(blobs).not.toContain(newDeleteSetBlob);

        // GC removed only replaced blobs: a fresh client still loads every edit.
        try {
            await expectFreshClientLoads({ a: 'a', b: 'b', c: 'c' });
        } finally {
            writer.destroy();
        }
    });

    it('reclaims blobs around a squash on both sides of an old-release fold', { timeout: 120000 }, async () => {
        const writer = new Y.Doc();
        const newCtx = ctxFor('new-client');
        const oldCtx = ctxFor('old-client');

        // 1-2. New fold, then an old-release fold over it.
        await pushEdit(writer, d => d.getMap('m').set('a', 'a'));
        expect((await compact(newCtx)).type).toBe('snapshot');
        const afterNew = await readMain();
        await pushEdit(writer, d => d.getMap('m').set('b', 'b'));
        expect((await oldCompact(oldCtx)).type).toBe('snapshot');
        expect((await readMain()).version).toBe(2);

        // 3. A current squash replaces the old fold's snapshot: it reclaims
        // the new-code blobs that fold orphaned along with its own.
        const squashed = await squashDocument({
            db, path, uid: 'new-client', lockTTL: 60_000, storage, isDestroyed: () => false, doc: writer,
        });
        expect(squashed.error).toBeUndefined();
        expect(squashed.success).toBe(true);
        const afterSquash = await readMain();
        expect(afterSquash.epoch).toBe(1);
        const squashBlob: string = afterSquash.snapshotStoragePath;
        const blobsAfterSquash = await expectOnlyReferencedBlobs(
            'blobs an old-release fold orphaned must not outlive the squash after it');
        expect(blobsAfterSquash).toEqual([squashBlob]);
        expect(blobsAfterSquash).not.toContain(afterNew.snapshotStoragePath);
        writer.destroy();

        // An epoch-1 editor, hydrated from the squash snapshot.
        const editor = new Y.Doc();
        Y.applyUpdate(editor, new Uint8Array(await getBytes(ref(storage, squashBlob))));
        expect(editor.getMap('m').toJSON()).toEqual({ a: 'a', b: 'b' });

        // 4. An old-release fold over the squash snapshot...
        await pushEdit(editor, d => d.getMap('m').set('c', 'c'), 1);
        expect((await oldCompact(oldCtx)).type).toBe('snapshot');
        const afterOld = await readMain();
        expect(afterOld.version).toBe(4);
        expect(afterOld.snapshotStoragePath).not.toBe(squashBlob);

        // 5. ...and the next current fold reclaims the squash blob it orphaned.
        await pushEdit(editor, d => d.getMap('m').set('d', 'd'), 1);
        expect((await compact(newCtx)).type).toBe('snapshot');
        expect((await readMain()).version).toBe(5);
        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);
        const blobs = await expectOnlyReferencedBlobs(
            'a squash blob replaced by an old-release fold must not outlive the folds after it');
        expect(blobs).not.toContain(squashBlob);

        try {
            await expectFreshClientLoads({ a: 'a', b: 'b', c: 'c', d: 'd' });
        } finally {
            editor.destroy();
        }
    });
});
