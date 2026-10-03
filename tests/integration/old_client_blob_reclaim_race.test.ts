/**
 * Regression test (mixed deployment): a peer still running the PREVIOUS
 * release (4297ea9) whose update listener is mid-download when a client on
 * the current code compacts must not report a corrupted document.
 *
 * Compactors (fold, delta) and squash on the current code reclaim a
 * storage-backed update's blob (`large_updates/`) once the transaction
 * that deleted its pointer document has committed. Current listeners
 * tolerate a download racing that delete: on `storage/object-not-found`
 * they re-read the pointer and skip the update when it is gone
 * (listener_blob_reclaim_race.test.ts). Clients on the previous release
 * cannot be changed, and their update listener quarantines on ANY
 * download failure and emits 'corrupted-document'. Deleting the blob right
 * after the commit therefore made an old peer that received the pointer
 * just before a new-code fold (e.g. on a slow mobile link) report
 * corruption for a correctly compacted document, and an app that alerts
 * the user or resets the doc on that event acts on a healthy document.
 *
 * Contract asserted: the old peer reports no 'corrupted-document'; after
 * a fold it also converges (the update arrives through the fold or
 * through the blob, whichever the fix keeps available). After a squash it
 * is fenced into the old epoch ('epoch-changed'), which is not corruption
 * either.
 *
 * Holding the old peer's `getBytes` until the new client has consumed the
 * pointer widens a window that is narrow but real in production (a
 * listener callback delayed by a busy main thread, the SDK's retry
 * backoff after a transient error, a suspended tab), the same method as
 * listener_blob_reclaim_race.test.ts.
 *
 * The previous release's sources are materialized from git (`git archive`)
 * into a temporary directory that links this checkout's node_modules, so
 * both providers share one Firebase SDK, one Yjs and the Storage mock
 * below, and run against the same emulator document. This needs
 * OLD_REVISION in the local object store (not in a shallow clone).
 *
 * @file old_client_blob_reclaim_race.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach, afterAll, vi } from 'vitest';

const { gate } = vi.hoisted(() => ({
    gate: {
        /** Hold the next large_updates/ download until released. */
        armed: false,
        heldPath: null as string | null,
        release: null as (() => void) | null,
        /** How the held download ended: 'ok' or the Storage error code. */
        outcome: null as string | null,
    },
}));

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getBytes: async (storageRef: any, maxDownloadSizeBytes?: number) => {
            const fullPath: string = storageRef?.fullPath ?? String(storageRef);
            if (gate.armed && fullPath.includes('/large_updates/')) {
                gate.armed = false;
                gate.heldPath = fullPath;
                await new Promise<void>(resolve => { gate.release = resolve; });
                try {
                    const bytes = await actual.getBytes(storageRef, maxDownloadSizeBytes);
                    gate.outcome = 'ok';
                    return bytes;
                } catch (e: any) {
                    gate.outcome = e?.code ?? String(e);
                    throw e;
                }
            }
            return actual.getBytes(storageRef, maxDownloadSizeBytes);
        },
    };
});

import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import * as Y from 'yjs';
import { collection, getDocs } from '@firebase/firestore';
import { getStorage, getMetadata, ref } from '@firebase/storage';
import { FireProvider } from '../../src/provider';
import { squashDocument } from '../../src/squash';
import { FIRESTORE_PATHS } from '../../src/types';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** The last release before the current change set. */
const OLD_REVISION = '4297ea9';

/** ~1.1 MB once encoded: above INLINE_UPDATE_LIMIT, so offloaded. */
const BIG_CHARS = 1_100_000;

/**
 * Extracts `src/` of OLD_REVISION into a temporary directory, links this
 * checkout's node_modules next to it and bundles its merge worker.
 */
function materializeOldSources(): string {
    const repoRoot = resolve(__dirname, '../..');
    try {
        execFileSync('git', ['-C', repoRoot, 'cat-file', '-e', `${OLD_REVISION}^{commit}`], { stdio: 'ignore' });
    } catch {
        throw new Error(`${OLD_REVISION} (the previous release) is not in this checkout's history; fetch it (e.g. git fetch --unshallow) to run this test`);
    }
    const dir = mkdtempSync(join(tmpdir(), `y-cinder-${OLD_REVISION}-`));
    execFileSync('bash', ['-c',
        `git -C "${repoRoot}" archive ${OLD_REVISION} src scripts/bundle-worker.js | tar -x -C "${dir}"`,
    ]);
    symlinkSync(join(repoRoot, 'node_modules'), join(dir, 'node_modules'));
    execFileSync('node', [join(dir, 'scripts/bundle-worker.js')], { cwd: dir, stdio: 'ignore' });
    return dir;
}

describe('Old-release peer vs. post-commit blob reclaim by a new compactor', () => {
    let app: any;
    let db: any;
    let counter = 0;
    let oldDir: string;
    let OldFireProvider: any;

    beforeAll(async () => {
        oldDir = materializeOldSources();
        const oldModule = await import(/* @vite-ignore */ join(oldDir, 'src/provider.ts'));
        OldFireProvider = oldModule.FireProvider;
        // Guard: the old release really is a separate module.
        expect(OldFireProvider).toBeTypeOf('function');
        expect(OldFireProvider).not.toBe(FireProvider);
    }, 60000);

    afterAll(() => {
        if (oldDir) rmSync(oldDir, { recursive: true, force: true });
    });

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        await clearFirestore(db);
        gate.armed = false;
        gate.heldPath = null;
        gate.release = null;
        gate.outcome = null;
    });

    const providerConfig = (ydoc: Y.Doc, path: string) => ({
        firebaseApp: app,
        ydoc,
        path,
        maxWaitTime: 50,
        // Compaction runs only when the test asks for it.
        maxUpdatesThreshold: 1000,
    });

    /**
     * Holds the old peer's listener download of a new client's oversized
     * update, lets the new client consume the pointer (`consume`), then
     * releases the download.
     *
     * @returns The old peer's 'corrupted-document' reports.
     */
    async function raceOldListenerDownload(
        consume: (writer: FireProvider) => Promise<void>,
        afterRelease: (oldReaderDoc: Y.Doc, oldEpochChanges: number[]) => Promise<void>,
    ): Promise<string[]> {
        const path = `integration-tests/old-client-blob-reclaim-${getStableDate()}-${counter++}`;
        const writerDoc = new Y.Doc();
        const oldReaderDoc = new Y.Doc();
        // Current code: writes the oversized update and compacts it.
        const writer = new FireProvider(providerConfig(writerDoc, path));
        // Previous release: only reads.
        const oldReader = new OldFireProvider(providerConfig(oldReaderDoc, path));
        const corrupted: string[] = [];
        oldReader.on('corrupted-document', (event: { docId: string; error: Error }) => {
            corrupted.push(`${event.docId}: ${(event.error as any)?.code ?? event.error?.message}`);
        });
        const epochChanges: number[] = [];
        oldReader.on('epoch-changed', (event: { epoch: number }) => {
            epochChanges.push(event.epoch);
        });

        try {
            await waitForConditionTruthy(() => writer.synced && oldReader.synced, {
                timeout: 30000, message: 'both providers synced',
            });

            // The writer's oversized save; the old peer's listener download
            // of its blob is held (the writer skips its own pointer, so the
            // first large_updates/ download is the old peer's).
            gate.armed = true;
            const saved = new Promise<void>(resolve => writer.once('saved', () => resolve()));
            writerDoc.getText('t').insert(0, 'x'.repeat(BIG_CHARS));
            await saved;
            await waitForConditionTruthy(() => gate.release !== null, {
                timeout: 20000, message: 'old peer listener download held',
            });
            const heldPath = gate.heldPath!;

            // The new client consumes the pointer while the old peer is
            // still downloading its blob.
            await consume(writer);
            expect((await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES))).size).toBe(0);

            // Let any reclaim the new client performs land before the old
            // peer's download resumes (a fix that keeps the blob around for
            // slow readers simply runs out this wait).
            const storage = getStorage(app);
            const deadline = Date.now() + 3000;
            while (Date.now() < deadline) {
                try {
                    await getMetadata(ref(storage, heldPath));
                } catch (e: any) {
                    if (e?.code === 'storage/object-not-found') break;
                }
                await new Promise(r => setTimeout(r, 100));
            }

            gate.release!();
            await waitForConditionTruthy(() => gate.outcome !== null, {
                timeout: 20000, message: 'held download settled',
            });
            console.log(`old peer's held download of ${heldPath} ended with: ${gate.outcome}`);

            await afterRelease(oldReaderDoc, epochChanges);

            // Give a late quarantine decision time to land before returning
            // what was reported.
            await new Promise(resolve => setTimeout(resolve, 1000));
            return corrupted;
        } finally {
            gate.release?.();
            await writer.destroy();
            await oldReader.destroy();
            writerDoc.destroy();
            oldReaderDoc.destroy();
        }
    }

    it('an old-release peer downloading a blob that a new-code fold compacts converges without a corrupted-document', async () => {
        const corrupted = await raceOldListenerDownload(
            async (writer) => { await writer.compact(); },
            async (oldReaderDoc) => {
                // The old peer converges (through the folded snapshot, or the blob).
                await waitForConditionEquals(() => oldReaderDoc.getText('t').length, BIG_CHARS, {
                    timeout: 30000, interval: 100, message: 'old peer converges',
                });
                expect(oldReaderDoc.store.pendingStructs).toBeNull();
            },
        );
        expect(corrupted).toEqual([]);
    }, 120000);

    it('an old-release peer downloading a blob that a new-code squash consumes reports no corrupted-document', async () => {
        // A squash fences the old peer into the old epoch (it gets
        // 'epoch-changed' and the app rebuilds); its document is still not
        // corrupt.
        const corrupted = await raceOldListenerDownload(
            async (writer) => {
                const result = await writer.squash();
                expect(result.success).toBe(true);
            },
            async (_oldReaderDoc, oldEpochChanges) => {
                // Fenced: nothing more to converge, but it reached the fence.
                await waitForConditionTruthy(() => oldEpochChanges.length > 0, {
                    timeout: 20000, message: 'old peer fenced by the squash',
                });
            },
        );
        expect(corrupted).toEqual([]);
    }, 120000);

    it('an old-release peer downloading a blob whose pointer the squash transaction itself deletes reports no corrupted-document', async () => {
        // provider.squash() drains the pointer through its preparatory
        // compaction (the case above). One that lands after that cycle is
        // deleted by the squash transaction instead, as squashDocument
        // with the provider's deferral list does here.
        const corrupted = await raceOldListenerDownload(
            async (writer) => {
                const result = await squashDocument({
                    db, path: writer.path, uid: 'squasher', lockTTL: 60000,
                    storage: getStorage(app), isDestroyed: () => false,
                    doc: writer.doc, deferredUpdateBlobs: [],
                });
                expect(result.error).toBeUndefined();
                expect(result.success).toBe(true);
            },
            async (_oldReaderDoc, oldEpochChanges) => {
                await waitForConditionTruthy(() => oldEpochChanges.length > 0, {
                    timeout: 20000, message: 'old peer fenced by the squash',
                });
            },
        );
        expect(corrupted).toEqual([]);
    }, 120000);
});
