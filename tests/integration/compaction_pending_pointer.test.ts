/**
 * Regression test: a client's compaction must not consume (and reclaim the
 * blob of) its OWN storage-backed update whose pointer document is still an
 * unacknowledged local write.
 *
 * An oversized save uploads its payload to `large_updates/` first, then
 * queues the pointer `addDoc`. Until the server acknowledges that write,
 * `getDocs` on the same Firestore instance returns the pointer with
 * `metadata.hasPendingWrites` (latency compensation), or serves the whole
 * query from cache while the SDK's streams are down — while unary RPCs
 * (the lock and commit transactions, Cloud Storage) can still succeed.
 * A compaction running in that window lists the pending pointer,
 * downloads its (existing) blob and folds it; the commit deletes the ref
 * blind (a no-op on the server: the document does not exist there yet),
 * and the post-commit blob reclaim then deletes the blob. When the queued
 * `addDoc` finally reaches the server it commits a pointer to a missing
 * object. Every later compaction on every client reads that pointer
 * first, aborts with storage/object-not-found and counts a persistent
 * failure ('compaction-failed', backoff growing to 30 min), so compaction
 * and squash stop for the document for good.
 *
 * "Write stream down, unary RPCs up" is simulated with the SDK's own
 * disableNetwork() on the writer's Firestore instance: writes queue
 * locally, reads are served from cache, transactions and Storage still
 * reach the emulator. Each device has its own Firebase app (own Firestore
 * client and cache). Ordering is explicit: the writer's compaction runs
 * (and settles) while its pointer is provably pending, and only then is
 * the network re-enabled and the pointer allowed to commit.
 *
 * Contract asserted (valid for any reasonable fix: skipping pending /
 * cache-served documents, reclaiming only refs verified at commit, or
 * healing a pointer whose blob is gone): once the oversized save has
 * committed, another device converges on the content, its compaction
 * does not fail, and afterwards no pointer document on the server
 * references a missing blob.
 *
 * Run alone (through the emulator isolation wrapper):
 *   isolated.sh bash scripts/test.sh tests/integration/compaction_pending_pointer.test.ts
 *
 * @file compaction_pending_pointer.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import * as Y from 'yjs';
import { initializeApp, getApps, FirebaseApp } from 'firebase/app';
import {
    getFirestore,
    connectFirestoreEmulator,
    disableNetwork,
    enableNetwork,
    collection,
    getDocs,
} from 'firebase/firestore';
import { getStorage, connectStorageEmulator, ref, getMetadata } from 'firebase/storage';
import { FireProvider } from '../../src/provider';
import { FIRESTORE_PATHS } from '../../src/types';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** ~1.1 MB once encoded: above INLINE_UPDATE_LIMIT, so the save is offloaded to Storage. */
const BIG_CHARS = 1_100_000;

/** A Firebase app of its own per device: own Firestore client, own cache. */
function deviceApp(name: string): FirebaseApp {
    const existing = getApps().find((app) => app.name === name);
    if (existing) return existing;
    const app = initializeApp({
        projectId: 'demo-test-project',
        apiKey: 'fake-api-key',
        storageBucket: 'demo-test-project.appspot.com',
    }, name);
    connectFirestoreEmulator(getFirestore(app), '127.0.0.1', 8080);
    connectStorageEmulator(getStorage(app), '127.0.0.1', 9199);
    return app;
}

/** Ids of the pointer documents on the server whose Storage blob does not exist. */
async function danglingPointers(app: FirebaseApp, path: string): Promise<string[]> {
    const snap = await getDocs(collection(getFirestore(app), path, FIRESTORE_PATHS.UPDATES));
    const dangling: string[] = [];
    // Sequential: the Storage emulator occasionally drops one of several
    // parallel metadata requests.
    for (const d of snap.docs) {
        const blobPath = d.data().updateStoragePath;
        if (typeof blobPath !== 'string') continue;
        try {
            await getMetadata(ref(getStorage(app), blobPath));
        } catch (e: any) {
            dangling.push(`${d.id} -> ${blobPath} (${e?.code ?? e})`);
        }
    }
    return dangling;
}

describe('Compaction vs. this client\'s own unacknowledged large-update pointer', () => {
    let counter = 0;

    beforeEach(async () => {
        const { db } = await setupEmulator();
        await clearFirestore(db);
    });

    it('a pending pointer compacted by its own client still commits with a readable blob, and other devices keep compacting', async () => {
        const run = `${getStableDate()}-${counter++}`;
        const path = `integration-tests/compaction-pending-pointer-${run}`;
        const appX = deviceApp(`pending-pointer-x-${run}`);
        const appY = deviceApp(`pending-pointer-y-${run}`);
        const dbX = getFirestore(appX);
        // Compaction runs only when the test asks for it.
        const config = { maxUpdatesThreshold: 1000, maxWaitTime: 20 };

        const docX = new Y.Doc();
        const writer = new FireProvider({ firebaseApp: appX, ydoc: docX, path, ...config });
        let reader: FireProvider | null = null;
        let networkDisabled = false;

        try {
            await waitForConditionTruthy(() => writer.synced, { timeout: 30000, message: 'writer synced' });

            // A base snapshot, and the clock offset measured while online
            // (the probe needs its write acknowledged).
            const firstSaved = new Promise<void>(resolve => writer.once('saved', () => resolve()));
            docX.getMap('m').set('a', 1);
            await firstSaved;
            await writer.compact();

            // The writer's write stream goes down; unary RPCs still work.
            // disableNetwork() is the public SDK API (apps use it for
            // offline modes); it stops only the watch and write streams,
            // while transactions talk to the backend directly by design,
            // as in browsers. Not an emulator quirk.
            await disableNetwork(dbX);
            networkDisabled = true;

            let bigSaved = false;
            writer.on('saved', () => { bigSaved = true; });
            docX.getMap('m').set('big', 'B'.repeat(BIG_CHARS));

            // The blob is uploaded and the pointer write is queued locally:
            // the writer's own reads now return it as a pending write.
            await waitForConditionTruthy(async () => {
                const snap = await getDocs(collection(dbX, path, FIRESTORE_PATHS.UPDATES));
                return snap.docs.some(d => typeof d.data().updateStoragePath === 'string' && d.metadata.hasPendingWrites);
            }, { timeout: 30000, message: 'pointer pending in the writer\'s local view' });

            // The writer compacts while its pointer is unacknowledged.
            await writer.compact();
            expect(bigSaved, 'precondition: the pointer must still be uncommitted while the writer compacts').toBe(false);

            // The write stream recovers and the queued pointer commits.
            await enableNetwork(dbX);
            networkDisabled = false;
            await waitForConditionTruthy(() => bigSaved, { timeout: 30000, message: 'oversized save committed' });

            // Another device opens the document and compacts it.
            const docY = new Y.Doc();
            reader = new FireProvider({ firebaseApp: appY, ydoc: docY, path, ...config });
            const compactionFailures: string[] = [];
            const corrupted: string[] = [];
            reader.on('compaction-failed', (event: { error: any }) => {
                compactionFailures.push(event.error?.code ?? String(event.error?.message ?? event.error));
            });
            reader.on('corrupted-document', (event: { docId: string; error: any }) => {
                corrupted.push(`${event.docId}: ${event.error?.code ?? event.error?.message}`);
            });
            const reader_ = reader;
            await waitForConditionTruthy(() => reader_.synced, { timeout: 30000, message: 'reader synced' });
            await reader.compact();

            const big = docY.getMap('m').get('big');
            expect({
                readerBigChars: typeof big === 'string' ? big.length : big,
                compactionFailures,
                corrupted,
                danglingPointers: await danglingPointers(appY, path),
            }).toEqual({
                readerBigChars: BIG_CHARS,
                compactionFailures: [],
                corrupted: [],
                danglingPointers: [],
            });
        } finally {
            if (networkDisabled) await enableNetwork(dbX);
            await writer.destroy();
            if (reader) await reader.destroy();
        }
    }, 120_000);
});
