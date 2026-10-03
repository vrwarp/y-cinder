/**
 * Regression: a compaction whose main-document read was served from a
 * stale cache must not report 'compaction-failed' when the base snapshot it
 * read about has already been replaced (and garbage-collected) by another
 * device's fold.
 *
 * Bug: compact() reads the main document with getDoc, which the SDK serves
 * from its local cache whenever it considers itself offline (its watch
 * stream is down), even though unary RPCs such as the lock transaction and
 * Cloud Storage may still reach their servers. If another device folded
 * meanwhile, the cached main document still names the snapshot that fold
 * replaced and deleted, so the fold path's base download fails with
 * `storage/object-not-found`. That happens before the commit transaction's
 * version check, which would have reported the race as "Document version
 * changed". isPersistentCompactionFailure treats object-not-found as a
 * failure retrying cannot fix, so the provider emits 'compaction-failed'
 * (README: "fails in a way retrying will not fix") and pauses automatic
 * compaction, although the document is fine: another client compacted it.
 *
 * Contract pinned here: when the base snapshot is missing only because the
 * compactor's view of the main document is stale (another device's fold
 * superseded it), compaction is treated like a version race: no
 * 'compaction-failed' event. (A fix may detect the cached read, read the
 * main document from the server, re-check the version after a failed base
 * download, or classify the miss as a race; all keep this test green.)
 *
 * Model (two devices, each with its own Firebase app and Firestore cache):
 *  1. Device A folds the document into snapshot v1 (Cloud Storage).
 *  2. Device B syncs; its cache holds the v1 main document and one more
 *     pending update from A.
 *  3. B's Firestore goes offline (`disableNetwork`: the watch stream stops,
 *     so its cache stays at v1; the SDK serves reads from cache). Unary
 *     RPCs (transactions) and Cloud Storage still reach the emulator,
 *     which is the "Firestore offline right after the lock, Storage
 *     reachable" condition from the field.
 *  4. A folds the pending update into v2 and garbage-collects v1's blob.
 *  5. B compacts: it takes the (free) lock from the server, but reads the
 *     main document from its cache and tries to download v1's blob.
 * Every step is awaited; nothing depends on timing.
 *
 * The sibling case: a storage-backed update document B read from its cache
 * after A's fold deleted it and reclaimed its blob fails the download the
 * same way, before the base is even fetched (second test).
 *
 * Run (needs the Firestore + Storage emulator, through the isolation
 * wrapper on shared machines):
 *   bash scripts/test.sh tests/integration/stale_base_read_compaction_failed.test.ts
 *
 * @file stale_base_read_compaction_failed.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as Y from 'yjs';
import { initializeApp, getApps, FirebaseApp } from 'firebase/app';
import {
    getFirestore,
    connectFirestoreEmulator,
    collection,
    getDocs,
    getDoc,
    doc,
    disableNetwork,
    enableNetwork,
    Firestore,
} from 'firebase/firestore';
import { getStorage, connectStorageEmulator, ref, getBytes } from 'firebase/storage';
import { FireProvider } from '../../src/provider';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

type CompactionFailedEvent = { error: Error; consecutiveFailures: number; retryInMs: number };

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

describe('compaction after a stale main-document read', () => {
    let db: Firestore;
    let path: string;
    let counter = 0;

    const live: FireProvider[] = [];
    let offlineDb: Firestore | null = null;

    beforeEach(async () => {
        const setup = await setupEmulator();
        db = setup.db;
        path = `tests/stale-base-read-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    afterEach(async () => {
        if (offlineDb) {
            await enableNetwork(offlineDb);
            offlineDb = null;
        }
        while (live.length > 0) {
            await live.pop()!.destroy();
        }
    }, 30_000);

    function device(appName: string, ydoc: Y.Doc): FireProvider {
        const provider = new FireProvider({
            firebaseApp: deviceApp(appName),
            ydoc,
            path,
            maxUpdatesThreshold: 1000, // compactions are started explicitly
            maxWaitTime: 50,
            historyFoldThreshold: 1, // every compaction folds into the base snapshot
            // As if measured earlier: the clock-skew probe writes, and B
            // compacts while its writes cannot be acknowledged.
            cachedClockOffset: 0,
        });
        live.push(provider);
        return provider;
    }

    async function serverMainDoc(): Promise<Record<string, any>> {
        const snap = await getDoc(doc(db, path));
        expect(snap.exists()).toBe(true);
        return snap.data()!;
    }

    async function waitForServerUpdates(count: number, message: string): Promise<void> {
        await waitForConditionTruthy(
            async () => (await getDocs(collection(db, path, 'updates'))).size >= count,
            { timeout: 20_000, message }
        );
    }

    it('does not report compaction-failed when another device\'s fold replaced the base snapshot it read from cache', { timeout: 120_000 }, async () => {
        // 1. Device A folds the document into a Storage-backed snapshot v1.
        const ydocA = new Y.Doc();
        const deviceA = device('stale-base-read-A', ydocA);
        await waitForConditionTruthy(() => deviceA.synced, { timeout: 30_000, message: 'device A synced' });
        ydocA.getMap('m').set('first', 1);
        await waitForServerUpdates(1, 'first edit persisted');
        await deviceA.compact();
        const v1 = await serverMainDoc();
        expect(v1.version).toBe(1);
        expect(typeof v1.snapshotStoragePath).toBe('string');

        // 2. Device B syncs, then receives A's next (pending) edit.
        const ydocB = new Y.Doc();
        const deviceB = device('stale-base-read-B', ydocB);
        const failures: CompactionFailedEvent[] = [];
        deviceB.on('compaction-failed', (event: CompactionFailedEvent) => { failures.push(event); });
        await waitForConditionTruthy(() => deviceB.synced, { timeout: 30_000, message: 'device B synced' });
        ydocA.getMap('m').set('second', 2);
        await waitForServerUpdates(1, 'second edit persisted');
        await waitForConditionTruthy(() => ydocB.getMap('m').get('second') === 2, {
            timeout: 20_000,
            message: 'device B received the pending edit',
        });

        // 3. B's Firestore goes offline: its cache stays at v1.
        offlineDb = getFirestore(deviceApp('stale-base-read-B'));
        await disableNetwork(offlineDb);

        // 4. A folds into v2 and garbage-collects v1's snapshot blob.
        await deviceA.compact();
        const v2 = await serverMainDoc();
        expect(v2.version).toBe(2);
        expect(v2.snapshotStoragePath).not.toBe(v1.snapshotStoragePath);
        await expect(getBytes(ref(getStorage(), v1.snapshotStoragePath))).rejects.toMatchObject({
            code: 'storage/object-not-found',
        });

        // 5. B compacts with its stale view of the main document. Another
        // device compacting is progress, not a document retrying cannot fix.
        await deviceB.compact();
        expect(
            failures.map(f => `${f.error.message} (consecutive failures: ${f.consecutiveFailures}, paused ${f.retryInMs} ms)`),
            'compaction-failed must not fire for a base snapshot another device\'s fold replaced'
        ).toEqual([]);
        // B's stale cycle committed nothing over A's fold.
        const after = await serverMainDoc();
        expect(after.version).toBe(2);
        expect(after.snapshotStoragePath).toBe(v2.snapshotStoragePath);
    });

    it('does not report compaction-failed when another device\'s fold reclaimed a storage-backed update it read from cache', { timeout: 120_000 }, async () => {
        // 1. Device A folds the document into snapshot v1, so B's cache
        // holds a main document to read while offline.
        const ydocA = new Y.Doc();
        const deviceA = device('stale-update-read-A', ydocA);
        await waitForConditionTruthy(() => deviceA.synced, { timeout: 30_000, message: 'device A synced' });
        ydocA.getMap('m').set('first', 1);
        await waitForServerUpdates(1, 'first edit persisted');
        await deviceA.compact();
        expect((await serverMainDoc()).version).toBe(1);

        // 2. Device B syncs, then receives A's next edit, large enough to
        // be stored in Cloud Storage behind a pointer document.
        const ydocB = new Y.Doc();
        const deviceB = device('stale-update-read-B', ydocB);
        const failures: CompactionFailedEvent[] = [];
        deviceB.on('compaction-failed', (event: CompactionFailedEvent) => { failures.push(event); });
        await waitForConditionTruthy(() => deviceB.synced, { timeout: 30_000, message: 'device B synced' });
        const BIG_CHARS = 1_100_000; // ~1.1 MB encoded, above INLINE_UPDATE_LIMIT
        ydocA.getText('t').insert(0, 'x'.repeat(BIG_CHARS));
        await waitForServerUpdates(1, 'large edit persisted');
        const pointers = (await getDocs(collection(db, path, 'updates'))).docs.map(d => d.data());
        expect(pointers).toHaveLength(1);
        const blobPath: string = pointers[0].updateStoragePath;
        expect(typeof blobPath).toBe('string');
        await waitForConditionTruthy(() => ydocB.getText('t').length === BIG_CHARS, {
            timeout: 30_000,
            message: 'device B received the large edit',
        });

        // 3. B's Firestore goes offline: its cache keeps the pointer.
        offlineDb = getFirestore(deviceApp('stale-update-read-B'));
        await disableNetwork(offlineDb);

        // 4. A folds the large edit into v2, deleting the pointer. Its blob
        // is reclaimed by A's next cycle (older releases may still be
        // downloading it right after the fold).
        await deviceA.compact();
        await deviceA.compact();
        const v2 = await serverMainDoc();
        expect(v2.version).toBe(2);
        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);
        await expect(getBytes(ref(getStorage(), blobPath))).rejects.toMatchObject({
            code: 'storage/object-not-found',
        });

        // 5. B compacts and downloads the reclaimed blob its cache names.
        await deviceB.compact();
        expect(
            failures.map(f => `${f.error.message} (consecutive failures: ${f.consecutiveFailures}, paused ${f.retryInMs} ms)`),
            'compaction-failed must not fire for an update another device\'s fold reclaimed'
        ).toEqual([]);
        const after = await serverMainDoc();
        expect(after.version).toBe(2);
        expect(after.snapshotStoragePath).toBe(v2.snapshotStoragePath);
    });
});
