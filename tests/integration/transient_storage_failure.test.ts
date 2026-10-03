/**
 * Regression test: a transient Cloud Storage failure must not permanently
 * drop a storage-backed update.
 *
 * Bug: oversized updates are offloaded to Cloud Storage (`large_updates/`)
 * and the update document only carries `updateStoragePath`. When the
 * reader's `getBytes` for that blob fails once with an error that a later
 * attempt could get past — `storage/retry-limit-exceeded` after an outage
 * outlasting the SDK's own retries, an expired token — the update is lost
 * for the rest of the session:
 *
 * - Initial sync: the document is skipped with `continue`, the sync still
 *   reports success and the listener cursor moves past it, so nothing ever
 *   re-fetches it.
 * - Live listener: ANY download failure quarantines the document id
 *   (`corruptedDocIds`) and emits 'corrupted-document', so it is skipped
 *   forever — even though nothing is wrong with its content.
 *
 * Update documents are immutable, so neither path is ever re-delivered; the
 * author's later (inline) updates depend on the lost one and stay parked as
 * pending structs. The reader stays diverged until reload. (By contrast, a
 * failed snapshot download in initial sync rethrows and the sync retries.)
 *
 * Contract asserted here: after a single transient download failure, the
 * reader still converges to the writer's content, and a network error is
 * not reported as a corrupted document. The companion tests pin the other
 * side: failures that can never succeed — a blob that is gone, a blob that
 * does not decode — are still skipped/quarantined instead of retried.
 *
 * @file transient_storage_failure.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { mockControls } = vi.hoisted(() => ({
    mockControls: {
        /** storage fullPath -> number of upcoming getBytes calls to fail */
        failuresByPath: new Map<string, number>(),
        /** fullPaths for which a failure was actually injected */
        injected: [] as string[],
    },
}));

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getBytes: async (storageRef: any, maxDownloadSizeBytes?: number) => {
            const fullPath: string = storageRef?.fullPath ?? String(storageRef);
            const remaining = mockControls.failuresByPath.get(fullPath) ?? 0;
            if (remaining > 0) {
                mockControls.failuresByPath.set(fullPath, remaining - 1);
                mockControls.injected.push(fullPath);
                // Exactly what the Storage SDK surfaces once its own internal
                // retries on 5xx / 429 / network errors are exhausted — i.e.
                // after maxOperationRetryTime (2 minutes by default) of
                // failing, not a sub-second blip. Simulated instantly here.
                throw new actual.StorageError(
                    actual.StorageErrorCode.RETRY_LIMIT_EXCEEDED,
                    'Max retry time for operation exceeded, please try again.',
                    503,
                );
            }
            return actual.getBytes(storageRef, maxDownloadSizeBytes);
        },
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { addDoc, collection, serverTimestamp, Bytes } from '@firebase/firestore';
import { ref, uploadBytes } from '@firebase/storage';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';
import { FIRESTORE_PATHS } from '../../src/types';
import { largeUpdatePath } from '../../src/sync-policy';
import { aggregateClockEnds, extractClockEnds } from '../../src/update-metadata';

const WRITER = 'writer-A';

const describeReader = (ydoc: Y.Doc) =>
    `reader text=${JSON.stringify(ydoc.getText('t').toString())}, ` +
    `pendingStructs=${ydoc.store.pendingStructs ? 'yes (writer updates parked on a missing dependency)' : 'none'}`;
const BIG = 'big-offloaded-content';
const MORE = '+more';

describe('Transient Cloud Storage failure on a storage-backed update', () => {
    let app: any;
    let db: any;
    let storage: any;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
        await clearFirestore(db);
        mockControls.failuresByPath.clear();
        mockControls.injected = [];
    });

    const createReader = (ydoc: Y.Doc, path: string) => new FireProvider({
        firebaseApp: app,
        ydoc,
        path,
        maxWaitTime: 50,
        maxUpdatesThreshold: 1000, // keep compaction out of the picture
    });

    /**
     * Writes the writer's first change the way the provider writes an
     * oversized update: blob uploaded to large_updates/, then a pointer
     * document carrying only `updateStoragePath` + clock metadata.
     */
    const writeStorageBackedUpdate = async (path: string, storagePath: string, update: Uint8Array) => {
        await uploadBytes(ref(storage, storagePath), update);
        await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
            updateStoragePath: storagePath,
            createdAt: serverTimestamp(),
            createdBy: WRITER,
            ...aggregateClockEnds(extractClockEnds(update)),
        });
    };

    /** A regular inline update document (the writer's follow-up edit). */
    const writeInlineUpdate = async (path: string, update: Uint8Array) => {
        await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
            update: Bytes.fromUint8Array(update),
            createdAt: serverTimestamp(),
            createdBy: WRITER,
            ...aggregateClockEnds(extractClockEnds(update)),
        });
    };

    const recordCorrupted = (provider: FireProvider) => {
        const corrupted: { docId: string; error: Error }[] = [];
        provider.on('corrupted-document', (event: { docId: string; error: Error }) => {
            corrupted.push(event);
        });
        return corrupted;
    };

    /** A pointer document for a blob that was never uploaded. */
    const writeDanglingPointer = async (path: string, storagePath: string, update: Uint8Array) => {
        const pointer = await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
            updateStoragePath: storagePath,
            createdAt: serverTimestamp(),
            createdBy: 'writer-gone',
            ...aggregateClockEnds(extractClockEnds(update)),
        });
        return pointer.id;
    };

    /** An independent edit by another writer (depends on nothing else). */
    const makeIndependentUpdate = (content: string) => {
        const d = new Y.Doc();
        d.getText('t').insert(0, content);
        return Y.encodeStateAsUpdate(d);
    };

    /** Writer edits: u1 (offloaded to Storage), then u2 which depends on u1. */
    const makeWriterUpdates = () => {
        const writerDoc = new Y.Doc();
        const text = writerDoc.getText('t');
        text.insert(0, BIG);
        const u1 = Y.encodeStateAsUpdate(writerDoc);
        const svAfterU1 = Y.encodeStateVector(writerDoc);
        text.insert(text.length, MORE);
        const u2 = Y.encodeStateAsUpdate(writerDoc, svAfterU1);
        return { u1, u2, expected: text.toString() };
    };

    it('initial sync: a storage-backed update whose download fails once is still applied', async () => {
        const path = `integration-tests/transient-storage-initial-${getStableDate()}-${counter++}`;
        const storagePath = largeUpdatePath(path, WRITER, Date.now());
        const { u1, u2, expected } = makeWriterUpdates();
        expect(expected).toBe(BIG + MORE);

        // Both updates are on the server before the reader connects
        await writeStorageBackedUpdate(path, storagePath, u1);
        await writeInlineUpdate(path, u2);

        // The reader's FIRST download of the blob fails transiently; every
        // later attempt succeeds.
        mockControls.failuresByPath.set(storagePath, 1);

        const readerDoc = new Y.Doc();
        const reader = createReader(readerDoc, path);
        const corrupted = recordCorrupted(reader);
        try {
            await waitForConditionTruthy(() => reader.synced, {
                timeout: 30000, interval: 50, message: 'reader should complete initial sync',
            });
            // Sanity: the failure was really injected on the reader's download
            expect(mockControls.injected).toEqual([storagePath]);

            // The reader must converge to the writer's content. Buggy code
            // skipped u1 for good, so u2 stays parked and the text stays ''.
            await waitForConditionEquals(() => readerDoc.getText('t').toString(), expected, {
                timeout: 10000, interval: 100,
                message: 'reader should apply the storage-backed update after a transient download failure',
                onFailure: () => describeReader(readerDoc),
            });
            expect(readerDoc.store.pendingStructs).toBeNull();

            // A network error is not a corrupted document
            expect(corrupted.map(e => e.docId)).toEqual([]);
        } finally {
            await reader.destroy();
        }
    }, 60000);

    it('live listener: a storage-backed update whose download fails once is still applied and not quarantined', async () => {
        const path = `integration-tests/transient-storage-live-${getStableDate()}-${counter++}`;
        const storagePath = largeUpdatePath(path, WRITER, Date.now());
        const { u1, u2, expected } = makeWriterUpdates();

        const readerDoc = new Y.Doc();
        const reader = createReader(readerDoc, path);
        const corrupted = recordCorrupted(reader);

        try {
            await waitForConditionTruthy(() => reader.synced, {
                timeout: 30000, interval: 50, message: 'reader should complete initial sync',
            });

            // The listener's FIRST download of the blob fails transiently;
            // every later attempt succeeds.
            mockControls.failuresByPath.set(storagePath, 1);

            // The writer publishes u1 (offloaded) and then the dependent u2
            await writeStorageBackedUpdate(path, storagePath, u1);
            await waitForConditionEquals(() => mockControls.injected.length, 1, {
                timeout: 10000, interval: 20,
                message: 'reader listener should attempt (and transiently fail) the blob download',
            });
            await writeInlineUpdate(path, u2);

            // The reader must converge to the writer's content. Buggy code
            // quarantined u1 for the session, so u2 stays parked and the
            // text stays ''.
            await waitForConditionEquals(() => readerDoc.getText('t').toString(), expected, {
                timeout: 10000, interval: 100,
                message: 'reader should apply the storage-backed update after a transient download failure',
                onFailure: () => describeReader(readerDoc),
            });
            expect(readerDoc.store.pendingStructs).toBeNull();

            // A network error is not a corrupted document
            expect(corrupted.map(e => e.docId)).toEqual([]);
        } finally {
            await reader.destroy();
        }
    }, 60000);

    it('initial sync: a storage-backed update whose blob is gone is skipped without failing the sync', async () => {
        const path = `integration-tests/transient-storage-missing-initial-${getStableDate()}-${counter++}`;
        // No retry can fetch this blob: rethrowing would burn every sync
        // retry and end in sync-failure, so it must still be skipped.
        await writeDanglingPointer(path, largeUpdatePath(path, 'writer-gone', Date.now()), makeIndependentUpdate('lost'));
        await writeInlineUpdate(path, makeIndependentUpdate('kept'));

        const readerDoc = new Y.Doc();
        const reader = createReader(readerDoc, path);
        const failures: unknown[] = [];
        reader.on('sync-failure', (e: unknown) => failures.push(e));
        try {
            await waitForConditionTruthy(() => reader.synced, {
                timeout: 15000, interval: 50, message: 'reader should complete initial sync despite the missing blob',
            });
            expect(readerDoc.getText('t').toString()).toBe('kept');
            expect(failures).toEqual([]);
            expect(mockControls.injected).toEqual([]);
        } finally {
            await reader.destroy();
        }
    }, 60000);

    it('live listener: a storage-backed update whose blob is gone is quarantined, not retried forever', async () => {
        const path = `integration-tests/transient-storage-missing-live-${getStableDate()}-${counter++}`;
        const readerDoc = new Y.Doc();
        const reader = createReader(readerDoc, path);
        const corrupted = recordCorrupted(reader);
        try {
            await waitForConditionTruthy(() => reader.synced, {
                timeout: 30000, interval: 50, message: 'reader should complete initial sync',
            });

            const docId = await writeDanglingPointer(path, largeUpdatePath(path, 'writer-gone', Date.now()), makeIndependentUpdate('lost'));
            await waitForConditionEquals(() => corrupted.map(e => e.docId).join(','), docId, {
                timeout: 10000, interval: 50,
                message: 'a pointer to a blob that is gone should be quarantined',
            });
            expect(mockControls.injected).toEqual([]);
        } finally {
            await reader.destroy();
        }
    }, 60000);

    it('live listener: a storage-backed update whose blob does not decode is still quarantined', async () => {
        const path = `integration-tests/transient-storage-garbage-live-${getStableDate()}-${counter++}`;
        const readerDoc = new Y.Doc();
        const reader = createReader(readerDoc, path);
        const corrupted = recordCorrupted(reader);
        try {
            await waitForConditionTruthy(() => reader.synced, {
                timeout: 30000, interval: 50, message: 'reader should complete initial sync',
            });

            // The blob downloads fine but is not a Yjs update
            const storagePath = largeUpdatePath(path, WRITER, Date.now());
            await uploadBytes(ref(storage, storagePath), new Uint8Array([255, 254, 253, 252, 251, 250, 249, 248]));
            const poison = await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
                updateStoragePath: storagePath,
                createdAt: serverTimestamp(),
                createdBy: WRITER,
            });

            await waitForConditionEquals(() => corrupted.map(e => e.docId).join(','), poison.id, {
                timeout: 10000, interval: 50,
                message: 'a storage-backed update that fails to apply should be quarantined',
            });

            // Sync carries on past the quarantined document
            await writeInlineUpdate(path, makeIndependentUpdate('after'));
            await waitForConditionEquals(() => readerDoc.getText('t').toString(), 'after', {
                timeout: 10000, interval: 100, message: 'later updates should still apply',
            });
            expect(mockControls.injected).toEqual([]);
        } finally {
            await reader.destroy();
        }
    }, 60000);
});
