/**
 * Regression test: a peer whose update listener is mid-download when
 * compaction reclaims the blob must not report a corrupted document.
 *
 * Compaction deletes a storage-backed update's blob (`large_updates/`)
 * right after the transaction that deleted its pointer document commits:
 * nothing references the blob any more, and before that change every
 * oversized save stayed in billed Storage forever. A peer whose listener
 * received the pointer just before the fold can still be downloading the
 * blob when it disappears, and `getBytes` then fails with
 * `storage/object-not-found` — the same error as a blob missing behind a
 * live pointer, which the listener quarantines and reports as
 * 'corrupted-document'.
 *
 * Here nothing is corrupt: the fold that consumed the pointer holds the
 * update and reaches the peer through its snapshot listener. Contract
 * asserted: no 'corrupted-document' event, and the peer converges.
 * (transient_storage_failure.test.ts pins the other side: a blob missing
 * behind a pointer that still exists is still quarantined.)
 *
 * @file listener_blob_reclaim_race.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

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

import * as Y from 'yjs';
import { collection, getDocs } from '@firebase/firestore';
import { FireProvider } from '../../src/provider';
import { FIRESTORE_PATHS } from '../../src/types';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** ~1.1 MB once encoded: above INLINE_UPDATE_LIMIT, so offloaded. */
const BIG_CHARS = 1_100_000;

describe('Update listener vs. post-commit blob reclaim', () => {
    let app: any;
    let db: any;
    let counter = 0;

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

    const createProvider = (ydoc: Y.Doc, path: string) => new FireProvider({
        firebaseApp: app,
        ydoc,
        path,
        maxWaitTime: 50,
        // Compaction runs only when the test asks for it.
        maxUpdatesThreshold: 1000,
    });

    it('a peer downloading a blob that a fold reclaims converges without a corrupted-document', async () => {
        const path = `integration-tests/listener-blob-reclaim-${getStableDate()}-${counter++}`;
        const writerDoc = new Y.Doc();
        const readerDoc = new Y.Doc();
        const writer = createProvider(writerDoc, path);
        const reader = createProvider(readerDoc, path);
        const corrupted: string[] = [];
        reader.on('corrupted-document', (event: { docId: string; error: Error }) => {
            corrupted.push(`${event.docId}: ${(event.error as any)?.code ?? event.error?.message}`);
        });

        try {
            await waitForConditionTruthy(() => writer.synced && reader.synced, {
                timeout: 30000, message: 'both providers synced',
            });

            // The writer's oversized save; the reader's listener download of
            // its blob is held (the writer skips its own pointer, so the
            // first large_updates/ download is the reader's).
            gate.armed = true;
            const saved = new Promise<void>(resolve => writer.once('saved', () => resolve()));
            writerDoc.getText('t').insert(0, 'x'.repeat(BIG_CHARS));
            await saved;
            await waitForConditionTruthy(() => gate.release !== null, {
                timeout: 20000, message: 'reader listener download held',
            });

            // The fold consumes the pointer and reclaims its blob while the
            // reader is still downloading it.
            await writer.compact();
            expect((await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES))).size).toBe(0);

            gate.release!();
            await waitForConditionTruthy(() => gate.outcome !== null, {
                timeout: 20000, message: 'held download settled',
            });
            // Precondition: the download really raced the reclaim.
            expect(gate.outcome).toBe('storage/object-not-found');

            // The fold delivers the update through the snapshot listener.
            await waitForConditionEquals(() => readerDoc.getText('t').length, BIG_CHARS, {
                timeout: 30000, interval: 100, message: 'reader converges through the folded snapshot',
            });
            expect(readerDoc.store.pendingStructs).toBeNull();

            // Give a late quarantine decision (it awaits a server read)
            // time to land before asserting nothing was reported.
            await new Promise(resolve => setTimeout(resolve, 1000));
            expect(corrupted).toEqual([]);
        } finally {
            gate.release?.();
            await writer.destroy();
            await reader.destroy();
            writerDoc.destroy();
            readerDoc.destroy();
        }
    }, 120000);
});
