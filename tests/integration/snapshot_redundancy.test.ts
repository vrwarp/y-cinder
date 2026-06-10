/**
 * Regression test: snapshot blobs must not be downloaded redundantly.
 *
 * Two bugs fixed:
 * 1. On connect, initial sync downloaded the storage-backed snapshot, then
 *    the snapshot listener's first delivery downloaded the same blob again
 *    (no redundancy check) — doubling bandwidth on every page load.
 * 2. Compaction never wrote an `origin` field, so the compacting client's
 *    own snapshot listener re-downloaded the snapshot it had just created,
 *    and other up-to-date clients re-downloaded data they already had.
 *
 * The fix: compaction stamps `origin`, and the snapshot listener checks the
 * stored state vector against the local doc before fetching the blob.
 *
 * @file snapshot_redundancy.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { mockControls } = vi.hoisted(() => ({
    mockControls: {
        snapshotDownloads: [] as string[],
    }
}));

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getBytes: async (storageRef: any) => {
            const fullPath = storageRef?.fullPath ?? String(storageRef);
            if (fullPath.includes('snapshot_v')) {
                mockControls.snapshotDownloads.push(fullPath);
            }
            return actual.getBytes(storageRef);
        },
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { doc as fsDoc, getDoc } from '@firebase/firestore';
import { waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('Snapshot download redundancy', () => {
    let app: any;
    let db: any;
    let counter = 0;

    const createProvider = (ydoc: Y.Doc, path: string) => {
        return new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxWaitTime: 50,
            maxUpdatesThreshold: 1000, // compaction only via explicit compact()
        });
    };

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        mockControls.snapshotDownloads = [];
    });

    it('compacting client does not re-download its own snapshot; up-to-date and fresh clients download at most once', async () => {
        const path = `integration-tests/snapshot-redundancy-${getStableDate()}-${counter++}`;

        // Seed content and compact it into a storage-backed snapshot
        const doc1 = new Y.Doc();
        const provider1 = createProvider(doc1, path);
        await waitForConditionTruthy(() => provider1.synced, { timeout: 10000 });
        doc1.getText('t').insert(0, 'compact me');
        await new Promise(r => setTimeout(r, 500));

        await provider1.compact();

        // Verify the snapshot exists
        const mainSnap = await getDoc(fsDoc(db, path));
        expect(mainSnap.data()?.snapshotStoragePath).toContain('snapshot_v');

        // Let the (potential) own-snapshot listener delivery settle
        await new Promise(r => setTimeout(r, 1500));

        // The compacting client already holds all the data — it must not
        // download the snapshot it just produced
        expect(mockControls.snapshotDownloads.length).toBe(0);
        await provider1.destroy();

        // A FRESH client needs the snapshot exactly once (initial sync);
        // the listener's first delivery must be skipped as redundant
        mockControls.snapshotDownloads = [];
        const doc2 = new Y.Doc();
        const provider2 = createProvider(doc2, path);
        await waitForConditionEquals(
            () => doc2.getText('t').toString(),
            'compact me',
            { timeout: 10000, interval: 100 }
        );
        await new Promise(r => setTimeout(r, 1500));
        expect(mockControls.snapshotDownloads.length).toBe(1);
        await provider2.destroy();

        // A reconnecting, fully-synced client needs no snapshot download at all
        mockControls.snapshotDownloads = [];
        const provider3 = createProvider(doc2, path);
        await waitForConditionTruthy(() => provider3.synced, { timeout: 10000 });
        await new Promise(r => setTimeout(r, 1500));
        expect(mockControls.snapshotDownloads.length).toBe(0);
        await provider3.destroy();
    }, 90000);
});
