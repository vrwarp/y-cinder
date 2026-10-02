/**
 * Regression test: a provider destroyed while its initial sync is
 * downloading the base snapshot from Cloud Storage must not write anything
 * afterwards.
 *
 * Bug: performInitialSync() checks isDestroyed() only after its Firestore
 * reads. The Storage downloads that follow (delete-set fingerprint, base
 * snapshot) are not followed by a check, and the final "push missing local
 * updates" step has no guard at all. destroy() neither awaits nor cancels
 * the in-flight sync, so when destroy() lands during the snapshot download
 * the destroyed provider still encodes its local diff and writes an update
 * document (or a Storage blob + pointer) under its own uid — after
 * teardown, e.g. after sign-out or a document switch.
 *
 * Contract: an initial sync interrupted by destroy() writes nothing to
 * Firestore or Cloud Storage, so no write starts after destroy() resolves
 * (its unsynced local state is pushed by the next provider attached to the
 * document instead). destroy() itself only flushes the provider's pending
 * updates, and this provider has none: its local content predates attach.
 *
 * @file initial_sync_destroy_during_download.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { mockControls } = vi.hoisted(() => {
    let releaseGate: () => void = () => { };
    let signalEntered: () => void = () => { };
    return {
        mockControls: {
            /** When set, snapshot downloads under this path block on the gate */
            gatePath: null as string | null,
            gate: Promise.resolve() as Promise<void>,
            entered: Promise.resolve() as Promise<void>,
            armGate() {
                this.gate = new Promise<void>(r => { releaseGate = r; });
                this.entered = new Promise<void>(r => { signalEntered = r; });
            },
            releaseGate: () => releaseGate(),
            signalEntered: () => signalEntered(),
            /** Set once destroy() has been called on the provider under test */
            destroyCalled: false,
            /** Storage uploads observed after destroy() was called */
            uploadsAfterDestroy: [] as string[],
            /** Initial-sync runs, so the test can wait for them to settle */
            syncRuns: [] as { path: string; promise: Promise<unknown> }[],
        },
    };
});

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getBytes: async (storageRef: any, ...rest: any[]) => {
            const fullPath: string = storageRef?.fullPath ?? String(storageRef);
            const gatePath = mockControls.gatePath;
            if (gatePath && fullPath.startsWith(gatePath) && fullPath.includes('snapshot_v')) {
                mockControls.signalEntered();
                await mockControls.gate;
            }
            return actual.getBytes(storageRef, ...rest);
        },
        uploadBytes: async (storageRef: any, ...rest: any[]) => {
            const fullPath: string = storageRef?.fullPath ?? String(storageRef);
            if (mockControls.destroyCalled) {
                mockControls.uploadsAfterDestroy.push(fullPath);
            }
            return actual.uploadBytes(storageRef, ...rest);
        },
    };
});

// Observe initial-sync runs (pass-through) so the test can deterministically
// wait for the destroyed provider's sync to finish before asserting.
vi.mock('../../src/sync', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        performInitialSync: (ctx: any) => {
            const promise = actual.performInitialSync(ctx);
            mockControls.syncRuns.push({ path: ctx.path, promise });
            return promise;
        },
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { collection, doc as fsDoc, getDoc, getDocs, query, where } from '@firebase/firestore';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('Initial sync vs destroy() during a Storage download', () => {
    let app: any;
    let db: any;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        mockControls.gatePath = null;
        mockControls.destroyCalled = false;
        mockControls.uploadsAfterDestroy = [];
        mockControls.syncRuns = [];
    });

    it('writes nothing to Firestore or Storage after destroy() lands during the snapshot download', async () => {
        const path = `integration-tests/initial-sync-destroy-download-${getStableDate()}-${counter++}`;

        // --- Seed: server state compacted into a Storage-backed snapshot ---
        const seedDoc = new Y.Doc();
        const seeder = new FireProvider({
            firebaseApp: app,
            ydoc: seedDoc,
            path,
            maxWaitTime: 50,
            maxUpdatesThreshold: 1000, // compaction only via explicit compact()
        });
        await waitForConditionTruthy(() => seeder.synced, { timeout: 30000 });
        seedDoc.getText('t').insert(0, 'server content');
        await waitForConditionTruthy(async () => {
            const snap = await getDocs(collection(db, path, 'updates'));
            return snap.size > 0;
        }, { timeout: 30000, interval: 100 });
        await seeder.compact();
        await seeder.destroy();

        const mainSnap = await getDoc(fsDoc(db, path));
        expect(mainSnap.data()?.snapshotStoragePath).toContain('snapshot_v');

        // --- Client with local-only content the server lacks ---
        // Content is written before the provider attaches, so it is not a
        // pending save: only initial sync's push step could upload it.
        const localDoc = new Y.Doc();
        localDoc.getText('t').insert(0, 'local only');

        // Hold the snapshot download for this document until released.
        mockControls.gatePath = path;
        mockControls.armGate();

        const provider = new FireProvider({
            firebaseApp: app,
            ydoc: localDoc,
            path,
            maxWaitTime: 50,
            maxUpdatesThreshold: 1000,
        });

        // Wait until initial sync is blocked inside the snapshot download.
        await Promise.race([
            mockControls.entered,
            new Promise((_, reject) => setTimeout(
                () => reject(new Error('initial sync never started the snapshot download')), 30000)),
        ]);

        // Tear the provider down while the download is in flight, then let
        // the download complete.
        mockControls.destroyCalled = true;
        const destroyed = provider.destroy();
        mockControls.releaseGate();
        await destroyed;

        // Let the destroyed provider's initial sync run to completion.
        await Promise.allSettled(
            mockControls.syncRuns.filter(r => r.path === path).map(r => r.promise)
        );
        // Small settle window for anything not chained to the sync promise.
        await new Promise(r => setTimeout(r, 500));

        // The destroyed provider must not have written an update document...
        const leaked = await getDocs(query(
            collection(db, path, 'updates'),
            where('createdBy', '==', provider.uid)
        ));
        expect(
            leaked.docs.map(d => d.id),
            'update documents written by the provider after destroy()'
        ).toEqual([]);

        // ...nor any Storage blob.
        expect(mockControls.uploadsAfterDestroy).toEqual([]);
    }, 90000);
});
