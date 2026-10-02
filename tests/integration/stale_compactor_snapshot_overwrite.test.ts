/**
 * Regression: a compactor (or squasher) that has LOST its lock must not be
 * able to change the snapshot another client already committed.
 *
 * Bug: the fold names its candidate blob after the version it read at the
 * start (`snapshot_v{N+1}.bin`; squash: `snapshot_e{E+1}_v{N+1}.bin`) and
 * uploads it BEFORE the lock-checked commit transaction. The lock is a TTL
 * lease with no fencing token, so:
 *
 *   1. A reads version N and gets as far as uploading its candidate, then
 *      stalls (backgrounded tab, sleeping laptop, slow network) past its
 *      lease.
 *   2. B acquires the lapsed lock, folds the SAME version N into the SAME
 *      path, commits (main -> snapshot_v{N+1}.bin with B's state vector) and
 *      deletes every update document it merged.
 *   3. A's upload lands last and replaces B's committed blob with A's older
 *      merge. A's transaction then aborts ("Lock lost"), but the committed
 *      snapshot is already gone. Updates only B folded are lost for every
 *      client, while the main state vector still claims them.
 *
 * The stall is modelled by holding A's first Cloud Storage upload behind an
 * explicit gate. Lease expiry is modelled through B's clock offset (B's
 * estimate of server time is more than one lockTTL past A's acquisition),
 * so no real timer has to elapse.
 *
 * @file stale_compactor_snapshot_overwrite.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { uploadGate } = vi.hoisted(() => ({
    uploadGate: {
        /** When set, the NEXT uploadBytes call is held until `release` resolves. */
        armed: null as null | {
            onReached: (fullPath: string) => void;
            release: Promise<void>;
        },
    },
}));

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        uploadBytes: async (storageRef: any, data: any, metadata?: any) => {
            const gate = uploadGate.armed;
            if (gate) {
                uploadGate.armed = null;
                gate.onReached(storageRef?.fullPath ?? String(storageRef));
                await gate.release;
            }
            return actual.uploadBytes(storageRef, data, metadata);
        },
    };
});

import * as Y from 'yjs';
import { collection, addDoc, getDocs, getDoc, doc, serverTimestamp, Bytes } from 'firebase/firestore';
import { ref, getBytes } from '@firebase/storage';
import { compact, CompactionContext } from '../../src/compaction';
import { squashDocument } from '../../src/squash';
import { FireProvider } from '../../src/provider';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

const LOCK_TTL = 60_000;
/** B's view of server time: more than one full lease after A acquired. */
const LEASE_LAPSED_OFFSET = LOCK_TTL + 5_000;

/** Holds the next uploadBytes call. `reached` resolves with its path. */
function holdNextUpload() {
    let release!: () => void;
    const released = new Promise<void>((r) => { release = r; });
    let onReached!: (fullPath: string) => void;
    const reached = new Promise<string>((r) => { onReached = r; });
    uploadGate.armed = { onReached, release: released };
    return { reached, release };
}

describe('Stale compactor must not overwrite a committed snapshot blob', () => {
    let app: any;
    let db: any;
    let storage: any;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
        uploadGate.armed = null;
        path = `tests/stale-compactor-overwrite-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    /** Loads the document the way any new device would and returns its content. */
    async function loadAsFreshClient(): Promise<Record<string, unknown>> {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 1000, // no compaction from the reader
        });
        try {
            await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: 'fresh client synced' });
            return ydoc.getMap('m').toJSON();
        } finally {
            await provider.destroy();
            ydoc.destroy();
        }
    }

    it('fold: a compactor whose lease lapsed mid-upload does not replace the winner\'s snapshot', { timeout: 90000 }, async () => {
        const writer = new Y.Doc();
        const map = writer.getMap('m');
        const pushUpdate = async (key: string) => {
            const sv = Y.encodeStateVector(writer);
            map.set(key, key);
            await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(writer, sv)),
                createdAt: serverTimestamp(),
                createdBy: 'writer',
            });
        };

        await pushUpdate('u1');
        await pushUpdate('u2');
        await pushUpdate('u3');

        const ctx = (uid: string, cachedClockOffset: number): CompactionContext => ({
            db,
            path,
            uid,
            lockTTL: LOCK_TTL,
            cachedClockOffset,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
            historyFoldThreshold: 1, // always fold into the base snapshot
        });

        // A folds u1..u3 and stalls while uploading its candidate.
        const gate = holdNextUpload();
        const aDone = compact(ctx('client-A', 0));
        const heldPath = await gate.reached;
        expect(heldPath).toContain(path); // A really is parked mid-upload

        // Meanwhile another device keeps editing...
        await pushUpdate('u4');
        await pushUpdate('u5');

        // ...and B, finding A's lease lapsed, folds everything and commits.
        const bResult = await compact(ctx('client-B', LEASE_LAPSED_OFFSET));
        expect(bResult.success).toBe(true);
        expect(bResult.type).toBe('snapshot');

        // A wakes up; its upload completes, its commit is rejected.
        gate.release();
        await aDone;

        // Everything B committed must still be there for any new client.
        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);
        const content = await loadAsFreshClient();
        expect(content).toEqual({ u1: 'u1', u2: 'u2', u3: 'u3', u4: 'u4', u5: 'u5' });

        writer.destroy();
    });

    it('squash: a squasher whose lease lapsed mid-upload does not replace the winner\'s epoch snapshot', { timeout: 90000 }, async () => {
        // Two devices about to squash the same (empty server-side) document.
        // B holds one more local edit than A.
        const docA = new Y.Doc();
        docA.getMap('m').set('k0', 'k0');
        docA.getMap('m').set('k1', 'k1');
        const docB = new Y.Doc();
        Y.applyUpdate(docB, Y.encodeStateAsUpdate(docA));
        docB.getMap('m').set('k2', 'k2');

        // A squashes and stalls while uploading its candidate.
        const gate = holdNextUpload();
        const aDone = squashDocument({
            db, path, uid: 'client-A', lockTTL: LOCK_TTL, cachedClockOffset: 0,
            storage, isDestroyed: () => false, doc: docA,
        });
        const heldPath = await gate.reached;
        expect(heldPath).toContain(path); // A really is parked mid-upload

        // B, finding A's lease lapsed, squashes and commits epoch 1.
        const bResult = await squashDocument({
            db, path, uid: 'client-B', lockTTL: LOCK_TTL, cachedClockOffset: LEASE_LAPSED_OFFSET,
            storage, isDestroyed: () => false, doc: docB,
        });
        expect(bResult.success).toBe(true);
        expect(bResult.epoch).toBe(1);

        // A wakes up; its upload completes, its commit is rejected.
        gate.release();
        await aDone;

        // The committed epoch must be B's: k2 must survive for new clients.
        const content = await loadAsFreshClient();
        expect(content).toEqual({ k0: 'k0', k1: 'k1', k2: 'k2' });

        docA.destroy();
        docB.destroy();
    });

    /*
     * Blob names are attempt-unique, so they can no longer be rebuilt from
     * the version: GC after a fold must delete the replaced blobs through
     * the paths the main document stored, or every fold leaks the last one.
     */
    it('fold: garbage-collects the replaced snapshot and delete-set blobs through their stored paths', { timeout: 60000 }, async () => {
        const writer = new Y.Doc();
        const pushUpdate = async (key: string) => {
            const sv = Y.encodeStateVector(writer);
            writer.getMap('m').set(key, key);
            await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(writer, sv)),
                createdAt: serverTimestamp(),
                createdBy: 'writer',
            });
        };
        const ctx: CompactionContext = {
            db,
            path,
            uid: 'client-gc',
            lockTTL: LOCK_TTL,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
            historyFoldThreshold: 1, // always fold into the base snapshot
            maxDeleteSetFieldBytes: 0, // always offload the delete-set too
        };
        const readMain = async () => (await getDoc(doc(db, path))).data()!;
        const blobExists = (blobPath: string) => getBytes(ref(storage, blobPath)).then(
            () => true,
            (e: any) => {
                if (e?.code === 'storage/object-not-found') return false;
                throw e;
            },
        );

        await pushUpdate('g1');
        expect((await compact(ctx)).type).toBe('snapshot');
        const first = await readMain();
        expect(first.deleteSetStoragePath).toBeTruthy();

        await pushUpdate('g2');
        expect((await compact(ctx)).type).toBe('snapshot');
        const second = await readMain();

        expect(await blobExists(first.snapshotStoragePath)).toBe(false);
        expect(await blobExists(first.deleteSetStoragePath)).toBe(false);
        expect(await blobExists(second.snapshotStoragePath)).toBe(true);
        expect(await blobExists(second.deleteSetStoragePath)).toBe(true);

        writer.destroy();
    });
});
