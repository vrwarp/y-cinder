/**
 * Unit tests for the pure sync helpers.
 *
 * These functions decide what the client already has and therefore what it
 * skips. A wrong answer here is silent: an item wrongly judged redundant is
 * never applied, and the local document is quietly missing data — no error,
 * no retry. They were previously private to sync.ts and reachable only
 * through emulator integration tests, so mutation testing could not see them
 * at all.
 */
import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest';
import * as Y from 'yjs';
import { toBase64 } from 'lib0/buffer';
import { FIREBASE_ORIGINS } from '../../src/types';
import { foldTailBaseClocks } from '../../src/compaction-policy';
import { writeStateVector } from '../../src/utils';
import { isWorkerMergeAvailable } from '../../src/merge-utils';

// Node has no Worker; tests opt into the worker-available gate explicitly.
// The diff itself always runs (on the main thread) through the real module.
vi.mock('../../src/merge-utils', async (importOriginal) => ({
    ...(await importOriginal<typeof import('../../src/merge-utils')>()),
    isWorkerMergeAvailable: vi.fn(() => false),
}));
import {
    applyItem,
    buildServerCoverage,
    collectServerBlobs,
    ensureDecodedSV,
    foldTailMayCatchUp,
    fingerprintIsRedundant,
    isItemRedundant,
    localCoversSnapshot,
    processHistoryMetadata,
    processSnapshotMetadata,
    processUpdateMetadata,
    refreshLocalClocks,
    rebaseIfPending,
    snapshotOverlap,
    diffSnapshotForLocal,
    SNAPSHOT_DIFF_MIN_OVERLAP,
    SNAPSHOT_DIFF_MIN_OVERLAP_MAIN_THREAD,
    transactionChangedDoc,
    type PendingUpdate,
} from '../../src/sync-helpers';

/** Minimal stand-in for a Firestore Bytes value. */
const bytes = (u8: Uint8Array) => ({ toUint8Array: () => u8 });

/** A doc with `count` edits from a fixed client, plus its encoded update. */
const makeDoc = (clientID: number, count: number) => {
    const doc = new Y.Doc();
    doc.clientID = clientID;
    const map = doc.getMap('m');
    for (let i = 0; i < count; i += 1) {
        doc.transact(() => map.set(`k${i}`, i));
    }
    return doc;
};

const svBase64 = (doc: Y.Doc) => toBase64(Y.encodeStateVector(doc));

/** Three sequential updates from client 1: [0,4), [4,8) and [8,12). */
const sequentialUpdates = () => {
    const doc = new Y.Doc();
    doc.clientID = 1;
    const updates: Uint8Array[] = [];
    doc.on('update', (u: Uint8Array) => updates.push(u));
    doc.getText('t').insert(0, 'aaaa');
    doc.getText('t').insert(4, 'bbbb');
    doc.getText('t').insert(8, 'cccc');
    return updates;
};

const updateItem = (u: Uint8Array, extra: Record<string, any> = {}): PendingUpdate =>
    ({ type: 'update', priority: 3, data: { update: bytes(u), ...extra } });

afterEach(() => {
    vi.restoreAllMocks();
});

describe('collectServerBlobs', () => {
    it('reads the right field for each item type', () => {
        const items: PendingUpdate[] = [
            { type: 'snapshot', priority: 1, data: { content: bytes(new Uint8Array([1])) } },
            { type: 'history', priority: 2, data: { segment: bytes(new Uint8Array([2, 2])) } },
            { type: 'update', priority: 3, data: { update: bytes(new Uint8Array([3, 3, 3])) } },
        ];

        expect(collectServerBlobs(items).map((b) => Array.from(b)))
            .toEqual([[1], [2, 2], [3, 3, 3]]);
    });

    it('skips items whose blob is absent (storage-backed or legacy)', () => {
        const items: PendingUpdate[] = [
            { type: 'update', priority: 3, data: { updateStoragePath: 'gs://x' } },
            { type: 'snapshot', priority: 1, data: {} },
            { type: 'update', priority: 3, data: { update: bytes(new Uint8Array([7])) } },
        ];

        expect(collectServerBlobs(items).map((b) => Array.from(b))).toEqual([[7]]);
    });

    it('never reads the wrong field for a type', () => {
        // A history item carrying an `update` field must contribute nothing:
        // only `segment` counts for history.
        const items: PendingUpdate[] = [
            { type: 'history', priority: 2, data: { update: bytes(new Uint8Array([9])) } },
        ];

        expect(collectServerBlobs(items)).toEqual([]);
    });

    it('returns an empty list for no items', () => {
        expect(collectServerBlobs([])).toEqual([]);
    });
});

describe('ensureDecodedSV', () => {
    it('decodes a base64 state vector into a client -> clock map', () => {
        const doc = makeDoc(42, 3);
        const data: any = { stateVector: svBase64(doc) };

        expect(ensureDecodedSV(data).get(42)).toBe(3);
    });

    it('caches the decoded map on the document data', () => {
        const doc = makeDoc(42, 1);
        const data: any = { stateVector: svBase64(doc) };
        const first = ensureDecodedSV(data);

        expect(ensureDecodedSV(data)).toBe(first);
        expect(data._decodedSV).toBe(first);
    });

    it('uses a cached map without re-reading stateVector', () => {
        const cached = new Map([[7, 9]]);
        const data: any = { stateVector: 'ignored-because-cached', _decodedSV: cached };

        expect(ensureDecodedSV(data)).toBe(cached);
    });
});

describe('localCoversSnapshot', () => {
    it('is true when the local doc is at or ahead of every remote clock', () => {
        const remote = makeDoc(1, 2);
        const local = new Y.Doc();
        Y.applyUpdate(local, Y.encodeStateAsUpdate(remote));

        expect(localCoversSnapshot({ stateVector: svBase64(remote) }, local)).toBe(true);
    });

    it('is true when the local doc is strictly ahead', () => {
        const remote = makeDoc(1, 1);
        const ahead = makeDoc(1, 5);

        expect(localCoversSnapshot({ stateVector: svBase64(remote) }, ahead)).toBe(true);
    });

    it('is false when any remote client is ahead of the local doc', () => {
        const remote = makeDoc(1, 5);
        const local = makeDoc(1, 2);

        expect(localCoversSnapshot({ stateVector: svBase64(remote) }, local)).toBe(false);
    });

    it('is false when the local doc has never seen a remote client', () => {
        const remote = makeDoc(99, 1);
        const local = makeDoc(1, 3);

        expect(localCoversSnapshot({ stateVector: svBase64(remote) }, local)).toBe(false);
    });

    it('is false — the safe direction — when stateVector is missing', () => {
        expect(localCoversSnapshot({}, new Y.Doc())).toBe(false);
    });

    it('is false — the safe direction — when stateVector is malformed', () => {
        vi.spyOn(console, 'warn').mockImplementation(() => undefined);

        expect(localCoversSnapshot({ stateVector: 'not-base64-!!' }, new Y.Doc())).toBe(false);
    });
});

describe('foldTailMayCatchUp', () => {
    /**
     * The replaced snapshot (the base) holds an earlier session (client 3)
     * and client 1's first edits; the fold's tail holds client 1's next
     * edit and a new session (client 2). Returns the base, the tail, the
     * folded server doc and the main-document fields compaction writes.
     */
    const foldedDocument = () => {
        const earlier = new Y.Doc();
        earlier.clientID = 3;
        earlier.getMap('m').set('earlier', true);
        const server = makeDoc(1, 3);
        Y.applyUpdate(server, Y.encodeStateAsUpdate(earlier));
        const base = Y.encodeStateAsUpdate(server);
        const baseSV = Y.decodeStateVector(Y.encodeStateVector(server));

        const updates: Uint8Array[] = [];
        server.on('update', (u: Uint8Array) => updates.push(u));
        server.transact(() => server.getMap('m').set('k3', 3));
        server.clientID = 2;
        server.transact(() => server.getMap('m').set('fresh', 1));
        const tail = Y.mergeUpdates(updates);

        const data: any = {
            stateVector: svBase64(server),
            version: 7,
            foldTailStoragePath: 'gs://tail_v7.bin',
            foldTailBaseClocks: toBase64(writeStateVector(foldTailBaseClocks(baseSV, Y.parseUpdateMeta(tail).to))),
            foldTailVersion: 7,
        };
        return { base, tail, server, data };
    };

    /** A device that held the replaced snapshot. */
    const holderOf = (base: Uint8Array) => {
        const doc = new Y.Doc();
        Y.applyUpdate(doc, base);
        return doc;
    };

    it('is true for a doc that holds the replaced snapshot, which the tail then completes', () => {
        const { base, tail, data } = foldedDocument();
        const local = holderOf(base);

        expect(localCoversSnapshot(data, local)).toBe(false);
        expect(foldTailMayCatchUp(data, local)).toBe(true);
        Y.applyUpdate(local, tail);
        expect(localCoversSnapshot(data, local)).toBe(true);
    });

    it('is true for a doc that also holds part of the tail', () => {
        const { base, server, data } = foldedDocument();
        const local = holderOf(base);
        // The new session (client 2) arrived; client 1's tail edit did not
        Y.applyUpdate(local, Y.encodeStateAsUpdate(server, writeStateVector(new Map([[1, 4], [3, 1]]))));
        expect(Y.getState(local.store, 2)).toBe(1);
        expect(Y.getState(local.store, 1)).toBe(3);

        expect(localCoversSnapshot(data, local)).toBe(false);
        expect(foldTailMayCatchUp(data, local)).toBe(true);
    });

    /* Two folds behind: the gap is in a client the tail does not touch. */
    it('is false when the doc is behind on a client the tail does not touch', () => {
        const { data } = foldedDocument();
        const withoutEarlierSession = makeDoc(1, 3);

        expect(foldTailMayCatchUp(data, withoutEarlierSession)).toBe(false);
    });

    it('is false when the doc is behind the base clock of a client the tail touches', () => {
        const { data } = foldedDocument();
        const local = makeDoc(1, 2);
        const earlier = new Y.Doc();
        earlier.clientID = 3;
        earlier.getMap('m').set('earlier', true);
        Y.applyUpdate(local, Y.encodeStateAsUpdate(earlier));

        expect(foldTailMayCatchUp(data, local)).toBe(false);
    });

    it('is false for an empty doc (a fresh client needs the snapshot)', () => {
        expect(foldTailMayCatchUp(foldedDocument().data, new Y.Doc())).toBe(false);
    });

    /*
     * A main-document writer that predates tails bumps the version and
     * leaves the fields behind: they describe another snapshot (possibly
     * another epoch) and must be ignored.
     */
    it('is false when the tail is bound to another version', () => {
        const { base, data } = foldedDocument();

        expect(foldTailMayCatchUp({ ...data, version: 8 }, holderOf(base))).toBe(false);
        expect(foldTailMayCatchUp({ ...data, foldTailVersion: undefined }, holderOf(base))).toBe(false);
    });

    it('is false when any tail field, the version or the state vector is missing', () => {
        const { base, data } = foldedDocument();

        for (const field of ['foldTailStoragePath', 'foldTailBaseClocks', 'stateVector', 'version']) {
            const fields = { ...data };
            delete fields[field];
            delete fields._decodedSV;
            expect(foldTailMayCatchUp(fields, holderOf(base))).toBe(false);
        }
    });

    it('is false — the safe direction — when the base clocks are malformed', () => {
        vi.spyOn(console, 'warn').mockImplementation(() => undefined);
        const { base, data } = foldedDocument();

        expect(foldTailMayCatchUp({ ...data, foldTailBaseClocks: 'not-base64-!!' }, holderOf(base))).toBe(false);
    });
});

describe('snapshotOverlap', () => {
    it('is the share of the snapshot clocks the local doc holds', () => {
        expect(snapshotOverlap(new Map([[1, 6], [2, 4]]), new Map([[1, 3], [2, 4]]))).toBe(0.7);
    });

    it('counts a local client ahead of the snapshot only up to the snapshot clock', () => {
        expect(snapshotOverlap(new Map([[1, 4], [2, 4]]), new Map([[1, 100]]))).toBe(0.5);
    });

    it('ignores local clients the snapshot does not have', () => {
        expect(snapshotOverlap(new Map([[1, 4]]), new Map([[9, 50]]))).toBe(0);
    });

    it('is 1 for a snapshot without structs', () => {
        expect(snapshotOverlap(new Map(), new Map([[1, 3]]))).toBe(1);
    });
});

describe('diffSnapshotForLocal', () => {
    const workerAvailable = (available: boolean) => vi.mocked(isWorkerMergeAvailable).mockReturnValue(available);

    beforeEach(() => {
        workerAvailable(false);
    });

    /**
     * A server doc whose first `held` edits (client 1) the local doc has,
     * followed by `missing` overwrites from client 2 (which delete).
     */
    const behind = (held: number, missing: number) => {
        const server = makeDoc(1, held);
        const local = new Y.Doc();
        Y.applyUpdate(local, Y.encodeStateAsUpdate(server));
        server.clientID = 2;
        const map = server.getMap('m');
        for (let i = 0; i < missing; i += 1) {
            server.transact(() => map.set(`k${i % held}`, -i));
        }
        return { server, local, snapshot: Y.encodeStateAsUpdate(server), data: { stateVector: svBase64(server) } };
    };

    const structsOf = (update: Uint8Array) =>
        Y.decodeUpdate(update).structs.filter((s) => !(s instanceof Y.Skip));

    it('returns only what the local doc lacks, reaching the same state as the whole snapshot', async () => {
        const { server, local, snapshot, data } = behind(10, 2);
        const viaFull = new Y.Doc();
        Y.applyUpdate(viaFull, Y.encodeStateAsUpdate(local));
        Y.applyUpdate(viaFull, snapshot);

        const diff = await diffSnapshotForLocal(snapshot, data, local);

        expect(structsOf(diff).map((s) => s.id.client)).toEqual([2, 2]);
        expect(diff.byteLength).toBeLessThan(snapshot.byteLength);
        Y.applyUpdate(local, diff);
        expect(local.getMap('m').toJSON()).toEqual(server.getMap('m').toJSON());
        expect(Y.encodeStateVector(local)).toEqual(Y.encodeStateVector(viaFull));
        expect(Y.encodeStateAsUpdate(local, Y.encodeStateVector(local)))
            .toEqual(Y.encodeStateAsUpdate(viaFull, Y.encodeStateVector(viaFull)));
    });

    it('keeps the full delete set (delete-set proof for the push guard)', async () => {
        const { snapshot, local, data } = behind(10, 2);

        const diff = await diffSnapshotForLocal(snapshot, data, local);

        expect(Y.equalDeleteSets(Y.decodeUpdate(diff).ds, Y.decodeUpdate(snapshot).ds)).toBe(true);
    });

    it('with the merge worker, diffs when the local doc holds half the snapshot', async () => {
        workerAvailable(true);
        // 10 of 20 clock units held
        const { snapshot, local, data } = behind(10, 10);
        expect(SNAPSHOT_DIFF_MIN_OVERLAP).toBe(0.5);

        expect(await diffSnapshotForLocal(snapshot, data, local)).not.toBe(snapshot);
    });

    it('with the merge worker, returns the snapshot itself when the local doc holds less than half', async () => {
        workerAvailable(true);
        // 10 of 21 clock units held: the diff would carry most of the snapshot
        const { snapshot, local, data } = behind(10, 11);

        expect(await diffSnapshotForLocal(snapshot, data, local)).toBe(snapshot);
    });

    it('on the main thread, diffs when the local doc holds three quarters of the snapshot', async () => {
        // 30 of 40 clock units held
        const { snapshot, local, data } = behind(30, 10);
        expect(SNAPSHOT_DIFF_MIN_OVERLAP_MAIN_THREAD).toBe(0.75);

        expect(await diffSnapshotForLocal(snapshot, data, local)).not.toBe(snapshot);
    });

    it('on the main thread, returns the snapshot itself below three quarters, where the diff would not pay off', async () => {
        // 30 of 41, then 10 of 20 clock units held
        const less = behind(30, 11);
        const half = behind(10, 10);

        expect(await diffSnapshotForLocal(less.snapshot, less.data, less.local)).toBe(less.snapshot);
        expect(await diffSnapshotForLocal(half.snapshot, half.data, half.local)).toBe(half.snapshot);
    });

    it('returns the snapshot itself for an empty local doc', async () => {
        const { snapshot, data } = behind(10, 2);

        expect(await diffSnapshotForLocal(snapshot, data, new Y.Doc())).toBe(snapshot);
    });

    it('returns the snapshot itself when the local doc already covers it', async () => {
        const { server, snapshot, data } = behind(10, 2);
        const local = new Y.Doc();
        Y.applyUpdate(local, Y.encodeStateAsUpdate(server));

        expect(await diffSnapshotForLocal(snapshot, data, local)).toBe(snapshot);
    });

    it('returns the snapshot itself without a stateVector to estimate the overlap from', async () => {
        const { snapshot, local } = behind(10, 2);

        expect(await diffSnapshotForLocal(snapshot, {}, local)).toBe(snapshot);
    });

    it('returns the snapshot itself when diffing fails, so the apply fails exactly as before', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
        const { local, data } = behind(10, 2);
        const corrupt = new Uint8Array([255, 255, 255, 255]);

        expect(await diffSnapshotForLocal(corrupt, data, local)).toBe(corrupt);
        expect(warn).toHaveBeenCalled();
    });
});

describe('processUpdateMetadata', () => {
    it('folds stored clientIDs/clientClocks into the server map', () => {
        const map = new Map<number, number>();
        processUpdateMetadata({ clientIDs: [1, 2], clientClocks: [10, 20] }, map);

        expect([...map]).toEqual([[1, 10], [2, 20]]);
    });

    it('keeps the highest clock per client and never lowers one', () => {
        const map = new Map<number, number>([[1, 50]]);
        processUpdateMetadata({ clientIDs: [1], clientClocks: [10] }, map);
        expect(map.get(1)).toBe(50);

        processUpdateMetadata({ clientIDs: [1], clientClocks: [70] }, map);
        expect(map.get(1)).toBe(70);
    });

    it('treats an equal clock as no advance', () => {
        const map = new Map<number, number>([[1, 30]]);
        processUpdateMetadata({ clientIDs: [1], clientClocks: [30] }, map);

        expect(map.get(1)).toBe(30);
    });

    it('falls back to parsing the update blob when metadata is absent', () => {
        const doc = makeDoc(7, 4);
        const map = new Map<number, number>();
        processUpdateMetadata({ update: bytes(Y.encodeStateAsUpdate(doc)) }, map);

        expect(map.get(7)).toBe(4);
    });

    it('prefers stored metadata over the blob when both are present', () => {
        const doc = makeDoc(7, 4);
        const map = new Map<number, number>();
        processUpdateMetadata(
            { clientIDs: [7], clientClocks: [99], update: bytes(Y.encodeStateAsUpdate(doc)) },
            map,
        );

        expect(map.get(7)).toBe(99);
    });

    it('ignores empty metadata arrays and a missing blob', () => {
        const map = new Map<number, number>();
        processUpdateMetadata({ clientIDs: [], clientClocks: [] }, map);

        expect(map.size).toBe(0);
    });
});

describe('processHistoryMetadata', () => {
    it('folds a stateVector field into the server map', () => {
        const doc = makeDoc(3, 6);
        const map = new Map<number, number>();
        processHistoryMetadata({ stateVector: svBase64(doc) }, map);

        expect(map.get(3)).toBe(6);
    });

    it('falls back to parsing the segment blob', () => {
        const doc = makeDoc(4, 2);
        const map = new Map<number, number>();
        processHistoryMetadata({ segment: bytes(Y.encodeStateAsUpdate(doc)) }, map);

        expect(map.get(4)).toBe(2);
    });

    it('prefers the stateVector field over the segment blob', () => {
        const doc = makeDoc(4, 2);
        const other = makeDoc(4, 11);
        const map = new Map<number, number>();
        processHistoryMetadata(
            { stateVector: svBase64(other), segment: bytes(Y.encodeStateAsUpdate(doc)) },
            map,
        );

        expect(map.get(4)).toBe(11);
    });

    it('never lowers an existing clock', () => {
        const doc = makeDoc(3, 1);
        const map = new Map<number, number>([[3, 40]]);
        processHistoryMetadata({ stateVector: svBase64(doc) }, map);

        expect(map.get(3)).toBe(40);
    });

    it('does nothing when neither field is present', () => {
        const map = new Map<number, number>();
        processHistoryMetadata({}, map);

        expect(map.size).toBe(0);
    });
});

describe('processSnapshotMetadata', () => {
    it('folds the snapshot stateVector into the server map', () => {
        const doc = makeDoc(5, 8);
        const map = new Map<number, number>();
        processSnapshotMetadata({ stateVector: svBase64(doc) }, map);

        expect(map.get(5)).toBe(8);
    });

    it('never lowers an existing clock', () => {
        const doc = makeDoc(5, 2);
        const map = new Map<number, number>([[5, 9]]);
        processSnapshotMetadata({ stateVector: svBase64(doc) }, map);

        expect(map.get(5)).toBe(9);
    });

    it('ignores a snapshot with no stateVector (never parses content)', () => {
        const doc = makeDoc(5, 3);
        const map = new Map<number, number>();
        processSnapshotMetadata({ content: bytes(Y.encodeStateAsUpdate(doc)) }, map);

        expect(map.size).toBe(0);
    });
});

describe('buildServerCoverage', () => {
    it('chains contiguous update ranges onto the snapshot state vector', () => {
        const [, u2, u3] = sequentialUpdates();

        expect(buildServerCoverage(new Map([[1, 4]]), [updateItem(u2), updateItem(u3)]).get(1)).toBe(12);
    });

    it('stops at a gap even though later clocks of the client are held', () => {
        const [u1, , u3] = sequentialUpdates();

        expect(buildServerCoverage(new Map(), [updateItem(u1), updateItem(u3)]).get(1)).toBe(4);
    });

    it('covers nothing when the server lacks the start of a client', () => {
        const [, u2, u3] = sequentialUpdates();

        expect(buildServerCoverage(new Map(), [updateItem(u2), updateItem(u3)]).has(1)).toBe(false);
    });

    it('never trusts the stored end clocks over the blob', () => {
        const [, , u3] = sequentialUpdates();
        const item = updateItem(u3, { clientIDs: [1], clientClocks: [12] });

        expect(buildServerCoverage(new Map(), [item]).has(1)).toBe(false);
    });

    it('sweeps ranges in clock order, whatever order the items arrive in', () => {
        const [u1, u2, u3] = sequentialUpdates();

        expect(buildServerCoverage(new Map(), [updateItem(u3), updateItem(u2), updateItem(u1)]).get(1)).toBe(12);
    });

    it('extends through overlapping ranges', () => {
        const [u1, u2, u3] = sequentialUpdates();
        const items = [updateItem(Y.mergeUpdates([u1, u2])), updateItem(Y.mergeUpdates([u2, u3]))];

        expect(buildServerCoverage(new Map(), items).get(1)).toBe(12);
    });

    it('reads history segments from their segment blob', () => {
        const [u1, u2] = sequentialUpdates();
        const item: PendingUpdate = { type: 'history', priority: 2, data: { segment: bytes(Y.mergeUpdates([u1, u2])) } };

        expect(buildServerCoverage(new Map(), [item]).get(1)).toBe(8);
    });

    it('ignores snapshot items and blobs stored under the wrong field', () => {
        const [u1] = sequentialUpdates();
        const items: PendingUpdate[] = [
            { type: 'snapshot', priority: 1, data: { content: bytes(u1) } },
            { type: 'history', priority: 2, data: { update: bytes(u1) } },
        ];

        expect(buildServerCoverage(new Map(), items).size).toBe(0);
    });

    it('keeps snapshot coverage that is already past a range, and leaves its input alone', () => {
        const [, u2] = sequentialUpdates();
        const snapshotSV = new Map([[1, 10], [2, 3]]);
        const coverage = buildServerCoverage(snapshotSV, [updateItem(u2)]);

        expect([...coverage]).toEqual([[1, 10], [2, 3]]);
        expect(coverage).not.toBe(snapshotSV);
    });

    it('treats a corrupt blob as covering nothing instead of throwing', () => {
        vi.spyOn(console, 'warn').mockImplementation(() => undefined);
        const [u1] = sequentialUpdates();
        const items = [updateItem(new Uint8Array([255, 255, 255, 255])), updateItem(u1)];

        expect(buildServerCoverage(new Map(), items).get(1)).toBe(4);
    });
});

describe('refreshLocalClocks', () => {
    it('reads the clock from the doc, not the end clock of an item parked behind a gap', () => {
        const [u1, , u3] = sequentialUpdates();
        const target = new Y.Doc();
        Y.applyUpdate(target, u1);
        Y.applyUpdate(target, u3);
        const localSVMap = new Map([[1, 4]]);

        refreshLocalClocks({ type: 'update', data: { clientIDs: [1], clientClocks: [12] } }, target, localSVMap);

        expect(localSVMap.get(1)).toBe(4);
    });

    it('advances to the item end once its structs are integrated', () => {
        const [u1, u2] = sequentialUpdates();
        const target = new Y.Doc();
        Y.applyUpdate(target, u1);
        Y.applyUpdate(target, u2);
        const localSVMap = new Map([[1, 4]]);

        refreshLocalClocks({ type: 'update', data: { update: bytes(u2) } }, target, localSVMap);

        expect(localSVMap.get(1)).toBe(8);
    });

    it('only touches the clients the item names', () => {
        const source = makeDoc(3, 6);
        const target = new Y.Doc();
        Y.applyUpdate(target, Y.encodeStateAsUpdate(source));
        const localSVMap = new Map([[9, 5]]);

        refreshLocalClocks({ type: 'history', data: { stateVector: svBase64(source) } }, target, localSVMap);

        expect([...localSVMap]).toEqual([[9, 5], [3, 6]]);
    });

    it('reads a snapshot item\'s clients from its stateVector', () => {
        const source = makeDoc(5, 2);
        const target = new Y.Doc();
        const localSVMap = new Map<number, number>();

        refreshLocalClocks({ type: 'snapshot', data: { stateVector: svBase64(source) } }, target, localSVMap);

        // Named by the snapshot, but nothing integrated locally yet.
        expect(localSVMap.get(5)).toBe(0);
    });
});

describe('rebaseIfPending', () => {
    /** Two consecutive ranges from client 1: "hello" (0..5), " world" (5..11). */
    const sameClientRanges = () => {
        const src = new Y.Doc();
        src.clientID = 1;
        const updates: Uint8Array[] = [];
        src.on('update', (u: Uint8Array) => updates.push(u));
        src.getText('t').insert(0, 'hello');
        src.getText('t').insert(5, ' world');
        return { u1: updates[0], u2: updates[1] };
    };

    it('leaves the cache alone when nothing is pending', () => {
        const doc = makeDoc(2, 3);
        const map = new Map<number, number>([[2, 1], [9, 4]]);

        rebaseIfPending(doc, map);

        expect([...map]).toEqual([[2, 1], [9, 4]]);
    });

    it('drops clocks claimed by a range parked behind a gap', () => {
        const { u1, u2 } = sameClientRanges();
        const doc = new Y.Doc();
        Y.applyUpdate(doc, Y.encodeStateAsUpdate(makeDoc(2, 3)));
        Y.applyUpdate(doc, u2);
        const map = new Map<number, number>([[2, 3]]);
        processUpdateMetadata({ update: bytes(u2) }, map);
        expect(map.get(1)).toBe(11);

        rebaseIfPending(doc, map);

        // The real state vector: client 1's structs are still pending,
        // so the range that fills the gap is not redundant.
        expect([...map]).toEqual([[2, 3]]);
        expect(isItemRedundant(
            { type: 'update', priority: 3, data: { clientIDs: [1], clientClocks: [5] } },
            map,
        )).toBe(false);

        Y.applyUpdate(doc, u1);
        expect(doc.getText('t').toString()).toBe('hello world');
    });
});

describe('isItemRedundant', () => {
    const local = new Map<number, number>([[1, 10]]);

    it('is true for a snapshot fully covered by the local state vector', () => {
        const doc = makeDoc(1, 4);

        expect(isItemRedundant(
            { type: 'snapshot', priority: 1, data: { stateVector: svBase64(doc) } },
            local,
        )).toBe(true);
    });

    it('is false for a snapshot carrying a clock beyond the local one', () => {
        const doc = makeDoc(1, 40);

        expect(isItemRedundant(
            { type: 'snapshot', priority: 1, data: { stateVector: svBase64(doc) } },
            local,
        )).toBe(false);
    });

    it('is false for a snapshot from a client the local doc has never seen', () => {
        const doc = makeDoc(777, 1);

        expect(isItemRedundant(
            { type: 'snapshot', priority: 1, data: { stateVector: svBase64(doc) } },
            local,
        )).toBe(false);
    });

    it('is true for a fully covered history segment', () => {
        const doc = makeDoc(1, 3);

        expect(isItemRedundant(
            { type: 'history', priority: 2, data: { stateVector: svBase64(doc) } },
            local,
        )).toBe(true);
    });

    it('is false for a history segment beyond the local clock', () => {
        const doc = makeDoc(1, 99);

        expect(isItemRedundant(
            { type: 'history', priority: 2, data: { stateVector: svBase64(doc) } },
            local,
        )).toBe(false);
    });

    /*
     * A state vector only spans structs; deletions add none. A segment that
     * mixes structs the client holds with deletions it lacks must still be
     * applied, or the deleted content comes back.
     */
    it('is false for a covered history segment flagged as carrying deletions', () => {
        const doc = makeDoc(1, 3);

        expect(isItemRedundant(
            { type: 'history', priority: 2, data: { stateVector: svBase64(doc), hasDeletions: true } },
            local,
        )).toBe(false);
    });

    it('is false for a history segment with an empty stateVector (deletions only)', () => {
        // What a delete-only segment carried before the hasDeletions flag:
        // an empty vector, which every local state "covers" vacuously.
        expect(isItemRedundant(
            { type: 'history', priority: 2, data: { stateVector: svBase64(new Y.Doc()) } },
            local,
        )).toBe(false);
    });

    it('is false — never skip — when a history stateVector fails to parse', () => {
        expect(isItemRedundant(
            { type: 'history', priority: 2, data: { stateVector: 'garbage!!' } },
            local,
        )).toBe(false);
    });

    it('is true for an update whose stored clocks are already covered', () => {
        expect(isItemRedundant(
            { type: 'update', priority: 3, data: { clientIDs: [1], clientClocks: [5] } },
            local,
        )).toBe(true);
    });

    it('is false for an update carrying a newer clock', () => {
        expect(isItemRedundant(
            { type: 'update', priority: 3, data: { clientIDs: [1], clientClocks: [50] } },
            local,
        )).toBe(false);
    });

    it('is false for an update with no stored clock metadata', () => {
        // Without metadata there is nothing to compare, so it must be
        // fetched and applied rather than assumed known.
        expect(isItemRedundant(
            { type: 'update', priority: 3, data: { update: bytes(new Uint8Array([0])) } },
            local,
        )).toBe(false);
    });

    it('is false for a snapshot or history item with no stateVector', () => {
        expect(isItemRedundant({ type: 'snapshot', priority: 1, data: {} }, local)).toBe(false);
        expect(isItemRedundant({ type: 'history', priority: 2, data: {} }, local)).toBe(false);
    });
});

/** A doc whose map overwrites leave deletions, and its delete-set fingerprint. */
const docWithDeletions = () => {
    const doc = new Y.Doc();
    doc.clientID = 1;
    const map = doc.getMap('m');
    for (let i = 0; i < 20; i += 1) {
        doc.transact(() => map.set(`k${i % 4}`, i));
    }
    // What compaction stores: structs-empty, the whole delete-set
    const fingerprint = () => Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc));
    return { doc, map, fingerprint };
};

const localDsOf = (doc: Y.Doc) => Y.createDeleteSetFromStructStore(doc.store);

describe('fingerprintIsRedundant', () => {
    it('is true when every deletion is already local', () => {
        const { doc, fingerprint } = docWithDeletions();
        const replica = new Y.Doc();
        Y.applyUpdate(replica, Y.encodeStateAsUpdate(doc));

        expect(Y.decodeUpdate(fingerprint()).ds.clients.size).toBe(1);
        expect(fingerprintIsRedundant(fingerprint(), localDsOf(replica))).toBe(true);
    });

    it('is false when a deletion is missing locally (a delete-only change)', () => {
        const { doc, map, fingerprint } = docWithDeletions();
        const replica = new Y.Doc();
        Y.applyUpdate(replica, Y.encodeStateAsUpdate(doc));
        map.delete('k1');

        expect(fingerprintIsRedundant(fingerprint(), localDsOf(replica))).toBe(false);
        Y.applyUpdate(replica, fingerprint());
        expect(replica.getMap('m').has('k1')).toBe(false);
    });

    it('is false for deletions past the local clock (Yjs must keep them pending)', () => {
        const { doc, fingerprint } = docWithDeletions();
        const behind = new Y.Doc();
        Y.applyUpdate(behind, Y.encodeStateAsUpdate(doc));
        const clock = Y.getState(behind.store, 1);
        doc.getMap('m').set('new', 1);
        doc.getMap('m').set('new', 2); // deletes a struct `behind` lacks

        const pastClock = Y.decodeUpdate(fingerprint()).ds.clients.get(1)!.filter(r => r.clock >= clock);
        expect(pastClock).toEqual([expect.objectContaining({ clock, len: 1 })]);
        expect(fingerprintIsRedundant(fingerprint(), localDsOf(behind))).toBe(false);
    });

    it('is false for a blob carrying structs, even when its deletions are local', () => {
        const { doc } = docWithDeletions();
        const replica = new Y.Doc();
        Y.applyUpdate(replica, Y.encodeStateAsUpdate(doc));
        doc.getMap('other').set('x', 1); // a struct the replica lacks
        const withStructs = Y.encodeStateAsUpdate(doc, Y.encodeStateVector(replica));

        expect(Y.decodeUpdate(withStructs).structs.length).toBeGreaterThan(0);
        expect(fingerprintIsRedundant(withStructs, localDsOf(replica))).toBe(false);
    });

    it('is false for an unparseable blob (applied, and quarantined, as before)', () => {
        expect(fingerprintIsRedundant(new Uint8Array([0xff, 0xff, 0xff]), localDsOf(new Y.Doc()))).toBe(false);
    });

    it('does not modify the local delete-set', () => {
        const { doc, map, fingerprint } = docWithDeletions();
        const replica = new Y.Doc();
        Y.applyUpdate(replica, Y.encodeStateAsUpdate(doc));
        map.delete('k2');
        const localDs = localDsOf(replica);
        const before = Y.encodeStateAsUpdate(replica);

        fingerprintIsRedundant(fingerprint(), localDs);

        expect(Y.equalDeleteSets(localDs, localDsOf(replica))).toBe(true);
        expect(Y.encodeStateAsUpdate(replica)).toEqual(before);
    });
});

describe('transactionChangedDoc', () => {
    /** Runs `f` in one transaction on `doc` and returns it, cleaned up. */
    const inTransaction = (doc: Y.Doc, f: () => void) => doc.transact((tr) => { f(); return tr; });

    it('is false when everything applied was already held', () => {
        const { doc, fingerprint } = docWithDeletions();
        const replica = new Y.Doc();
        Y.applyUpdate(replica, Y.encodeStateAsUpdate(doc));

        expect(transactionChangedDoc(inTransaction(replica, () => {
            Y.applyUpdate(replica, Y.encodeStateAsUpdate(doc));
            Y.applyUpdate(replica, fingerprint());
        }))).toBe(false);
    });

    it('is false for an empty transaction', () => {
        expect(transactionChangedDoc(inTransaction(new Y.Doc(), () => { }))).toBe(false);
    });

    it('is true when a struct was integrated', () => {
        const { doc } = docWithDeletions();
        const replica = new Y.Doc();

        expect(transactionChangedDoc(inTransaction(replica, () => {
            Y.applyUpdate(replica, Y.encodeStateAsUpdate(doc));
        }))).toBe(true);
    });

    it('is true for a delete-only change (no struct, state vector unchanged)', () => {
        const { doc, map, fingerprint } = docWithDeletions();
        const replica = new Y.Doc();
        Y.applyUpdate(replica, Y.encodeStateAsUpdate(doc));
        map.delete('k3');

        const tr = inTransaction(replica, () => Y.applyUpdate(replica, fingerprint()));

        expect(tr.afterState).toEqual(tr.beforeState);
        expect(transactionChangedDoc(tr)).toBe(true);
    });

    it('is true when the only change adds a client', () => {
        const replica = new Y.Doc();
        replica.getMap('m').set('a', 1);
        const other = new Y.Doc();
        other.clientID = replica.clientID + 1;
        other.getMap('n').set('b', 2);

        expect(transactionChangedDoc(inTransaction(replica, () => {
            Y.applyUpdate(replica, Y.encodeStateAsUpdate(other));
        }))).toBe(true);
    });
});

describe('applyItem', () => {
    it('applies a snapshot with the snapshot origin', () => {
        const source = makeDoc(1, 2);
        const target = new Y.Doc();
        const origins: unknown[] = [];
        target.on('afterTransaction', (tr) => origins.push(tr.origin));

        const applied = applyItem(
            { type: 'snapshot', priority: 1, data: { content: bytes(Y.encodeStateAsUpdate(source)) } },
            target,
        );

        expect(applied).toBe(true);
        expect(target.getMap('m').get('k0')).toBe(0);
        expect(origins).toContain(FIREBASE_ORIGINS.SNAPSHOT);
    });

    it('applies a history segment with the history origin', () => {
        const source = makeDoc(1, 2);
        const target = new Y.Doc();
        const origins: unknown[] = [];
        target.on('afterTransaction', (tr) => origins.push(tr.origin));

        expect(applyItem(
            { type: 'history', priority: 2, data: { segment: bytes(Y.encodeStateAsUpdate(source)) } },
            target,
        )).toBe(true);
        expect(origins).toContain(FIREBASE_ORIGINS.HISTORY);
    });

    it('applies an update with the update origin', () => {
        const source = makeDoc(1, 2);
        const target = new Y.Doc();
        const origins: unknown[] = [];
        target.on('afterTransaction', (tr) => origins.push(tr.origin));

        expect(applyItem(
            { type: 'update', priority: 3, data: { update: bytes(Y.encodeStateAsUpdate(source)) } },
            target,
        )).toBe(true);
        expect(origins).toContain(FIREBASE_ORIGINS.UPDATE);
    });

    it('returns false when the item carries no blob for its type', () => {
        expect(applyItem({ type: 'snapshot', priority: 1, data: {} }, new Y.Doc())).toBe(false);
        expect(applyItem({ type: 'history', priority: 2, data: {} }, new Y.Doc())).toBe(false);
        expect(applyItem({ type: 'update', priority: 3, data: {} }, new Y.Doc())).toBe(false);
    });

    it('ignores a blob stored under the wrong field for its type', () => {
        expect(applyItem(
            { type: 'snapshot', priority: 1, data: { update: bytes(new Uint8Array([1])) } },
            new Y.Doc(),
        )).toBe(false);
    });

    it('returns false instead of throwing on a corrupt blob', () => {
        vi.spyOn(console, 'error').mockImplementation(() => undefined);

        expect(applyItem(
            { type: 'update', priority: 3, data: { update: bytes(new Uint8Array([255, 255, 255, 255])) } },
            new Y.Doc(),
        )).toBe(false);
    });
});

/*
 * Conjunct independence.
 *
 * Several guards read `clientIDs?.length > 0 && clientClocks?.length > 0`.
 * Every existing test supplies both arrays or neither, so mutating either
 * half changed nothing observable. A half-written document (one array
 * present, the other empty) is exactly what a partial write or an older
 * client produces, and it must fall through to the blob rather than be
 * read as authoritative metadata.
 */
describe('clock-metadata guards require BOTH arrays', () => {
    const doc = makeDoc(7, 4);
    const blob = () => bytes(Y.encodeStateAsUpdate(doc));

    it('processUpdateMetadata ignores clientIDs without clientClocks', () => {
        const map = new Map<number, number>();

        processUpdateMetadata({ clientIDs: [7], clientClocks: [], update: blob() }, map);

        // Fell through to the blob, which reports clock 4 — not the empty metadata.
        expect(map.get(7)).toBe(4);
    });

    it('processUpdateMetadata ignores clientClocks without clientIDs', () => {
        const map = new Map<number, number>();

        processUpdateMetadata({ clientIDs: [], clientClocks: [99], update: blob() }, map);

        expect(map.get(7)).toBe(4);
    });

    it('processUpdateMetadata with neither array and no blob does nothing', () => {
        const map = new Map<number, number>();

        processUpdateMetadata({ clientIDs: [], clientClocks: [] }, map);

        expect(map.size).toBe(0);
    });

    it('isItemRedundant ignores half-written update metadata', () => {
        const local = new Map<number, number>([[1, 10]]);

        // Clocks alone must not be read as "covered".
        expect(isItemRedundant(
            { type: 'update', priority: 3, data: { clientIDs: [], clientClocks: [5] } },
            local,
        )).toBe(false);
        expect(isItemRedundant(
            { type: 'update', priority: 3, data: { clientIDs: [1], clientClocks: [] } },
            local,
        )).toBe(false);
    });
});

describe('state-vector folding treats absent and zero clocks alike', () => {
    it('starts from 0 for a client the map has never seen', () => {
        const map = new Map<number, number>();

        processUpdateMetadata({ clientIDs: [5], clientClocks: [1] }, map);

        expect(map.get(5)).toBe(1);
    });

    it('advances from an explicit 0 rather than treating it as missing', () => {
        const map = new Map<number, number>([[5, 0]]);

        processUpdateMetadata({ clientIDs: [5], clientClocks: [1] }, map);

        expect(map.get(5)).toBe(1);
    });

    it('does not regress a client to 0 when a lower clock arrives', () => {
        const map = new Map<number, number>([[5, 8]]);

        processUpdateMetadata({ clientIDs: [5], clientClocks: [0] }, map);

        expect(map.get(5)).toBe(8);
    });

    it('applies the same rule when folding from a blob', () => {
        const map = new Map<number, number>([[7, 99]]);

        processUpdateMetadata({ update: bytes(Y.encodeStateAsUpdate(makeDoc(7, 2))) }, map);

        expect(map.get(7)).toBe(99);
    });
});

describe('applyItem requires the field to match the declared type', () => {
    it('does not apply an update item whose update field is missing', () => {
        const target = new Y.Doc();

        expect(applyItem(
            { type: 'update', priority: 3, data: { segment: bytes(new Uint8Array([1])) } },
            target,
        )).toBe(false);
    });

    it('does not apply a history item carrying only content', () => {
        const target = new Y.Doc();

        expect(applyItem(
            { type: 'history', priority: 2, data: { content: bytes(new Uint8Array([1])) } },
            target,
        )).toBe(false);
    });
});
