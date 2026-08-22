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
import { describe, it, expect, vi, afterEach } from 'vitest';
import * as Y from 'yjs';
import { toBase64 } from 'lib0/buffer';
import { FIREBASE_ORIGINS } from '../../src/types';
import {
    applyItem,
    collectServerBlobs,
    ensureDecodedSV,
    isItemRedundant,
    localCoversSnapshot,
    processHistoryMetadata,
    processSnapshotMetadata,
    processUpdateMetadata,
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
