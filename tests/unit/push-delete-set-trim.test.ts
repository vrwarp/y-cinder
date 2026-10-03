/**
 * Unit tests for withoutServerDeletions.
 *
 * Yjs embeds the document's complete delete-set in every diff produced by
 * encodeStateAsUpdate(doc, sv). The initial-sync push strips the deletions
 * the server blobs already prove, so one offline edit is not uploaded as an
 * O(delete-set) update document.
 *
 * Contract asserted here:
 *  - correctness: a peer holding the server blobs that applies the trimmed
 *    diff ends up with the local document's content AND delete-set;
 *  - the structs section is kept byte for byte;
 *  - every range the server's union contains is dropped, and only those;
 *  - the delete-set encoding matches Yjs's own byte for byte (otherwise
 *    the diff would come back untrimmed: these tests pin the layout);
 *  - anything unexpected returns the diff unchanged.
 *
 * @file push-delete-set-trim.test.ts
 */

import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { Bytes } from '@firebase/firestore';
import { withoutServerDeletions } from '../../src/update-metadata';
import { buildServerCoverage, PendingUpdate } from '../../src/sync-helpers';
import { mergeUpdatesWithMeta } from '../../src/merge-core';
import { writeStateVector } from '../../src/utils';
import { SeededRandom } from './prng';

type DeleteSet = ReturnType<typeof Y.decodeUpdate>['ds'];

const storeDs = (doc: Y.Doc): DeleteSet => Y.createDeleteSetFromStructStore((doc as any).store);
const dsOf = (update: Uint8Array): DeleteSet => Y.decodeUpdate(update).ds;

function rangesOf(ds: DeleteSet): number {
    let n = 0;
    ds.clients.forEach((items) => { n += items.length; });
    return n;
}

/** Copy: Y.mergeDeleteSets widens its inputs' DeleteItems in place. */
function clone(ds: DeleteSet): DeleteSet {
    const copy = Y.createDeleteSet();
    ds.clients.forEach((items, client) => copy.clients.set(client, items.map(({ clock, len }) => ({ clock, len }))));
    return copy;
}

/** Whether the union of `outer` contains every deletion of `inner`. */
function dsContains(outer: DeleteSet, inner: DeleteSet): boolean {
    return Y.equalDeleteSets(Y.mergeDeleteSets([clone(outer)]), Y.mergeDeleteSets([clone(outer), clone(inner)]));
}

function docState(doc: Y.Doc): string {
    // Map entries sorted: iteration order follows local insertion order
    const map = Object.entries(doc.getMap('m').toJSON()).sort(([a], [b]) => a.localeCompare(b));
    return JSON.stringify([doc.getText('t').toString(), map, doc.getArray('a').toJSON()]);
}

/** The edit's own update(s), merged: what a push minimally needs. */
function capture(doc: Y.Doc, edit: () => void): Uint8Array {
    const updates: Uint8Array[] = [];
    const onUpdate = (u: Uint8Array) => updates.push(u);
    doc.on('update', onUpdate);
    edit();
    doc.off('update', onUpdate);
    return Y.mergeUpdates(updates);
}

function applyAll(doc: Y.Doc, updates: Uint8Array[]): Y.Doc {
    for (const u of updates) Y.applyUpdate(doc, u);
    return doc;
}

describe('withoutServerDeletions', () => {
    it('pushes an insert-only offline edit exactly as the edit itself', () => {
        const doc = new Y.Doc();
        const text = doc.getText('t');
        text.insert(0, 'hello brave new world');
        text.delete(0, 6);
        text.delete(6, 4);
        const serverBlob = Y.encodeStateAsUpdate(doc);
        const serverSV = Y.encodeStateVector(doc);

        const floor = capture(doc, () => text.insert(0, 'offline '));
        const diff = Y.encodeStateAsUpdate(doc, serverSV);
        expect(rangesOf(dsOf(diff))).toBe(2);

        const trimmed = withoutServerDeletions(diff, () => [serverBlob]);

        expect(Array.from(trimmed)).toEqual(Array.from(floor));
        expect(rangesOf(dsOf(trimmed))).toBe(0);
        const peer = applyAll(new Y.Doc(), [serverBlob, trimmed]);
        expect(docState(peer)).toBe(docState(doc));
    });

    it('matches the Yjs delete-set encoding: many clients, multi-byte clocks, GC ranges', () => {
        // Clients in non-sorted order (the encoding sorts them descending),
        // clocks past 127 and 16383 (multi-byte varints), GC'd deletions.
        let state: Uint8Array | null = null;
        for (const clientID of [5, 900_000_001, 77, 3_000_000_000, 128]) {
            const d = new Y.Doc();
            d.clientID = clientID;
            if (state) Y.applyUpdate(d, state);
            const text = d.getText('t');
            text.insert(text.length, 'x'.repeat(clientID === 77 ? 20_000 : 300));
            for (let i = 0; i < 40; i++) text.delete(Math.min(i * 3, text.length - 1), 1);
            d.getMap('m').set('k', clientID);
            state = Y.encodeStateAsUpdate(d);
            d.destroy();
        }
        const doc = applyAll(new Y.Doc(), [state!]);
        expect(storeDs(doc).clients.size).toBeGreaterThan(3);

        // Structs-empty diff against a server that holds everything
        const diff = Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc));
        const trimmed = withoutServerDeletions(diff, () => [state!]);

        // Header only: zero struct clients, zero delete-set clients
        expect(Array.from(trimmed)).toEqual([0, 0]);
    });

    it('keeps a genuine offline deletion, including one adjacent to a server deletion', () => {
        const doc = new Y.Doc();
        const text = doc.getText('t');
        text.insert(0, 'hello world, again and again');
        text.delete(0, 5); // 'hello' is on the server
        text.delete(10, 6); // ' again' is on the server
        const serverBlob = Y.encodeStateAsUpdate(doc);
        const serverSV = Y.encodeStateVector(doc);

        text.delete(0, 6); // offline: ' world', contiguous with 'hello'
        text.insert(0, 'x'); // and a struct
        const diff = Y.encodeStateAsUpdate(doc, serverSV);
        expect(rangesOf(dsOf(diff))).toBe(2);

        const trimmed = withoutServerDeletions(diff, () => [serverBlob]);

        // The merged local range (hello + world) is only partly on the
        // server: kept whole. The ' again' range is dropped.
        expect(rangesOf(dsOf(trimmed))).toBe(1);
        const peer = applyAll(new Y.Doc(), [serverBlob, trimmed]);
        expect(peer.getText('t').toString()).toBe(text.toString());
        expect(dsContains(storeDs(peer), storeDs(doc))).toBe(true);
    });

    it('proves coverage across several blobs, whose ranges join only once merged', () => {
        const doc = new Y.Doc();
        const text = doc.getText('t');
        text.insert(0, 'abcdefghij');
        const base = Y.encodeStateAsUpdate(doc);
        const del1 = capture(doc, () => text.delete(2, 2)); // 'cd'
        const del2 = capture(doc, () => text.delete(2, 2)); // 'ef', adjacent to 'cd'
        const serverSV = Y.encodeStateVector(doc);
        text.insert(0, 'z');

        const diff = Y.encodeStateAsUpdate(doc, serverSV);
        expect(rangesOf(dsOf(diff))).toBe(1); // locally one range: 'cdef'

        const trimmed = withoutServerDeletions(diff, () => [base, del1, del2]);
        expect(rangesOf(dsOf(trimmed))).toBe(0);
        // Neither blob alone contains it
        expect(withoutServerDeletions(diff, () => [base, del1])).toBe(diff);
        expect(withoutServerDeletions(diff, () => [base, del2])).toBe(diff);
    });

    it('returns the diff itself when nothing can be dropped', () => {
        const doc = new Y.Doc();
        doc.getText('t').insert(0, 'hello');
        const insertOnly = Y.encodeStateAsUpdate(doc);
        let asked = false;
        // No deletions at all: the server blobs are not even requested
        expect(withoutServerDeletions(insertOnly, () => { asked = true; return []; })).toBe(insertOnly);
        expect(asked).toBe(false);

        doc.getText('t').delete(0, 2);
        const diff = Y.encodeStateAsUpdate(doc);
        expect(withoutServerDeletions(diff, () => [])).toBe(diff);
        expect(withoutServerDeletions(diff, () => [insertOnly])).toBe(diff);
    });

    it('returns the diff unchanged when it cannot be parsed or does not end with its delete-set', () => {
        const doc = new Y.Doc();
        doc.getText('t').insert(0, 'hello');
        doc.getText('t').delete(0, 2);
        const full = Y.encodeStateAsUpdate(doc);

        const garbage = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x0f]);
        expect(withoutServerDeletions(garbage, () => [full])).toBe(garbage);

        const trailing = new Uint8Array(full.byteLength + 1);
        trailing.set(full);
        trailing[full.byteLength] = 7;
        expect(withoutServerDeletions(trailing, () => [full])).toBe(trailing);
    });

    it('ignores server blobs that fail to parse', () => {
        const doc = new Y.Doc();
        const text = doc.getText('t');
        text.insert(0, 'hello world');
        text.delete(0, 6);
        const serverBlob = Y.encodeStateAsUpdate(doc);
        const serverSV = Y.encodeStateVector(doc);
        text.insert(0, '!');
        const diff = Y.encodeStateAsUpdate(doc, serverSV);
        const corrupt = new Uint8Array([0xff, 0xff, 0xff]);

        expect(withoutServerDeletions(diff, () => [corrupt])).toBe(diff);
        expect(rangesOf(dsOf(withoutServerDeletions(diff, () => [corrupt, serverBlob])))).toBe(0);
    });

    it('trims a diff that also carries parked (pending) structs', () => {
        const source = new Y.Doc();
        source.clientID = 1;
        const text = source.getText('t');
        text.insert(0, 'hello world');
        text.delete(0, 6);
        const serverBlob = Y.encodeStateAsUpdate(source);
        const serverSV = Y.encodeStateVector(source);
        const gapFiller = capture(source, () => text.insert(0, 'A'));
        const parked = capture(source, () => text.insert(0, 'B'));

        // The local doc received the second insert without the first:
        // Yjs parks it in pendingStructs, and encodeStateAsUpdate merges it in.
        const local = applyAll(new Y.Doc(), [serverBlob, parked]);
        expect((local.store as any).pendingStructs).not.toBeNull();
        const diff = Y.encodeStateAsUpdate(local, serverSV);

        const trimmed = withoutServerDeletions(diff, () => [serverBlob]);

        expect(rangesOf(dsOf(trimmed))).toBe(0);
        expect(Y.parseUpdateMeta(trimmed)).toEqual(Y.parseUpdateMeta(diff));
        const peer = applyAll(new Y.Doc(), [serverBlob, trimmed, gapFiller]);
        expect(peer.getText('t').toString()).toBe(text.toString());
    });

    it('property: server blobs + trimmed diff reproduce the local document and its delete-set', () => {
        const SEEDS = 200;
        let trimmedSeeds = 0;
        let droppedRanges = 0;

        for (let seed = 1; seed <= SEEDS; seed++) {
            const rng = new SeededRandom(seed * 7919);
            const edit = (doc: Y.Doc) => {
                const text = doc.getText('t');
                const map = doc.getMap('m');
                const arr = doc.getArray('a');
                switch (rng.int(0, 6)) {
                    case 0:
                    case 1:
                        text.insert(rng.int(0, text.length), rng.string(rng.int(1, 6)));
                        break;
                    case 2:
                        if (text.length > 0) {
                            const at = rng.int(0, text.length - 1);
                            text.delete(at, rng.int(1, Math.min(4, text.length - at)));
                        }
                        break;
                    case 3:
                        map.set(`k${rng.int(0, 4)}`, rng.int(0, 1000)); // overwrite = deletion
                        break;
                    case 4:
                        map.delete(`k${rng.int(0, 4)}`);
                        break;
                    case 5:
                        arr.insert(rng.int(0, arr.length), [rng.string(3)]);
                        break;
                    case 6:
                        if (arr.length > 0) arr.delete(rng.int(0, arr.length - 1), 1);
                        break;
                }
            };

            // History: each session is a fresh client (versicle mints one
            // per launch) editing everything that came before it.
            const updates: Uint8Array[] = [];
            const sessions = rng.int(2, 10);
            for (let s = 0; s < sessions; s++) {
                const d = new Y.Doc();
                d.clientID = 1_000 * seed + s;
                applyAll(d, updates);
                const onUpdate = (u: Uint8Array) => updates.push(u);
                d.on('update', onUpdate);
                const ops = rng.int(1, 8);
                for (let i = 0; i < ops; i++) d.transact(() => edit(d));
                d.off('update', onUpdate);
                d.destroy();
            }

            // Server: a GC'd fold of a prefix (snapshot + fingerprint),
            // history segments, pending updates (some duplicating what a
            // segment holds), and possibly a tail it never received.
            const n = updates.length;
            const foldEnd = rng.int(0, n);
            const segEnd = rng.int(foldEnd, n);
            const pendingEnd = rng.int(segEnd, n);
            const fold = foldEnd > 0 ? mergeUpdatesWithMeta(updates.slice(0, foldEnd), { gc: true }) : null;
            const segments: Uint8Array[] = [];
            for (let i = foldEnd; i < segEnd;) {
                const end = Math.min(segEnd, i + rng.int(1, 4));
                segments.push(mergeUpdatesWithMeta(updates.slice(i, end), { gc: false }).result);
                i = end;
            }
            const pending = updates.slice(segEnd, pendingEnd);
            for (let i = foldEnd; i < segEnd; i++) {
                if (rng.bool(0.2)) pending.push(updates[i]);
            }

            const proofBlobs = [...(fold ? [fold.dsUpdate] : []), ...segments, ...pending];
            const items: PendingUpdate[] = [
                ...(fold ? [{ type: 'update' as const, data: { update: Bytes.fromUint8Array(fold.dsUpdate) }, priority: 2 }] : []),
                ...segments.map((seg): PendingUpdate => ({ type: 'history', data: { segment: Bytes.fromUint8Array(seg) }, priority: 2 })),
                ...pending.map((u): PendingUpdate => ({ type: 'update', data: { update: Bytes.fromUint8Array(u) }, priority: 3 })),
            ];
            const snapshotSV = fold ? Y.decodeStateVector(fold.stateVector) : new Map<number, number>();
            const serverSV = writeStateVector(buildServerCoverage(snapshotSV, items));

            // Reconnecting client: everything, plus offline edits by a
            // fresh client (inserts, deletions of server content, overwrites).
            const local = applyAll(new Y.Doc(), updates);
            local.clientID = 999_000_000 + seed;
            const offlineOps = rng.int(0, 4);
            for (let i = 0; i < offlineOps; i++) local.transact(() => edit(local));

            const diff = Y.encodeStateAsUpdate(local, serverSV);
            const trimmed = withoutServerDeletions(diff, () => proofBlobs);

            // Correctness: content and delete-set both reach the peer.
            const peer = applyAll(new Y.Doc(), [...(fold ? [fold.result, fold.dsUpdate] : []), ...segments, ...pending, trimmed]);
            expect((peer.store as any).pendingStructs, `seed ${seed}`).toBeNull();
            expect(docState(peer), `seed ${seed}`).toBe(docState(local));
            expect(Y.equalDeleteSets(storeDs(peer), storeDs(local)), `seed ${seed}: delete-sets differ`).toBe(true);

            // Structs section untouched; never larger.
            expect(Y.parseUpdateMeta(trimmed), `seed ${seed}`).toEqual(Y.parseUpdateMeta(diff));
            expect(trimmed.byteLength).toBeLessThanOrEqual(diff.byteLength);

            // union(server, pushed) contains the diff's delete-set, and
            // every range the server's union contains was dropped.
            const serverDs = Y.mergeDeleteSets(proofBlobs.map(dsOf));
            const pushedDs = dsOf(trimmed);
            expect(dsContains(Y.mergeDeleteSets([clone(serverDs), pushedDs]), dsOf(diff)), `seed ${seed}`).toBe(true);
            pushedDs.clients.forEach((items, client) => {
                for (const item of items) {
                    const single = Y.createDeleteSet();
                    single.clients.set(client, [{ clock: item.clock, len: item.len }]);
                    expect(dsContains(serverDs, single), `seed ${seed}: kept a range the server holds`).toBe(false);
                }
            });

            if (trimmed !== diff) {
                trimmedSeeds++;
                droppedRanges += rangesOf(dsOf(diff)) - rangesOf(pushedDs);
            }
            local.destroy();
            peer.destroy();
        }

        // The property is not vacuous: most histories had deletions to drop.
        expect(trimmedSeeds).toBeGreaterThan(SEEDS / 2);
        expect(droppedRanges).toBeGreaterThan(SEEDS);
    });
});
