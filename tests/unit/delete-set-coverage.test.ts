/**
 * Regression tests: delete-set coverage must not treat local-only deletions
 * as already present on the server.
 *
 * Bug: deleteSetCoveredByBlobs() proves coverage with
 *   Y.equalDeleteSets(serverDs, Y.mergeDeleteSets([serverDs, localDs]))
 * but Y.mergeDeleteSets only shallow-copies the DeleteItem arrays and then
 * extends the left-hand DeleteItem in place while merging adjacent or
 * overlapping ranges. When a local deletion touches (or starts at the same
 * clock as) a deletion the server already has, the server's own DeleteItem
 * is stretched to cover the local range, so the "union equals server" check
 * compares a set with itself and reports coverage. The initial-sync fast
 * path (server covers every local struct, i.e. any deletion-only offline
 * edit) then decides there is nothing to push, and the deletion never
 * reaches the server or other devices.
 *
 * The contract asserted here: a local deletion that no server blob
 * contains is never reported as covered, and checking coverage does not
 * change the caller's answer for blobs it has not been shown.
 *
 * @file delete-set-coverage.test.ts
 */

import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { diffCarriesNewData, deleteSetCoveredByBlobs } from '../../src/update-metadata';

const localDeleteSet = (doc: Y.Doc) => Y.createDeleteSetFromStructStore((doc as any).store);

describe('delete-set coverage with deletions contiguous to server deletions', () => {
    it('reports an offline deletion adjacent to a server deletion as NOT covered (struct-store fast path)', () => {
        const doc = new Y.Doc();
        const text = doc.getText('t');
        text.insert(0, 'hello world');
        text.delete(0, 5); // 'hello' deleted and saved to the server
        const serverBlob = Y.encodeStateAsUpdate(doc);

        // Offline (no provider attached): delete ' world'. Clock range is
        // contiguous with the server's 'hello' deletion on the same client.
        text.delete(0, 6);
        expect(text.toString()).toBe('');

        // The server blob really lacks the second deletion
        const serverView = new Y.Doc();
        Y.applyUpdate(serverView, serverBlob);
        expect(serverView.getText('t').toString()).toBe(' world');

        expect(deleteSetCoveredByBlobs(localDeleteSet(doc), () => [serverBlob])).toBe(false);
    });

    it('reports a deletion-only diff adjacent to a server deletion as carrying new data', () => {
        const doc = new Y.Doc();
        const text = doc.getText('t');
        text.insert(0, 'hello world');
        text.delete(0, 5);
        const serverBlob = Y.encodeStateAsUpdate(doc);

        text.delete(0, 6);

        // Structs-empty diff against a server that has every struct
        const diff = Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc));
        expect(Y.decodeUpdate(diff).structs.length).toBe(0);

        expect(diffCarriesNewData(diff, () => [serverBlob])).toBe(true);
    });

    it('reports a deletion that extends the end of a server deletion as NOT covered', () => {
        const doc = new Y.Doc();
        const text = doc.getText('t');
        text.insert(0, 'abcdefghij');
        text.delete(2, 3); // 'cde' (clocks 2..4) is on the server
        const serverBlob = Y.encodeStateAsUpdate(doc);

        text.delete(2, 2); // offline: 'fg' (clocks 5..6), touching the end
        expect(text.toString()).toBe('abhij');

        expect(deleteSetCoveredByBlobs(localDeleteSet(doc), () => [serverBlob])).toBe(false);
    });

    it('does not let an earlier failed check inflate coverage for a later server blob', () => {
        const doc = new Y.Doc();
        doc.clientID = 1; // pinned so 'xyz' below always integrates after 'hello world'
        const text = doc.getText('t');
        text.insert(0, 'hello world');

        // Small server blob: only the 'hello' deletion (delete-set only)
        const svBefore = Y.encodeStateVector(doc);
        text.delete(0, 5);
        const helloDeletionOnly = Y.encodeStateAsUpdate(doc, svBefore);

        // A second, unrelated client's text that is deleted on the server too
        const other = new Y.Doc();
        other.clientID = 2;
        other.getText('t').insert(0, 'xyz');
        Y.applyUpdate(doc, Y.encodeStateAsUpdate(other));
        text.delete(text.length - 3, 3); // remove 'xyz' (other client's structs)
        expect(text.toString()).toBe(' world');

        // Larger server blob: full state including both deletions so far
        const fullServerState = Y.encodeStateAsUpdate(doc);
        expect(fullServerState.byteLength).toBeGreaterThan(helloDeletionOnly.byteLength);

        // Offline: delete ' world' — contiguous with the 'hello' deletion
        text.delete(0, 6);
        expect(text.toString()).toBe('');

        // Neither blob (nor their union) contains the ' world' deletion
        const serverView = new Y.Doc();
        Y.applyUpdate(serverView, fullServerState);
        Y.applyUpdate(serverView, helloDeletionOnly);
        expect(serverView.getText('t').toString()).toBe(' world');

        expect(
            deleteSetCoveredByBlobs(localDeleteSet(doc), () => [fullServerState, helloDeletionOnly])
        ).toBe(false);
    });

    it("does not mutate the caller's delete set", () => {
        const doc = new Y.Doc();
        const text = doc.getText('t');
        text.insert(0, 'hello world');
        const base = Y.encodeStateAsUpdate(doc);

        // The server deleted ' world' (clocks 5..10)
        const server = new Y.Doc();
        Y.applyUpdate(server, base);
        server.getText('t').delete(5, 6);
        const serverBlob = Y.encodeStateAsUpdate(server);

        // Locally only 'hello' (clocks 0..4) is deleted: the local
        // DeleteItem is the left-hand side of the merge with the server's
        text.delete(0, 5);
        const localDs = localDeleteSet(doc);
        const snapshot = (ds: typeof localDs) =>
            [...ds.clients].map(([client, items]) => [client, items.map(i => [i.clock, i.len])]);
        const before = snapshot(localDs);

        expect(deleteSetCoveredByBlobs(localDs, () => [serverBlob])).toBe(false);
        expect(snapshot(localDs)).toEqual(before);
    });
});
