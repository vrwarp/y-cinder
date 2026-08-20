/**
 * Unit tests for diffCarriesNewData.
 *
 * Yjs embeds the document's complete delete-set in every diff produced by
 * encodeStateAsUpdate(doc, sv); diffCarriesNewData decides whether such a
 * diff actually carries data (structs or uncovered deletions) the server
 * is missing.
 *
 * @file diff-new-data.test.ts
 */

import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { diffCarriesNewData, deleteSetCoveredByBlobs } from '../../src/update-metadata';

describe('diffCarriesNewData', () => {
    it('returns true when the diff contains structs', () => {
        const doc = new Y.Doc();
        doc.getText('t').insert(0, 'hello');
        const diff = Y.encodeStateAsUpdate(doc); // vs empty SV → all structs

        expect(diffCarriesNewData(diff, () => [])).toBe(true);
    });

    it('returns false for a delete-set-only diff fully covered by server blobs', () => {
        const doc = new Y.Doc();
        doc.getText('t').insert(0, 'hello world');
        doc.getText('t').delete(0, 6);

        // Server holds the full document state (structs + delete-set)
        const serverBlob = Y.encodeStateAsUpdate(doc);

        // Diff against a server that has everything → structs empty, full DS
        const diff = Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc));
        expect(diff.byteLength).toBeGreaterThan(2); // the bug trigger

        expect(diffCarriesNewData(diff, () => [serverBlob])).toBe(false);
    });

    it('returns true when local has deletions the server lacks', () => {
        const doc = new Y.Doc();
        doc.getText('t').insert(0, 'hello world');

        // Server snapshot was taken BEFORE the deletion
        const serverBlob = Y.encodeStateAsUpdate(doc);

        // Offline deletion: no new structs, only delete-set entries
        doc.getText('t').delete(0, 6);
        const diff = Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc));

        expect(diffCarriesNewData(diff, () => [serverBlob])).toBe(true);
    });

    it('returns false when deletions are covered across multiple server blobs', () => {
        const doc = new Y.Doc();
        doc.getText('t').insert(0, 'abcdef');
        const blob1 = Y.encodeStateAsUpdate(doc); // structs

        const sv1 = Y.encodeStateVector(doc);
        doc.getText('t').delete(0, 3);
        const blob2 = Y.encodeStateAsUpdate(doc, sv1); // deletion arrived later

        const diff = Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc));
        expect(diffCarriesNewData(diff, () => [blob1, blob2])).toBe(false);
    });

    it('returns true (pushes) when the diff is unparseable', () => {
        const garbage = new Uint8Array([255, 254, 253, 252]);
        expect(diffCarriesNewData(garbage, () => [])).toBe(true);
    });

    it('ignores corrupted server blobs (under-approximates coverage)', () => {
        const doc = new Y.Doc();
        doc.getText('t').insert(0, 'hello');
        doc.getText('t').delete(0, 2);
        const diff = Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc));

        const garbageBlob = new Uint8Array([255, 254, 253]);
        // Only corrupted blobs → no coverage proof → must push
        expect(diffCarriesNewData(diff, () => [garbageBlob])).toBe(true);

        // Valid blob alongside the corrupted one → covered
        const validBlob = Y.encodeStateAsUpdate(doc);
        expect(diffCarriesNewData(diff, () => [garbageBlob, validBlob])).toBe(false);
    });
});

describe('deleteSetCoveredByBlobs (encode-free reconnect fast path)', () => {
    it('agrees with the diff-based guard, using the store delete-set directly', () => {
        const doc = new Y.Doc();
        const map = doc.getMap('m');
        for (let i = 0; i < 50; i++) {
            map.set('k' + (i % 10), i); // overwrite churn → deletions
        }
        map.delete('k3');

        const serverBlob = Y.encodeStateAsUpdate(doc);
        const localDs = Y.createDeleteSetFromStructStore((doc as any).store);

        // Server holds everything → covered (no push), no diff encode needed
        expect(deleteSetCoveredByBlobs(localDs, () => [serverBlob])).toBe(true);

        // Now delete something the server has not seen
        map.delete('k7');
        const newerDs = Y.createDeleteSetFromStructStore((doc as any).store);
        expect(deleteSetCoveredByBlobs(newerDs, () => [serverBlob])).toBe(false);

        // The legacy diff-based guard must agree in both directions
        const diff = Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc));
        expect(diffCarriesNewData(diff, () => [serverBlob])).toBe(true);
        doc.destroy();
    });

    it('proves coverage from a structs-empty fingerprint alone', () => {
        const doc = new Y.Doc();
        const text = doc.getText('t');
        text.insert(0, 'aging document with deletions');
        text.delete(0, 6);
        text.delete(5, 3);

        // The fingerprint compaction stores: structs-empty, full DS
        const fingerprint = Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc));
        const localDs = Y.createDeleteSetFromStructStore((doc as any).store);

        expect(deleteSetCoveredByBlobs(localDs, () => [fingerprint])).toBe(true);
        doc.destroy();
    });

    it('empty local delete-set is covered without decoding any blob', () => {
        const doc = new Y.Doc();
        doc.getMap('m').set('k', 1); // no deletions
        const localDs = Y.createDeleteSetFromStructStore((doc as any).store);
        let called = false;
        expect(deleteSetCoveredByBlobs(localDs, () => { called = true; return []; })).toBe(true);
        expect(called).toBe(false);
        doc.destroy();
    });
});
