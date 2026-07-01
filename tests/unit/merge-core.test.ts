/**
 * Unit tests for merge-core: GC-aware merging used by compaction.
 *
 * Safety properties under test:
 * 1. GC preserves everything sync correctness depends on: text content,
 *    state vector, and delete-set — only deleted-item *content* is dropped.
 * 2. Updates with missing dependencies (Yjs pending structs) are returned
 *    unchanged instead of being rebuilt, which would silently drop the
 *    queued data.
 * 3. Plain merge (gc: false / omitted) is byte-identical to Y.mergeUpdates.
 * 4. GC actually shrinks a high-churn document's snapshot.
 */

import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { mergeUpdatesCore, gcMergedUpdate } from '../../src/merge-core';

/** Builds a doc with contiguous churn and returns its incremental updates. */
function churnedUpdates(): Uint8Array[] {
    const doc = new Y.Doc();
    doc.clientID = 4242;
    const blobs: Uint8Array[] = [];
    doc.on('update', (u: Uint8Array) => blobs.push(u));
    const t = doc.getText('t');
    for (let i = 0; i < 100; i++) {
        t.insert(t.length, `sentence number ${i} with content `);
        if (t.length > 400) t.delete(0, 200); // contiguous stale-range removal
    }
    doc.destroy();
    return blobs;
}

function textOf(update: Uint8Array): string {
    const doc = new Y.Doc();
    Y.applyUpdate(doc, update);
    const s = doc.getText('t').toString();
    doc.destroy();
    return s;
}

describe('mergeUpdatesCore', () => {
    it('without gc is byte-identical to Y.mergeUpdates', () => {
        const blobs = churnedUpdates();
        expect(mergeUpdatesCore(blobs)).toEqual(Y.mergeUpdates(blobs));
        expect(mergeUpdatesCore(blobs, { gc: false })).toEqual(Y.mergeUpdates(blobs));
    });

    it('with gc shrinks a churned document while preserving content, SV, and DS', () => {
        const blobs = churnedUpdates();
        const plain = Y.mergeUpdates(blobs);
        const gcd = mergeUpdatesCore(blobs, { gc: true });

        // Shrinks: deleted content dominates this document's history
        expect(gcd.byteLength).toBeLessThan(plain.byteLength * 0.7);

        // Same live content
        expect(textOf(gcd)).toBe(textOf(plain));

        // Same state vector (sync redundancy checks depend on this)
        expect(Y.encodeStateVectorFromUpdate(gcd))
            .toEqual(Y.encodeStateVectorFromUpdate(plain));

        // Same delete-set coverage (reconnect push guard depends on this)
        const dsPlain = Y.decodeUpdate(plain).ds;
        const dsGcd = Y.decodeUpdate(gcd).ds;
        expect(Y.equalDeleteSets(dsPlain, dsGcd)).toBe(true);
    });

    it('converges with a client holding the full un-GCd history', () => {
        const blobs = churnedUpdates();
        const gcd = mergeUpdatesCore(blobs, { gc: true });

        const full = new Y.Doc();
        Y.applyUpdate(full, Y.mergeUpdates(blobs));
        const fresh = new Y.Doc();
        Y.applyUpdate(fresh, gcd);

        fresh.getText('t').insert(0, 'NEW-');
        Y.applyUpdate(full, Y.encodeStateAsUpdate(fresh, Y.encodeStateVector(full)));
        Y.applyUpdate(fresh, Y.encodeStateAsUpdate(full, Y.encodeStateVector(fresh)));

        expect(full.getText('t').toString()).toBe(fresh.getText('t').toString());
        full.destroy();
        fresh.destroy();
    });
});

describe('mergeUpdatesCore with nested types (arrays of Y.Maps)', () => {
    /**
     * Deleting a nested type (e.g. a Y.Map row inside a Y.Array) takes a
     * different GC path than in-place edits: the whole subtree is replaced
     * by GC id-range structs (parentGCd). This covers array move
     * (delete + re-insert) and delete/recreate workloads.
     */
    function arrayChurnUpdates(): Uint8Array[] {
        const doc = new Y.Doc();
        doc.clientID = 555;
        const blobs: Uint8Array[] = [];
        doc.on('update', (u: Uint8Array) => blobs.push(u));
        const a = doc.getArray<Y.Map<any>>('rows');
        const makeRow = (id: number, i: number) => {
            const m = new Y.Map<any>();
            m.set('id', id);
            m.set('v', `value ${id} revision ${i} with some content payload`);
            return m;
        };
        doc.transact(() => {
            for (let i = 0; i < 20; i++) a.push([makeRow(i, 0)]);
        });
        // Churn: delete + recreate rows (the "move"/"replace" pattern)
        for (let i = 0; i < 80; i++) {
            const idx = i % a.length;
            doc.transact(() => {
                const id = a.get(idx).get('id');
                a.delete(idx, 1);
                a.insert(idx, [makeRow(id, i + 1)]);
            });
        }
        const json = JSON.stringify(a.toJSON());
        doc.destroy();
        return Object.assign(blobs, { json }) as Uint8Array[] & { json: string };
    }

    it('shrinks nested-type churn while preserving structure and content', () => {
        const blobs = arrayChurnUpdates() as Uint8Array[] & { json: string };
        const plain = Y.mergeUpdates(blobs);
        const gcd = mergeUpdatesCore(blobs, { gc: true });

        expect(gcd.byteLength).toBeLessThan(plain.byteLength * 0.7);

        const rebuild = (u: Uint8Array) => {
            const d = new Y.Doc();
            Y.applyUpdate(d, u);
            const s = JSON.stringify(d.getArray('rows').toJSON());
            d.destroy();
            return s;
        };
        expect(rebuild(gcd)).toBe(rebuild(plain));
        expect(rebuild(gcd)).toBe(blobs.json);
        expect(Y.encodeStateVectorFromUpdate(gcd))
            .toEqual(Y.encodeStateVectorFromUpdate(plain));
    });

    it('converges with an un-GCd client after concurrent nested edits', () => {
        const blobs = arrayChurnUpdates();
        const full = new Y.Doc();
        Y.applyUpdate(full, Y.mergeUpdates(blobs));
        const fresh = new Y.Doc();
        Y.applyUpdate(fresh, mergeUpdatesCore(blobs, { gc: true }));

        (fresh.getArray('rows').get(0) as Y.Map<any>).set('v', 'fresh-edit');
        (full.getArray('rows').get(3) as Y.Map<any>).set('v', 'full-edit');
        Y.applyUpdate(full, Y.encodeStateAsUpdate(fresh, Y.encodeStateVector(full)));
        Y.applyUpdate(fresh, Y.encodeStateAsUpdate(full, Y.encodeStateVector(fresh)));

        expect(JSON.stringify(fresh.getArray('rows').toJSON()))
            .toBe(JSON.stringify(full.getArray('rows').toJSON()));
        full.destroy();
        fresh.destroy();
    });
});

describe('gcMergedUpdate', () => {
    it('returns the input unchanged when dependencies are missing', () => {
        const blobs = churnedUpdates();
        // Drop the first half: the merged blob now references structs
        // whose dependencies are absent (pendingStructs on apply)
        const gapped = Y.mergeUpdates(blobs.slice(Math.floor(blobs.length / 2)));

        // Sanity: this really does queue pending structs
        const probe = new Y.Doc({ gc: true });
        Y.applyUpdate(probe, gapped);
        expect((probe.store as any).pendingStructs).not.toBeNull();
        probe.destroy();

        expect(gcMergedUpdate(gapped)).toBe(gapped);
    });

    it('never returns a larger update than its input', () => {
        // A tiny, churn-free doc: re-encoding cannot win, must not lose
        const doc = new Y.Doc();
        doc.getText('t').insert(0, 'ab');
        const merged = Y.mergeUpdates([Y.encodeStateAsUpdate(doc)]);
        doc.destroy();

        const result = gcMergedUpdate(merged);
        expect(result.byteLength).toBeLessThanOrEqual(merged.byteLength);
        expect(textOf(result)).toBe('ab');
    });
});
