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
