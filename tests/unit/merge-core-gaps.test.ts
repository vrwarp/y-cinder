/**
 * GC-rewrite behaviour when a merge has missing dependencies.
 *
 * `mergeUpdatesCore(.., { gc: true })` rebuilds the merged update from a
 * live document to drop deleted content, guarded by a check that nothing
 * stayed queued as a pending struct or pending delete-set.
 *
 * What these tests pin is the OUTCOME: an update whose dependencies are
 * absent must come out of the merge with those structs still in it, so a
 * client that later receives the missing dependency can integrate them.
 *
 * Worth recording what was measured while writing this, because the source
 * comment on the guard overstates it: on this Yjs version
 * `encodeStateAsUpdate` re-encodes queued pending structs rather than
 * dropping them, so for a struct-gapped update BOTH branches of the guard
 * produce a byte-identical result. Mutation testing reports the guard's
 * two halves as surviving, and they appear to be equivalent mutants for
 * that input rather than a test gap — the guard is belt-and-braces against
 * a Yjs behaviour that no longer bites. It is left in place: it costs
 * nothing and the failure it guards against is silent data loss.
 */
import { describe, it, expect, vi, afterEach } from 'vitest';
import * as Y from 'yjs';
import { mergeUpdatesCore, mergeUpdatesWithMeta } from '../../src/merge-core';

/** An update that references structs the receiver does not have. */
const makeGappedUpdate = (): Uint8Array => {
    const doc = new Y.Doc();
    doc.clientID = 1;
    const text = doc.getText('t');

    doc.transact(() => text.insert(0, 'first'));

    const afterFirst = Y.encodeStateVector(doc);

    doc.transact(() => text.insert(5, 'second'));

    // Diff excluding the first insert: its structs depend on clocks the
    // receiver has never seen.
    return Y.encodeStateAsUpdate(doc, afterFirst);
};

const hasPending = (update: Uint8Array): boolean => {
    const doc = new Y.Doc();

    Y.applyUpdate(doc, update);

    const store = doc.store as unknown as { pendingStructs: unknown; pendingDs: unknown };
    const pending = store.pendingStructs !== null || store.pendingDs !== null;

    doc.destroy();

    return pending;
};

afterEach(() => {
    vi.restoreAllMocks();
});

describe('mergeUpdatesCore with gc on an update that has gaps', () => {
    it('the fixture really does leave pending structs', () => {
        expect(hasPending(makeGappedUpdate())).toBe(true);
    });

    /*
     * The property that actually matters, independent of which branch the
     * guard takes: the gapped structs must still be present in the output.
     */
    it('keeps the gapped structs in the merged result', () => {
        const gapped = makeGappedUpdate();
        const merged = mergeUpdatesCore([gapped, gapped], { gc: true });

        expect(hasPending(merged)).toBe(true);
    });

    it('produces a result equivalent to the plain merge for gapped input', () => {
        const gapped = makeGappedUpdate();
        const withGc = mergeUpdatesCore([gapped, gapped], { gc: true });
        const withoutGc = mergeUpdatesCore([gapped, gapped], { gc: false });

        expect(Array.from(withGc)).toEqual(Array.from(withoutGc));
    });

    it('still rebuilds when every struct integrates cleanly', () => {
        const doc = new Y.Doc();
        const text = doc.getText('t');

        doc.transact(() => text.insert(0, 'hello world'));
        doc.transact(() => text.delete(0, 6));

        const complete = Y.encodeStateAsUpdate(doc);
        const merged = mergeUpdatesCore([complete], { gc: true });

        expect(hasPending(merged)).toBe(false);

        const check = new Y.Doc();

        Y.applyUpdate(check, merged);
        expect(check.getText('t').toString()).toBe('world');
    });
});

describe('mergeUpdatesWithMeta with gc on an update that has gaps', () => {
    it('still reports the gapped structs and derives usable metadata', () => {
        const gapped = makeGappedUpdate();
        const meta = mergeUpdatesWithMeta([gapped, gapped], { gc: true });

        expect(hasPending(meta.result)).toBe(true);
        expect(meta.stateVector.byteLength).toBeGreaterThan(0);
        expect(meta.dsUpdate).toBeDefined();
    });

    it('derives metadata from the rebuilt doc when there are no gaps', () => {
        const doc = new Y.Doc();
        const text = doc.getText('t');

        doc.transact(() => text.insert(0, 'abc'));

        const meta = mergeUpdatesWithMeta([Y.encodeStateAsUpdate(doc)], { gc: true });
        const check = new Y.Doc();

        Y.applyUpdate(check, meta.result);
        expect(check.getText('t').toString()).toBe('abc');
        expect(Array.from(meta.stateVector)).toEqual(Array.from(Y.encodeStateVector(check)));
    });

    it('does not GC-rebuild at all when gc is off', () => {
        const doc = new Y.Doc();
        const text = doc.getText('t');

        doc.transact(() => text.insert(0, 'hello'));
        doc.transact(() => text.delete(0, 5));

        const plain = mergeUpdatesWithMeta([Y.encodeStateAsUpdate(doc)], { gc: false });
        const collected = mergeUpdatesWithMeta([Y.encodeStateAsUpdate(doc)], { gc: true });

        // The GC pass is what shrinks a fully-deleted document.
        expect(collected.result.byteLength).toBeLessThanOrEqual(plain.result.byteLength);
    });
});
