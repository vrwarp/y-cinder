/**
 * Unit tests for the squash protocol's preconditions.
 *
 * A squash rebuilds the document into a new id space, so squashing while
 * the local client is behind the server silently DROPS whatever it had not
 * yet received — no error, no retry, no way to notice afterwards. These
 * checks are the only thing preventing that, and they previously ran only
 * inside squashDocument between Firestore reads.
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { toBase64 } from 'lib0/buffer';
import {
    isNotQuiescent,
    isSquashPreempted,
    localCoversDeletions,
    localCoversPendingDoc,
    readVersionEpoch,
    squashSnapshotPath,
    stateVectorCovers,
    stillHoldsLock,
} from '../../src/squash-policy';

const docWith = (clientID: number, edits: number) => {
    const doc = new Y.Doc();
    doc.clientID = clientID;
    const map = doc.getMap('m');
    for (let i = 0; i < edits; i += 1) {
        doc.transact(() => map.set(`k${i}`, i));
    }
    return doc;
};

const svOf = (doc: Y.Doc) => Y.decodeStateVector(Y.encodeStateVector(doc));
const svB64 = (doc: Y.Doc) => toBase64(Y.encodeStateVector(doc));

describe('stateVectorCovers', () => {
    it('covers a remote vector the local doc is level with', () => {
        const doc = docWith(1, 3);

        expect(stateVectorCovers(svOf(doc), svB64(doc))).toBe(true);
    });

    it('covers a remote vector the local doc is ahead of', () => {
        expect(stateVectorCovers(svOf(docWith(1, 9)), svB64(docWith(1, 2)))).toBe(true);
    });

    it('does not cover a remote vector that is ahead', () => {
        expect(stateVectorCovers(svOf(docWith(1, 2)), svB64(docWith(1, 9)))).toBe(false);
    });

    it('does not cover a client the local doc has never seen', () => {
        expect(stateVectorCovers(svOf(docWith(1, 5)), svB64(docWith(42, 1)))).toBe(false);
    });

    it('treats an absent state vector as nothing to be behind of', () => {
        expect(stateVectorCovers(new Map(), undefined)).toBe(true);
        expect(stateVectorCovers(new Map(), '')).toBe(true);
    });

    /*
     * An unparseable vector must block the squash. Treating it as covered
     * would let a squash proceed against data we cannot prove we hold.
     */
    it('refuses to claim coverage of an unparseable state vector', () => {
        expect(stateVectorCovers(svOf(docWith(1, 5)), 'not-base64-!!')).toBe(false);
    });
});

describe('localCoversPendingDoc', () => {
    const local = new Map<number, number>([[1, 10]]);

    it('covers a document whose per-client clocks are already held', () => {
        expect(localCoversPendingDoc(local, { clientIDs: [1], clientClocks: [5] })).toBe(true);
    });

    it('does not cover a document carrying a newer clock', () => {
        expect(localCoversPendingDoc(local, { clientIDs: [1], clientClocks: [50] })).toBe(false);
    });

    it('checks every client in the list, not just the first', () => {
        expect(localCoversPendingDoc(local, { clientIDs: [1, 2], clientClocks: [5, 1] })).toBe(false);
    });

    it('falls back to the state vector when clock arrays are absent', () => {
        expect(localCoversPendingDoc(svOf(docWith(1, 2)), { stateVector: svB64(docWith(1, 1)) })).toBe(true);
        expect(localCoversPendingDoc(svOf(docWith(1, 1)), { stateVector: svB64(docWith(1, 9)) })).toBe(false);
    });

    it('ignores mismatched clock arrays and falls through', () => {
        // Lengths disagree, so the clocks cannot be trusted; with no state
        // vector and a payload present, this must block.
        expect(localCoversPendingDoc(local, { clientIDs: [1, 2], clientClocks: [5], update: 'bytes' }))
            .toBe(false);
    });

    /*
     * The conservative case: a document that carries data but offers no
     * metadata to verify it against must block the squash.
     */
    it.each(['update', 'segment', 'updateStoragePath'])(
        'refuses to assume coverage of an unverifiable %s payload',
        (field) => {
            expect(localCoversPendingDoc(local, { [field]: 'something' })).toBe(false);
        },
    );

    it('covers an empty document that carries nothing at all', () => {
        expect(localCoversPendingDoc(local, {})).toBe(true);
        expect(localCoversPendingDoc(local, null)).toBe(true);
    });
});

/*
 * State vectors do not advance on deletion, so a squasher that missed a
 * server-side deletion passes every state-vector check. This is the check
 * that keeps the squashed epoch from resurrecting that content.
 */
describe('localCoversDeletions', () => {
    /** Writer with 'abcdefghij' as one struct; returns the doc and its insert. */
    const writer = () => {
        const doc = new Y.Doc();
        doc.clientID = 1;
        doc.getText('t').insert(0, 'abcdefghij');
        return { doc, insert: Y.encodeStateAsUpdate(doc) };
    };
    const replica = (...updates: Uint8Array[]) => {
        const doc = new Y.Doc();
        doc.clientID = 2;
        updates.forEach(u => Y.applyUpdate(doc, u));
        return doc;
    };
    /** Applies `edit` to `doc` and returns just that change as an update. */
    const change = (doc: Y.Doc, edit: () => void) => {
        const before = Y.encodeStateVector(doc);
        edit();
        return Y.encodeStateAsUpdate(doc, before);
    };

    it('covers a blob whose deletions are all applied locally', () => {
        const { doc, insert } = writer();
        const deletion = change(doc, () => doc.getText('t').delete(0, 6));

        expect(localCoversDeletions(replica(insert, deletion), deletion)).toBe(true);
    });

    it('does not cover a delete-only update the local doc never applied', () => {
        const { doc, insert } = writer();
        const deletion = change(doc, () => doc.getText('t').delete(0, 6));

        expect(localCoversDeletions(replica(insert), deletion)).toBe(false);
    });

    it('reads deletions from a structs-empty delete-set fingerprint', () => {
        const { doc, insert } = writer();
        doc.getText('t').delete(0, 6);
        const fingerprint = Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc));
        expect(Y.decodeUpdate(fingerprint).structs).toHaveLength(0);

        expect(localCoversDeletions(replica(insert), fingerprint)).toBe(false);
        expect(localCoversDeletions(replica(Y.encodeStateAsUpdate(doc)), fingerprint)).toBe(true);
    });

    it('checks every local struct a deleted range spans', () => {
        const { doc, insert } = writer();
        const first = change(doc, () => doc.getText('t').delete(0, 3));
        const second = change(doc, () => doc.getText('t').delete(0, 3));
        const both = Y.mergeUpdates([first, second]);

        // Locally 'abc' is a deleted struct and 'def…' a live one
        expect(localCoversDeletions(replica(insert, first), both)).toBe(false);
        expect(localCoversDeletions(replica(insert, first, second), both)).toBe(true);
    });

    /*
     * The clone cannot resurrect a struct it does not hold, and holding
     * fewer structs than the server is the state-vector checks' job.
     */
    it('ignores deletions of structs the local doc does not hold', () => {
        const doc = new Y.Doc();
        doc.clientID = 1;
        const first = change(doc, () => doc.getText('t').insert(0, 'abcde'));
        change(doc, () => doc.getText('t').insert(5, 'fghij'));
        const tail = change(doc, () => doc.getText('t').delete(5, 5));
        const deletion = Y.mergeUpdates([tail, change(doc, () => doc.getText('t').delete(0, 5))]);

        // Never saw client 1 at all
        expect(localCoversDeletions(replica(), deletion)).toBe(true);
        // Holds only 'abcde': 'fghij' is beyond its state...
        expect(localCoversDeletions(replica(first), tail)).toBe(true);
        // ...but a live 'abcde' is still caught
        expect(localCoversDeletions(replica(first), deletion)).toBe(false);
        const partial = replica(first);
        partial.getText('t').delete(0, 5);
        expect(localCoversDeletions(partial, deletion)).toBe(true);
    });

    it('covers a blob that carries no deletions', () => {
        const { insert } = writer();

        expect(localCoversDeletions(replica(insert), insert)).toBe(true);
    });

    it('refuses to claim coverage of an unparseable blob', () => {
        expect(localCoversDeletions(replica(), new Uint8Array([255, 255, 255]))).toBe(false);
    });
});

describe('isNotQuiescent', () => {
    const base = { updateCount: 0, historyCount: 0, maxUpdates: 100, maxHistory: 20 };

    it('is quiescent at or below both ceilings', () => {
        expect(isNotQuiescent({ ...base, updateCount: 100, historyCount: 20 })).toBe(false);
    });

    it('is not quiescent one past the update ceiling', () => {
        expect(isNotQuiescent({ ...base, updateCount: 101 })).toBe(true);
    });

    it('is not quiescent one past the history ceiling', () => {
        expect(isNotQuiescent({ ...base, historyCount: 21 })).toBe(true);
    });

    it('is quiescent when both are empty', () => {
        expect(isNotQuiescent(base)).toBe(false);
    });
});

describe('readVersionEpoch', () => {
    it('reads both fields', () => {
        expect(readVersionEpoch({ version: 4, epoch: 2 })).toEqual({ version: 4, epoch: 2 });
    });

    it('defaults a missing document to zero/zero', () => {
        expect(readVersionEpoch(undefined)).toEqual({ version: 0, epoch: 0 });
        expect(readVersionEpoch(null)).toEqual({ version: 0, epoch: 0 });
        expect(readVersionEpoch({})).toEqual({ version: 0, epoch: 0 });
    });

    it('ignores non-numeric values', () => {
        expect(readVersionEpoch({ version: '4', epoch: null })).toEqual({ version: 0, epoch: 0 });
    });

    it('preserves explicit zeros', () => {
        expect(readVersionEpoch({ version: 0, epoch: 0 })).toEqual({ version: 0, epoch: 0 });
    });
});

describe('isSquashPreempted', () => {
    const expected = { version: 3, epoch: 1 };

    it('is not preempted when nothing moved', () => {
        expect(isSquashPreempted({ version: 3, epoch: 1 }, expected)).toBe(false);
    });

    it('is preempted when the version moved (someone compacted)', () => {
        expect(isSquashPreempted({ version: 4, epoch: 1 }, expected)).toBe(true);
    });

    it('is preempted when the epoch moved (someone squashed)', () => {
        expect(isSquashPreempted({ version: 3, epoch: 2 }, expected)).toBe(true);
    });

    it('is preempted when either moved backwards too', () => {
        expect(isSquashPreempted({ version: 2, epoch: 1 }, expected)).toBe(true);
        expect(isSquashPreempted({ version: 3, epoch: 0 }, expected)).toBe(true);
    });
});

describe('stillHoldsLock', () => {
    it('holds when the owner matches', () => {
        expect(stillHoldsLock({ owner: 'me' }, 'me')).toBe(true);
    });

    it('does not hold when another client took it', () => {
        expect(stillHoldsLock({ owner: 'other' }, 'me')).toBe(false);
    });

    it('does not hold when the lock document is gone', () => {
        expect(stillHoldsLock(undefined, 'me')).toBe(false);
        expect(stillHoldsLock(null, 'me')).toBe(false);
    });

    it('does not hold when the owner field is missing', () => {
        expect(stillHoldsLock({}, 'me')).toBe(false);
    });
});

describe('squashSnapshotPath', () => {
    it('includes epoch, version and attempt so blobs never collide', () => {
        expect(squashSnapshotPath('docs/a', 2, 7, 'x1')).toBe('docs/a/snapshot_e2_v7_x1.bin');
    });

    it('distinguishes epochs at the same version', () => {
        expect(squashSnapshotPath('docs/a', 1, 7, 'x1')).not.toBe(squashSnapshotPath('docs/a', 2, 7, 'x1'));
    });

    it('distinguishes versions within an epoch', () => {
        expect(squashSnapshotPath('docs/a', 2, 7, 'x1')).not.toBe(squashSnapshotPath('docs/a', 2, 8, 'x1'));
    });

    /* A squasher whose lease lapsed mid-upload must not hit the winner's blob. */
    it('distinguishes attempts squashing from the same version', () => {
        expect(squashSnapshotPath('docs/a', 2, 7, 'x1')).not.toBe(squashSnapshotPath('docs/a', 2, 7, 'x2'));
    });
});
