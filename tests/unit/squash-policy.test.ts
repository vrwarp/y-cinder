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
    it('includes both epoch and version so blobs never collide', () => {
        expect(squashSnapshotPath('docs/a', 2, 7)).toBe('docs/a/snapshot_e2_v7.bin');
    });

    it('distinguishes epochs at the same version', () => {
        expect(squashSnapshotPath('docs/a', 1, 7)).not.toBe(squashSnapshotPath('docs/a', 2, 7));
    });

    it('distinguishes versions within an epoch', () => {
        expect(squashSnapshotPath('docs/a', 2, 7)).not.toBe(squashSnapshotPath('docs/a', 2, 8));
    });
});
