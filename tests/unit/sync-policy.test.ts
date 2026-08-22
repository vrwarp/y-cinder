/**
 * Unit tests for the sync listener / initial-sync decisions.
 *
 * These pick which remote documents reach the local doc. A wrong answer is
 * silent in both directions: a document wrongly dropped is data the client
 * never receives, and a foreign-epoch document wrongly applied parks
 * unresolvable structs in the doc forever (which also disables GC).
 */
import { describe, it, expect } from 'vitest';
import {
    diffHasPayload,
    diffNeedsStorage,
    epochTag,
    hasMorePages,
    largeUpdatePath,
    orderByApplyPriority,
    serverCoversLocalStructs,
    pickPaginationCursorIndex,
    planIncomingUpdate,
    shouldTriggerCompaction,
    survivesEpochFence,
} from '../../src/sync-policy';

const ctx = (over: Partial<Parameters<typeof planIncomingUpdate>[1]> = {}) => ({
    uid: 'me',
    currentEpoch: 0,
    localSVMap: new Map<number, number>(),
    ...over,
});

describe('planIncomingUpdate', () => {
    it('applies an inline update from another client', () => {
        expect(planIncomingUpdate({ createdBy: 'other', update: 'bytes' }, ctx()))
            .toEqual({ kind: 'apply-inline' });
    });

    it('downloads a Storage-backed update', () => {
        expect(planIncomingUpdate({ createdBy: 'other', updateStoragePath: 'gs://u' }, ctx()))
            .toEqual({ kind: 'download', storagePath: 'gs://u' });
    });

    it('prefers the inline payload when both are present', () => {
        expect(planIncomingUpdate({ createdBy: 'other', update: 'b', updateStoragePath: 'gs://u' }, ctx()))
            .toEqual({ kind: 'apply-inline' });
    });

    /*
     * The epoch fence is first and absolute — it must win over every other
     * consideration, including our own writes and quarantine.
     */
    it('drops a foreign-epoch document ahead of every other check', () => {
        expect(planIncomingUpdate({ epoch: 1, createdBy: 'me', update: 'b' }, ctx({ currentEpoch: 0 })))
            .toEqual({ kind: 'drop-foreign-epoch' });
        expect(planIncomingUpdate({ epoch: 0, update: 'b' }, ctx({ currentEpoch: 2 })))
            .toEqual({ kind: 'drop-foreign-epoch' });
    });

    it('accepts a document on a matching non-zero epoch', () => {
        expect(planIncomingUpdate({ epoch: 3, createdBy: 'other', update: 'b' }, ctx({ currentEpoch: 3 })))
            .toEqual({ kind: 'apply-inline' });
    });

    /*
     * Our own writes are reported separately rather than merged into the
     * generic skip: the caller still folds their metadata into the cached
     * state vector, so collapsing the two would silently stale that cache.
     */
    it('reports our own write distinctly from other skips', () => {
        expect(planIncomingUpdate({ createdBy: 'me', update: 'b' }, ctx()))
            .toEqual({ kind: 'skip-own' });
    });

    it('skips an update the local state vector already covers', () => {
        const plan = planIncomingUpdate(
            { createdBy: 'other', clientIDs: [1], clientClocks: [5], update: 'b' },
            ctx({ localSVMap: new Map([[1, 10]]) }),
        );

        expect(plan).toEqual({ kind: 'skip-redundant' });
    });

    it('applies an update carrying a clock beyond the local one', () => {
        const plan = planIncomingUpdate(
            { createdBy: 'other', clientIDs: [1], clientClocks: [50], update: 'b' },
            ctx({ localSVMap: new Map([[1, 10]]) }),
        );

        expect(plan).toEqual({ kind: 'apply-inline' });
    });

    it('ignores empty clock metadata rather than treating it as covered', () => {
        expect(planIncomingUpdate(
            { createdBy: 'other', clientIDs: [], clientClocks: [], update: 'b' },
            ctx(),
        )).toEqual({ kind: 'apply-inline' });
    });

    it('skips a quarantined document', () => {
        expect(planIncomingUpdate(
            { createdBy: 'other', update: 'b' },
            ctx({ docId: 'poison', corruptedDocIds: new Set(['poison']) }),
        )).toEqual({ kind: 'skip-quarantined' });
    });

    it('does not skip a document whose id is not quarantined', () => {
        expect(planIncomingUpdate(
            { createdBy: 'other', update: 'b' },
            ctx({ docId: 'fine', corruptedDocIds: new Set(['poison']) }),
        )).toEqual({ kind: 'apply-inline' });
    });

    it('skips a document with no payload at all', () => {
        expect(planIncomingUpdate({ createdBy: 'other' }, ctx())).toEqual({ kind: 'skip-empty' });
        expect(planIncomingUpdate(null, ctx())).toEqual({ kind: 'skip-empty' });
    });
});

describe('shouldTriggerCompaction', () => {
    const base = {
        size: 100,
        maxUpdatesThreshold: 50,
        now: 100_000,
        lastTriggerAt: 0,
        cooldownMs: 10_000,
        hardCap: 500,
    };

    it('does not trigger at or below the threshold', () => {
        expect(shouldTriggerCompaction({ ...base, size: 50 })).toBe(false);
        expect(shouldTriggerCompaction({ ...base, size: 49 })).toBe(false);
    });

    it('triggers just above the threshold when the cooldown has elapsed', () => {
        expect(shouldTriggerCompaction({ ...base, size: 51 })).toBe(true);
    });

    it('suppresses a second trigger inside the cooldown', () => {
        expect(shouldTriggerCompaction({ ...base, lastTriggerAt: 95_000 })).toBe(false);
    });

    it('triggers again exactly at the cooldown boundary', () => {
        expect(shouldTriggerCompaction({ ...base, lastTriggerAt: 90_000 })).toBe(true);
    });

    /*
     * At the realtime hard cap the client is about to stop seeing new
     * updates at all, so falling further behind is worse than contending.
     */
    it('ignores the cooldown once the hard cap is reached', () => {
        expect(shouldTriggerCompaction({ ...base, size: 500, lastTriggerAt: 99_999 })).toBe(true);
        expect(shouldTriggerCompaction({ ...base, size: 501, lastTriggerAt: 99_999 })).toBe(true);
    });

    it('still respects the cooldown just below the hard cap', () => {
        expect(shouldTriggerCompaction({ ...base, size: 499, lastTriggerAt: 99_999 })).toBe(false);
    });
});

describe('survivesEpochFence', () => {
    it('always keeps the snapshot — the main document defines the epoch', () => {
        expect(survivesEpochFence({ type: 'snapshot', data: { epoch: 99 } }, 1)).toBe(true);
        expect(survivesEpochFence({ type: 'snapshot', data: {} }, 7)).toBe(true);
    });

    it('keeps same-epoch updates and history', () => {
        expect(survivesEpochFence({ type: 'update', data: { epoch: 2 } }, 2)).toBe(true);
        expect(survivesEpochFence({ type: 'history', data: { epoch: 2 } }, 2)).toBe(true);
    });

    it('drops foreign-epoch updates and history', () => {
        expect(survivesEpochFence({ type: 'update', data: { epoch: 1 } }, 2)).toBe(false);
        expect(survivesEpochFence({ type: 'history', data: { epoch: 3 } }, 2)).toBe(false);
    });

    it('treats an epoch-less document as epoch 0', () => {
        expect(survivesEpochFence({ type: 'update', data: {} }, 0)).toBe(true);
        expect(survivesEpochFence({ type: 'update', data: {} }, 1)).toBe(false);
    });
});

describe('pickPaginationCursorIndex', () => {
    const doc = (pending: boolean) => ({ metadata: { hasPendingWrites: pending } });

    it('uses the last document when everything has committed', () => {
        expect(pickPaginationCursorIndex([doc(false), doc(false), doc(false)])).toBe(2);
    });

    /*
     * A document whose serverTimestamp has not committed sorts
     * unpredictably; using it as a cursor makes the next query invalid.
     */
    it('walks back past uncommitted documents at the tail', () => {
        expect(pickPaginationCursorIndex([doc(false), doc(false), doc(true), doc(true)])).toBe(1);
    });

    it('reports no usable cursor when the whole page is uncommitted', () => {
        expect(pickPaginationCursorIndex([doc(true), doc(true)])).toBe(-1);
    });

    it('reports no usable cursor for an empty page', () => {
        expect(pickPaginationCursorIndex([])).toBe(-1);
    });

    it('can select the first document', () => {
        expect(pickPaginationCursorIndex([doc(false), doc(true)])).toBe(0);
    });
});

describe('hasMorePages', () => {
    it('continues after a full page', () => {
        expect(hasMorePages(100, 100)).toBe(true);
    });

    it('stops after a short page', () => {
        expect(hasMorePages(99, 100)).toBe(false);
        expect(hasMorePages(0, 100)).toBe(false);
    });
});

describe('serverCoversLocalStructs', () => {
    it('covers when the server is level on every client', () => {
        expect(serverCoversLocalStructs(new Map([[1, 5]]), new Map([[1, 5]]))).toBe(true);
    });

    it('covers when the server is ahead', () => {
        expect(serverCoversLocalStructs(new Map([[1, 5]]), new Map([[1, 9]]))).toBe(true);
    });

    it('does not cover when the server is behind on any client', () => {
        expect(serverCoversLocalStructs(new Map([[1, 5], [2, 3]]), new Map([[1, 9], [2, 1]]))).toBe(false);
    });

    it('does not cover a client the server has never seen', () => {
        expect(serverCoversLocalStructs(new Map([[7, 1]]), new Map([[1, 9]]))).toBe(false);
    });

    it('covers trivially when the local document is empty', () => {
        expect(serverCoversLocalStructs(new Map(), new Map())).toBe(true);
    });

    it('checks every client, not only the first', () => {
        expect(serverCoversLocalStructs(new Map([[1, 1], [2, 1], [3, 99]]), new Map([[1, 1], [2, 1], [3, 1]])))
            .toBe(false);
    });
});

describe('diffHasPayload', () => {
    /* A structs-empty, deletions-empty update is a two-byte header. */
    it('rejects an empty two-byte diff', () => {
        expect(diffHasPayload(2)).toBe(false);
        expect(diffHasPayload(0)).toBe(false);
    });

    it('accepts anything larger', () => {
        expect(diffHasPayload(3)).toBe(true);
    });
});

describe('diffNeedsStorage', () => {
    it('stays inline at or below the limit', () => {
        expect(diffNeedsStorage(1_000, 1_000)).toBe(false);
        expect(diffNeedsStorage(999, 1_000)).toBe(false);
    });

    it('needs storage one byte over', () => {
        expect(diffNeedsStorage(1_001, 1_000)).toBe(true);
    });
});

describe('epochTag', () => {
    it('omits the field entirely at epoch 0', () => {
        expect(epochTag(0)).toEqual({});
        expect('epoch' in epochTag(0)).toBe(false);
    });

    it('writes the epoch once past 0', () => {
        expect(epochTag(4)).toEqual({ epoch: 4 });
    });
});

describe('orderByApplyPriority', () => {
    /*
     * Applying an update before the snapshot it depends on parks it in
     * pendingStructs, which also disables GC on the document.
     */
    it('puts the snapshot first, then history, then updates', () => {
        const items = [
            { priority: 3, name: 'update' },
            { priority: 1, name: 'snapshot' },
            { priority: 2, name: 'history' },
        ];

        expect(orderByApplyPriority(items).map((i) => i.name))
            .toEqual(['snapshot', 'history', 'update']);
    });

    it('does not mutate the input array', () => {
        const items = [{ priority: 3 }, { priority: 1 }];
        const ordered = orderByApplyPriority(items);

        expect(items[0].priority).toBe(3);
        expect(ordered).not.toBe(items);
    });

    it('keeps equal priorities together', () => {
        const items = [
            { priority: 2, name: 'a' },
            { priority: 2, name: 'b' },
            { priority: 1, name: 'snap' },
        ];

        expect(orderByApplyPriority(items).map((i) => i.name)).toEqual(['snap', 'a', 'b']);
    });

    it('handles an empty list', () => {
        expect(orderByApplyPriority([])).toEqual([]);
    });
});

describe('largeUpdatePath', () => {
    it('namespaces by document, client and time', () => {
        expect(largeUpdatePath('docs/a', 'client7', 1_700_000))
            .toBe('docs/a/large_updates/client7_1700000.bin');
    });

    it('keeps concurrent pushes from different clients distinct', () => {
        expect(largeUpdatePath('docs/a', 'c1', 5)).not.toBe(largeUpdatePath('docs/a', 'c2', 5));
    });

    it('keeps successive pushes from one client distinct', () => {
        expect(largeUpdatePath('docs/a', 'c1', 5)).not.toBe(largeUpdatePath('docs/a', 'c1', 6));
    });
});
