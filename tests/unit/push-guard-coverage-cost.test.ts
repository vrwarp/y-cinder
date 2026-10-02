/**
 * Performance regression test: the initial-sync push guard must not re-check
 * delete-set coverage once per small server blob.
 *
 * On every start whose server state vector covers the local structs,
 * performInitialSync calls
 *
 *     deleteSetCoveredByBlobs(localDs, () => collectServerBlobs(pendingUpdates))
 *
 * where the blobs are the delete-set fingerprint, the history segments and
 * every update document written since the last compaction (up to
 * maxUpdatesThreshold - 1 = 49 in versicle). The guard decodes blobs
 * smallest-first and, after EACH one, proves coverage with
 * clone(serverDs) + clone(localDs) + Y.mergeDeleteSets + Y.equalDeleteSets
 * — O(|DS|) with allocation. Pending update documents are tiny next to the
 * fingerprint of an aged document, so all of them are decoded first and
 * each triggers a full O(|DS|) check that cannot succeed yet: the cost is
 * O(pending x |DS|) on the main thread of every start, although one or a
 * logarithmic number of checks give the same verdict (coverage is monotone
 * in the union of server sets).
 *
 * Measured with implementation-agnostic counters (no wall clock):
 *   - local DS passes: every indexed read of the local DeleteItem arrays is
 *     counted through a proxy, divided by the local range count. Any correct
 *     guard reads each local range at least once; one that re-reads them
 *     all per server blob scales with the blob count.
 *   - DeleteItems copied through Y.mergeDeleteSets (the allocating merge).
 *
 * Both are pinned as a scaling ratio between 6 and 48 pending update docs
 * at a fixed delete-set size (~3k ranges): the current guard scales ~6x,
 * a guard that checks at doubling checkpoints or once at the end scales
 * ~1-1.6x. The benchmark with timings is
 * benchmarks/push-guard-coverage.bench.ts.
 *
 * @file push-guard-coverage-cost.test.ts
 */

import { describe, it, expect, vi, beforeAll } from 'vitest';

const counters = vi.hoisted(() => ({ mergeItems: 0 }));

vi.mock('yjs', async (importOriginal) => {
    const actual = await importOriginal<typeof import('yjs')>();
    return {
        ...actual,
        mergeDeleteSets: (dss: Parameters<typeof actual.mergeDeleteSets>[0]) => {
            for (const ds of dss) ds.clients.forEach(items => { counters.mergeItems += items.length; });
            return actual.mergeDeleteSets(dss);
        },
    };
});

import * as Y from 'yjs';
import { deleteSetCoveredByBlobs } from '../../src/update-metadata';
import { mergeUpdatesWithMeta } from '../../src/merge-core';

type DeleteSet = ReturnType<typeof Y.decodeUpdate>['ds'];

const SESSIONS = 30;
const EVENTS_PER_SESSION = 100;
const MAX_PENDING = 48;

function dsRanges(ds: DeleteSet): number {
    let n = 0;
    ds.clients.forEach(items => { n += items.length; });
    return n;
}

/**
 * Copy of `ds` whose DeleteItem arrays count every indexed element read
 * (index loops, for..of, map, slice and forEach all go through `get`).
 */
function countingDeleteSet(ds: DeleteSet): { ds: DeleteSet; reads: () => number } {
    let reads = 0;
    const handler: ProxyHandler<unknown[]> = {
        get(target, prop, receiver) {
            if (typeof prop === 'string' && /^\d+$/.test(prop)) reads++;
            return Reflect.get(target, prop, receiver);
        },
    };
    const out = Y.createDeleteSet();
    ds.clients.forEach((items, client) => {
        out.clients.set(client, new Proxy(items.slice(), handler) as typeof items);
    });
    return { ds: out, reads: () => reads };
}

interface Fixture {
    snapshot: Uint8Array;
    fingerprint: Uint8Array;
    pending: Uint8Array[];
}

/**
 * An aged document persisted the way y-cinder stores it: a GC'd snapshot
 * with its delete-set fingerprint (as compaction writes them), followed by
 * small pending update documents — one per debounced save — each of which
 * overwrites a map key (one deletion) like a page turn does.
 */
function buildFixture(): Fixture {
    const live = new Y.Doc();
    const map = live.getMap('progress');
    const updates: Uint8Array[] = [];
    live.on('update', (u: Uint8Array) => updates.push(u));
    for (let s = 0; s < SESSIONS; s++) {
        live.clientID = 1000 + s; // fresh clientID per session (versicle)
        for (let e = 0; e < EVENTS_PER_SESSION; e++) {
            live.transact(() => {
                map.set('currentCfi', `s${s}e${e}`); // overwrite: one deletion
                map.set(`session:${s}:${e}`, e); // live entry keeps ranges fragmented
            });
        }
    }
    const { result: snapshot, dsUpdate: fingerprint } = mergeUpdatesWithMeta(updates, { gc: true });

    live.clientID = 9000;
    const pending: Uint8Array[] = [];
    for (let i = 0; i < MAX_PENDING; i++) {
        live.transact(() => {
            map.set('currentCfi', `pending${i}`);
            map.set(`pending:${i}`, i);
        });
        pending.push(updates[updates.length - 1]);
    }
    live.destroy();
    return { snapshot, fingerprint, pending };
}

interface Cost {
    verdict: boolean;
    localRanges: number;
    localPasses: number;
    mergeItems: number;
}

/**
 * Warm start with `k` pending update docs: the local doc holds everything
 * the server holds (snapshot skipped by localCoversSnapshot), so the guard
 * sees [fingerprint, ...pending] and must answer "covered".
 */
function warmStartCost(fx: Fixture, k: number, extraLocalDeletion = false): Cost {
    const doc = new Y.Doc();
    doc.clientID = 424242;
    Y.applyUpdate(doc, fx.snapshot);
    for (const u of fx.pending.slice(0, k)) Y.applyUpdate(doc, u);
    if (extraLocalDeletion) {
        // Offline deletion-only edit the server lacks
        doc.getMap('progress').delete('session:0:0');
    }
    const localDs = Y.createDeleteSetFromStructStore((doc as any).store);
    doc.destroy();

    const blobs = [fx.fingerprint, ...fx.pending.slice(0, k)];
    const counted = countingDeleteSet(localDs);
    counters.mergeItems = 0;
    const verdict = deleteSetCoveredByBlobs(counted.ds, () => blobs);
    const localRanges = dsRanges(localDs);
    return {
        verdict,
        localRanges,
        localPasses: counted.reads() / localRanges,
        mergeItems: counters.mergeItems,
    };
}

describe('initial-sync push guard cost vs pending update documents', () => {
    let fx: Fixture;
    let small: Cost;
    let large: Cost;

    beforeAll(() => {
        fx = buildFixture();
        small = warmStartCost(fx, 6);
        large = warmStartCost(fx, MAX_PENDING);
    });

    it('fixture has the aged-document shape (pending docs smaller than the fingerprint)', () => {
        expect(small.localRanges).toBeGreaterThan(2500);
        for (const u of fx.pending) expect(u.byteLength).toBeLessThan(fx.fingerprint.byteLength);
        // Same verdict in every case: fully synced, nothing to push...
        expect(small.verdict).toBe(true);
        expect(large.verdict).toBe(true);
        // ...and an offline deletion is still detected
        expect(warmStartCost(fx, MAX_PENDING, true).verdict).toBe(false);
    });

    it('reads the local delete-set a bounded number of times, not once per pending update doc', () => {
        const ratio = large.localPasses / small.localPasses;
        expect(
            ratio,
            `local DS passes: ${small.localPasses.toFixed(1)} with 6 pending, ` +
            `${large.localPasses.toFixed(1)} with ${MAX_PENDING} pending ` +
            `(${large.localRanges} local ranges)`,
        ).toBeLessThan(2.5);
    });

    it('merge/allocation work does not scale with the number of pending update docs', () => {
        const ratio = large.mergeItems / small.mergeItems;
        expect(
            ratio,
            `DeleteItems through Y.mergeDeleteSets: ${small.mergeItems} with 6 pending, ` +
            `${large.mergeItems} with ${MAX_PENDING} pending`,
        ).toBeLessThan(2.5);
    });
});
