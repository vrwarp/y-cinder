/**
 * Benchmark: Y.Array-heavy documents (lists, kanban boards, rows, layers).
 *
 * Array workloads churn differently than text or map-key overwrites:
 *
 * - **Move = delete + re-insert.** Yjs has no native move, so every
 *   reorder duplicates the item's content in history: the old copy
 *   becomes a tombstone, the new copy is live.
 * - **Full-array rewrite** (`arr.delete(0, len); arr.insert(0, rows)`) is
 *   a common anti-pattern when apps sync external state into Yjs — each
 *   "edit" tombstones N elements.
 * - **Deleting a nested type** (a Y.Map row inside an array) lets Yjs GC
 *   the *entire subtree* into plain GC id-ranges (parentGCd), which merge
 *   with adjacent ranges — unlike in-place map-key overwrites whose
 *   tombstone structure must be kept individually.
 *
 * All of this makes array churn the workload where GC compaction matters
 * most: without it the full-rewrite pattern pushes a snapshot past
 * Firestore's 1 MB document limit within a few hundred operations.
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { mergeUpdatesCore } from '../src/merge-core';
import { simulateChurnWorkload, ChurnWorkloadOptions, fmtBytes } from './helpers';
import { SeededRandom } from '../tests/unit/prng';

type Scenario = Omit<ChurnWorkloadOptions, 'compact' | 'seed'> & { name: string; seed: number };

const COMMON = { compactionThreshold: 50 };

const SCENARIOS: Scenario[] = [
    {
        name: 'reorder-heavy (kanban moves)',
        seed: 111_001,
        sessions: 15,
        opsPerSession: 100,
        ...COMMON,
        setup: (doc, rng) => {
            const cards = [];
            for (let i = 0; i < 100; i++) {
                cards.push({ id: `c${i}`, title: rng.string(30), desc: rng.string(80) });
            }
            doc.getArray('cards').insert(0, cards);
        },
        op: (doc, rng) => {
            const a = doc.getArray<any>('cards');
            if (a.length < 2) return;
            const from = rng.int(0, a.length - 1);
            const card = a.get(from);
            doc.transact(() => {
                a.delete(from, 1);
                a.insert(rng.int(0, a.length), [card]);
            });
        },
        materialize: (doc) => doc.getArray('cards').toJSON(),
    },
    {
        name: 'full-array rewrite (anti-pattern)',
        seed: 111_002,
        sessions: 10,
        opsPerSession: 60,
        ...COMMON,
        setup: (doc, rng) => {
            const rows = [];
            for (let i = 0; i < 50; i++) rows.push({ id: i, v: rng.string(40) });
            doc.getArray('rows').insert(0, rows);
        },
        op: (doc, rng) => {
            const a = doc.getArray<any>('rows');
            doc.transact(() => {
                const rows = a.toArray().map((r: any) => ({ ...r }));
                const i = rng.int(0, rows.length - 1);
                rows[i] = { ...rows[i], v: rng.string(40) };
                a.delete(0, a.length);
                a.insert(0, rows);
            });
        },
        materialize: (doc) => doc.getArray('rows').toJSON(),
    },
    {
        name: 'nested rows, delete+recreate',
        seed: 111_003,
        sessions: 15,
        opsPerSession: 100,
        ...COMMON,
        setup: (doc, rng) => {
            const a = doc.getArray<Y.Map<any>>('rows');
            doc.transact(() => {
                for (let i = 0; i < 100; i++) {
                    const m = new Y.Map<any>();
                    m.set('id', i);
                    m.set('v', rng.string(40));
                    m.set('n', 0);
                    a.push([m]);
                }
            });
        },
        op: (doc, rng) => {
            const a = doc.getArray<Y.Map<any>>('rows');
            if (a.length === 0) return;
            const idx = rng.int(0, a.length - 1);
            const id = a.get(idx).get('id');
            doc.transact(() => {
                a.delete(idx, 1);
                const m = new Y.Map<any>();
                m.set('id', id);
                m.set('v', rng.string(40));
                m.set('n', rng.int(0, 1e6));
                a.insert(idx, [m]);
            });
        },
        materialize: (doc) =>
            // Order-insensitive view: rows keep identity by id
            (doc.getArray('rows').toJSON() as any[]).sort((x, y) => x.id - y.id),
    },
];

function runScenario(s: Scenario) {
    const plain = simulateChurnWorkload({ ...s, compact: (blobs) => Y.mergeUpdates(blobs) });
    const gc = simulateChurnWorkload({ ...s, compact: (blobs) => mergeUpdatesCore(blobs, { gc: true }) });
    return { plain, gc };
}

describe('Array-heavy documents', () => {
    it('GC compaction bounds all three array churn patterns', () => {
        console.log('\nscenario                          | plain merge |        GC | ratio');
        console.log('----------------------------------+-------------+-----------+------');
        for (const s of SCENARIOS) {
            const { plain, gc } = runScenario(s);
            const p = plain.snapshot!.byteLength;
            const g = gc.snapshot!.byteLength;
            console.log(
                s.name.padEnd(34) + '|' +
                fmtBytes(p).padStart(12) + ' |' +
                fmtBytes(g).padStart(10) + ' |' +
                `${(p / g).toFixed(1)}x`.padStart(6)
            );

            // Equivalence between strategies
            expect(gc.finalJson).toBe(plain.finalJson);
            // State vectors preserved
            expect(Y.encodeStateVectorFromUpdate(gc.snapshot!))
                .toEqual(Y.encodeStateVectorFromUpdate(plain.snapshot!));
            // Every array pattern must compact at least 2x under churn
            expect(g).toBeLessThan(p * 0.5);
        }
        console.log('');
    });

    it('full-rewrite anti-pattern exceeds Firestore doc limit without GC', () => {
        const s = SCENARIOS[1];
        const { plain, gc } = runScenario(s);
        const totalOps = s.sessions * s.opsPerSession;
        console.log(
            `\nAfter ${totalOps} full-array rewrites of 50 rows: ` +
            `plain=${fmtBytes(plain.snapshot!.byteLength)} (> 1MB Firestore limit), ` +
            `gc=${fmtBytes(gc.snapshot!.byteLength)}\n`
        );
        // The un-GC'd snapshot blows past 1 MB in only a few hundred ops —
        // this is the pattern that motivated defaulting gcCompaction on.
        expect(plain.snapshot!.byteLength).toBeGreaterThan(1_048_576);
        expect(gc.snapshot!.byteLength).toBeLessThan(100_000);
    });

    it('GC of deleted nested types converges with clients holding full history', () => {
        // Build churned array-of-Y.Maps history, GC it, then have an
        // un-GC'd client and a GC-bootstrapped client edit concurrently.
        const rng = new SeededRandom(424243);
        const doc = new Y.Doc();
        doc.clientID = 77;
        const blobs: Uint8Array[] = [];
        doc.on('update', (u: Uint8Array) => blobs.push(u));
        const a = doc.getArray<Y.Map<any>>('rows');
        doc.transact(() => {
            for (let i = 0; i < 30; i++) {
                const m = new Y.Map<any>();
                m.set('id', i);
                m.set('v', rng.string(40));
                a.push([m]);
            }
        });
        for (let i = 0; i < 100; i++) {
            const idx = rng.int(0, a.length - 1);
            doc.transact(() => {
                const id = a.get(idx).get('id');
                a.delete(idx, 1);
                const m = new Y.Map<any>();
                m.set('id', id);
                m.set('v', rng.string(40));
                a.insert(rng.int(0, a.length), [m]);
            });
        }
        doc.destroy();

        const gcd = mergeUpdatesCore(blobs, { gc: true });
        const plain = Y.mergeUpdates(blobs);
        expect(gcd.byteLength).toBeLessThan(plain.byteLength);

        const full = new Y.Doc();   // holds complete un-GC'd history
        Y.applyUpdate(full, plain);
        const fresh = new Y.Doc();  // bootstrapped from GC'd snapshot
        Y.applyUpdate(fresh, gcd);

        // Concurrent edits on both sides, then exchange
        (fresh.getArray('rows').get(0) as Y.Map<any>).set('v', 'edited-by-fresh');
        (full.getArray('rows').get(5) as Y.Map<any>).set('v', 'edited-by-full');
        Y.applyUpdate(full, Y.encodeStateAsUpdate(fresh, Y.encodeStateVector(full)));
        Y.applyUpdate(fresh, Y.encodeStateAsUpdate(full, Y.encodeStateVector(fresh)));

        expect(JSON.stringify(fresh.getArray('rows').toJSON()))
            .toBe(JSON.stringify(full.getArray('rows').toJSON()));
        full.destroy();
        fresh.destroy();
    });
});
