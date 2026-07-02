/**
 * Benchmark: object-modification-heavy documents (Y.Map overwrites).
 *
 * Canvas/whiteboard/board-style apps store objects as nested Y.Maps and
 * mutate them constantly (drags = bursts of x/y overwrites). Every
 * `map.set` on an existing key tombstones the previous item, so history
 * grows with the number of *modifications*, not objects.
 *
 * Two structural facts this benchmark quantifies:
 *
 * 1. GC compaction removes tombstone *content* but must keep tombstone
 *    *structure* (item ids, key names, origins) for CRDT convergence.
 *    With tiny numeric values the structure dominates, so GC's win is
 *    modest; with string values (labels, serialized props) content
 *    dominates and GC approaches the live-state floor. Both are compared
 *    against that floor (a fresh doc holding only live state) to show how
 *    much of the snapshot is inherent CRDT overhead vs removable content.
 *
 * 2. Remote modification bursts arrive as many small update documents.
 *    Applying each in its own top-level transaction fires one observer
 *    flush + 'update' event per document — the listener now batches a
 *    delivery into a single transaction (createUpdateListener), measured
 *    here as individual-vs-batched apply.
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { mergeUpdatesCore } from '../src/merge-core';
import { simulateObjectWorkload, medianMs, fmtBytes, fmtMs } from './helpers';

const BASE = {
    objects: 150,
    sessions: 20,
    opsPerSession: 120,
    compactionThreshold: 50,
};

function run(valueStyle: 'numeric' | 'text', seed: number) {
    const plain = simulateObjectWorkload({
        ...BASE, seed, valueStyle,
        compact: (blobs) => Y.mergeUpdates(blobs),
    });
    const gc = simulateObjectWorkload({
        ...BASE, seed, valueStyle,
        compact: (blobs) => mergeUpdatesCore(blobs, { gc: true }),
    });
    return { plain, gc };
}

describe('Object-modification-heavy documents', () => {
    it('numeric overwrites (drags): GC bounded win, tombstone structure dominates', () => {
        const { plain, gc } = run('numeric', 31337);

        const p = plain.samples[plain.samples.length - 1];
        const g = gc.samples[gc.samples.length - 1];
        console.log(`\n[numeric values] ${plain.totalOps} ops on ${BASE.objects} objects:`);
        console.log(`  snapshot plain merge: ${fmtBytes(p.snapshotBytes)}`);
        console.log(`  snapshot GC:          ${fmtBytes(g.snapshotBytes)}  (${(p.snapshotBytes / g.snapshotBytes).toFixed(2)}x)`);
        console.log(`  live-state floor:     ${fmtBytes(gc.liveStateFloorBytes)}  (fresh doc, no history)`);
        console.log(`  → tombstone structure the CRDT must keep: ${fmtBytes(g.snapshotBytes - gc.liveStateFloorBytes)}\n`);

        // Equivalence between strategies
        expect(gc.finalJson).toBe(plain.finalJson);

        // GC never loses, and must show a real (if modest) win here
        expect(g.snapshotBytes).toBeLessThanOrEqual(p.snapshotBytes);
        expect(g.snapshotBytes).toBeLessThan(p.snapshotBytes * 0.95);
    });

    it('string-value overwrites (labels/props): GC approaches the live-state floor', () => {
        const { plain, gc } = run('text', 60601);

        const p = plain.samples[plain.samples.length - 1];
        const g = gc.samples[gc.samples.length - 1];
        console.log(`\n[string values] ${plain.totalOps} ops on ${BASE.objects} objects:`);
        console.log(`  snapshot plain merge: ${fmtBytes(p.snapshotBytes)}`);
        console.log(`  snapshot GC:          ${fmtBytes(g.snapshotBytes)}  (${(p.snapshotBytes / g.snapshotBytes).toFixed(2)}x)`);
        console.log(`  live-state floor:     ${fmtBytes(gc.liveStateFloorBytes)}\n`);

        expect(gc.finalJson).toBe(plain.finalJson);
        // Content dominates: GC must cut the snapshot at least in half
        expect(g.snapshotBytes).toBeLessThan(p.snapshotBytes * 0.5);
    });

    it('remote modification burst: per-update transactions vs one batched transaction', () => {
        // Build a base document of objects, then a burst of small remote
        // updates (another client dragging objects around)
        const base = new Y.Doc();
        base.clientID = 1;
        base.transact(() => {
            const root = base.getMap('objects');
            for (let i = 0; i < 100; i++) {
                const o = new Y.Map();
                root.set(`obj${i}`, o);
                o.set('x', i); o.set('y', i); o.set('color', 0xabcdef);
            }
        });
        const baseState = Y.encodeStateAsUpdate(base);

        const remote = new Y.Doc();
        remote.clientID = 2;
        Y.applyUpdate(remote, baseState);
        const burst: Uint8Array[] = [];
        remote.on('update', (u: Uint8Array) => burst.push(u));
        const rroot = remote.getMap('objects');
        for (let s = 0; s < 150; s++) {
            remote.transact(() => {
                const o = rroot.get(`obj${s % 100}`) as Y.Map<any>;
                o.set('x', 1000 + s);
                o.set('y', 1000 - s);
            });
        }
        remote.destroy();
        expect(burst.length).toBe(150);

        const makeReceiver = () => {
            const doc = new Y.Doc();
            doc.clientID = 3;
            Y.applyUpdate(doc, baseState);
            let observerFlushes = 0;
            doc.getMap('objects').observeDeep(() => observerFlushes++);
            let updateEvents = 0;
            doc.on('update', () => updateEvents++);
            return { doc, counts: () => ({ observerFlushes, updateEvents }) };
        };

        const applyIndividually = () => {
            const r = makeReceiver();
            for (const u of burst) Y.applyUpdate(r.doc, u, 'remote');
            const c = r.counts();
            r.doc.destroy();
            return c;
        };
        const applyBatched = () => {
            const r = makeReceiver();
            r.doc.transact(() => {
                for (const u of burst) Y.applyUpdate(r.doc, u, 'remote');
            }, 'remote');
            const c = r.counts();
            r.doc.destroy();
            return c;
        };

        const individual = applyIndividually();
        const batched = applyBatched();
        const individualMs = medianMs(() => applyIndividually(), 9);
        const batchedMs = medianMs(() => applyBatched(), 9);

        console.log(`\nApplying a ${burst.length}-update remote drag burst:`);
        console.log(`  per-update transactions: ${fmtMs(individualMs)}  (${individual.observerFlushes} observer flushes, ${individual.updateEvents} update events)`);
        console.log(`  one batched transaction: ${fmtMs(batchedMs)}  (${batched.observerFlushes} observer flush, ${batched.updateEvents} update event)\n`);

        expect(individual.observerFlushes).toBe(burst.length);
        expect(batched.observerFlushes).toBe(1);
        expect(batched.updateEvents).toBe(1);
    });
});
