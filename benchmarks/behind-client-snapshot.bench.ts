/**
 * Behind-client snapshot apply benchmark
 *
 * A device that comes back after a few sessions on another device holds
 * most of the document locally (versicle hydrates it from y-idb) but is
 * BEHIND the server snapshot: a fold ran since it last synced, so
 * `localCoversSnapshot` is false. Both `performInitialSync` (applyItem in
 * the transact loop) and `createSnapshotListener` then hand the WHOLE
 * multi-MB snapshot to `Y.applyUpdate` on the live document, on the main
 * thread. Yjs decodes and materializes every struct, including the 90-99%
 * the local doc already holds, and resolves each one's origins against
 * the store before skipping it during integration: O(snapshot) main-thread
 * work that grows with document age, for O(missing) new data.
 *
 * `Y.diffUpdate(snapshot, encodeStateVector(localDoc))` extracts exactly
 * the missing structs plus the delete-set. It is pure (bytes in, bytes
 * out), so it can run in the merge worker, leaving only the small diff
 * apply on the main thread.
 *
 * Workload: the versicle-shaped model (versicle-workload.ts, same seed as
 * the aging suites), aged on one long-lived document whose clientID
 * changes every session. That produces the same document as hydrating a
 * fresh Y.Doc per session (3.31 MB, 147.9k items at 240 sessions, checked
 * against the per-session-hydration loop) in ~2 s instead of ~55 s. The
 * server snapshot is the GC fold of the full history
 * (`mergeUpdatesWithMeta(..., { gc: true })`, as compaction writes it);
 * the local doc is the full state `gap` sessions earlier.
 *
 * Per scenario it reports deterministic counters (structs the main thread
 * decodes, how many of them the local doc already had, diff size) and
 * median timings (interleaved A/B, ITERATIONS runs) of:
 *   - today:  Y.applyUpdate(localDoc, snapshot)            [main thread]
 *   - fix:    Y.diffUpdate(snapshot, localSV)              [worker-capable]
 *             + Y.applyUpdate(localDoc, diff)              [main thread]
 * and checks the two end states are identical (JSON, state vector, delete
 * set).
 *
 * Run with: npx vitest run --config benchmarks/vitest.config.ts benchmarks/behind-client-snapshot.bench.ts
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { mergeUpdatesWithMeta } from '../src/merge-core';
import { createSim, runSession, clientIdForSession } from './versicle-workload';
import { fmtBytes, fmtMs } from './helpers';

const SEED = 20260820; // same seed as the versicle aging suites
const ITERATIONS = 7;

/** [document age in sessions, sessions the local doc is behind] */
const SCENARIOS: Array<[number, number]> = [
    [60, 8],
    [120, 8],
    [240, 8],
    [240, 1],
    [240, 32],
];

const VERSICLE_ROOTS = ['library', 'progress', 'annotations', 'reading-list', 'vocabulary', 'lexicon', 'contentAnalysis', 'devices', 'searchHistory', 'meta'];

function materialize(doc: Y.Doc): string {
    const out: Record<string, unknown> = {};
    for (const name of VERSICLE_ROOTS) out[name] = doc.getMap(name).toJSON();
    return JSON.stringify(out);
}

/** Structs-empty update carrying the doc's full delete set (canonical: clients sorted, ranges merged). */
function deleteSetBytes(doc: Y.Doc): Uint8Array {
    return Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc));
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.byteLength !== b.byteLength) return false;
    for (let i = 0; i < a.byteLength; i++) if (a[i] !== b[i]) return false;
    return true;
}

interface StructCounts {
    structs: number;
    /** Structs whose whole clock range the local doc already holds */
    redundant: number;
    dsRanges: number;
}

function countStructs(update: Uint8Array, localSV: Map<number, number>): StructCounts {
    const { structs, ds } = Y.decodeUpdate(update);
    let n = 0, redundant = 0;
    for (const s of structs) {
        if (s instanceof Y.Skip) continue;
        n++;
        if (s.id.clock + s.length <= (localSV.get(s.id.client) || 0)) redundant++;
    }
    let dsRanges = 0;
    (ds as any).clients.forEach((ranges: unknown[]) => { dsRanges += ranges.length; });
    return { structs: n, redundant, dsRanges };
}

function median(xs: number[]): number {
    const s = [...xs].sort((a, b) => a - b);
    return s[Math.floor(s.length / 2)];
}

interface Row {
    age: number;
    gap: number;
    snapshotBytes: number;
    snapshot: StructCounts;
    diffBytes: number;
    diff: StructCounts;
    fullApplyMs: number;
    diffMs: number;
    diffApplyMs: number;
}

describe('behind-client snapshot apply (versicle workload)', () => {
    it('measures full-snapshot main-thread apply vs worker-side diff + small apply', () => {
        // --- Age one document, capturing the full state at every session count a scenario needs
        const needed = new Set<number>();
        for (const [age, gap] of SCENARIOS) { needed.add(age); needed.add(age - gap); }
        const maxAge = Math.max(...SCENARIOS.map(([age]) => age));

        const sim = createSim({ seed: SEED });
        const world = new Y.Doc();
        const states = new Map<number, Uint8Array>();
        for (let s = 0; s < maxAge; s++) {
            world.clientID = clientIdForSession(SEED, s);
            runSession(sim, world);
            if (needed.has(s + 1)) states.set(s + 1, Y.encodeStateAsUpdate(world));
        }
        world.destroy();

        const rows: Row[] = [];
        for (const [age, gap] of SCENARIOS) {
            // Server snapshot exactly as a GC fold writes it
            const snapshot = mergeUpdatesWithMeta([states.get(age)!], { gc: true }).result;
            const localState = states.get(age - gap)!;
            const makeLocal = () => {
                const d = new Y.Doc();
                Y.applyUpdate(d, localState);
                return d;
            };

            // --- Deterministic counters
            const probe = makeLocal();
            const localSV = Y.encodeStateVector(probe);
            const localSVMap = Y.decodeStateVector(localSV);
            probe.destroy();
            const diff = Y.diffUpdate(snapshot, localSV);
            const snapCounts = countStructs(snapshot, localSVMap);
            const diffCounts = countStructs(diff, localSVMap);

            // --- Equivalence: full apply and diff apply reach the same state
            const viaFull = makeLocal();
            viaFull.transact(() => Y.applyUpdate(viaFull, snapshot));
            const viaDiff = makeLocal();
            viaDiff.transact(() => Y.applyUpdate(viaDiff, Y.diffUpdate(snapshot, Y.encodeStateVector(viaDiff))));
            expect(materialize(viaDiff)).toBe(materialize(viaFull));
            expect(bytesEqual(Y.encodeStateVector(viaDiff), Y.encodeStateVector(viaFull))).toBe(true);
            expect(bytesEqual(deleteSetBytes(viaDiff), deleteSetBytes(viaFull))).toBe(true);
            viaFull.destroy();
            viaDiff.destroy();

            // --- Timings: interleaved, alternating order, medians
            const fullT: number[] = [], diffT: number[] = [], applyT: number[] = [];
            const runFull = () => {
                const d = makeLocal();
                const t0 = performance.now();
                // Same shape as the initial-sync transact loop
                d.transact(() => Y.applyUpdate(d, snapshot));
                fullT.push(performance.now() - t0);
                d.destroy();
            };
            const runDiff = () => {
                const d = makeLocal();
                const t0 = performance.now();
                const delta = Y.diffUpdate(snapshot, Y.encodeStateVector(d));
                const t1 = performance.now();
                d.transact(() => Y.applyUpdate(d, delta));
                const t2 = performance.now();
                diffT.push(t1 - t0);
                applyT.push(t2 - t1);
                d.destroy();
            };
            for (let i = 0; i < ITERATIONS; i++) {
                if (i % 2 === 0) { runFull(); runDiff(); } else { runDiff(); runFull(); }
            }

            rows.push({
                age, gap,
                snapshotBytes: snapshot.byteLength,
                snapshot: snapCounts,
                diffBytes: diff.byteLength,
                diff: diffCounts,
                fullApplyMs: median(fullT),
                diffMs: median(diffT),
                diffApplyMs: median(applyT),
            });
        }

        console.log('\n=== Behind-client snapshot apply (versicle workload, seed ' + SEED + ') ===');
        console.log('main-thread structs decoded: today = every snapshot struct; with a worker-side diff = diff structs only');
        console.log('age | behind | snapshot | snap structs | already held | dsRanges | diff | diff structs | TODAY main apply | diff (worker) | diff apply (main) | main-thread ratio');
        for (const r of rows) {
            console.log([
                r.age, r.gap,
                fmtBytes(r.snapshotBytes), r.snapshot.structs,
                `${r.snapshot.redundant} (${(100 * r.snapshot.redundant / r.snapshot.structs).toFixed(1)}%)`,
                r.snapshot.dsRanges,
                fmtBytes(r.diffBytes), r.diff.structs,
                fmtMs(r.fullApplyMs), fmtMs(r.diffMs), fmtMs(r.diffApplyMs),
                (r.fullApplyMs / r.diffApplyMs).toFixed(1) + 'x',
            ].join(' | '));
        }

        for (const r of rows) {
            // The diff carries exactly the structs the local doc lacks
            expect(r.diff.redundant).toBe(0);
            expect(r.diff.structs).toBe(r.snapshot.structs - r.snapshot.redundant);
            // and the full delete set
            expect(r.diff.dsRanges).toBe(r.snapshot.dsRanges);
        }
        // The headline scenario: most of what today's main-thread apply decodes is already local
        const headline = rows.find(r => r.age === 240 && r.gap === 8)!;
        expect(headline.snapshot.redundant / headline.snapshot.structs).toBeGreaterThan(0.9);
        expect(headline.diffBytes).toBeLessThan(headline.snapshotBytes / 10);
    }, 600_000);
});
