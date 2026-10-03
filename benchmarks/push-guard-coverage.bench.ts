/**
 * Initial-sync push guard: delete-set coverage against the REAL blob set
 *
 * On every cold start, warm start and listener-error re-sync whose server
 * state vector covers the local structs, `performInitialSync` decides
 * whether to push by calling
 *
 *     deleteSetCoveredByBlobs(localDs, () => collectServerBlobs(pendingUpdates))
 *
 * on the main thread. `pendingUpdates` holds EVERYTHING initial sync
 * fetched for the current epoch: the delete-set fingerprint, the history
 * segments, every update document written since the last compaction (0 to
 * maxUpdatesThreshold - 1, more when compaction lags) and, on a fresh
 * client, the snapshot.
 *
 * The guard decodes blobs smallest-first and re-checks coverage after each
 * one; every check clones the server and local delete-sets, merges them and
 * compares (O(|DS|) with allocation). Pending update documents (~0.1-1 KB)
 * are far smaller than the fingerprint (tens of KB on an aged doc), so they
 * are all decoded first and each one triggers a full O(|DS|) check that
 * cannot succeed yet: the guard costs O(blobs-smaller-than-fingerprint x
 * |DS|) instead of O(|DS|).
 *
 * docs/performance.md ("Fix 3") reports this guard as 0.04 ms flat; that
 * measurement passed [fingerprint, ...segments] right after a compaction,
 * i.e. with no pending update documents. This suite ages a versicle-shaped
 * document through the delta-compaction pipeline (same workload, seed and
 * thresholds as versicle-aging-fixed.bench.ts, no squash) and measures the
 * guard at the steady-state worst case — 49 pending updates, one short of
 * a compaction — next to:
 *
 *   - the same blob set without pending updates (the docs' measurement),
 *   - a fresh client (snapshot downloaded and in the blob set),
 *   - the single-check floor: one blob carrying the complete server
 *     delete-set (one decode + one O(|DS|) check) — what a guard that
 *     checks O(1) times would cost,
 *   - a sweep over the number of pending updates at fixed age.
 *
 * Deterministic counter: "local DS passes" = how many times the guard read
 * the entire local delete-set (DeleteItem reads through a counting proxy /
 * local DS ranges). It equals the number of O(|DS|) coverage checks.
 * Timings are medians of 7 interleaved runs.
 *
 * Run with: npx vitest run --config benchmarks/vitest.config.ts benchmarks/push-guard-coverage.bench.ts
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { mergeUpdatesWithMeta } from '../src/merge-core';
import { deleteSetCoveredByBlobs } from '../src/update-metadata';
import { createSim, runSession, clientIdForSession, VersicleSimState } from './versicle-workload';
import { fmtBytes, fmtMs } from './helpers';

const SEED = 20260820; // same seed as the versicle aging suites
const TARGETS = [60, 120, 240]; // sessions (60 events each)
const THRESHOLD = 50; // versicle's maxUpdatesThreshold
const FOLD_THRESHOLD = 8; // historyFoldThreshold default
const ROUNDS = 7;
const SWEEP = [0, 6, 12, 24, 49];

type DeleteSet = ReturnType<typeof Y.decodeUpdate>['ds'];

/** Ranges in a delete-set. */
function dsRanges(ds: DeleteSet): number {
    let n = 0;
    ds.clients.forEach(items => { n += items.length; });
    return n;
}

/**
 * Copy of `ds` whose per-client DeleteItem arrays count every indexed
 * element read. Every way of walking an array (index loop, for..of, map,
 * slice, forEach) goes through the proxy's get trap.
 */
function countingDeleteSet(ds: DeleteSet): { ds: DeleteSet; reads: () => number } {
    let reads = 0;
    const handler: ProxyHandler<unknown[]> = {
        get(target, prop, receiver) {
            if (typeof prop === 'string' && prop.length > 0 && prop.charCodeAt(0) >= 48 && prop.charCodeAt(0) <= 57) {
                reads++;
            }
            return Reflect.get(target, prop, receiver);
        },
    };
    const out = Y.createDeleteSet();
    ds.clients.forEach((items, client) => {
        out.clients.set(client, new Proxy(items.slice(), handler) as typeof items);
    });
    return { ds: out, reads: () => reads };
}

/** Local DS passes the guard makes (= O(|DS|) coverage checks). */
function localPasses(localDs: DeleteSet, blobs: Uint8Array[]): { passes: number; verdict: boolean } {
    const counted = countingDeleteSet(localDs);
    const verdict = deleteSetCoveredByBlobs(counted.ds, () => blobs);
    return { passes: counted.reads() / dsRanges(localDs), verdict };
}

/** "ms (passes)" cell */
function cell(c: { ms: number; passes: number }): string {
    return `${fmtMs(c.ms)} (${Number.isInteger(c.passes) ? c.passes : c.passes.toFixed(1)})`;
}

function median(xs: number[]): number {
    const s = xs.slice().sort((a, b) => a - b);
    return s[Math.floor(s.length / 2)];
}

/** Runs each scenario ROUNDS times, interleaved; returns medians in ms. */
function interleavedMedians(scenarios: Array<() => void>): number[] {
    const times: number[][] = scenarios.map(() => []);
    for (let r = 0; r < ROUNDS; r++) {
        scenarios.forEach((fn, i) => {
            const t0 = performance.now();
            fn();
            times[i].push(performance.now() - t0);
        });
    }
    return times.map(median);
}

function docFrom(blobs: Uint8Array[]): Y.Doc {
    const d = new Y.Doc();
    d.clientID = clientIdForSession(SEED, 999_999);
    for (const b of blobs) Y.applyUpdate(d, b);
    return d;
}

interface Row {
    session: number;
    dsRanges: number;
    snapshotBytes: number;
    fingerprintBytes: number;
    segments: number;
    segmentBytes: number[];
    pendingBytes: number;
    smallerThanFp: number;
    warm: { ms: number; passes: number };
    fresh: { ms: number; passes: number };
    compacted: { ms: number; passes: number };
    floor: { ms: number; passes: number };
    sweep: Array<{ k: number; ms: number; passes: number }>;
}

function measure(
    session: number,
    snapshot: Uint8Array,
    fingerprint: Uint8Array,
    segments: Uint8Array[],
    pending: Uint8Array[],
): Row {
    // A client that is fully synced with the server (warm start from y-idb,
    // or a fresh client after its initial apply): its delete-set equals the
    // union of the server blobs'.
    const synced = docFrom([snapshot, ...segments, ...pending]);
    const localDs = Y.createDeleteSetFromStructStore((synced as any).store);
    // The same client right after a compaction (no pending update docs)
    const compactedDoc = docFrom([snapshot, ...segments]);
    const compactedDs = Y.createDeleteSetFromStructStore((compactedDoc as any).store);
    // One structs-empty blob holding the complete server delete-set
    const unionDs = Y.encodeStateAsUpdate(synced, Y.encodeStateVector(synced));

    const warmBlobs = [fingerprint, ...segments, ...pending];
    const freshBlobs = [snapshot, fingerprint, ...segments, ...pending];
    const compactedBlobs = [fingerprint, ...segments];

    const scenarios: Array<[Uint8Array[], DeleteSet]> = [
        [warmBlobs, localDs],
        [freshBlobs, localDs],
        [compactedBlobs, compactedDs],
        [[unionDs], localDs],
    ];
    // Same verdict everywhere: fully synced, nothing to push
    const counts = scenarios.map(([blobs, ds]) => localPasses(ds, blobs));
    for (const c of counts) expect(c.verdict).toBe(true);
    const ms = interleavedMedians(scenarios.map(([blobs, ds]) => () => {
        deleteSetCoveredByBlobs(ds, () => blobs);
    }));

    // Sweep the number of pending update docs at this age
    const sweepDocs = SWEEP.map(k => {
        const d = docFrom([snapshot, ...segments, ...pending.slice(0, k)]);
        const ds = Y.createDeleteSetFromStructStore((d as any).store);
        d.destroy();
        return { k, ds, blobs: [fingerprint, ...segments, ...pending.slice(0, k)] };
    });
    const sweepPasses = sweepDocs.map(s => {
        const c = localPasses(s.ds, s.blobs);
        expect(c.verdict).toBe(true);
        return c.passes;
    });
    const sweepMs = interleavedMedians(sweepDocs.map(s => () => {
        deleteSetCoveredByBlobs(s.ds, () => s.blobs);
    }));

    synced.destroy();
    compactedDoc.destroy();

    return {
        session,
        dsRanges: dsRanges(localDs),
        snapshotBytes: snapshot.byteLength,
        fingerprintBytes: fingerprint.byteLength,
        segments: segments.length,
        segmentBytes: segments.map(s => s.byteLength),
        pendingBytes: pending.reduce((a, b) => a + b.byteLength, 0),
        smallerThanFp: warmBlobs.filter(b => b.byteLength < fingerprint.byteLength).length,
        warm: { ms: ms[0], passes: counts[0].passes },
        fresh: { ms: ms[1], passes: counts[1].passes },
        compacted: { ms: ms[2], passes: counts[2].passes },
        floor: { ms: ms[3], passes: counts[3].passes },
        sweep: SWEEP.map((k, i) => ({ k, ms: sweepMs[i], passes: sweepPasses[i] })),
    };
}

describe('initial-sync push guard: delete-set coverage vs pending update docs', () => {
    it('measures the guard against the real cold/warm-start blob set on an aged doc', () => {
        const sim: VersicleSimState = createSim({ seed: SEED });

        let snapshot: Uint8Array | null = null;
        let fingerprint: Uint8Array | null = null;
        let segments: Uint8Array[] = [];
        let pending: Uint8Array[] = [];

        const compactCycle = () => {
            if (segments.length + 1 < FOLD_THRESHOLD && snapshot !== null) {
                // DELTA: pending updates -> one history segment
                segments.push(mergeUpdatesWithMeta(pending, { gc: false }).result);
            } else {
                // FOLD: everything -> new GC'd snapshot + fingerprint
                const merged = mergeUpdatesWithMeta(
                    [...(snapshot ? [snapshot] : []), ...segments, ...pending], { gc: true });
                snapshot = merged.result;
                fingerprint = merged.dsUpdate;
                segments = [];
            }
            pending = [];
        };

        const rows: Row[] = [];
        let next = 0;
        // One live doc, fresh clientID per session (versicle mints one per
        // page load). Its update stream is exactly what a re-hydrated doc
        // would produce, without paying an O(document) hydrate per session.
        const live = new Y.Doc();
        for (let s = 0; next < TARGETS.length; s++) {
            live.clientID = clientIdForSession(SEED, s);
            const { blobs } = runSession(sim, live);
            for (const b of blobs) {
                pending.push(b);
                // Steady-state worst case: one update short of compaction
                if (next < TARGETS.length && s + 1 >= TARGETS[next] &&
                    pending.length === THRESHOLD - 1 && snapshot !== null) {
                    rows.push(measure(s + 1, snapshot, fingerprint!, segments.slice(), pending.slice()));
                    next++;
                }
                if (pending.length >= THRESHOLD) compactCycle();
            }
        }
        live.destroy();

        console.log('\n=== Initial-sync push guard (deleteSetCoveredByBlobs) — 49 pending update docs ===');
        console.log('session | dsRanges | snapshot | fingerprint | segs (sizes) | pending total | blobs < fp | warm: ms (passes) | fresh: ms (passes) | 0 pending: ms (passes) | single-check floor: ms (passes)');
        for (const r of rows) {
            console.log([
                r.session, r.dsRanges, fmtBytes(r.snapshotBytes), fmtBytes(r.fingerprintBytes),
                `${r.segments} (${r.segmentBytes.map(fmtBytes).join(', ')})`, fmtBytes(r.pendingBytes),
                r.smallerThanFp,
                cell(r.warm), cell(r.fresh), cell(r.compacted), cell(r.floor),
            ].join(' | '));
        }
        console.log('\n--- warm-start guard vs number of pending update docs (same age) ---');
        console.log('session | ' + SWEEP.map(k => `k=${k}: ms (passes)`).join(' | '));
        for (const r of rows) {
            console.log([r.session, ...r.sweep.map(cell)].join(' | '));
        }

        // Structural facts the measurement depends on (deterministic):
        for (const r of rows) {
            // the single-check floor makes at most two local passes
            expect(r.floor.passes).toBeLessThanOrEqual(2);
            expect(r.warm.passes).toBeGreaterThanOrEqual(1);
        }
    }, 600_000);
});
