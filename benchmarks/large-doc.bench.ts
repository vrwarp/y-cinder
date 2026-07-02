/**
 * Benchmark: very large single documents (multi-megabyte snapshots).
 *
 * Costs that are negligible on small documents become main-thread stalls
 * at scale. Profiled on a realistic fragmented document (~60-char items,
 * 10 clients, scattered churn — NOT a few giant merged items):
 *
 *   10 MB / 300k structs: lazy SV walk 329 ms, delete-set fingerprint
 *   344 ms, full decode 464 ms, GC rebuild ~3.1 s.
 *
 * Compaction previously merged in the Web Worker but then walked the
 * result twice on the MAIN thread (validation/state-vector + delete-set
 * fingerprint) — ~670 ms of UI stall per compaction at 10 MB. The
 * `mergeUpdatesWithMeta` path derives both during the merge (worker-side
 * in browsers), and on the GC path they're nearly free because the
 * rebuilt Y.Doc is already in hand. This benchmark measures that saving
 * and tracks the other large-doc scaling numbers.
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { mergeUpdatesWithMeta, mergeUpdatesCore } from '../src/merge-core';
import { extractAllMetadata, extractClockEnds } from '../src/update-metadata';
import { SeededRandom } from '../tests/unit/prng';
import { fmtBytes, fmtMs } from './helpers';

/**
 * Builds a large snapshot with realistic item granularity: sentence-sized
 * inserts at scattered positions from multiple clients, deletions of OLD
 * content (fragmenting the delete-set the way real multi-session editing
 * does).
 */
function buildLargeSnapshot(targetMB: number): Uint8Array {
    const rng = new SeededRandom(1_000_000 + targetMB);
    const clients = 10;
    const insertsNeeded = Math.ceil((targetMB * 1_048_576) / 60);
    const perClient = Math.ceil(insertsNeeded / clients);
    let carry: Uint8Array | null = null;
    for (let c = 0; c < clients; c++) {
        const doc = new Y.Doc();
        doc.clientID = 50_000 + c;
        if (carry) Y.applyUpdate(doc, carry);
        const text = doc.getText('body');
        doc.transact(() => {
            for (let i = 0; i < perClient; i++) {
                text.insert(rng.int(0, text.length), rng.string(58) + '. ');
                if (i % 8 === 0 && text.length > 2000) {
                    text.delete(rng.int(0, text.length - 200), rng.int(30, 150));
                }
            }
        });
        carry = Y.encodeStateAsUpdate(doc);
        doc.destroy();
    }
    return carry!;
}

function time<T>(fn: () => T): [number, T] {
    const t0 = performance.now();
    const r = fn();
    return [performance.now() - t0, r];
}

describe('Very large single documents', () => {
    // ~1 MB and ~4 MB keep the benchmark runtime reasonable while showing
    // the scaling trend; the prototype numbers above extend it to 10 MB.
    const SIZES_MB = [1, 4];

    for (const mb of SIZES_MB) {
        it(`scaling at ~${mb} MB: compaction meta derivation moves off the hot path`, () => {
            const snapshot = buildLargeSnapshot(mb);

            // Compaction-shaped input: big snapshot + 50 small updates
            const small: Uint8Array[] = [];
            {
                const d = new Y.Doc();
                d.clientID = 7;
                Y.applyUpdate(d, snapshot);
                d.on('update', (u: Uint8Array) => small.push(u));
                const rng = new SeededRandom(mb);
                const body = d.getText('body');
                for (let i = 0; i < 50; i++) {
                    body.insert(rng.int(0, body.length), rng.string(40));
                }
                d.destroy();
            }
            const blobs = [snapshot, ...small];

            // OLD compaction flow: worker merges, then the MAIN THREAD
            // validates + derives metadata with two lazy walks.
            const [mergeMs, candidate] = time(() => mergeUpdatesCore(blobs, { gc: true }));
            const [svWalkMs, sv] = time(() => Y.encodeStateVectorFromUpdate(candidate));
            const [dsWalkMs, dsUpdate] = time(() => Y.diffUpdate(candidate, sv));
            const oldMainThreadMs = svWalkMs + dsWalkMs;

            // NEW flow: one call returns result + metadata; in browsers the
            // whole thing runs in the merge worker (main-thread cost ~0).
            // Also measure the total CPU: deriving meta from the built doc
            // is cheaper than re-walking the candidate binary.
            const [withMetaMs, meta] = time(() => mergeUpdatesWithMeta(blobs, { gc: true }));

            // Metadata extraction fast path at scale (save-path cost for
            // a snapshot-sized offline diff)
            const [decodeMs] = time(() => extractAllMetadata(snapshot));
            const [lazyMs] = time(() => extractClockEnds(snapshot));

            console.log(`\n~${mb} MB snapshot (${fmtBytes(snapshot.byteLength)}):`);
            console.log(`  merge+GC (worker-side in browsers):          ${fmtMs(mergeMs)}`);
            console.log(`  OLD main-thread meta walks (SV + DS):        ${fmtMs(oldMainThreadMs)}  ← was UI stall per compaction`);
            console.log(`  NEW merge+meta total (worker-side):          ${fmtMs(withMetaMs)}  (main thread: ~0)`);
            console.log(`  delete-set fingerprint size:                 ${fmtBytes(meta.dsUpdate.byteLength)} (inline cap 700 KB)`);
            console.log(`  save-path metadata: decodeUpdate vs lazy SV: ${fmtMs(decodeMs)} vs ${fmtMs(lazyMs)}\n`);

            // Equivalence with the old flow
            expect(meta.stateVector).toEqual(sv);
            expect(meta.dsUpdate).toEqual(dsUpdate);
            expect(meta.result).toEqual(candidate);

            // Gross-regression guard only (2x slack: wall-clock timing on a
            // shared/loaded machine is noisy). Typically the combined
            // derivation costs LESS than merge + separate walks because the
            // GC path reuses the built doc.
            expect(withMetaMs).toBeLessThan((mergeMs + oldMainThreadMs) * 2);

            // Fingerprint must stay storable inline at these scales, or
            // reconnecting clients lose the early-exit fast path
            expect(meta.dsUpdate.byteLength).toBeLessThan(700_000);
        }, 240_000);
    }
});
