/**
 * Benchmark: snapshot growth and compaction cost on a long-lived document.
 *
 * Simulates a document edited across many sessions with bounded live
 * content but high historical churn, compacting every N updates the way
 * FireProvider does. Compares:
 *
 * - `merge`  — the previous strategy: Y.mergeUpdates only. Never
 *              garbage-collects, so the snapshot carries the content of
 *              every character ever deleted.
 * - `gc`     — the current strategy (mergeUpdatesCore with gc: true):
 *              merge, then rewrite through a Y.Doc with GC enabled.
 *
 * The snapshot size directly drives real-world cost on aged documents:
 * every compaction downloads + merges + re-uploads it, and every fresh
 * client downloads it on first sync.
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { mergeUpdatesCore } from '../src/merge-core';
import { simulateAgedDocument, materializeText, fmtBytes, fmtMs } from './helpers';

const SCENARIO = {
    seed: 20260701,
    sessions: 40,
    editsPerSession: 150,
    compactionThreshold: 50, // DEFAULTS.MAX_UPDATES_THRESHOLD
    targetLiveChars: 2_000,
};

describe('Aged document: snapshot growth and compaction cost', () => {
    it('GC compaction bounds snapshot size; plain merge grows with total churn', () => {
        const plain = simulateAgedDocument({
            ...SCENARIO,
            compact: (blobs) => Y.mergeUpdates(blobs),
        });
        const gc = simulateAgedDocument({
            ...SCENARIO,
            compact: (blobs) => mergeUpdatesCore(blobs, { gc: true }),
        });

        // --- Report ---
        console.log(`\nScenario: ${plain.totalEdits} edits over ${SCENARIO.sessions} sessions, ` +
            `live content ~${plain.liveChars} chars, compaction every ${SCENARIO.compactionThreshold} updates\n`);
        console.log('compaction |    edits | snapshot (merge) | snapshot (gc) | merge ms | gc ms');
        console.log('-----------+----------+------------------+---------------+----------+------');
        const stride = Math.max(1, Math.floor(plain.samples.length / 12));
        for (let i = 0; i < plain.samples.length; i += stride) {
            const p = plain.samples[i];
            const g = gc.samples[i];
            console.log(
                String(p.index).padStart(10) + ' |' +
                String(p.editsSoFar).padStart(9) + ' |' +
                fmtBytes(p.snapshotBytes).padStart(17) + ' |' +
                fmtBytes(g?.snapshotBytes ?? 0).padStart(14) + ' |' +
                fmtMs(p.mergeMs).padStart(9) + ' |' +
                fmtMs(g?.mergeMs ?? 0).padStart(6)
            );
        }
        const pLast = plain.samples[plain.samples.length - 1];
        const gLast = gc.samples[gc.samples.length - 1];
        console.log(
            `\nFinal snapshot:  merge=${fmtBytes(pLast.snapshotBytes)}  ` +
            `gc=${fmtBytes(gLast.snapshotBytes)}  ` +
            `(${(pLast.snapshotBytes / gLast.snapshotBytes).toFixed(1)}x smaller with GC)`
        );
        console.log(
            `Total compaction CPU: merge=${fmtMs(plain.totalMergeMs)}  gc=${fmtMs(gc.totalMergeMs)}\n`
        );

        // --- Integrity: both strategies materialize identical content ---
        expect(materializeText(gc.snapshot, gc.pending)).toBe(plain.finalText);
        expect(materializeText(plain.snapshot, plain.pending)).toBe(plain.finalText);

        // --- State vectors preserved by GC (sync metadata stays correct) ---
        const svPlain = [...Y.decodeStateVector(Y.encodeStateVectorFromUpdate(plain.snapshot!)).entries()].sort();
        const svGc = [...Y.decodeStateVector(Y.encodeStateVectorFromUpdate(gc.snapshot!)).entries()].sort();
        expect(svGc).toEqual(svPlain);

        // --- The point: GC keeps the aged snapshot dramatically smaller ---
        expect(gLast.snapshotBytes).toBeLessThan(pLast.snapshotBytes * 0.5);

        // Plain merge must grow well beyond live content on this scenario,
        // otherwise the benchmark isn't exercising churn at all.
        expect(pLast.snapshotBytes).toBeGreaterThan(gLast.snapshotBytes * 2);
    });
});
