/**
 * Benchmark: the CPU and transfer a zero-update compaction wastes.
 *
 * When compact() finds no pending update documents but one or more
 * current-epoch history segments, it currently FOLDS: download the base
 * snapshot, merge + GC base + history (worker CPU, O(snapshot)), upload a
 * new snapshot, rewrite the main document. Nothing new is added, and the
 * fold is not due (one segment against historyFoldThreshold 8). squash()
 * reaches this on every call made with nothing pending (it compacts first,
 * unconditionally), and its own new-epoch snapshot then supersedes the
 * fold; late lock winners and app-driven compact() calls reach it too.
 *
 * This suite measures, on the versicle-shaped aged document at three ages,
 * exactly the work such a fold does — mergeUpdatesWithMeta([base, segment],
 * { gc: true }), the call compaction makes (worker-side in browsers) — next
 * to the squash's own clone + encode, for scale. The emulator regression
 * test tests/integration/zero_update_compaction.test.ts counts the same
 * cost as Storage transfers, merges and main-document rewrites.
 *
 * Run with: npx vitest run --config benchmarks/vitest.config.ts benchmarks/zero-update-compaction.bench.ts
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { mergeUpdatesWithMeta } from '../src/merge-core';
import { buildSquashedDoc } from '../src/squash';
import { createSim, runSession, clientIdForSession, materializeVersicleDoc } from './versicle-workload';
import { fmtBytes, fmtMs, medianMs } from './helpers';

const SEED = 20260820; // same seed as the versicle-aging suites
/** 1,440 / 5,760 / 14,400 events (60 events per session) */
const AGES = [24, 96, 240];
const RUNS = 5;
const VERSICLE_ROOTS = ['library', 'progress', 'annotations', 'reading-list', 'vocabulary', 'lexicon', 'contentAnalysis', 'devices', 'searchHistory', 'meta'];

describe('zero-update compaction: the fold that adds nothing', () => {
    it('reports the O(snapshot) cost of folding base + 1 segment with no new updates', () => {
        const rows: string[] = [];

        for (const sessions of AGES) {
            const sim = createSim({ seed: SEED });
            const doc = new Y.Doc();
            for (let s = 0; s < sessions; s++) {
                doc.clientID = clientIdForSession(SEED, s);
                runSession(sim, doc);
            }
            // Base snapshot as a fold leaves it (the live doc is gc: true, so
            // its state is already the GC'd snapshot), then one delta
            // segment: the next session's saves merged without GC, exactly
            // as tryDeltaCompaction builds it.
            const base = Y.encodeStateAsUpdate(doc);
            const ageEvents = sim.totalEvents;
            doc.clientID = clientIdForSession(SEED, sim.sessionCount);
            const segment = mergeUpdatesWithMeta(runSession(sim, doc).blobs, { gc: false }).result;

            // The zero-update fold: what compact() runs today.
            let folded: Uint8Array = new Uint8Array();
            const foldMs = medianMs(() => {
                folded = mergeUpdatesWithMeta([base, segment], { gc: true }).result;
            }, RUNS);
            // Same content (Y.Map key order can differ with integration order)
            expect(JSON.parse(materializeVersicleDoc(folded, []))).toEqual(JSON.parse(materializeVersicleDoc(base, [segment])));

            // For scale: the squash that supersedes it (clone + encode).
            for (const name of VERSICLE_ROOTS) doc.getMap(name);
            let squashedBytes = 0;
            const squashMs = medianMs(() => {
                const squashed = buildSquashedDoc(doc, 1);
                squashedBytes = Y.encodeStateAsUpdate(squashed).byteLength;
                squashed.destroy();
            }, RUNS);
            doc.destroy();

            rows.push(
                String(ageEvents).padStart(7) + ' |' +
                fmtBytes(base.byteLength).padStart(10) + ' |' +
                fmtBytes(segment.byteLength).padStart(9) + ' |' +
                fmtBytes(base.byteLength + folded.byteLength).padStart(15) + ' |' +
                fmtMs(foldMs).padStart(13) + ' |' +
                fmtBytes(squashedBytes).padStart(13) + ' |' +
                fmtMs(squashMs).padStart(13),
            );
        }

        console.log(`\nZero-update compaction (0 updates, 1 segment, historyFoldThreshold 8) — median of ${RUNS} runs`);
        console.log('cost avoided by not folding: transfer + merge CPU + a main-document broadcast to every client\n');
        console.log(' events |      base |  segment | fold transfer | fold merge ms | squash bytes | squash CPU ms');
        console.log('--------+-----------+----------+---------------+---------------+--------------+--------------');
        for (const r of rows) console.log(r);
    });
});
