/**
 * Returning-device catch-up after a missed fold
 *
 * versicle shape: device B is warm (hydrated from y-idb) and fully synced,
 * then away while device A keeps reading. A's saves cross one FOLD
 * (every historyFoldThreshold x maxUpdatesThreshold = 400 saves, about 6-7
 * sessions). The fold merges base + history + updates into a new snapshot
 * and deletes the sources, so the data B lacks now lives only inside the
 * multi-MB snapshot. B's warm start (and the snapshot listener) see
 * `!localCoversSnapshot` and fall back to downloading the WHOLE snapshot
 * and applying it with a full Y.applyUpdate (src/sync.ts initial sync and
 * createSnapshotListener), although B already holds nearly all of it.
 *
 * This benchmark ages one document through the same workload and the
 * provider's persistence shape (delta compaction + periodic fold, as in
 * versicle-aging-fixed.bench.ts), hydrates B at checkpoints, lets A cross
 * exactly one fold, and then measures B's catch-up two ways:
 *
 *  - CURRENT: what sync.ts does today. Download the snapshot (Storage),
 *    apply fingerprint + snapshot + post-fold segments/updates.
 *  - TAIL (the fix): the fold additionally publishes
 *    mergeUpdates(historyToMerge + updatesToFold) (gc off) with its base
 *    clocks — the replaced snapshot's state vector restricted to the
 *    clients the tail touches. B covers the replaced snapshot, so it
 *    downloads/applies the tail instead, re-checks localCoversSnapshot, and
 *    needs the snapshot only if that still fails. The publish decision
 *    (foldTailBaseClocks, shouldPublishFoldTail) and the reader's pre-check
 *    (foldTailMayCatchUp) are the shipped functions.
 *
 * Deterministic costs reported: Storage bytes downloaded, structs decoded
 * from the Storage blob, B's real diff. CPU is the median of interleaved
 * runs (each on a fresh clone of warm B, clone outside the timed region).
 *
 * Run just this scenario with:
 *   npx vitest run --config benchmarks/vitest.config.ts benchmarks/returning-device.bench.ts
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { toBase64 } from 'lib0/buffer';
import { mergeUpdatesWithMeta } from '../src/merge-core';
import { foldTailBaseClocks, shouldPublishFoldTail } from '../src/compaction-policy';
import { foldTailMayCatchUp, localCoversSnapshot } from '../src/sync-helpers';
import { DEFAULTS } from '../src/types';
import { writeStateVector } from '../src/utils';
import { createSim, runSession, clientIdForSession, VersicleSimState } from './versicle-workload';
import { fmtBytes, fmtMs } from './helpers';

const SEED = 20260820; // same seed as the aging suites
const THRESHOLD = 50; // maxUpdatesThreshold
const FOLD_THRESHOLD = 8; // historyFoldThreshold (default)
/** Sessions at which device B is hydrated and then goes away */
const CHECKPOINTS = [60, 120, 240];
/** Sessions A keeps reading after the fold B missed (left in history/updates) */
const SESSIONS_AFTER_FOLD = 2;
const REPS = 7;
const B_CLIENT = 4_242_424_242;

const ROOTS = ['library', 'progress', 'annotations', 'reading-list', 'vocabulary', 'lexicon', 'contentAnalysis', 'devices', 'searchHistory', 'meta'];

function docJson(d: Y.Doc): string {
    const out: Record<string, unknown> = {};
    for (const name of ROOTS) out[name] = d.getMap(name).toJSON();
    return JSON.stringify(out);
}

function svCovers(local: Map<number, number>, remote: Map<number, number>): boolean {
    for (const [client, clock] of remote) {
        if ((local.get(client) || 0) < clock) return false;
    }
    return true;
}

function median(xs: number[]): number {
    const s = xs.slice().sort((a, b) => a - b);
    return s[Math.floor(s.length / 2)];
}

interface CatchUpRow {
    hydratedAt: number;
    measuredAt: number;
    editsAway: number;
    snapshotBytes: number;
    tailBytes: number;
    fingerprintBytes: number;
    postFoldFirestoreBytes: number;
    diffBytes: number;
    snapshotStructs: number;
    tailStructs: number;
    currentMs: number;
    tailMs: number;
    snapshotApplyMs: number;
    tailApplyMs: number;
}

describe('returning device after a missed fold (versicle workload)', () => {
    it('measures catch-up transfer and CPU: full snapshot vs fold tail', () => {
        const sim: VersicleSimState = createSim({ seed: SEED });

        // --- Server state, persisted exactly like the provider does ---
        let snapshot: Uint8Array | null = null;
        let snapshotSv: Uint8Array | null = null;
        let fingerprint: Uint8Array | null = null;
        let segments: Uint8Array[] = [];
        let pending: Uint8Array[] = [];
        // What the fix additionally publishes with the last fold
        let foldTail: Uint8Array | null = null;
        let foldTailClocks: string | null = null;
        let foldBaseSv: Uint8Array | null = null;
        let folds = 0;

        const compactCycle = () => {
            if (segments.length + 1 < FOLD_THRESHOLD && snapshot !== null) {
                // DELTA: pending updates -> one history segment
                segments.push(mergeUpdatesWithMeta(pending, { gc: false }).result);
            } else {
                // FOLD: base + history + updates -> new GC'd snapshot
                const tailSources = [...segments, ...pending];
                const merged = mergeUpdatesWithMeta([...(snapshot ? [snapshot] : []), ...tailSources], { gc: true });
                foldTail = null;
                foldTailClocks = null;
                if (snapshot) {
                    // As compaction's publishFoldTail
                    const tail = mergeUpdatesWithMeta(tailSources, { gc: false });
                    const clocks = toBase64(writeStateVector(foldTailBaseClocks(
                        Y.decodeStateVector(snapshotSv!),
                        Y.decodeStateVector(tail.stateVector),
                    )));
                    if (shouldPublishFoldTail({
                        tailBytes: tail.result.byteLength,
                        snapshotBytes: merged.result.byteLength,
                        tailFieldsLength: clocks.length,
                        stateVectorB64Length: toBase64(merged.stateVector).length,
                        inlineDeleteSetBytes: merged.dsUpdate.byteLength,
                        inlineLimit: DEFAULTS.INLINE_UPDATE_LIMIT,
                    })) {
                        foldTail = tail.result;
                        foldTailClocks = clocks;
                    }
                }
                foldBaseSv = snapshotSv;
                snapshot = merged.result;
                snapshotSv = merged.stateVector;
                fingerprint = merged.dsUpdate;
                segments = [];
                folds++;
            }
            pending = [];
        };

        // Device A: one live doc, a fresh clientID per session (versicle
        // mints a new Y.Doc per page load). Every blob A produces is
        // persisted, so at a session boundary A's doc == server state.
        const live = new Y.Doc();
        const runOne = (s: number) => {
            live.clientID = clientIdForSession(SEED, s);
            const { blobs } = runSession(sim, live);
            for (const b of blobs) {
                pending.push(b);
                if (pending.length >= THRESHOLD) compactCycle();
            }
        };

        const rows: CatchUpRow[] = [];
        let s = 0;
        for (const checkpoint of CHECKPOINTS) {
            while (s < checkpoint) runOne(s++);

            // B is warm and fully synced at the checkpoint (y-idb hydration)
            const bState = Y.encodeStateAsUpdate(live);
            const bSv = Y.decodeStateVector(Y.encodeStateVectorFromUpdate(bState));
            const eventsAtHydration = sim.totalEvents;

            // A keeps reading until it has crossed exactly one fold, then a
            // couple more sessions (they stay in history/updates).
            const foldsAtHydration = folds;
            while (folds === foldsAtHydration) runOne(s++);
            for (let k = 0; k < SESSIONS_AFTER_FOLD; k++) runOne(s++);
            expect(folds - foldsAtHydration).toBe(1);

            expect(foldTail).not.toBeNull(); // the fold published its tail
            const snap = snapshot!;
            const sv = snapshotSv!;
            const fp = fingerprint!;
            const tail = foldTail!;
            const baseSv = Y.decodeStateVector(foldBaseSv!);
            const postFold = [...segments, ...pending];
            // The main document as the fold wrote it
            const snapshotData = {
                stateVector: toBase64(sv),
                version: folds,
                foldTailStoragePath: 'tail.bin',
                foldTailBaseClocks: foldTailClocks!,
                foldTailVersion: folds,
            };

            // Preconditions of the scenario: B covers the replaced
            // snapshot but not the new one, so sync.ts downloads the blob.
            expect(svCovers(bSv, baseSv)).toBe(true);
            expect(svCovers(bSv, Y.decodeStateVector(sv))).toBe(false);

            const loadWarmB = (): Y.Doc => {
                const d = new Y.Doc();
                d.clientID = B_CLIENT;
                Y.applyUpdate(d, bState);
                return d;
            };

            const applyPostFold = (d: Y.Doc) => {
                for (const blob of postFold) Y.applyUpdate(d, blob);
            };

            // CURRENT: !localCoversSnapshot -> getBytes(snapshot) + full apply
            const catchUpCurrent = (d: Y.Doc): { downloaded: number; ms: number; applyMs: number } => {
                const t0 = performance.now();
                let applyMs = 0;
                d.transact(() => {
                    expect(localCoversSnapshot(snapshotData, d)).toBe(false);
                    const a0 = performance.now();
                    Y.applyUpdate(d, snap);
                    applyMs = performance.now() - a0;
                    Y.applyUpdate(d, fp);
                    applyPostFold(d);
                });
                return { downloaded: snap.byteLength, ms: performance.now() - t0, applyMs };
            };

            // TAIL: foldTailMayCatchUp -> tail, re-check, fallback
            const catchUpTail = (d: Y.Doc): { downloaded: number; ms: number; applyMs: number; fellBack: boolean } => {
                const t0 = performance.now();
                let applyMs = 0;
                let downloaded = 0;
                let fellBack = false;
                d.transact(() => {
                    expect(localCoversSnapshot(snapshotData, d)).toBe(false);
                    if (foldTailMayCatchUp(snapshotData, d)) {
                        downloaded += tail.byteLength;
                        const a0 = performance.now();
                        Y.applyUpdate(d, tail);
                        applyMs = performance.now() - a0;
                    }
                    Y.applyUpdate(d, fp);
                    if (!localCoversSnapshot(snapshotData, d)) {
                        fellBack = true;
                        downloaded += snap.byteLength;
                        Y.applyUpdate(d, snap);
                    }
                    applyPostFold(d);
                });
                return { downloaded, ms: performance.now() - t0, applyMs, fellBack };
            };

            // Reference result + correctness of the tail path
            const ref = loadWarmB();
            const bSvBytes = Y.encodeStateVector(ref);
            catchUpCurrent(ref);
            const diffBytes = Y.encodeStateAsUpdate(ref, bSvBytes).byteLength;
            const refJson = docJson(ref);
            const refSv = toBase64(Y.encodeStateVector(ref));
            ref.destroy();

            const viaTail = loadWarmB();
            const tailRun = catchUpTail(viaTail);
            expect(tailRun.fellBack).toBe(false);
            expect(docJson(viaTail)).toBe(refJson);
            expect(toBase64(Y.encodeStateVector(viaTail))).toBe(refSv);
            viaTail.destroy();

            // Interleaved timing (clone outside the timed region)
            const cur: number[] = [], tl: number[] = [], curApply: number[] = [], tlApply: number[] = [];
            for (let r = 0; r < REPS; r++) {
                const order = r % 2 === 0 ? ['current', 'tail'] : ['tail', 'current'];
                for (const mode of order) {
                    const d = loadWarmB();
                    if (mode === 'current') {
                        const m = catchUpCurrent(d);
                        cur.push(m.ms); curApply.push(m.applyMs);
                    } else {
                        const m = catchUpTail(d);
                        tl.push(m.ms); tlApply.push(m.applyMs);
                    }
                    d.destroy();
                }
            }

            rows.push({
                hydratedAt: checkpoint,
                measuredAt: s,
                editsAway: sim.totalEvents - eventsAtHydration,
                snapshotBytes: snap.byteLength,
                tailBytes: tail.byteLength,
                fingerprintBytes: fp.byteLength,
                postFoldFirestoreBytes: postFold.reduce((n, b) => n + b.byteLength, 0),
                diffBytes,
                snapshotStructs: Y.decodeUpdate(snap).structs.length,
                tailStructs: Y.decodeUpdate(tail).structs.length,
                currentMs: median(cur),
                tailMs: median(tl),
                snapshotApplyMs: median(curApply),
                tailApplyMs: median(tlApply),
            });
        }
        live.destroy();

        console.log('\n=== Returning device after one missed fold (versicle workload) ===');
        console.log(`seed=${SEED} maxUpdatesThreshold=${THRESHOLD} historyFoldThreshold=${FOLD_THRESHOLD} reps=${REPS} (medians)`);
        console.log('hydrated@ | caught-up@ | edits away | B real diff | Storage dl: CURRENT (snapshot) | TAIL | amplification CUR/TAIL | structs decoded CUR/TAIL | +Firestore (both) | fingerprint | CPU to synced CUR/TAIL | blob apply CUR/TAIL');
        for (const r of rows) {
            console.log([
                r.hydratedAt, r.measuredAt, r.editsAway, fmtBytes(r.diffBytes),
                fmtBytes(r.snapshotBytes), fmtBytes(r.tailBytes),
                `${(r.snapshotBytes / r.diffBytes).toFixed(1)}x / ${(r.tailBytes / r.diffBytes).toFixed(1)}x`,
                `${r.snapshotStructs} / ${r.tailStructs}`,
                fmtBytes(r.postFoldFirestoreBytes), fmtBytes(r.fingerprintBytes),
                `${fmtMs(r.currentMs)} / ${fmtMs(r.tailMs)}`,
                `${fmtMs(r.snapshotApplyMs)} / ${fmtMs(r.tailApplyMs)}`,
            ].join(' | '));
        }

        // The current path's transfer is O(snapshot): it grows with document
        // age while what B actually lacks does not.
        expect(rows[rows.length - 1].snapshotBytes).toBeGreaterThan(rows[0].snapshotBytes * 2);
        for (const r of rows) {
            expect(r.tailBytes).toBeLessThan(r.snapshotBytes);
        }
    }, 600_000);
});
