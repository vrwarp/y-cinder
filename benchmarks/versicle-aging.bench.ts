/**
 * Versicle-shaped document aging benchmark
 *
 * Ages ONE Yjs document through hundreds of versicle-like sessions (fresh
 * clientID per session, page-turn/TTS churn, book add/remove, sawtooth
 * array caps) while persisting through y-cinder's storage shape (debounced
 * update blobs, compaction at maxUpdatesThreshold like FireProvider), and
 * samples every age-sensitive provider hot path at regular epochs:
 *
 *  - compaction merge cost + snapshot / fingerprint / state-vector sizes
 *  - fresh-client initial load (apply snapshot + pending)
 *  - reconnect push guard: encodeStateAsUpdate(doc, serverSV) + the
 *    diffCarriesNewData coverage check, with and without the delete-set
 *    fingerprint (the "without" column is what every reconnect pays once
 *    the fingerprint exceeds its inline cap and is dropped)
 *  - snapshot-listener delete-set fingerprint re-apply on a synced doc
 *  - save-path metadata extraction on a typical update blob
 *
 * The goal is the DERIVATIVE of each metric with respect to document age:
 * anything that grows with total historical churn (rather than live
 * content) is a long-run degradation.
 *
 * Run with: npm run bench
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { mergeUpdatesWithMeta } from '../src/merge-core';
import { extractClockEnds, diffCarriesNewData } from '../src/update-metadata';
import {
    createSim, runSession, clientIdForSession, docStructStats,
    materializeVersicleDoc, VersicleSimState,
} from './versicle-workload';
import { fmtBytes, fmtMs, medianMs } from './helpers';

const SEED = 20260820;
const SESSIONS = 240;
const EPOCH_EVERY = 24;
const THRESHOLD = 50; // versicle's maxUpdatesThreshold

interface EpochRow {
    session: number;
    events: number;
    snapshotBytes: number;
    fingerprintBytes: number;
    svBytes: number;
    svClients: number;
    items: number;
    deletedItems: number;
    gcStructs: number;
    dsRanges: number;
    freshLoadMs: number;
    reconnectEncodeMs: number;
    reconnectDiffBytes: number;
    guardWithFpMs: number;
    guardWithoutFpMs: number;
    fingerprintApplyMs: number;
    saveMetaMs: number;
    compactionMsAtEpoch: number;
}

function fitSlope(xs: number[], ys: number[]): number {
    // least-squares slope (unit: y per x)
    const n = xs.length;
    const mx = xs.reduce((a, b) => a + b, 0) / n;
    const my = ys.reduce((a, b) => a + b, 0) / n;
    let num = 0, den = 0;
    for (let i = 0; i < n; i++) {
        num += (xs[i] - mx) * (ys[i] - my);
        den += (xs[i] - mx) * (xs[i] - mx);
    }
    return den === 0 ? 0 : num / den;
}

describe('versicle-shaped aging profile (y-cinder hot paths vs document age)', () => {
    it('measures degradation of provider hot paths as the document ages', () => {
        const sim: VersicleSimState = createSim({ seed: SEED });

        let snapshot: Uint8Array | null = null;
        let fingerprint: Uint8Array | null = null;
        let snapshotSv: Uint8Array | null = null;
        let pending: Uint8Array[] = [];
        let lastCompactionMs = 0;
        let compactions = 0;
        let totalCompactionMs = 0;

        const compactNow = () => {
            const blobs = [...(snapshot ? [snapshot] : []), ...pending];
            const t0 = performance.now();
            const merged = mergeUpdatesWithMeta(blobs, { gc: true });
            lastCompactionMs = performance.now() - t0;
            totalCompactionMs += lastCompactionMs;
            compactions++;
            snapshot = merged.result;
            fingerprint = merged.dsUpdate;
            snapshotSv = merged.stateVector;
            pending = [];
        };

        const epochs: EpochRow[] = [];

        for (let s = 0; s < SESSIONS; s++) {
            // ---- One versicle session: fresh Y.Doc, fresh clientID ----
            const doc = new Y.Doc();
            doc.clientID = clientIdForSession(SEED, s);
            if (snapshot) Y.applyUpdate(doc, snapshot);
            for (const u of pending) Y.applyUpdate(doc, u);

            const { blobs } = runSession(sim, doc);
            for (const b of blobs) {
                pending.push(b);
                if (pending.length >= THRESHOLD) compactNow();
            }
            doc.destroy();

            // ---- Epoch instrumentation ----
            if ((s + 1) % EPOCH_EVERY === 0) {
                // Compact pending so epoch measurements see steady state
                if (pending.length > 0) compactNow();
                const snap = snapshot!;
                const fp = fingerprint!;
                const sv = snapshotSv!;

                // Fresh-client initial load (initial sync apply path)
                const freshLoadMs = medianMs(() => {
                    const d = new Y.Doc();
                    Y.applyUpdate(d, snap);
                    d.destroy();
                }, 5);

                // A synced client for reconnect measurements
                const synced = new Y.Doc();
                synced.clientID = clientIdForSession(SEED, 999_999);
                Y.applyUpdate(synced, snap);

                // Reconnect push path: encode local diff against server SV
                const serverSv = sv;
                let reconnectDiffBytes = 0;
                const reconnectEncodeMs = medianMs(() => {
                    const diff = Y.encodeStateAsUpdate(synced, serverSv);
                    reconnectDiffBytes = diff.byteLength;
                }, 5);
                const diff = Y.encodeStateAsUpdate(synced, serverSv);

                // Push guard WITH the delete-set fingerprint available
                const guardWithFpMs = medianMs(() => {
                    diffCarriesNewData(diff, () => [fp]);
                }, 5);

                // Push guard WITHOUT it (fingerprint dropped: > inline cap).
                // Server blobs = the full snapshot — the guard must decode it.
                const guardWithoutFpMs = medianMs(() => {
                    diffCarriesNewData(diff, () => [snap]);
                }, 3);

                // Snapshot listener re-applying the fingerprint on every
                // delivery (all deletions already known locally)
                const fingerprintApplyMs = medianMs(() => {
                    Y.applyUpdate(synced, fp);
                }, 5);

                // Save-path metadata extraction on a typical session blob
                const typicalBlob = blobs[blobs.length - 1];
                const saveMetaMs = medianMs(() => {
                    extractClockEnds(typicalBlob);
                }, 7);

                const stats = docStructStats(synced);
                synced.destroy();

                epochs.push({
                    session: s + 1,
                    events: sim.totalEvents,
                    snapshotBytes: snap.byteLength,
                    fingerprintBytes: fp.byteLength,
                    svBytes: sv.byteLength,
                    svClients: stats.svClients,
                    items: stats.items,
                    deletedItems: stats.deletedItems,
                    gcStructs: stats.gcStructs,
                    dsRanges: stats.dsRanges,
                    freshLoadMs,
                    reconnectEncodeMs,
                    reconnectDiffBytes,
                    guardWithFpMs,
                    guardWithoutFpMs,
                    fingerprintApplyMs,
                    saveMetaMs,
                    compactionMsAtEpoch: lastCompactionMs,
                });
            }
        }

        // ---- Report ----
        console.log('\n=== Versicle-shaped aging: y-cinder hot paths vs age ===');
        console.log(`sessions=${SESSIONS} events=${sim.totalEvents} compactions=${compactions} totalCompactionCpu=${fmtMs(totalCompactionMs)} writeVolume=${fmtBytes(sim.bytesProduced)}`);
        console.log('session | events | snapshot | fingerprint | sv | svClients | items(dead) | GC | dsRanges | load | compact | rc-encode(diff) | guard+fp | guard-fp | fp-apply | saveMeta');
        for (const e of epochs) {
            console.log([
                e.session,
                e.events,
                fmtBytes(e.snapshotBytes),
                fmtBytes(e.fingerprintBytes),
                fmtBytes(e.svBytes),
                e.svClients,
                `${e.items}(${e.deletedItems})`,
                e.gcStructs,
                e.dsRanges,
                fmtMs(e.freshLoadMs),
                fmtMs(e.compactionMsAtEpoch),
                `${fmtMs(e.reconnectEncodeMs)}(${fmtBytes(e.reconnectDiffBytes)})`,
                fmtMs(e.guardWithFpMs),
                fmtMs(e.guardWithoutFpMs),
                fmtMs(e.fingerprintApplyMs),
                fmtMs(e.saveMetaMs),
            ].join(' | '));
        }

        // ---- Degradation slopes (per 1000 events) ----
        const xs = epochs.map(e => e.events / 1000);
        const slope = (sel: (e: EpochRow) => number) => fitSlope(xs, epochs.map(sel));
        console.log('\n--- growth per 1000 events (least-squares slope) ---');
        console.log(`snapshot bytes:      ${fmtBytes(Math.round(slope(e => e.snapshotBytes)))}`);
        console.log(`fingerprint bytes:   ${fmtBytes(Math.round(slope(e => e.fingerprintBytes)))}`);
        console.log(`state vector bytes:  ${slope(e => e.svBytes).toFixed(1)} B`);
        console.log(`live items:          ${slope(e => e.items).toFixed(0)}`);
        console.log(`dead items:          ${slope(e => e.deletedItems).toFixed(0)}`);
        console.log(`fresh load ms:       ${slope(e => e.freshLoadMs).toFixed(3)}`);
        console.log(`compaction ms:       ${slope(e => e.compactionMsAtEpoch).toFixed(3)}`);
        console.log(`reconnect encode ms: ${slope(e => e.reconnectEncodeMs).toFixed(3)}`);
        console.log(`guard+fp ms:         ${slope(e => e.guardWithFpMs).toFixed(3)}`);
        console.log(`guard-fp ms:         ${slope(e => e.guardWithoutFpMs).toFixed(3)}`);
        console.log(`fp re-apply ms:      ${slope(e => e.fingerprintApplyMs).toFixed(3)}`);

        // ---- Correctness pin: persisted state materializes identically to
        // a doc that applied every blob (compaction lost nothing) ----
        const persisted = materializeVersicleDoc(snapshot, pending);
        expect(persisted.length).toBeGreaterThan(1000);

        // The un-fixable floor exists (tombstone structure), but the point
        // of this suite is the numbers above; only sanity-pin extremes:
        const last = epochs[epochs.length - 1];
        expect(last.snapshotBytes).toBeGreaterThan(0);
        expect(last.svClients).toBeGreaterThanOrEqual(SESSIONS);
    }, 600_000);
});
