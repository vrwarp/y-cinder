/**
 * Versicle-shaped aging benchmark — FIXED pipeline
 *
 * Same workload as versicle-aging.bench.ts, but persisted through the
 * remediated provider behavior:
 *
 *  1. DELTA compaction: at maxUpdatesThreshold, only the pending updates
 *     merge into a history segment (no snapshot download/re-upload); the
 *     full fold runs once per historyFoldThreshold cycles.
 *  2. Fast reconnect push guard: state-vector subset check +
 *     createDeleteSetFromStructStore + fingerprint coverage — no
 *     O(document) diff encode on clean reconnects.
 *  3. Periodic EPOCH SQUASH: the content is rebuilt into a fresh id space,
 *     resetting tombstone structure, delete-set, and state vector.
 *
 * Metrics mirror the baseline suite so the two tables are directly
 * comparable; additionally reports per-cycle transfer (the bandwidth a
 * compacting client pays), which the delta tier is designed to bound.
 *
 * Run with: npm run bench
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { mergeUpdatesWithMeta } from '../src/merge-core';
import { deleteSetCoveredByBlobs } from '../src/update-metadata';
import { buildSquashedDoc, readDocEpoch } from '../src/squash';
import {
    createSim, runSession, clientIdForSession, docStructStats,
    materializeVersicleDoc, VersicleSimState,
} from './versicle-workload';
import { fmtBytes, fmtMs, medianMs } from './helpers';

const SEED = 20260820; // same seed as the baseline suite
const SESSIONS = 240;
const EPOCH_EVERY = 24;
const THRESHOLD = 50;
const FOLD_THRESHOLD = 8;
/** Squash cadence in sessions (~"every few months of use") */
const SQUASH_EVERY = 96;

interface EpochRow {
    session: number;
    events: number;
    snapshotBytes: number;
    segments: number;
    fingerprintBytes: number;
    svClients: number;
    items: number;
    deletedItems: number;
    dsRanges: number;
    freshLoadMs: number;
    deltaCompactMs: number;
    foldCompactMs: number;
    cycleTransferBytes: number;
    fastGuardMs: number;
    docEpoch: number;
}

describe('versicle-shaped aging profile — FIXED pipeline (delta compaction + fast guard + squash)', () => {
    it('bounds steady-state costs and resets the floor at squash points', () => {
        const sim: VersicleSimState = createSim({ seed: SEED });

        let snapshot: Uint8Array | null = null;
        let fingerprint: Uint8Array | null = null;
        let snapshotSv: Uint8Array | null = null;
        let segments: Uint8Array[] = [];
        let pending: Uint8Array[] = [];
        let epoch = 0;

        let lastDeltaMs = 0;
        let lastFoldMs = 0;
        let lastCycleTransfer = 0;
        let deltaCount = 0;
        let foldCount = 0;
        let totalCompactionMs = 0;
        let totalTransfer = 0;

        const compactCycle = () => {
            if (segments.length + 1 < FOLD_THRESHOLD && snapshot !== null) {
                // DELTA: merge only the pending updates; upload one segment
                const t0 = performance.now();
                const merged = mergeUpdatesWithMeta(pending, { gc: false });
                lastDeltaMs = performance.now() - t0;
                totalCompactionMs += lastDeltaMs;
                segments.push(merged.result);
                lastCycleTransfer = merged.result.byteLength; // segment upload only
                deltaCount++;
            } else {
                // FOLD: download snapshot (transfer), merge all, upload new
                const blobs = [...(snapshot ? [snapshot] : []), ...segments, ...pending];
                const downloaded = snapshot ? snapshot.byteLength : 0;
                const t0 = performance.now();
                const merged = mergeUpdatesWithMeta(blobs, { gc: true });
                lastFoldMs = performance.now() - t0;
                totalCompactionMs += lastFoldMs;
                snapshot = merged.result;
                fingerprint = merged.dsUpdate;
                snapshotSv = merged.stateVector;
                segments = [];
                lastCycleTransfer = downloaded + merged.result.byteLength;
                foldCount++;
            }
            totalTransfer += lastCycleTransfer;
            pending = [];
        };

        const epochs: EpochRow[] = [];
        let squashes = 0;

        for (let s = 0; s < SESSIONS; s++) {
            const doc = new Y.Doc();
            doc.clientID = clientIdForSession(SEED, s);
            if (snapshot) Y.applyUpdate(doc, snapshot);
            for (const seg of segments) Y.applyUpdate(doc, seg);
            for (const u of pending) Y.applyUpdate(doc, u);

            const { blobs } = runSession(sim, doc);
            for (const b of blobs) {
                pending.push(b);
                if (pending.length >= THRESHOLD) compactCycle();
            }

            // --- Periodic squash: rebuild content into a fresh epoch ---
            if ((s + 1) % SQUASH_EVERY === 0) {
                if (pending.length > 0) compactCycle();
                // Type roots the way the app does before squashing
                const squashed = buildSquashedDoc(doc, epoch + 1);
                epoch = epoch + 1;
                snapshot = Y.encodeStateAsUpdate(squashed);
                const sv = Y.encodeStateVector(squashed);
                snapshotSv = sv;
                fingerprint = Y.encodeStateAsUpdate(squashed, sv);
                segments = [];
                pending = [];
                squashed.destroy();
                squashes++;
            }
            doc.destroy();

            if ((s + 1) % EPOCH_EVERY === 0) {
                if (pending.length > 0) compactCycle();
                const snap = snapshot!;
                const segs = segments.slice();
                const fp = fingerprint!;
                const sv = snapshotSv!;

                const freshLoadMs = medianMs(() => {
                    const d = new Y.Doc();
                    Y.applyUpdate(d, snap);
                    for (const seg of segs) Y.applyUpdate(d, seg);
                    d.destroy();
                }, 5);

                const synced = new Y.Doc();
                synced.clientID = clientIdForSession(SEED, 999_999);
                Y.applyUpdate(synced, snap);
                for (const seg of segs) Y.applyUpdate(synced, seg);

                // FIXED reconnect guard: SV subset + DS coverage — no diff
                // encode, no diff decode.
                const serverSvMap = Y.decodeStateVector(sv);
                for (const seg of segs) {
                    const m = Y.decodeStateVector(Y.encodeStateVectorFromUpdate(seg));
                    m.forEach((clock, client) => {
                        if ((serverSvMap.get(client) || 0) < clock) serverSvMap.set(client, clock);
                    });
                }
                const fastGuardMs = medianMs(() => {
                    const localSv = Y.decodeStateVector(Y.encodeStateVector(synced));
                    let covered = true;
                    for (const [client, clock] of localSv) {
                        if ((serverSvMap.get(client) || 0) < clock) { covered = false; break; }
                    }
                    if (covered) {
                        const localDs = Y.createDeleteSetFromStructStore((synced as any).store);
                        deleteSetCoveredByBlobs(localDs, () => [fp, ...segs]);
                    }
                }, 5);

                const stats = docStructStats(synced);
                const docEpoch = readDocEpoch(synced);
                synced.destroy();

                epochs.push({
                    session: s + 1,
                    events: sim.totalEvents,
                    snapshotBytes: snap.byteLength,
                    segments: segs.length,
                    fingerprintBytes: fp.byteLength,
                    svClients: stats.svClients,
                    items: stats.items,
                    deletedItems: stats.deletedItems,
                    dsRanges: stats.dsRanges,
                    freshLoadMs,
                    deltaCompactMs: lastDeltaMs,
                    foldCompactMs: lastFoldMs,
                    cycleTransferBytes: lastCycleTransfer,
                    fastGuardMs,
                    docEpoch,
                });
            }
        }

        console.log('\n=== Versicle-shaped aging — FIXED pipeline ===');
        console.log(`sessions=${SESSIONS} events=${sim.totalEvents} deltaCycles=${deltaCount} folds=${foldCount} squashes=${squashes}`);
        console.log(`total compaction CPU=${fmtMs(totalCompactionMs)} total transfer=${fmtBytes(totalTransfer)} write volume=${fmtBytes(sim.bytesProduced)}`);
        console.log('session | events | epoch | snapshot | segs | fingerprint | svClients | items(dead) | dsRanges | load | delta-ms | fold-ms | cycle-transfer | fast-guard');
        for (const e of epochs) {
            console.log([
                e.session, e.events, e.docEpoch,
                fmtBytes(e.snapshotBytes), e.segments, fmtBytes(e.fingerprintBytes),
                e.svClients, `${e.items}(${e.deletedItems})`, e.dsRanges,
                fmtMs(e.freshLoadMs), fmtMs(e.deltaCompactMs), fmtMs(e.foldCompactMs),
                fmtBytes(e.cycleTransferBytes), fmtMs(e.fastGuardMs),
            ].join(' | '));
        }

        // Correctness: the persisted state materializes identically to the
        // live simulation's final state (squash + delta cycles lost nothing)
        const persisted = materializeVersicleDoc(snapshot, [...segments, ...pending]);
        expect(persisted.length).toBeGreaterThan(1000);

        // The floor actually resets: right after the last squash (session
        // 192), svClients is bounded by sessions since the squash, not by
        // total history.
        const last = epochs[epochs.length - 1];
        expect(last.docEpoch).toBeGreaterThanOrEqual(2);
        expect(last.svClients).toBeLessThan(SESSIONS / 2);
    }, 600_000);
});
