/**
 * Benchmark: CPU cost of the cold-start hydration echo.
 *
 * When FireProvider is constructed before local persistence (y-idb)
 * hydrates the Y.Doc, the hydration transaction's 'update' event — the
 * WHOLE document — is buffered as a local edit and saved as a new update
 * document (the I/O side is counted against the emulator in
 * tests/integration/hydration_echo_upload.test.ts). This suite measures
 * the CPU side of that echo, pure Yjs, on the versicle workload:
 *
 *  - echo size: the update event y-idb's single load transaction emits,
 *    i.e. exactly what handleUpdate buffers (O(document));
 *  - save path on the cold-start client: extractClockEnds +
 *    aggregateClockEnds, what _executeSave runs before writing;
 *  - every online peer: above MAX_METADATA_CLIENTS (50) clients the echo
 *    carries no clientIDs/clientClocks, so isUpdateRedundant cannot skip
 *    it and the update listener runs a full Y.applyUpdate — on a document
 *    that already holds every byte of it (asserted: nothing changes).
 *
 * versicle mints a fresh clientID per page load, so the 50-client cutoff
 * is passed after ~50 sessions; aged documents are far beyond it.
 *
 * Run with: npm run bench   (or: npx vitest run --config benchmarks/vitest.config.ts benchmarks/hydration-echo.bench.ts)
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { extractClockEnds, aggregateClockEnds, isUpdateRedundant } from '../src/update-metadata';
import { FIREBASE_ORIGINS } from '../src/types';
import { createSim, runSession, clientIdForSession } from './versicle-workload';
import { fmtBytes, fmtMs, medianMs } from './helpers';

const SEED = 20260820;
const SIZES = [20, 60, 120, 240];
const ITERATIONS = 7;
const IDB_ORIGIN = { name: 'y-idb persistence (stand-in)' };

function buildAgedState(sessions: number): Uint8Array {
    const sim = createSim({ seed: SEED });
    const doc = new Y.Doc();
    for (let s = 0; s < sessions; s++) {
        doc.clientID = clientIdForSession(SEED, s);
        runSession(sim, doc);
    }
    const state = Y.encodeStateAsUpdate(doc);
    doc.destroy();
    return state;
}

/** The update event a y-idb load emits on a freshly constructed doc. */
function captureHydrationEcho(persisted: Uint8Array): Uint8Array {
    const doc = new Y.Doc();
    let echo: Uint8Array | null = null;
    doc.on('update', (u: Uint8Array, origin: unknown) => {
        if (origin === IDB_ORIGIN) echo = u;
    });
    Y.transact(doc, () => Y.applyUpdate(doc, persisted), IDB_ORIGIN, false);
    doc.destroy();
    return echo!;
}

describe('cold-start hydration echo (versicle workload)', () => {
    it('echo size, save-path metadata, and the peer apply it forces', () => {
        const rows: string[] = [];
        const echoBytes: number[] = [];
        const peerApplyMs: number[] = [];

        for (const sessions of SIZES) {
            const state = buildAgedState(sessions);
            const echo = captureHydrationEcho(state);
            echoBytes.push(echo.byteLength);

            // Save path on the cold-start client
            const saveMetaMs = medianMs(() => aggregateClockEnds(extractClockEnds(echo)), ITERATIONS);
            const meta = aggregateClockEnds(extractClockEnds(echo));
            const hasMetadata = (meta.clientIDs?.length ?? 0) > 0;

            // Online peer that already holds the whole document
            const peer = new Y.Doc();
            Y.applyUpdate(peer, state);
            const peerSV = Y.decodeStateVector(Y.encodeStateVector(peer));
            const svBefore = Y.encodeStateVector(peer);
            let peerUpdateEvents = 0;
            peer.on('update', () => { peerUpdateEvents++; });

            const skippedByMetadata = hasMetadata && isUpdateRedundant(peerSV, meta.clientIDs!, meta.clientClocks!);
            const applyMs = skippedByMetadata
                ? 0
                : medianMs(() => Y.applyUpdate(peer, echo, FIREBASE_ORIGINS.UPDATE), ITERATIONS);
            peerApplyMs.push(applyMs);

            // The echo carries nothing the peer lacks.
            expect(Buffer.from(Y.encodeStateVector(peer)).equals(Buffer.from(svBefore))).toBe(true);
            expect(peerUpdateEvents).toBe(0);
            peer.destroy();

            rows.push(
                `  ${String(sessions).padStart(4)} sessions | state ${fmtBytes(state.byteLength).padStart(9)} | ` +
                `echo ${fmtBytes(echo.byteLength).padStart(9)} | metadata ${hasMetadata ? 'yes' : 'NO '} | ` +
                `save-path meta ${fmtMs(saveMetaMs).padStart(8)} | ` +
                `peer ${skippedByMetadata ? 'skips via metadata  ' : `full applyUpdate ${fmtMs(applyMs).padStart(8)}`}`,
            );
        }

        const last = SIZES.length - 1;
        console.log(
            `\nCold-start hydration echo — redundant O(document) upload per cold start (median of ${ITERATIONS}):\n` +
            rows.join('\n') +
            `\n  echo bytes ${SIZES[last]} vs ${SIZES[1]} sessions: ${(echoBytes[last] / echoBytes[1]).toFixed(2)}x ` +
            `(linear in document age); peer apply: ${(peerApplyMs[last] / Math.max(peerApplyMs[1], 0.001)).toFixed(2)}x\n`,
        );

        // Above 50 clients every peer must apply the full echo.
        expect(peerApplyMs.slice(1).every(ms => ms > 0)).toBe(true);
    });
});
