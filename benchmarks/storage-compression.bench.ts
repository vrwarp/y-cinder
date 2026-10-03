/**
 * Storage blob compression benchmark (versicle-shaped aged document)
 *
 * Every Cloud Storage blob y-cinder writes — fold snapshots, squash
 * snapshots, offloaded large updates, offloaded delete-set fingerprints —
 * used to be uploaded and downloaded as raw Yjs V1 bytes
 * (`uploadBytes(ref, v1)` / `getBytes(ref)`). Storage egress is billed per
 * byte, and on mobile it is the user's bandwidth, so every snapshot
 * transfer — a fresh or returning device's cold start, the snapshot
 * listener on a fold the local doc does not cover, and every fold's
 * download + upload — paid the uncompressed size. The library now uploads
 * every blob through `uploadBlob` (src/storage-blobs.ts): gzip via
 * `gzipBlob` (CompressionStream, round-trip checked), stored with
 * Content-Encoding: gzip so readers still receive raw V1.
 *
 * This benchmark ages one versicle document through the CURRENT provider
 * persistence shape (delta compaction every THRESHOLD updates, a GC'd fold
 * every FOLD_THRESHOLD cycles, no squash — the worst case, like
 * versicle-aging-fixed.bench.ts without its periodic squash) and measures,
 * deterministically:
 *
 *   - the size of each blob kind as raw V1 (what is stored today) vs
 *     gzip(V1) at levels 1 and 6, V2, and gzip(V2), at checkpoints;
 *   - the cumulative Storage bytes moved by all folds over the document's
 *     life (download old snapshot + upload new one), raw vs compressed;
 *   - per-trigger transfer at the final age (cold start, fold);
 *
 * and, as medians of interleaved runs at the final age, the CPU price of
 * compressing (Node zlib, the web CompressionStream API and the library's
 * own `gzipBlob`) next to the fold merge and the V1-vs-V2 apply-speed
 * trade-off, plus the size of what the library actually uploads.
 *
 * Byte counts are deterministic (seeded workload, deterministic client
 * ids; the squash column is the one exception, see below). The integration
 * counterpart, which counts the bytes the provider actually moves through
 * uploadBytes / getBytes against the emulator, is
 * tests/integration/storage_blob_compression.test.ts.
 *
 * Run with: npx vitest run --config benchmarks/vitest.config.ts benchmarks/storage-compression.bench.ts
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { gzipSync, gunzipSync } from 'node:zlib';
import { mergeUpdatesWithMeta } from '../src/merge-core';
import { buildSquashedDoc } from '../src/squash';
import { gzipBlob } from '../src/gzip';
import {
    createSim, runSession, clientIdForSession, materializeVersicleDoc,
} from './versicle-workload';
import { fmtBytes, fmtMs } from './helpers';

const SEED = 20260820; // same seed as the versicle aging suites
const SESSIONS = 240; // 14,400 events
const CHECKPOINT_EVERY = 48;
const THRESHOLD = 50;
const FOLD_THRESHOLD = 8;
const TIMING_RUNS = 7;

const gz = (b: Uint8Array, level: number) => gzipSync(b, { level }).byteLength;
const v2 = (b: Uint8Array) => Y.convertUpdateFormatV1ToV2(b);
const ratio = (raw: number, packed: number) => (raw / packed).toFixed(2) + 'x';

interface BlobSizes {
    raw: number;
    gz1: number;
    gz6: number;
    v2: number;
    gzV2: number;
}

function sizes(b: Uint8Array): BlobSizes {
    const asV2 = v2(b);
    return { raw: b.byteLength, gz1: gz(b, 1), gz6: gz(b, 6), v2: asV2.byteLength, gzV2: gz(asV2, 6) };
}

function fmtSizes(s: BlobSizes): string {
    return `${fmtBytes(s.raw)} → gz1 ${fmtBytes(s.gz1)} (${ratio(s.raw, s.gz1)}), gz6 ${fmtBytes(s.gz6)} (${ratio(s.raw, s.gz6)}), ` +
        `V2 ${fmtBytes(s.v2)} (${ratio(s.raw, s.v2)}), gz(V2) ${fmtBytes(s.gzV2)} (${ratio(s.raw, s.gzV2)})`;
}

async function streamGzip(bytes: Uint8Array): Promise<Uint8Array> {
    const stream = new Blob([bytes]).stream().pipeThrough(new CompressionStream('gzip'));
    return new Uint8Array(await new Response(stream).arrayBuffer());
}

async function streamGunzip(bytes: Uint8Array): Promise<Uint8Array> {
    const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
    return new Uint8Array(await new Response(stream).arrayBuffer());
}

function median(xs: number[]): number {
    const s = xs.slice().sort((a, b) => a - b);
    return s[Math.floor(s.length / 2)];
}

describe('Storage blob compression on an aged versicle doc', () => {
    it('measures blob sizes, lifetime fold transfer and compression CPU', async () => {
        const sim = createSim({ seed: SEED });

        let snapshot: Uint8Array | null = null;
        let snapshotSizes: BlobSizes | null = null; // sizes of `snapshot`, computed at its upload
        let segments: Uint8Array[] = [];
        let pending: Uint8Array[] = [];
        let lastFoldMs = 0;

        // Lifetime Storage transfer of every fold (download the previous
        // snapshot + upload the new one), per encoding.
        const foldTransfer = { folds: 0, raw: 0, gz1: 0, gz6: 0, gzV2: 0 };
        const segmentSizes: BlobSizes[] = [];

        const compactCycle = () => {
            if (segments.length + 1 < FOLD_THRESHOLD && snapshot !== null) {
                // DELTA: only the pending updates merge into an inline segment
                const merged = mergeUpdatesWithMeta(pending, { gc: false });
                segments.push(merged.result);
                if (segmentSizes.length < 64) segmentSizes.push(sizes(merged.result));
            } else {
                // FOLD: download base, merge everything, upload new snapshot
                const t0 = performance.now();
                const merged = mergeUpdatesWithMeta([...(snapshot ? [snapshot] : []), ...segments, ...pending], { gc: true });
                lastFoldMs = performance.now() - t0;
                const next = merged.result;
                const down = snapshotSizes; // the base downloaded is the previous fold's upload
                const up = sizes(next);
                foldTransfer.folds++;
                foldTransfer.raw += (down?.raw ?? 0) + up.raw;
                foldTransfer.gz1 += (down?.gz1 ?? 0) + up.gz1;
                foldTransfer.gz6 += (down?.gz6 ?? 0) + up.gz6;
                foldTransfer.gzV2 += (down?.gzV2 ?? 0) + up.gzV2;
                snapshot = next;
                snapshotSizes = up;
                segments = [];
            }
            pending = [];
        };

        const checkpoints: Array<{
            session: number; events: number; snapshot: BlobSizes; fingerprint: BlobSizes; squash: BlobSizes;
        }> = [];

        let finalSnapshot: Uint8Array = new Uint8Array(0);
        let finalFingerprint: Uint8Array = new Uint8Array(0);
        const doc = new Y.Doc();
        for (let s = 0; s < SESSIONS; s++) {
            doc.clientID = clientIdForSession(SEED, s); // fresh client per app launch
            const { blobs } = runSession(sim, doc);
            for (const b of blobs) {
                pending.push(b);
                if (pending.length >= THRESHOLD) compactCycle();
            }
            if ((s + 1) % CHECKPOINT_EVERY === 0) {
                // Measure the snapshot a fold would produce at this age (what
                // the next cold start downloads) without disturbing the
                // pipeline's own delta/fold cadence.
                const atAge = mergeUpdatesWithMeta([...(snapshot ? [snapshot] : []), ...segments, ...pending], { gc: true });
                // The squash snapshot's size varies by ~3% between runs:
                // buildSquashedDoc mints a random clientID, whose varint
                // width (4 or 5 bytes) repeats in every origin reference.
                const squashed = buildSquashedDoc(doc, 1);
                const squashBlob = Y.encodeStateAsUpdate(squashed);
                squashed.destroy();
                checkpoints.push({
                    session: s + 1,
                    events: sim.totalEvents,
                    snapshot: sizes(atAge.result),
                    fingerprint: sizes(atAge.dsUpdate),
                    squash: sizes(squashBlob),
                });
                if (s + 1 === SESSIONS) {
                    finalSnapshot = atAge.result;
                    finalFingerprint = atAge.dsUpdate;
                }
            }
        }

        const finalSizes = checkpoints[checkpoints.length - 1].snapshot;

        // --- Correctness of the encodings measured --------------------------
        expect(gunzipSync(gzipSync(finalSnapshot, { level: 1 }))).toEqual(Buffer.from(finalSnapshot));
        const viaStream = await streamGunzip(await streamGzip(finalSnapshot));
        expect(Buffer.from(viaStream)).toEqual(Buffer.from(finalSnapshot));
        const expectedJson = materializeVersicleDoc(finalSnapshot, []);
        {
            const d = new Y.Doc();
            Y.applyUpdateV2(d, v2(finalSnapshot));
            const viaV2 = materializeVersicleDoc(Y.encodeStateAsUpdate(d), []);
            d.destroy();
            expect(viaV2).toBe(expectedJson);
        }

        // What uploadBlob stores for the final snapshot
        const libraryUpload = await gzipBlob(finalSnapshot);
        expect(gunzipSync(libraryUpload)).toEqual(Buffer.from(finalSnapshot));

        // --- CPU price at the final age (medians, interleaved) --------------
        const finalGz1 = gzipSync(finalSnapshot, { level: 1 });
        const finalV2 = v2(finalSnapshot);
        const t: Record<string, number[]> = {
            gzip1: [], gzip6: [], gunzip: [], streamGzip: [], streamGunzip: [], libraryGzip: [], applyV1: [], applyV2: [],
        };
        const time = (k: string, fn: () => void) => {
            const t0 = performance.now();
            fn();
            t[k].push(performance.now() - t0);
        };
        const timeAsync = async (k: string, fn: () => Promise<unknown>) => {
            const t0 = performance.now();
            await fn();
            t[k].push(performance.now() - t0);
        };
        for (let i = 0; i < TIMING_RUNS; i++) {
            time('gzip1', () => gzipSync(finalSnapshot, { level: 1 }));
            time('gzip6', () => gzipSync(finalSnapshot, { level: 6 }));
            time('gunzip', () => gunzipSync(finalGz1));
            await timeAsync('streamGzip', () => streamGzip(finalSnapshot));
            await timeAsync('streamGunzip', () => streamGunzip(finalGz1));
            await timeAsync('libraryGzip', () => gzipBlob(finalSnapshot));
            time('applyV1', () => { const d = new Y.Doc(); Y.applyUpdate(d, finalSnapshot); d.destroy(); });
            time('applyV2', () => { const d = new Y.Doc(); Y.applyUpdateV2(d, finalV2); d.destroy(); });
        }
        const m = Object.fromEntries(Object.entries(t).map(([k, v]) => [k, median(v)]));

        // --- Report -----------------------------------------------------------
        const seg = segmentSizes.reduce((a, s) => ({
            raw: a.raw + s.raw, gz1: a.gz1 + s.gz1, gz6: a.gz6 + s.gz6, v2: a.v2 + s.v2, gzV2: a.gzV2 + s.gzV2,
        }), { raw: 0, gz1: 0, gz6: 0, v2: 0, gzV2: 0 });
        const n = Math.max(1, segmentSizes.length);
        const segAvg: BlobSizes = {
            raw: Math.round(seg.raw / n), gz1: Math.round(seg.gz1 / n), gz6: Math.round(seg.gz6 / n),
            v2: Math.round(seg.v2 / n), gzV2: Math.round(seg.gzV2 / n),
        };

        console.log('\n=== Storage blobs: raw V1 vs compressed — versicle aged doc ===');
        console.log(`sessions=${SESSIONS} events=${sim.totalEvents} folds=${foldTransfer.folds}`);
        console.log('session | events | snapshot raw | gz1 | gz6 | V2 | gz(V2) | fingerprint raw | gz1 | gz(V2) | squash snapshot raw | gz1');
        for (const c of checkpoints) {
            console.log([
                c.session, c.events,
                c.snapshot.raw, `${c.snapshot.gz1} (${ratio(c.snapshot.raw, c.snapshot.gz1)})`,
                `${c.snapshot.gz6} (${ratio(c.snapshot.raw, c.snapshot.gz6)})`,
                `${c.snapshot.v2} (${ratio(c.snapshot.raw, c.snapshot.v2)})`,
                `${c.snapshot.gzV2} (${ratio(c.snapshot.raw, c.snapshot.gzV2)})`,
                c.fingerprint.raw, `${c.fingerprint.gz1} (${ratio(c.fingerprint.raw, c.fingerprint.gz1)})`,
                `${c.fingerprint.gzV2} (${ratio(c.fingerprint.raw, c.fingerprint.gzV2)})`,
                c.squash.raw, `${c.squash.gz1} (${ratio(c.squash.raw, c.squash.gz1)})`,
            ].join(' | '));
        }
        console.log(`\nfinal snapshot:     ${fmtSizes(finalSizes)}`);
        console.log(`final fingerprint:  ${fmtSizes(sizes(finalFingerprint))}`);
        console.log(`delta segment avg (first ${segmentSizes.length}): ${fmtSizes(segAvg)}`);

        console.log('\nStorage bytes per trigger at the final age:');
        console.log(`  cold start (fresh/returning device) snapshot download: raw ${fmtBytes(finalSizes.raw)}, gz1 ${fmtBytes(finalSizes.gz1)}, gz6 ${fmtBytes(finalSizes.gz6)}, gz(V2) ${fmtBytes(finalSizes.gzV2)} — saves ${fmtBytes(finalSizes.raw - finalSizes.gz1)} (gz1)`);
        console.log(`  one fold (download + upload): raw ${fmtBytes(2 * finalSizes.raw)}, gz1 ${fmtBytes(2 * finalSizes.gz1)}, gz6 ${fmtBytes(2 * finalSizes.gz6)}, gz(V2) ${fmtBytes(2 * finalSizes.gzV2)} — saves ${fmtBytes(2 * (finalSizes.raw - finalSizes.gz1))} (gz1)`);
        console.log(`  library upload (uploadBlob → gzipBlob): ${libraryUpload.byteLength} B = ${fmtBytes(libraryUpload.byteLength)} (${ratio(finalSizes.raw, libraryUpload.byteLength)}) — ` +
            `a cold start downloads it, a fold moves ${fmtBytes(2 * libraryUpload.byteLength)} instead of ${fmtBytes(2 * finalSizes.raw)}`);
        console.log(`\nLifetime fold Storage transfer (${foldTransfer.folds} folds): raw ${fmtBytes(foldTransfer.raw)}, ` +
            `gz1 ${fmtBytes(foldTransfer.gz1)} (${ratio(foldTransfer.raw, foldTransfer.gz1)}), ` +
            `gz6 ${fmtBytes(foldTransfer.gz6)} (${ratio(foldTransfer.raw, foldTransfer.gz6)}), ` +
            `gz(V2) ${fmtBytes(foldTransfer.gzV2)} (${ratio(foldTransfer.raw, foldTransfer.gzV2)})`);

        console.log(`\nCPU at the final age (median of ${TIMING_RUNS}, snapshot ${fmtBytes(finalSnapshot.byteLength)}):`);
        console.log(`  last fold merge (GC):        ${fmtMs(lastFoldMs)}`);
        console.log(`  zlib gzip level 1:           ${fmtMs(m.gzip1)}`);
        console.log(`  zlib gzip level 6:           ${fmtMs(m.gzip6)}`);
        console.log(`  zlib gunzip:                 ${fmtMs(m.gunzip)}`);
        console.log(`  CompressionStream gzip:      ${fmtMs(m.streamGzip)}`);
        console.log(`  DecompressionStream gunzip:  ${fmtMs(m.streamGunzip)}`);
        console.log(`  library gzipBlob (+ verify): ${fmtMs(m.libraryGzip)}`);
        console.log(`  apply V1 (fresh doc):        ${fmtMs(m.applyV1)}`);
        console.log(`  apply V2 (fresh doc):        ${fmtMs(m.applyV2)}`);

        // The headroom this benchmark documents: gzip of the raw V1 snapshot
        // is well under half its size at every checkpoint, and the saving
        // grows linearly with document age.
        for (const c of checkpoints) {
            expect(c.snapshot.gz1 / c.snapshot.raw).toBeLessThan(0.5);
            expect(c.squash.gz1 / c.squash.raw).toBeLessThan(0.5);
        }
        expect(foldTransfer.gz1 / foldTransfer.raw).toBeLessThan(0.5);
        // ...and the library realizes it
        expect(libraryUpload.byteLength / finalSizes.raw).toBeLessThan(0.5);
        const first = checkpoints[0].snapshot, last = finalSizes;
        expect(last.raw - last.gz1).toBeGreaterThan(3 * (first.raw - first.gz1));
    }, 600_000);
});
