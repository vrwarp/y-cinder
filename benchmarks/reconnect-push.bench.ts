/**
 * Benchmark: what the initial-sync push uploads when a long-lived document
 * reconnects with a few offline edits.
 *
 * When the local doc holds structs the server lacks (offline reading, or a
 * save lost when the previous session ended), `performInitialSync` pushes
 * `Y.encodeStateAsUpdate(ydoc, serverSV)` (src/sync.ts, step 5). Yjs embeds
 * the document's COMPLETE delete-set in every such diff, so a handful of
 * bytes of new structs ships as an O(delete-set) update document. Every
 * online peer then integrates that whole delete-set, and the next delta
 * compaction folds it into a history segment flagged `hasDeletions` — which
 * fresh and returning clients always apply until the next fold.
 *
 * The push now drops the deletions the server blobs already prove
 * (`withoutServerDeletions`); "full diff" is what was pushed before, and
 * "trim" is what dropping them costs the pushing client on top of the
 * encode.
 *
 * The workload is versicle-shaped (benchmarks/versicle-workload.ts) and
 * persisted through the current pipeline: delta compaction every
 * THRESHOLD updates, a GC'd fold (with its delete-set fingerprint) every
 * FOLD_THRESHOLD cycles, no squash. At each checkpoint a reconnecting
 * client hydrated with the full server state (as y-idb would hold it) makes
 * an offline edit and runs the push decision with the real sync helpers.
 *
 * Two offline edits:
 *  - insert-only: one new map key — one struct, no deletion;
 *  - page-turn:   three overwrites of an existing progress map — three
 *                 structs plus three genuinely new deletions the push MUST
 *                 carry.
 *
 * "floor" is the offline edit's own update (Y.mergeUpdates of what the
 * edit emitted): its structs plus exactly the deletions the server cannot
 * have — the O(new data) payload a push actually needs.
 *
 * Deterministic counters (bytes, delete-set ranges) are the primary
 * metric; timings are medians of TIMING_RUNS runs.
 *
 * Run with: npx vitest run --config benchmarks/vitest.config.ts benchmarks/reconnect-push.bench.ts
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { Bytes } from '@firebase/firestore';
import { toBase64 } from 'lib0/buffer';
import { mergeUpdatesWithMeta } from '../src/merge-core';
import { updateHasDeletions, withoutServerDeletions } from '../src/update-metadata';
import {
    PendingUpdate, buildServerCoverage, collectServerBlobs, processSnapshotMetadata,
} from '../src/sync-helpers';
import { serverCoversLocalStructs, diffHasPayload } from '../src/sync-policy';
import { writeStateVector } from '../src/utils';
import { DEFAULTS } from '../src/types';
import {
    createSim, runSession, clientIdForSession, docStructStats, VersicleSimState,
} from './versicle-workload';
import { fmtBytes, fmtMs } from './helpers';

const SEED = 20260820; // same seed as the versicle aging suites
const SESSIONS = 240;
const CHECKPOINTS = new Set([30, 60, 120, 240]);
const THRESHOLD = 50;
const FOLD_THRESHOLD = 8;
const TIMING_RUNS = 5;

/** Persisted server state, shaped like what initial sync reads. */
interface ServerState {
    snapshot: Uint8Array;
    snapshotSv: Uint8Array;
    fingerprint: Uint8Array;
    segments: Uint8Array[];
    pending: Uint8Array[];
}

type OfflineEdit = (doc: Y.Doc) => void;

const OFFLINE_EDITS: Record<string, OfflineEdit> = {
    'insert-only': (doc) => {
        const known = doc.getMap('vocabulary').get('knownCharacters') as Y.Map<unknown>;
        known.set('offline-probe', 1);
    },
    'page-turn': (doc) => {
        const root = doc.getMap('progress').get('progress') as Y.Map<unknown>;
        let target: Y.Map<unknown> | null = null;
        root.forEach((perBook) => {
            if (target) return;
            (perBook as Y.Map<unknown>).forEach((perDevice) => {
                if (!target) target = perDevice as Y.Map<unknown>;
            });
        });
        if (!target) throw new Error('no progress entry to overwrite');
        const m = target as Y.Map<unknown>;
        m.set('currentCfi', 'epubcfi(/6/8!/4/2/1:0)');
        m.set('percentage', 0.5);
        m.set('lastRead', 1_800_000_000_000);
    },
};

function hydrate(server: ServerState, clientID?: number): Y.Doc {
    const doc = new Y.Doc();
    if (clientID !== undefined) doc.clientID = clientID;
    Y.applyUpdate(doc, server.snapshot);
    for (const seg of server.segments) Y.applyUpdate(doc, seg);
    for (const u of server.pending) Y.applyUpdate(doc, u);
    return doc;
}

/** The items performInitialSync collects when the local doc covers the snapshot. */
function serverItems(server: ServerState): PendingUpdate[] {
    return [
        // The fingerprint is queued as a structs-empty update (sync.ts 3a)
        { type: 'update', data: { update: Bytes.fromUint8Array(server.fingerprint) }, priority: 2 },
        ...server.segments.map((seg): PendingUpdate =>
            ({ type: 'history', data: { segment: Bytes.fromUint8Array(seg) }, priority: 2 })),
        ...server.pending.map((u): PendingUpdate =>
            ({ type: 'update', data: { update: Bytes.fromUint8Array(u) }, priority: 3 })),
    ];
}

/**
 * The initial-sync push decision as src/sync.ts step 5 makes it, built from
 * the same helpers. Returns the bytes that would be written as the pushed
 * update document (null = nothing pushed), the full diff Yjs encodes, and
 * the blobs that prove server delete-set coverage (the snapshot content is
 * never among them; serverItems() holds the fingerprint instead).
 */
function currentPushPayload(doc: Y.Doc, server: ServerState): {
    diff: Uint8Array | null; fullDiff: Uint8Array; serverSV: Uint8Array; proofBlobs: () => Uint8Array[];
} {
    const snapshotSVMap = new Map<number, number>();
    processSnapshotMetadata({ stateVector: toBase64(server.snapshotSv) }, snapshotSVMap);
    const items = serverItems(server);
    const serverSVMap = buildServerCoverage(snapshotSVMap, items);
    const serverSV = writeStateVector(serverSVMap);
    const exactLocalSV = Y.decodeStateVector(Y.encodeStateVector(doc));
    if (serverCoversLocalStructs(exactLocalSV, serverSVMap)) {
        throw new Error('scenario expects the local doc to hold structs the server lacks');
    }
    const proofBlobs = () => collectServerBlobs(items);
    const fullDiff = Y.encodeStateAsUpdate(doc, serverSV);
    const localDiff = withoutServerDeletions(fullDiff, proofBlobs);
    const shouldPush = diffHasPayload(localDiff.byteLength);
    return { diff: shouldPush ? localDiff : null, fullDiff, serverSV, proofBlobs };
}

const VERSICLE_MAPS = ['library', 'progress', 'annotations', 'reading-list', 'vocabulary', 'lexicon', 'contentAnalysis', 'devices', 'searchHistory', 'meta'];

function docJson(doc: Y.Doc): string {
    return JSON.stringify(VERSICLE_MAPS.map(name => doc.getMap(name).toJSON()));
}

function dsRangesOf(update: Uint8Array): number {
    let n = 0;
    Y.decodeUpdate(update).ds.clients.forEach((items) => { n += items.length; });
    return n;
}

function structsOf(update: Uint8Array): number {
    return Y.decodeUpdate(update).structs.length;
}

/** Median over fresh peers (built outside the timer) of applying `update`. */
function peerApplyMs(server: ServerState, update: Uint8Array): number {
    const peers = Array.from({ length: TIMING_RUNS }, () => hydrate(server));
    const times = peers.map((peer) => {
        const t0 = performance.now();
        Y.applyUpdate(peer, update);
        const t = performance.now() - t0;
        peer.destroy();
        return t;
    });
    times.sort((a, b) => a - b);
    return times[Math.floor(times.length / 2)];
}

function medianOf(fn: () => void): number {
    const times: number[] = [];
    for (let i = 0; i < TIMING_RUNS; i++) {
        const t0 = performance.now();
        fn();
        times.push(performance.now() - t0);
    }
    times.sort((a, b) => a - b);
    return times[Math.floor(times.length / 2)];
}

interface Row {
    session: number;
    edit: string;
    docDsRanges: number;
    floorBytes: number;
    floorDsRanges: number;
    pushedBytes: number;
    pushedDsRanges: number;
    pushedStructs: number;
    fullDiffBytes: number;
    encodeMs: number;
    trimMs: number;
    peerApplyPushMs: number;
    peerApplyFloorMs: number;
    segmentPushBytes: number;
    segmentPushDsRanges: number;
    segmentPushHasDeletions: boolean;
    segmentFloorBytes: number;
    segmentFloorDsRanges: number;
    segmentFloorHasDeletions: boolean;
}

describe('initial-sync push after offline edits on an aged document', () => {
    it('measures the pushed update against the offline edit it carries', () => {
        const sim: VersicleSimState = createSim({ seed: SEED });

        let server: ServerState | null = null;
        let snapshot: Uint8Array | null = null;
        let snapshotSv: Uint8Array | null = null;
        let fingerprint: Uint8Array | null = null;
        let segments: Uint8Array[] = [];
        let pending: Uint8Array[] = [];

        const compactCycle = () => {
            if (segments.length + 1 < FOLD_THRESHOLD && snapshot !== null) {
                segments.push(mergeUpdatesWithMeta(pending, { gc: false }).result);
            } else {
                const merged = mergeUpdatesWithMeta(
                    [...(snapshot ? [snapshot] : []), ...segments, ...pending], { gc: true });
                snapshot = merged.result;
                snapshotSv = merged.stateVector;
                fingerprint = merged.dsUpdate;
                segments = [];
            }
            pending = [];
        };

        const rows: Row[] = [];

        for (let s = 0; s < SESSIONS; s++) {
            const doc = new Y.Doc();
            doc.clientID = clientIdForSession(SEED, s);
            if (snapshot) Y.applyUpdate(doc, snapshot);
            for (const seg of segments) Y.applyUpdate(doc, seg);
            for (const u of pending) Y.applyUpdate(doc, u);
            const { blobs } = runSession(sim, doc);
            doc.destroy();
            for (const b of blobs) {
                pending.push(b);
                if (pending.length >= THRESHOLD) compactCycle();
            }

            if (!CHECKPOINTS.has(s + 1)) continue;
            server = {
                snapshot: snapshot!, snapshotSv: snapshotSv!, fingerprint: fingerprint!,
                segments: segments.slice(), pending: pending.slice(),
            };

            for (const [edit, apply] of Object.entries(OFFLINE_EDITS)) {
                // Reconnecting client: full server state locally (y-idb),
                // fresh clientID (versicle mints one per launch), one
                // offline edit the server has never seen.
                const local = hydrate(server, clientIdForSession(SEED, 900_000 + s));
                const offline: Uint8Array[] = [];
                const capture = (u: Uint8Array) => offline.push(u);
                local.on('update', capture);
                local.transact(() => apply(local));
                local.off('update', capture);
                const floor = Y.mergeUpdates(offline);

                const { diff, fullDiff, serverSV, proofBlobs } = currentPushPayload(local, server);
                expect(diff).not.toBeNull();
                const pushed = diff!;

                // Correctness of both payloads: a peer holding the server
                // state converges to the local doc with either one.
                const expected = docJson(local);
                for (const payload of [pushed, floor]) {
                    const peer = hydrate(server);
                    Y.applyUpdate(peer, payload);
                    expect(docJson(peer)).toBe(expected);
                    peer.destroy();
                }

                const encodeMs = medianOf(() => { Y.encodeStateAsUpdate(local, serverSV); });
                // What the pushing client pays on top of the encode to drop
                // the deletions the server already holds.
                const trimMs = medianOf(() => { withoutServerDeletions(fullDiff, proofBlobs); });
                const segPush = mergeUpdatesWithMeta([...server.pending, pushed], { gc: false });
                const segFloor = mergeUpdatesWithMeta([...server.pending, floor], { gc: false });

                rows.push({
                    session: s + 1,
                    edit,
                    docDsRanges: docStructStats(local).dsRanges,
                    floorBytes: floor.byteLength,
                    floorDsRanges: dsRangesOf(floor),
                    pushedBytes: pushed.byteLength,
                    pushedDsRanges: dsRangesOf(pushed),
                    pushedStructs: structsOf(pushed),
                    fullDiffBytes: fullDiff.byteLength,
                    encodeMs,
                    trimMs,
                    peerApplyPushMs: peerApplyMs(server, pushed),
                    peerApplyFloorMs: peerApplyMs(server, floor),
                    segmentPushBytes: segPush.result.byteLength,
                    segmentPushDsRanges: dsRangesOf(segPush.result),
                    segmentPushHasDeletions: updateHasDeletions(segPush.dsUpdate),
                    segmentFloorBytes: segFloor.result.byteLength,
                    segmentFloorDsRanges: dsRangesOf(segFloor.result),
                    segmentFloorHasDeletions: updateHasDeletions(segFloor.dsUpdate),
                });
                local.destroy();
            }
        }

        console.log('\n=== Initial-sync push of offline edits on an aged versicle doc ===');
        console.log(`(inline limit ${fmtBytes(DEFAULTS.INLINE_UPDATE_LIMIT)}; timings = median of ${TIMING_RUNS})`);
        console.log('session | edit | doc dsRanges | floor (dsRanges) | pushed (dsRanges, structs) | push/floor | full diff | encode / trim | peer apply push / floor | next segment push (ds, hasDel) | next segment floor (ds, hasDel)');
        for (const r of rows) {
            console.log([
                r.session, r.edit, r.docDsRanges,
                `${fmtBytes(r.floorBytes)} (${r.floorDsRanges})`,
                `${fmtBytes(r.pushedBytes)} (${r.pushedDsRanges}, ${r.pushedStructs})`,
                `${(r.pushedBytes / r.floorBytes).toFixed(0)}x`,
                fmtBytes(r.fullDiffBytes),
                `${fmtMs(r.encodeMs)} / ${fmtMs(r.trimMs)}`,
                `${fmtMs(r.peerApplyPushMs)} / ${fmtMs(r.peerApplyFloorMs)}`,
                `${fmtBytes(r.segmentPushBytes)} (${r.segmentPushDsRanges}, ${r.segmentPushHasDeletions})`,
                `${fmtBytes(r.segmentFloorBytes)} (${r.segmentFloorDsRanges}, ${r.segmentFloorHasDeletions})`,
            ].join(' | '));
        }
        const at = (session: number, edit: string) => rows.find(r => r.session === session && r.edit === edit)!;
        for (const edit of Object.keys(OFFLINE_EDITS)) {
            const lo = at(60, edit), hi = at(240, edit);
            console.log(`${edit}: pushed bytes 240/60 = ${(hi.pushedBytes / lo.pushedBytes).toFixed(2)}x, ` +
                `floor bytes 240/60 = ${(hi.floorBytes / lo.floorBytes).toFixed(2)}x, ` +
                `doc dsRanges 240/60 = ${(hi.docDsRanges / lo.docDsRanges).toFixed(2)}x`);
        }

        // The page-turn floor genuinely carries deletions; the insert-only
        // one carries none — what a push needs is O(new data) either way.
        expect(at(240, 'insert-only').floorDsRanges).toBe(0);
        expect(at(240, 'page-turn').floorDsRanges).toBeGreaterThan(0);
    }, 600_000);
});
