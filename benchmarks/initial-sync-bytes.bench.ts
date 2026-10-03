/**
 * Benchmark: initial sync round-trips Storage downloads through Firestore
 * `Bytes`.
 *
 * `performInitialSync` downloads the base snapshot (and every
 * storage-backed update / offloaded delete-set fingerprint) from Cloud
 * Storage as an ArrayBuffer and wraps it with `Bytes.fromUint8Array` only
 * to park it in `data.content` / `data.update`. In @firebase/firestore 4.x
 * that is `binaryString += String.fromCharCode(array[i])` for every byte:
 * a cons-string rope (one heap node per byte, ~32x the blob size) that is
 * superlinear in time and flattened on first read. Every reader on the
 * initial-sync path then copies the whole blob back out with
 * `toUint8Array()` — `applyItem`, `collectServerBlobs` (eager, for every
 * blob INCLUDING the snapshot, before the push guard's smallest-first
 * early exit), `buildServerCoverage`, and `processUpdateMetadata` for the
 * metadata-less fingerprint. The snapshot listener already applies
 * `new Uint8Array(buffer)` directly; only initial sync takes this route.
 *
 * Part 1 isolates the SDK conversion costs per blob size on aged
 * versicle-shaped snapshots (the trigger: every cold start that downloads
 * the snapshot — fresh install, cleared storage, and the two-device case,
 * since versicle mints a fresh clientID per launch).
 *
 * Part 2 runs the real `performInitialSync` for a fresh client with
 * Firestore/Storage stubbed at the SDK boundary (queries, getDoc,
 * getBytes) but the REAL `Bytes` class, instrumented to count and time
 * every conversion, and compares the total with the inherent apply cost.
 *
 * Deterministic counterpart: tests/unit/initial-sync-bytes.test.ts.
 *
 * Run with: npx vitest run --config benchmarks/vitest.config.ts benchmarks/initial-sync-bytes.bench.ts
 */
import { describe, it, expect, vi, beforeAll } from 'vitest';
import * as Y from 'yjs';
import v8 from 'node:v8';
import vm from 'node:vm';
import { toBase64 } from 'lib0/buffer';
import { Bytes } from '@firebase/firestore';
import { performInitialSync } from '../src/sync';
import { mergeUpdatesWithMeta } from '../src/merge-core';
import { aggregateClockEnds, extractClockEnds, updateHasDeletions } from '../src/update-metadata';
import { createSim, runSession, clientIdForSession, materializeVersicleDoc } from './versicle-workload';
import { fmtBytes, fmtMs } from './helpers';

// ---------------------------------------------------------------------------
// Firestore / Storage stubbed at the SDK boundary (Bytes stays real)
// ---------------------------------------------------------------------------

const server = vi.hoisted(() => ({
    updates: [] as Record<string, any>[],
    history: [] as Record<string, any>[],
    main: null as Record<string, any> | null,
    storage: new Map<string, Uint8Array>(),
}));

vi.mock('@firebase/firestore', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/firestore')>();
    const join = (parts: unknown[]) => parts.filter(p => typeof p === 'string').join('/');
    return {
        ...actual,
        collection: (_db: unknown, ...parts: unknown[]) => ({ kind: 'collection', path: join(parts) }),
        doc: (_db: unknown, ...parts: unknown[]) => ({ kind: 'doc', path: join(parts) }),
        query: (ref: any, ...constraints: any[]) => ({ ...ref, constraints }),
        orderBy: (field: string) => ({ orderBy: field }),
        startAfter: (cursor: any) => ({ startAfter: cursor }),
        limit: (n: number) => ({ limit: n }),
        serverTimestamp: () => ({ serverTimestamp: true }),
        getDocs: async (q: any) => {
            const rows = q.path.endsWith('/updates') ? server.updates
                : q.path.endsWith('/history') ? server.history
                    : [];
            const after = q.constraints?.find((c: any) => c.startAfter)?.startAfter;
            const start = after ? Number(after.id.slice('doc-'.length)) + 1 : 0;
            const max = q.constraints?.find((c: any) => c.limit)?.limit ?? rows.length;
            const docs = rows.slice(start, start + max).map((row, i) => ({
                id: `doc-${start + i}`,
                ref: { path: `doc-${start + i}` },
                metadata: { hasPendingWrites: false },
                data: () => ({ ...row }),
            }));
            return { docs, empty: docs.length === 0, size: docs.length, forEach: (fn: (d: unknown) => void) => docs.forEach(fn) };
        },
        getDoc: async () => ({
            exists: () => server.main !== null,
            data: () => (server.main ? { ...server.main } : undefined),
        }),
        addDoc: async () => ({ id: 'added' }),
    };
});

vi.mock('@firebase/storage', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/storage')>();
    return {
        ...actual,
        ref: (_storage: unknown, path: string) => ({ fullPath: path }),
        getBytes: async (r: { fullPath: string }) => {
            const blob = server.storage.get(r.fullPath);
            if (!blob) throw Object.assign(new Error('object-not-found'), { code: 'storage/object-not-found' });
            return blob.slice().buffer; // a fresh ArrayBuffer, like a download
        },
        uploadBytes: async () => undefined,
        deleteObject: async () => undefined,
    };
});

// ---------------------------------------------------------------------------
// Instrumentation
// ---------------------------------------------------------------------------

interface Conversions {
    wrapCalls: number; wrapBytes: number; wrapMs: number;
    unwrapCalls: number; unwrapBytes: number; unwrapMs: number;
}

/** Counts and times every Bytes conversion while `fn` runs. */
async function withBytesInstrumentation<T>(fn: () => Promise<T>): Promise<[T, Conversions]> {
    const origFrom = Bytes.fromUint8Array;
    const origTo = Bytes.prototype.toUint8Array;
    const c: Conversions = { wrapCalls: 0, wrapBytes: 0, wrapMs: 0, unwrapCalls: 0, unwrapBytes: 0, unwrapMs: 0 };
    Bytes.fromUint8Array = function (array: Uint8Array): Bytes {
        const t0 = performance.now();
        const out = origFrom.call(Bytes, array);
        c.wrapMs += performance.now() - t0;
        c.wrapCalls++;
        c.wrapBytes += array.byteLength;
        return out;
    };
    Bytes.prototype.toUint8Array = function (this: Bytes): Uint8Array {
        const t0 = performance.now();
        const out = origTo.call(this);
        c.unwrapMs += performance.now() - t0;
        c.unwrapCalls++;
        c.unwrapBytes += out.byteLength;
        return out;
    };
    try {
        return [await fn(), c];
    } finally {
        Bytes.fromUint8Array = origFrom;
        Bytes.prototype.toUint8Array = origTo;
    }
}

/** Forced GC for retained-heap measurements (no --expose-gc needed). */
function getGc(): () => void {
    v8.setFlagsFromString('--expose-gc');
    return vm.runInNewContext('gc') as () => void;
}

function median(xs: number[]): number {
    const s = [...xs].sort((a, b) => a - b);
    return s[Math.floor(s.length / 2)];
}

const MB = 1024 * 1024;

// ---------------------------------------------------------------------------
// Aged versicle-shaped server states
// ---------------------------------------------------------------------------

const SEED = 20260820;
/** Snapshot ages sampled (sessions of 60 events): ~0.4 / 1.1 / 2.1 / 3.5 MB */
const CHECKPOINTS = [24, 72, 144, 240];

interface ServerState {
    sessions: number;
    events: number;
    /** GC'd snapshot (what a fold writes to Storage) */
    snapshot: Uint8Array;
    stateVector: Uint8Array;
    /** structs-empty delete-set fingerprint (inline on the main doc) */
    fingerprint: Uint8Array;
    /** The next session's saves: what sits in history/updates at cold start */
    tail: Uint8Array[];
}

/**
 * Ages one document through versicle sessions (fresh clientID each, like
 * versicle's per-launch Y.Doc). A gc'd live doc encodes to the same shape
 * a GC fold produces, without paying for the compaction cycles.
 */
function ageDocument(): ServerState[] {
    const sim = createSim({ seed: SEED });
    const doc = new Y.Doc();
    const states: ServerState[] = [];
    let session = 0;
    for (const target of CHECKPOINTS) {
        while (session < target) {
            doc.clientID = clientIdForSession(SEED, session++);
            runSession(sim, doc);
        }
        const stateVector = Y.encodeStateVector(doc);
        const snapshot = Y.encodeStateAsUpdate(doc);
        const fingerprint = Y.encodeStateAsUpdate(doc, stateVector);
        const events = sim.totalEvents;
        doc.clientID = clientIdForSession(SEED, session++);
        const { blobs } = runSession(sim, doc);
        states.push({ sessions: target, events, snapshot, stateVector, fingerprint, tail: blobs });
    }
    doc.destroy();
    return states;
}

const PATH = 'docs/initial-sync-bytes-bench';

/** Server tiers exactly as compaction / provider._save write them. */
function installServer(st: ServerState): { inlineBytes: number; storageBytes: number } {
    server.updates = [];
    server.history = [];
    server.storage.clear();
    const snapPath = `${PATH}/snapshots/${st.sessions}.bin`;
    server.storage.set(snapPath, st.snapshot);
    // Inline Bytes as the browser SDK delivers them (base64 → flat string).
    const inline = (u: Uint8Array) => Bytes.fromBase64String(toBase64(u));
    server.main = {
        snapshotStoragePath: snapPath,
        stateVector: toBase64(st.stateVector),
        deleteSet: inline(st.fingerprint),
        version: 1,
    };
    let inlineBytes = st.fingerprint.byteLength;
    let storageBytes = st.snapshot.byteLength;

    // Another device's delta-compacted history segment + its newer saves,
    // one of them storage-backed.
    const half = Math.floor(st.tail.length / 2);
    const seg = mergeUpdatesWithMeta(st.tail.slice(0, half), { gc: false });
    server.history.push({
        stateVector: toBase64(seg.stateVector),
        ...(updateHasDeletions(seg.dsUpdate) ? { hasDeletions: true } : {}),
        createdBy: 'device-b',
        segment: inline(seg.result),
    });
    inlineBytes += seg.result.byteLength;
    st.tail.slice(half).forEach((u, i) => {
        const meta = aggregateClockEnds(extractClockEnds(u));
        if (i === 0) {
            const p = `${PATH}/large_updates/device-b_${st.sessions}.bin`;
            server.storage.set(p, u);
            server.updates.push({ createdBy: 'device-b', updateStoragePath: p, ...meta });
            storageBytes += u.byteLength;
        } else {
            server.updates.push({ createdBy: 'device-b', update: inline(u), ...meta });
            inlineBytes += u.byteLength;
        }
    });
    return { inlineBytes, storageBytes };
}

// ---------------------------------------------------------------------------

describe('initial sync: Storage downloads round-tripped through Firestore Bytes', () => {
    let states: ServerState[] = [];

    beforeAll(() => {
        vi.spyOn(console, 'log').mockImplementation(() => { });
        states = ageDocument();
    }, 120_000);

    it('part 1: SDK Bytes conversion cost vs snapshot size (aged versicle snapshots)', () => {
        const gc = getGc();
        const RUNS = 5;
        const rows: string[] = [];
        const wrapMsPerMB: number[] = [];

        for (const st of states) {
            const blob = st.snapshot;
            const wrap: number[] = [], first: number[] = [], later: number[] = [], copy: number[] = [], apply: number[] = [];
            // Interleaved per run: wrap → first unwrap (flattens the rope)
            // → later unwrap, then the alternatives on the same blob.
            for (let r = 0; r < RUNS; r++) {
                const downloaded = new Uint8Array(blob.slice().buffer);
                gc();
                let t0 = performance.now();
                const b = Bytes.fromUint8Array(downloaded);
                wrap.push(performance.now() - t0);
                t0 = performance.now();
                const u1 = b.toUint8Array();
                first.push(performance.now() - t0);
                t0 = performance.now();
                const u2 = b.toUint8Array();
                later.push(performance.now() - t0);
                expect(u1.byteLength).toBe(blob.byteLength);
                expect(u2.byteLength).toBe(blob.byteLength);

                gc();
                t0 = performance.now();
                const plain = new Uint8Array(blob.slice().buffer);
                copy.push(performance.now() - t0);

                t0 = performance.now();
                const d = new Y.Doc();
                Y.applyUpdate(d, plain);
                apply.push(performance.now() - t0);
                d.destroy();
            }

            // Retained heap of one wrapped blob, before and after its first
            // read flattens the rope (forced GC, reference held).
            gc(); gc();
            const h0 = process.memoryUsage().heapUsed;
            const held = Bytes.fromUint8Array(new Uint8Array(blob));
            gc(); gc();
            const ropeHeap = process.memoryUsage().heapUsed - h0;
            held.toUint8Array();
            gc(); gc();
            const flatHeap = process.memoryUsage().heapUsed - h0;
            expect(held.isEqual(held)).toBe(true); // keep `held` alive until here

            const mb = blob.byteLength / MB;
            wrapMsPerMB.push(median(wrap) / mb);
            rows.push([
                `${String(st.events).padStart(6)} ev`,
                fmtBytes(blob.byteLength).padStart(9),
                fmtMs(median(wrap)).padStart(8),
                fmtMs(median(first)).padStart(8),
                fmtMs(median(later)).padStart(8),
                fmtMs(median(copy)).padStart(8),
                fmtMs(median(apply)).padStart(8),
                `${(ropeHeap / blob.byteLength).toFixed(1)}x (${fmtBytes(ropeHeap)})`.padStart(18),
                `${(flatHeap / blob.byteLength).toFixed(1)}x`.padStart(14),
            ].join(' | '));
        }

        console.info('\nSDK Bytes conversions on an aged snapshot (median of 5, interleaved):');
        console.info('   age    |   blob    | fromU8A  | 1st toU8A| later    | U8A copy | fresh apply | rope heap (held)  | after 1st read');
        for (const r of rows) console.info('  ' + r);
        console.info(`  fromUint8Array ms/MB: ${wrapMsPerMB.map(x => x.toFixed(0)).join(' → ')} (superlinear: ${(wrapMsPerMB[wrapMsPerMB.length - 1] / wrapMsPerMB[0]).toFixed(1)}x per byte at ${fmtBytes(states[states.length - 1].snapshot.byteLength)} vs ${fmtBytes(states[0].snapshot.byteLength)})\n`);
    }, 300_000);

    it('part 2: performInitialSync on a fresh client — conversion share of the cold start', async () => {
        const RUNS = 5;
        const rows: string[] = [];
        for (const st of [states[1], states[states.length - 1]]) {
            const { inlineBytes, storageBytes } = installServer(st);
            const expected = JSON.parse(materializeVersicleDoc(st.snapshot, st.tail));

            const total: number[] = [], convMs: number[] = [], inherent: number[] = [];
            let last: Conversions | null = null;
            for (let r = 0; r < RUNS; r++) {
                const client = new Y.Doc();
                const t0 = performance.now();
                const [result, conv] = await withBytesInstrumentation(() => performInitialSync({
                    db: {} as any,
                    storage: {} as any,
                    path: PATH,
                    doc: client,
                    uid: 'fresh-client',
                    maxUpdatesThreshold: 50,
                    isDestroyed: () => false,
                }));
                total.push(performance.now() - t0);
                convMs.push(conv.wrapMs + conv.unwrapMs);
                last = conv;
                expect(result.success).toBe(true);
                expect(result.localUpdatesPushed).toBe(false);
                if (r === 0) {
                    // Converged (key order of Y.Map JSON follows integration order)
                    expect(JSON.parse(materializeVersicleDoc(Y.encodeStateAsUpdate(client), []))).toEqual(expected);
                }
                client.destroy();

                // Inherent floor: the same blobs applied to a fresh doc in
                // one transaction, straight from Uint8Arrays.
                const t1 = performance.now();
                const d = new Y.Doc();
                d.transact(() => {
                    Y.applyUpdate(d, st.snapshot);
                    Y.applyUpdate(d, st.fingerprint);
                    for (const u of st.tail) Y.applyUpdate(d, u);
                });
                inherent.push(performance.now() - t1);
                d.destroy();
            }
            const c = last!;
            rows.push(
                `snapshot ${fmtBytes(st.snapshot.byteLength)} (${st.events} events; storage ${fmtBytes(storageBytes)}, inline ${fmtBytes(inlineBytes)}, ${server.updates.length} updates + 1 segment):\n` +
                `    performInitialSync total:      ${fmtMs(median(total))}\n` +
                `    of which Bytes conversions:    ${fmtMs(median(convMs))} (${(100 * median(convMs) / median(total)).toFixed(0)}%) — ` +
                `fromUint8Array ${c.wrapCalls}x ${fmtBytes(c.wrapBytes)} (${fmtMs(c.wrapMs)}), toUint8Array ${c.unwrapCalls}x ${fmtBytes(c.unwrapBytes)} (${fmtMs(c.unwrapMs)})\n` +
                `    bytes converted per byte fetched: ${((c.wrapBytes + c.unwrapBytes) / (storageBytes + inlineBytes)).toFixed(2)}\n` +
                `    inherent apply (same blobs as Uint8Array, one transaction): ${fmtMs(median(inherent))}`,
            );
        }
        console.info('\nFresh-client performInitialSync, SDK stubbed at the boundary, real Bytes (median of 5):');
        for (const r of rows) console.info('  ' + r);
        console.info('');
    }, 300_000);
});
