/**
 * Performance regression: a local doc BEHIND the server snapshot must not
 * have the whole snapshot decoded onto it on the main thread.
 *
 * A returning device (versicle: hydrated from y-idb, a few sessions behind
 * because another device ran meanwhile and a fold rewrote the snapshot)
 * fails `localCoversSnapshot`. `performInitialSync` (applyItem in the
 * transact loop) and `createSnapshotListener` then pass the full snapshot
 * to `Y.applyUpdate(liveDoc, ...)`: Yjs decodes and materializes every
 * struct — 90-99% of which the local doc already holds — on the main
 * thread, O(snapshot) per returning device and growing with document age.
 * `Y.diffUpdate(snapshot, encodeStateVector(liveDoc))` is pure (it can run
 * in the merge worker) and leaves only the missing structs for the live
 * document.
 *
 * Counter: every update handed to Y.applyUpdate / Y.applyUpdateV2 on the
 * LIVE document is recorded (yjs is wrapped at the module boundary) and
 * decoded afterwards to count its structs, and how many of them the local
 * doc already held before syncing. That is the main-thread integration
 * work, independent of where (worker or main-thread fallback) a diff is
 * computed. No wall-clock thresholds.
 *
 * Firestore and Storage are faked at the SDK boundary: an empty update and
 * history tier and a main document pointing at a Storage snapshot, exactly
 * as a GC fold (`mergeUpdatesWithMeta(..., { gc: true })`) writes it.
 * Workload: the versicle model from benchmarks/versicle-workload.ts.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { toBase64 } from 'lib0/buffer';

const rec = vi.hoisted(() => ({
    /** The live document under sync; applies to any other doc are ignored */
    target: null as unknown,
    applied: [] as { update: Uint8Array; v2: boolean }[],
    /** Server state served by the fakes */
    mainDoc: null as Record<string, unknown> | null,
    blobs: new Map<string, Uint8Array>(),
    listeners: [] as ((snapshot: unknown) => unknown)[],
}));

vi.mock('yjs', async (importOriginal) => {
    const actual = await importOriginal<typeof import('yjs')>();
    return {
        ...actual,
        applyUpdate: (doc: any, update: Uint8Array, origin?: unknown) => {
            if (doc === rec.target) rec.applied.push({ update, v2: false });
            return actual.applyUpdate(doc, update, origin);
        },
        applyUpdateV2: (doc: any, update: Uint8Array, origin?: unknown, decoder?: any) => {
            if (doc === rec.target) rec.applied.push({ update, v2: true });
            return actual.applyUpdateV2(doc, update, origin, decoder);
        },
    };
});

vi.mock('@firebase/firestore', () => {
    class FakeBytes {
        constructor(private readonly bytes: Uint8Array) { }
        static fromUint8Array(bytes: Uint8Array) { return new FakeBytes(bytes); }
        toUint8Array() { return this.bytes; }
    }
    const refPath = (parts: any[]) =>
        parts.map(p => (typeof p === 'string' ? p : p?.path)).filter(Boolean).join('/');
    const emptyQuery = { docs: [], empty: true, size: 0, forEach: () => undefined };
    return {
        getFirestore: vi.fn(() => ({})),
        collection: vi.fn((_db: unknown, ...parts: any[]) => ({ kind: 'collection', path: refPath(parts) })),
        doc: vi.fn((_db: unknown, ...parts: any[]) => ({ kind: 'doc', path: refPath(parts) })),
        query: vi.fn((ref: any, ...constraints: unknown[]) => ({ ...ref, constraints })),
        orderBy: vi.fn((field: string) => ({ orderBy: field })),
        startAfter: vi.fn((cursor: unknown) => ({ startAfter: cursor })),
        limit: vi.fn((n: number) => ({ limit: n })),
        limitToLast: vi.fn((n: number) => ({ limitToLast: n })),
        serverTimestamp: vi.fn(() => ({ serverTimestamp: true })),
        deleteField: vi.fn(() => ({ deleteField: true })),
        addDoc: vi.fn(async () => ({ id: 'added' })),
        // Update and history tiers are empty: everything lives in the fold
        getDocs: vi.fn(async () => emptyQuery),
        getDoc: vi.fn(async () => ({
            exists: () => rec.mainDoc !== null,
            // A fresh object per read: sync code annotates the data it gets
            data: () => (rec.mainDoc ? { ...rec.mainDoc } : undefined),
        })),
        runTransaction: vi.fn(async () => undefined),
        Bytes: FakeBytes,
        onSnapshot: vi.fn((_target: unknown, next: (s: unknown) => unknown) => {
            rec.listeners.push(next);
            return () => undefined;
        }),
    };
});

vi.mock('@firebase/storage', () => ({
    getStorage: vi.fn(() => ({})),
    ref: vi.fn((_s: unknown, path: string) => ({ path })),
    uploadBytes: vi.fn(async () => undefined),
    getBytes: vi.fn(async (r: { path: string }) => {
        const blob = rec.blobs.get(r.path);
        if (!blob) throw new Error(`storage/object-not-found: ${r.path}`);
        return blob.slice().buffer;
    }),
    deleteObject: vi.fn(async () => undefined),
}));

import * as Y from 'yjs';
import { Bytes } from '@firebase/firestore';
import { performInitialSync, createSnapshotListener, type SyncContext } from '../../src/sync';
import { mergeUpdatesWithMeta } from '../../src/merge-core';
import { createSim, runSession, clientIdForSession } from '../../benchmarks/versicle-workload';

const SEED = 20260820;
const PATH = 'docs/versicle';
const SNAPSHOT_PATH = `${PATH}/snapshots/v7.bin`;
const SNAPSHOT_VERSION = 7;
const VERSICLE_ROOTS = ['library', 'progress', 'annotations', 'reading-list', 'vocabulary', 'lexicon', 'contentAnalysis', 'devices', 'searchHistory', 'meta'];

/** Session counts whose full state a test needs (age, age - gap) */
const AGES = [24, 96];
const GAP = 2;

const states = new Map<number, Uint8Array>();
function stateAt(session: number): Uint8Array {
    if (states.size === 0) {
        const wanted = new Set<number>();
        for (const age of AGES) { wanted.add(age); wanted.add(age - GAP); }
        const sim = createSim({ seed: SEED });
        const world = new Y.Doc();
        for (let s = 0; s < Math.max(...AGES); s++) {
            world.clientID = clientIdForSession(SEED, s);
            runSession(sim, world);
            if (wanted.has(s + 1)) states.set(s + 1, Y.encodeStateAsUpdate(world));
        }
        world.destroy();
    }
    return states.get(session)!;
}

/** JSON with object keys sorted: Y.Map key order depends on integration order */
function canonical(value: unknown): unknown {
    if (Array.isArray(value)) return value.map(canonical);
    if (value && typeof value === 'object') {
        const out: Record<string, unknown> = {};
        for (const k of Object.keys(value).sort()) out[k] = canonical((value as Record<string, unknown>)[k]);
        return out;
    }
    return value;
}

function materialize(doc: Y.Doc): string {
    const out: Record<string, unknown> = {};
    for (const name of VERSICLE_ROOTS) out[name] = doc.getMap(name).toJSON();
    return JSON.stringify(canonical(out));
}

interface Fixture {
    age: number;
    localState: Uint8Array;
    snapshot: Uint8Array;
    serverJson: string;
    serverSV: Uint8Array;
    /** Structs in the snapshot (what a full apply decodes) */
    snapshotStructs: number;
    /** Structs the local doc lacks (what a diff against its SV carries) */
    missingStructs: number;
}

function buildFixture(age: number): Fixture {
    const localState = stateAt(age - GAP);
    const fold = mergeUpdatesWithMeta([stateAt(age)], { gc: true });

    const local = new Y.Doc();
    Y.applyUpdate(local, localState);
    const missing = Y.decodeUpdate(Y.diffUpdate(fold.result, Y.encodeStateVector(local)))
        .structs.filter(s => !(s instanceof Y.Skip)).length;
    local.destroy();

    const server = new Y.Doc();
    Y.applyUpdate(server, fold.result);
    const serverJson = materialize(server);
    server.destroy();

    // Server: one GC fold in Storage, inline delete-set fingerprint, no
    // update/history documents.
    rec.blobs.set(SNAPSHOT_PATH, fold.result);
    rec.mainDoc = {
        snapshotStoragePath: SNAPSHOT_PATH,
        stateVector: toBase64(fold.stateVector),
        deleteSet: Bytes.fromUint8Array(fold.dsUpdate),
        version: SNAPSHOT_VERSION,
        origin: 'other-device',
    };

    return {
        age,
        localState,
        snapshot: fold.result,
        serverJson,
        serverSV: fold.stateVector,
        snapshotStructs: Y.decodeUpdate(fold.result).structs.filter(s => !(s instanceof Y.Skip)).length,
        missingStructs: missing,
    };
}

/** The returning device's live doc: hydrated (y-idb) with the state GAP sessions back */
function hydrateLocal(fx: Fixture): Y.Doc {
    const doc = new Y.Doc();
    doc.clientID = clientIdForSession(SEED, 900_000 + fx.age);
    Y.applyUpdate(doc, fx.localState);
    return doc;
}

function makeCtx(doc: Y.Doc): SyncContext {
    return {
        db: {} as any,
        storage: {} as any,
        path: PATH,
        doc,
        uid: 'returning-device',
        maxUpdatesThreshold: 50,
        isDestroyed: () => false,
        corruptedDocIds: new Set<string>(),
    };
}

interface ApplyCounts {
    /** Structs decoded by Y.applyUpdate on the live doc */
    structs: number;
    /** ...of which the live doc already held the whole clock range before syncing */
    alreadyHeld: number;
    bytes: number;
}

/** Starts recording live-doc applies; returns a function that tallies them. */
function track(doc: Y.Doc): () => ApplyCounts {
    const svBefore = Y.decodeStateVector(Y.encodeStateVector(doc));
    rec.target = doc;
    rec.applied = [];
    return () => {
        rec.target = null;
        let structs = 0, alreadyHeld = 0, bytes = 0;
        for (const { update, v2 } of rec.applied) {
            bytes += update.byteLength;
            const decoded = v2 ? Y.decodeUpdateV2(update) : Y.decodeUpdate(update);
            for (const s of decoded.structs) {
                if (s instanceof Y.Skip) continue;
                structs++;
                if (s.id.clock + s.length <= (svBefore.get(s.id.client) || 0)) alreadyHeld++;
            }
        }
        return { structs, alreadyHeld, bytes };
    };
}

async function initialSyncCounts(fx: Fixture): Promise<ApplyCounts> {
    const doc = hydrateLocal(fx);
    const tally = track(doc);
    const result = await performInitialSync(makeCtx(doc));
    const counts = tally();

    // Correctness first: the behind doc converges to the server state
    expect(result.success).toBe(true);
    expect(materialize(doc)).toBe(fx.serverJson);
    const svMap = Y.decodeStateVector(Y.encodeStateVector(doc));
    for (const [client, clock] of Y.decodeStateVector(fx.serverSV)) {
        expect(svMap.get(client) || 0).toBeGreaterThanOrEqual(clock);
    }
    doc.destroy();
    return counts;
}

beforeEach(() => {
    rec.target = null;
    rec.applied = [];
    rec.mainDoc = null;
    rec.blobs.clear();
    rec.listeners = [];
});

describe('behind-client snapshot: main-thread apply volume', () => {
    it('initial sync decodes only the missing structs onto the live doc, not the whole snapshot', async () => {
        const fx = buildFixture(24);
        const counts = await initialSyncCounts(fx);

        // The local doc already holds ~90% of the snapshot (more as the doc ages)
        expect(fx.missingStructs).toBeLessThan(fx.snapshotStructs / 5);

        expect(
            counts.alreadyHeld,
            `live doc decoded ${counts.alreadyHeld} structs it already held ` +
            `(${counts.structs} decoded in total, snapshot has ${fx.snapshotStructs}, ` +
            `${fx.missingStructs} missing; ${counts.bytes} bytes applied vs ${fx.snapshot.byteLength}-byte snapshot)`,
        ).toBe(0);
        expect(counts.structs).toBeLessThanOrEqual(2 * fx.missingStructs);
    });

    it('main-thread apply volume scales with the missing data, not with document age', async () => {
        const young = buildFixture(24);
        const youngCounts = await initialSyncCounts(young);
        const old = buildFixture(96);
        const oldCounts = await initialSyncCounts(old);

        // Same gap (2 sessions) at 4x the document age: the snapshot grows
        // ~3.8x, the missing data does not.
        expect(old.snapshotStructs / young.snapshotStructs).toBeGreaterThan(3);
        expect(old.missingStructs / young.missingStructs).toBeLessThan(1.5);

        const ratio = oldCounts.structs / youngCounts.structs;
        expect(
            ratio,
            `structs decoded on the live doc: ${youngCounts.structs} at ${young.age} sessions -> ` +
            `${oldCounts.structs} at ${old.age} sessions (missing: ${young.missingStructs} -> ${old.missingStructs})`,
        ).toBeLessThan(2);
    });

    it('snapshot listener decodes only the missing structs onto a behind live doc', async () => {
        const fx = buildFixture(24);
        const doc = hydrateLocal(fx);
        const ctx = makeCtx(doc);

        // Initial sync processed an older fold; a new one (version 7) arrives
        const unsubscribe = createSnapshotListener(ctx, SNAPSHOT_VERSION - 1);
        expect(rec.listeners).toHaveLength(1);

        const tally = track(doc);
        await rec.listeners[0]({ exists: () => true, data: () => ({ ...rec.mainDoc! }) });
        const counts = tally();
        unsubscribe();

        expect(ctx.corruptedDocIds!.size).toBe(0);
        expect(materialize(doc)).toBe(fx.serverJson);
        doc.destroy();

        expect(
            counts.alreadyHeld,
            `live doc decoded ${counts.alreadyHeld} structs it already held ` +
            `(${counts.structs} decoded in total, snapshot has ${fx.snapshotStructs}, ${fx.missingStructs} missing)`,
        ).toBe(0);
        expect(counts.structs).toBeLessThanOrEqual(2 * fx.missingStructs);
    });
});
