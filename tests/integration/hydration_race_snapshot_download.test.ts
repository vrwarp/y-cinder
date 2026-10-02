/**
 * Performance regression test (Cloud Storage I/O + main-thread apply): a
 * cold start whose initial sync reads the main document before local
 * persistence (y-idb) has hydrated the Y.Doc must not download and apply
 * the Storage snapshot that IndexedDB already holds.
 *
 * Cost: `performInitialSync` decides whether to fetch the snapshot blob
 * with `localCoversSnapshot(data, ydoc)` immediately after the main-doc
 * read. versicle builds a fresh Y.Doc + provider on every page load, and
 * IndexedDB open/read is async, so the provider is usually constructed
 * BEFORE the persisted state lands. On an aged document y-idb hydration
 * takes 100 ms to 1 s (docs/performance.md: 37 ms -> 1,004 ms with age),
 * while initial sync reaches the decision after 3 serial round trips
 * (5 with the clock-skew probe). Whenever the network wins that race, the
 * still-empty doc "does not cover" the snapshot and the provider downloads
 * the whole O(snapshot) blob. Then either
 *  - the download finishes first: initial sync applies the snapshot on the
 *    main thread and y-idb's hydration lands on top of it — a full decode
 *    that changes nothing; or
 *  - hydration lands while the blob is still downloading: initial sync
 *    re-reads the local state vector before applying and skips the
 *    snapshot, so the download itself is the waste.
 * Nothing in FireProviderConfig let the app say "wait for my local
 * persistence first"; `localReady` now does.
 *
 * Model: a versicle document aged through 100 sessions (one fresh clientID
 * per session) is folded into a Storage snapshot on the server; the
 * cold-start device's IndexedDB holds exactly the same state (it was in
 * sync when the app closed). Hydration is applied the way y-idb does it:
 * one transaction, origin = the persistence instance, local = false.
 *
 * The hydration transaction's 'update' event (the whole document when it
 * lands on an empty doc) is buffered by the provider as a local edit and
 * would be re-uploaded by the debounced save. That echo is a SEPARATE cost
 * (tracked as hydration-echo-presync-dup-upload): its size is reported
 * here, but the buffer is discarded before destroy() (maxWaitTime is long
 * enough that no save starts during a run), so nothing a cold start does
 * changes the server state and every run sees the same seeded document.
 * Note the interplay the bench shows: today, every provider-first cold
 * start pays EITHER the echo (hydration won the race) OR this download
 * (hydration lost it). Making initial sync wait for hydration removes the
 * download but makes the echo unconditional until that fix lands too.
 *
 * Three parts:
 *  - `bench:` scenarios report the numbers (always pass, they only assert
 *    convergence): the race with an injected per-op latency (hydration H ms
 *    after construction, with/without the clock-skew probe, Storage at
 *    50 Mbit/s, 5 interleaved runs, medians; plus the README-style control
 *    ordering "hydrate, then construct"), and the deterministic adversarial
 *    ordering at two document ages to show the download is O(snapshot).
 *    `localReady` rows pass the option itself (code that predates it
 *    ignores it, so they match the provider-first rows there).
 *    Run just those with `-t bench`.
 *  - `regression:` pins the cost with a counter: when the app passes its
 *    persistence readiness (`localReady`, versicle: `idbPersistence.
 *    whenSynced`) and hydration lands right after the main-doc read
 *    returned, initial sync downloads 0 snapshot bytes. Failed before the
 *    option existed: the decision ran on the empty doc.
 *  - `guard:` tests pin what the fix must keep: a `localReady` that never
 *    settles, or rejects, must not stall initial sync.
 *
 * Determinism: the regression ordering does not depend on timing. The
 * provider's main-document `getDoc` is wrapped; hydration is scheduled on
 * the macrotask AFTER that read resolves, while the unfixed decision runs
 * in the same microtask chain as the read's continuation (the fingerprint
 * is inline, so there is no await in between). A fallback timer hydrates
 * 1.5 s after construction in case a fix awaits `localReady` before it
 * issues the main-doc read at all. Every assertion is a byte/operation
 * count, never a wall-clock bound.
 *
 * Run (needs the Firestore + Storage emulator):
 *   bash scripts/test.sh tests/integration/hydration_race_snapshot_download.test.ts
 *   bash scripts/test.sh tests/integration/hydration_race_snapshot_download.test.ts -t bench
 *
 * @file hydration_race_snapshot_download.test.ts
 */

import { describe, it, expect, beforeAll, afterEach, vi } from 'vitest';

const { io } = vi.hoisted(() => ({
    io: {
        /** Artificial latency (ms) added before every one-shot SDK call */
        latencyMs: 0,
        /** Storage download bandwidth (bytes per ms); 0 = emulator speed */
        bytesPerMs: 0,
        /** Provider path whose main-document read is watched */
        watchPath: null as string | null,
        /** Called (synchronously) when the watched main-doc read resolves */
        onMainDocRead: null as (() => void) | null,
        /**
         * When set, the watched main-doc read is handed to the provider only
         * once this settles (bench: emulates the proposed fix, which awaits
         * local persistence right before the snapshot decision).
         */
        mainDocGate: null as Promise<void> | null,
        downloads: [] as { path: string; bytes: number }[],
        reset() {
            this.downloads = [];
        },
    },
}));

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    const delay = async () => {
        if (io.latencyMs > 0) await new Promise(r => setTimeout(r, io.latencyMs));
    };
    return {
        ...actual,
        getDoc: async (ref: any) => {
            await delay();
            const snap = await actual.getDoc(ref);
            if (io.watchPath !== null && ref?.path === io.watchPath) {
                io.onMainDocRead?.();
                if (io.mainDocGate) await io.mainDocGate;
            }
            return snap;
        },
        getDocs: async (q: any) => { await delay(); return actual.getDocs(q); },
        setDoc: async (...a: any[]) => { await delay(); return actual.setDoc(...a); },
        deleteDoc: async (ref: any) => { await delay(); return actual.deleteDoc(ref); },
        addDoc: async (ref: any, data: any) => { await delay(); return actual.addDoc(ref, data); },
    };
});

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getBytes: async (storageRef: any, ...rest: any[]) => {
            if (io.latencyMs > 0) await new Promise(r => setTimeout(r, io.latencyMs));
            const buffer = await actual.getBytes(storageRef, ...rest);
            if (io.bytesPerMs > 0) await new Promise(r => setTimeout(r, buffer.byteLength / io.bytesPerMs));
            io.downloads.push({ path: storageRef.fullPath, bytes: buffer.byteLength });
            return buffer;
        },
    };
});

import * as Y from 'yjs';
import { addDoc, collection, doc as fsDoc, getDoc, getDocs, serverTimestamp } from 'firebase/firestore';
import { getBytes, ref, uploadBytes } from 'firebase/storage';
import { FireProvider } from '../../src/provider';
import { compact } from '../../src/compaction';
import { aggregateClockEnds, extractClockEnds } from '../../src/update-metadata';
import { FIREBASE_ORIGINS, FireProviderConfig } from '../../src/types';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';
import { createSim, runSession, clientIdForSession, materializeVersicleDoc } from '../../benchmarks/versicle-workload';

/** Stand-in for the IndexeddbPersistence instance y-idb uses as origin. */
const IDB_ORIGIN = { name: 'y-idb persistence (test stand-in)' };

const SEED = 20260820;
/** Per-op latency of the race bench (a decent network, ~25 ms per request) */
const BENCH_LATENCY_MS = 25;
/** Storage transfer rate of the race bench: 50 Mbit/s (decent Wi-Fi) */
const BENCH_MBPS = 50;
const RUNS = 5;
/** Regression fallback: hydrate this long after construction at the latest */
const FALLBACK_HYDRATE_MS = 1500;

/** Written before the option existed; FireProviderConfig.localReady now has this type. */
type ConfigWithLocalReady = FireProviderConfig & { localReady?: Promise<unknown> };

/** Persisted state of a versicle document aged through `sessions` sessions. */
function agedState(sessions: number): Uint8Array {
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

/** Whether a Yjs transaction integrated any struct or deletion. */
const txChanged = (tr: any): boolean =>
    tr.deleteSet.clients.size > 0 ||
    [...tr.afterState].some(([client, clock]: [number, number]) => tr.beforeState.get(client) !== clock);

/** Initial sync's apply + y-idb's apply (both full decodes when a snapshot was downloaded). */
const totalApplyMs = (r: ColdStartResult) => Math.max(0, r.providerApplyMs) + r.hydrationApplyMs;
/**
 * The full decode that changed nothing: y-idb's hydration landing on top of
 * the snapshot initial sync downloaded and applied. (When hydration instead
 * lands while the blob is still downloading, initial sync re-reads the
 * local state vector before applying and skips the snapshot as redundant:
 * the download is then the only waste.)
 */
const redundantApplyMs = (r: ColdStartResult) => (r.hydrationNoop ? r.hydrationApplyMs : 0);

const median = (xs: number[]) => {
    const s = [...xs].sort((a, b) => a - b);
    const m = Math.floor(s.length / 2);
    return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const fmt = (n: number) => n.toLocaleString('en-US');
const ms = (n: number) => `${n.toFixed(0)} ms`;

interface Seeded {
    path: string;
    persisted: Uint8Array;
    expectedJson: string;
    snapshotBytes: number;
    sessions: number;
}

type HydrationPlan =
    /** Hydration lands `afterMs` after construction (the race) */
    | { kind: 'timer'; afterMs: number }
    /** Next macrotask after the provider's main-doc read resolves (+ fallback) */
    | { kind: 'after-main-doc-read'; fallbackMs: number }
    /** Control: hydration takes `afterMs`, THEN the provider is constructed */
    | { kind: 'before-construct'; afterMs: number };

interface ColdStartOptions {
    probe: boolean;
    latencyMs: number;
    /** Simulated Storage download bandwidth (Mbit/s); omitted = emulator speed */
    mbps?: number;
    /**
     * Bench only: hold the provider's main-doc read until hydration has
     * landed — what the proposed `localReady` wait does (update/history
     * reads still overlap hydration), emulated without touching src/.
     */
    emulateFix?: boolean;
    /** Pass the persistence readiness to the provider (proposed option) */
    localReady?: 'hydration' | 'never' | 'reject';
}

interface ColdStartResult {
    snapshotDownloads: number;
    snapshotBytes: number;
    /** ms from "page load" (t0) to 'sync'; t0 precedes construction only in the control ordering */
    syncedMs: number;
    /** ms from t0 to the start of the hydration transaction (-1: never) */
    hydratedMs: number;
    /** What triggered hydration (timer / main-doc-read / fallback / before-construct) */
    trigger: string;
    /** Duration of initial sync's apply transaction */
    providerApplyMs: number;
    /** Initial sync's apply changed nothing (everything was already hydrated) */
    providerApplyNoop: boolean;
    /** Duration of the hydration transaction */
    hydrationApplyMs: number;
    /** The hydration transaction changed nothing (landed after the snapshot apply) */
    hydrationNoop: boolean;
    /** Bytes the provider buffered from the hydration transaction (discarded) */
    echoBytes: number;
    contentOk: boolean;
}

describe('Cold start: snapshot download racing local-persistence hydration', () => {
    let app: any;
    let db: any;
    let storage: any;
    let counter = 0;
    const live: FireProvider[] = [];
    const seeded = new Map<number, Seeded>();

    beforeAll(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
    });

    afterEach(async () => {
        io.latencyMs = 0;
        io.bytesPerMs = 0;
        io.watchPath = null;
        io.onMainDocRead = null;
        io.mainDocGate = null;
        while (live.length > 0) await release(live.pop()!);
    });

    /** Destroys a provider without letting it write the hydration echo. */
    const release = async (p: FireProvider) => {
        const i = live.indexOf(p);
        if (i >= 0) live.splice(i, 1);
        (p as any)._pendingUpdates = [];
        (p as any)._pendingSince = null;
        await p.destroy();
    };

    /**
     * Puts `sessions` of versicle history on the server the normal way: one
     * storage-backed update document folded by compaction into a Storage
     * snapshot (+ inline delete-set fingerprint). Cached per age; cold
     * starts never write, so one seeded document serves every run.
     */
    const seed = async (sessions: number): Promise<Seeded> => {
        const cached = seeded.get(sessions);
        if (cached) return cached;
        const persisted = agedState(sessions);
        const path = `tests/hydration-race-${sessions}-${getStableDate()}-${Date.now()}-${counter++}`;
        const blobPath = `${path}/large_updates/seed_${Date.now()}.bin`;
        await uploadBytes(ref(storage, blobPath), persisted);
        await addDoc(collection(db, path, 'updates'), {
            updateStoragePath: blobPath,
            createdAt: serverTimestamp(),
            createdBy: 'seed',
            ...aggregateClockEnds(extractClockEnds(persisted)),
        });
        const result = await compact({
            db, path, uid: 'seed-compactor', lockTTL: 60000, compactionLimit: 500,
            isDestroyed: () => false, storage, cachedClockOffset: 0,
        });
        expect(result.success).toBe(true);
        const main = (await getDoc(fsDoc(db, path))).data()!;
        expect(main.snapshotStoragePath, 'snapshot lives in Storage').toBeTruthy();
        // The deterministic ordering relies on an INLINE fingerprint (no
        // await between the main-doc read and the snapshot decision).
        expect(main.deleteSet, 'inline delete-set fingerprint').toBeTruthy();
        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);
        const snapshotBytes = (await getBytes(ref(storage, main.snapshotStoragePath))).byteLength;
        const s: Seeded = {
            path,
            persisted,
            expectedJson: materializeVersicleDoc(persisted, []),
            snapshotBytes,
            sessions,
        };
        seeded.set(sessions, s);
        io.reset();
        return s;
    };

    /** One page load against a seeded document. */
    const coldStart = async (s: Seeded, plan: HydrationPlan, opts: ColdStartOptions): Promise<ColdStartResult> => {
        io.reset();
        io.latencyMs = opts.latencyMs;
        io.bytesPerMs = opts.mbps ? (opts.mbps * 1e6) / 8 / 1000 : 0;
        const doc = new Y.Doc();

        // Transaction timing by origin (performInitialSync applies under
        // FIREBASE_ORIGINS.UPDATE; y-idb under its persistence instance).
        const started = new Map<any, number>();
        let providerApplyMs = -1;
        let providerApplyNoop = true;
        let hydrationApplyMs = 0;
        let hydrationNoop = false;
        doc.on('beforeTransaction', (tr: any) => started.set(tr, performance.now()));
        doc.on('afterTransaction', (tr: any) => {
            const dur = performance.now() - (started.get(tr) ?? performance.now());
            if (tr.origin === FIREBASE_ORIGINS.UPDATE && providerApplyMs < 0) {
                // The first one is initial sync's apply (listeners attach later)
                providerApplyMs = dur;
                providerApplyNoop = !txChanged(tr);
            } else if (tr.origin === IDB_ORIGIN) {
                hydrationApplyMs = dur;
                hydrationNoop = !txChanged(tr);
            }
        });

        let markHydrated!: () => void;
        const hydrated = new Promise<void>(res => { markHydrated = res; });
        let resolveReady!: () => void;
        let rejectReady!: (e: Error) => void;
        const ready = new Promise<void>((res, rej) => { resolveReady = res; rejectReady = rej; });
        ready.catch(() => { /* observed by the provider, if at all */ });

        const t0 = performance.now();
        let hydratedMs = -1;
        let trigger = '';
        const hydrate = (why: string) => {
            if (hydratedMs >= 0) return;
            hydratedMs = performance.now() - t0;
            trigger = why;
            Y.transact(doc, () => Y.applyUpdate(doc, s.persisted), IDB_ORIGIN, false);
            markHydrated();
            if (opts.localReady === 'hydration') resolveReady();
        };

        if (plan.kind === 'before-construct') {
            await new Promise(r => setTimeout(r, plan.afterMs));
            hydrate('before-construct');
        }

        const config: ConfigWithLocalReady = {
            firebaseApp: app,
            ydoc: doc,
            path: s.path,
            // Skips the clock-skew probe (3 ops, 2 serial round trips)
            ...(opts.probe ? {} : { cachedClockOffset: 0 }),
            maxUpdatesThreshold: 1000,
            // No save may start during a run (see the file header: echo)
            maxWaitTime: 60_000,
            maxAggregationTime: 600_000,
        };
        if (opts.localReady === 'hydration') config.localReady = ready;
        if (opts.localReady === 'never') config.localReady = new Promise(() => { /* never settles */ });
        if (opts.localReady === 'reject') {
            config.localReady = ready;
            setTimeout(() => rejectReady(new Error('IndexedDB unavailable')), 0);
        }

        const timers: ReturnType<typeof setTimeout>[] = [];
        if (opts.emulateFix) {
            io.watchPath = s.path;
            io.mainDocGate = hydrated;
        }
        if (plan.kind === 'after-main-doc-read') {
            io.watchPath = s.path;
            io.onMainDocRead = () => { timers.push(setTimeout(() => hydrate('main-doc-read'), 0)); };
            timers.push(setTimeout(() => hydrate('fallback'), plan.fallbackMs));
        }

        let syncedMs = -1;
        const provider = new FireProvider(config);
        live.push(provider);
        provider.on('sync', () => { if (syncedMs < 0) syncedMs = performance.now() - t0; });
        if (plan.kind === 'timer') timers.push(setTimeout(() => hydrate('timer'), plan.afterMs));

        await waitForConditionTruthy(() => provider.synced, { timeout: 90000, interval: 5, message: 'cold-start client synced' });
        if (plan.kind !== 'before-construct' && opts.localReady !== 'never' && opts.localReady !== 'reject') {
            await waitForConditionTruthy(() => hydratedMs >= 0, { timeout: 30000, interval: 5, message: 'hydration landed' });
        }
        timers.forEach(clearTimeout);
        io.watchPath = null;
        io.onMainDocRead = null;
        io.mainDocGate = null;
        // Let a snapshot-listener delivery (if any) settle before counting
        await new Promise(r => setTimeout(r, 300));

        const echoBytes = ((provider as any)._pendingUpdates as Uint8Array[])
            .reduce((a, u) => a + u.byteLength, 0);
        await release(provider);
        io.latencyMs = 0;
        io.bytesPerMs = 0;

        const snaps = io.downloads.filter(d => d.path.startsWith(`${s.path}/`) && d.path.includes('/snapshot_'));
        const contentOk = materializeVersicleDoc(Y.encodeStateAsUpdate(doc), []) === s.expectedJson;
        doc.destroy();
        return {
            snapshotDownloads: snaps.length,
            snapshotBytes: snaps.reduce((a, d) => a + d.bytes, 0),
            syncedMs,
            hydratedMs,
            trigger,
            providerApplyMs,
            providerApplyNoop,
            hydrationApplyMs,
            hydrationNoop,
            echoBytes,
            contentOk,
        };
    };

    it('bench: race between hydration and initial sync (25 ms per op, 50 Mbit/s, hydration H ms after construction)', async () => {
        const s = await seed(100);
        // Warm the connection and the code paths (not reported)
        await coldStart(s, { kind: 'timer', afterMs: 0 }, { probe: false, latencyMs: 0 });

        const configs: { label: string; plan: HydrationPlan; probe: boolean; emulateFix?: boolean; localReady?: 'hydration'; mbps?: number }[] = [];
        for (const probe of [true, false]) {
            for (const h of [10, 150, 400]) {
                configs.push({ label: `provider first, H=${h} ms, probe ${probe ? 'on' : 'off'}`, plan: { kind: 'timer', afterMs: h }, probe });
            }
        }
        // What the proposed fix would do, emulated: same race, but the
        // snapshot decision waits for hydration.
        for (const probe of [true, false]) {
            for (const h of [150, 400]) {
                configs.push({ label: `emulated fix: provider first, H=${h} ms, probe ${probe ? 'on' : 'off'}`, plan: { kind: 'timer', afterMs: h }, probe, emulateFix: true });
            }
        }
        // The fix itself: the same race, with the hydration's readiness
        // passed as localReady (ignored by code that predates the option,
        // so these rows match the provider-first rows there).
        for (const probe of [true, false]) {
            for (const h of [150, 400]) {
                configs.push({ label: `localReady: provider first, H=${h} ms, probe ${probe ? 'on' : 'off'}`, plan: { kind: 'timer', afterMs: h }, probe, localReady: 'hydration' });
            }
        }
        // The README-recommended ordering (construct after whenSynced): no
        // race, but initial sync only starts once hydration is done — an
        // upper bound on time-to-synced for a fix that overlaps the reads
        // with hydration.
        for (const [h, probe] of [[150, false], [400, false], [400, true]] as const) {
            configs.push({ label: `control: hydrate (${h} ms) then construct, probe ${probe ? 'on' : 'off'}`, plan: { kind: 'before-construct', afterMs: h }, probe });
        }
        // Fast link (emulator-speed Storage): the download completes BEFORE
        // hydration, so initial sync applies the snapshot and y-idb's apply
        // lands on top of it — the redundant main-thread decode.
        for (const emulateFix of [false, true]) {
            configs.push({
                label: `${emulateFix ? 'emulated fix: ' : ''}provider first, H=400 ms, probe off, Storage unthrottled`,
                plan: { kind: 'timer', afterMs: 400 }, probe: false, emulateFix, mbps: 0,
            });
        }
        configs.push({
            label: 'localReady: provider first, H=400 ms, probe off, Storage unthrottled',
            plan: { kind: 'timer', afterMs: 400 }, probe: false, localReady: 'hydration', mbps: 0,
        });

        const results = new Map<string, ColdStartResult[]>();
        for (let run = 0; run < RUNS; run++) {
            // Interleaved: every configuration once per round
            for (const c of configs) {
                const r = await coldStart(s, c.plan, { probe: c.probe, latencyMs: BENCH_LATENCY_MS, mbps: c.mbps ?? BENCH_MBPS, emulateFix: c.emulateFix, localReady: c.localReady });
                expect(r.contentOk, `${c.label}: converged`).toBe(true);
                if (!results.has(c.label)) results.set(c.label, []);
                results.get(c.label)!.push(r);
            }
        }

        console.log(`\n=== hydration race: ${s.sessions}-session versicle doc, snapshot ${fmt(s.snapshotBytes)} B, ` +
            `persisted state ${fmt(s.persisted.byteLength)} B, ${BENCH_LATENCY_MS} ms per op, Storage at ${BENCH_MBPS} Mbit/s unless noted, ` +
            `${RUNS} interleaved runs (medians) ===`);
        console.log('| ordering | runs with a snapshot download | bytes downloaded / run | synced at (from page load) | main-thread apply (initial sync + hydration) | of which redundant | hydration echo buffered (separate issue) |');
        console.log('| --- | ---: | ---: | ---: | ---: | ---: | ---: |');
        for (const c of configs) {
            const rs = results.get(c.label)!;
            const dl = rs.filter(r => r.snapshotDownloads > 0).length;
            console.log(`| ${c.label} | ${dl}/${rs.length} | ${fmt(median(rs.map(r => r.snapshotBytes)))} B | ` +
                `${ms(median(rs.map(r => r.syncedMs)))} | ${ms(median(rs.map(totalApplyMs)))} | ` +
                `${ms(median(rs.map(redundantApplyMs)))} | ${fmt(median(rs.map(r => r.echoBytes)))} B |`);
        }
    }, 600_000);

    it('bench: deterministic adversarial ordering — hydration lands right after the main-doc read (download is O(snapshot))', async () => {
        console.log('\n=== hydration lands on the macrotask after the main-doc read (no injected latency, probe off, 5 runs each) ===');
        console.log('| sessions | snapshot | persisted state | variant | snapshot downloads per run | bytes downloaded / run | downloaded snapshot skipped at apply | main-thread apply (median) |');
        console.log('| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |');
        for (const sessions of [25, 100]) {
            const s = await seed(sessions);
            for (const variant of ['current', 'emulated fix', 'localReady'] as const) {
                const rs: ColdStartResult[] = [];
                for (let run = 0; run < RUNS; run++) {
                    const r = await coldStart(s, { kind: 'after-main-doc-read', fallbackMs: 60_000 }, {
                        probe: false, latencyMs: 0,
                        emulateFix: variant === 'emulated fix',
                        localReady: variant === 'localReady' ? 'hydration' : undefined,
                    });
                    expect(r.contentOk).toBe(true);
                    expect(r.trigger).toBe('main-doc-read');
                    rs.push(r);
                }
                const skipped = rs.filter(r => r.snapshotDownloads > 0 && r.providerApplyNoop).length;
                console.log(`| ${sessions} | ${fmt(s.snapshotBytes)} B | ${fmt(s.persisted.byteLength)} B | ${variant} | ` +
                    `${rs.map(r => r.snapshotDownloads).join('/')} | ${fmt(median(rs.map(r => r.snapshotBytes)))} B | ` +
                    `${skipped}/${rs.length} | ${ms(median(rs.map(totalApplyMs)))} |`);
            }
        }
    }, 600_000);

    it('regression: with localReady, hydration landing just after the main-doc read costs no snapshot download', async () => {
        const s = await seed(100);
        const r = await coldStart(
            s,
            { kind: 'after-main-doc-read', fallbackMs: FALLBACK_HYDRATE_MS },
            { probe: false, latencyMs: 0, localReady: 'hydration' },
        );
        expect(r.contentOk, 'cold-start doc converged to the seeded content').toBe(true);
        console.log(`regression: trigger=${r.trigger} snapshotDownloads=${r.snapshotDownloads} ` +
            `bytes=${fmt(r.snapshotBytes)} providerApply=${ms(r.providerApplyMs)} (no-op ${r.providerApplyNoop}) ` +
            `hydrationApply=${ms(r.hydrationApplyMs)} (no-op ${r.hydrationNoop})`);
        // IndexedDB already holds everything the snapshot holds: initial sync
        // must decide AFTER local persistence has hydrated the doc.
        expect(r.snapshotBytes, `snapshot bytes downloaded (snapshot is ${fmt(s.snapshotBytes)} B; hydration trigger: ${r.trigger})`).toBe(0);
        expect(r.snapshotDownloads).toBe(0);
        // ...so y-idb's apply is the only full decode on this cold start.
        expect(r.hydrationNoop, 'hydration applied onto an empty doc, not on top of a downloaded copy').toBe(false);
    }, 180_000);

    it('guard: a localReady that never settles does not stall initial sync', async () => {
        const s = await seed(100);
        const r = await coldStart(s, { kind: 'timer', afterMs: 60_000 }, { probe: false, latencyMs: 0, localReady: 'never' });
        // Nothing hydrated: the server snapshot is the only source.
        expect(r.syncedMs).toBeGreaterThan(0);
        expect(r.snapshotDownloads).toBe(1);
        expect(r.contentOk).toBe(true);
    }, 180_000);

    it('guard: a rejected localReady does not stall initial sync', async () => {
        const s = await seed(100);
        const r = await coldStart(s, { kind: 'timer', afterMs: 60_000 }, { probe: false, latencyMs: 0, localReady: 'reject' });
        expect(r.syncedMs).toBeGreaterThan(0);
        expect(r.snapshotDownloads).toBe(1);
        expect(r.contentOk).toBe(true);
    }, 180_000);
});
