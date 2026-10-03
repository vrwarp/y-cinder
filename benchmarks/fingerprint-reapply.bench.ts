/**
 * Benchmark: the delete-set fingerprint was re-applied on every start (and
 * on every new fold delivery) even when the local doc already held every
 * deletion it carries.
 *
 * Compaction stores a structs-empty "fingerprint" update — the snapshot's
 * full delete-set — on the main document (`deleteSet`, or offloaded to
 * Storage as `deleteSetStoragePath`). Two paths applied it unconditionally
 * (both now skip it when `fingerprintIsRedundant`; see "After the fix"):
 *
 *  1. `performInitialSync` pushes it as an `update` item without
 *     clientIDs/clientClocks, so `isItemRedundant` can never judge it and the
 *     apply loop always runs `Y.applyUpdate` on it — on every cold start,
 *     warm start and listener-error re-sync, including a fully synced
 *     (y-idb-hydrated) client whose doc already contains every deletion.
 *  2. `createSnapshotListener` applies it before `localCoversSnapshot` on
 *     every delivery of a NEW fold version (the version gate only skips
 *     versions already processed), so every online client pays it once per
 *     fold, even when the fold only merged data it already holds.
 *
 * Yjs' `readAndApplyDeleteSet` binary-searches every range and walks every
 * struct inside it; on an aged document that is tens of thousands of dead
 * structs, finds nothing to delete, and fires no 'update' event. The cost
 * is linear in the delete-set (i.e. in document age, until a squash).
 *
 * The SDK is faked at the boundary (queries, getDoc, onSnapshot, Storage
 * getBytes) with the REAL `Bytes` class; the real `performInitialSync` and
 * `createSnapshotListener` run against a server in the shape compaction
 * writes (GC'd snapshot in Storage, inline fingerprint, stateVector,
 * version). `Y.applyUpdate` is wrapped (pass-through) only while a probe is
 * armed, to attribute time and count the delete-set ranges re-applied;
 * `Y.createDeleteSetFromStructStore` likewise, to count local delete-set
 * builds. "listener delivery" times the whole snapshot-listener callback.
 *
 * For comparison it also times the containment test a fix would need:
 * decode the fingerprint's delete-set and check every range against the
 * local delete-set (`Y.createDeleteSetFromStructStore`, which the push guard
 * computes anyway right after the apply loop).
 *
 * Workload: the versicle model (benchmarks/versicle-workload.ts, same seed
 * as the aging suites), one persistent doc whose clientID changes every
 * session, sampled at 60 / 120 / 240 sessions.
 *
 * Baseline (unfixed code, shared 4-CPU box; median over 5 process runs of
 * the per-run medians of 7):
 *
 *   events | fingerprint (ranges) | dead structs | fp apply / warm boot | fp apply / fold
 *    3,600 |  8.9 KB  (3,136)     |  20,958      | 1.6 ms              |  1.0 ms
 *    7,200 | 17.7 KB  (6,241)     |  42,402      | 3.7 ms              |  3.0 ms
 *   14,400 | 35.2 KB (12,394)     |  84,647      | 8.4 ms of 18.0 ms   | 10.2 ms
 *
 * 0 'update' events, 0 downloads and 0 pushes in every case: the apply
 * changes nothing. The containment test costs 0.5 ms at 14,400 events
 * (+2.0 ms for the local delete-set, which the push guard builds anyway).
 *
 * After the fix (A/B against the parent commit, 7 interleaved process runs
 * each, medians of the per-run medians of 7; the box was busier than for
 * the baseline above):
 *
 *   events | warm boot       | fp applies (ranges) | localDs builds | listener delivery/fold
 *    3,600 |  5.4 ->  3.6 ms | 1 (3,136)  -> 0     | 1 -> 1 / boot  | 1.5 -> 1.2 ms
 *    7,200 | 11.2 ->  7.8 ms | 1 (6,241)  -> 0     | 1 -> 1 / boot  | 5.4 -> 2.5 ms
 *   14,400 | 24.1 -> 15.9 ms | 1 (12,394) -> 0     | 1 -> 1 / boot  | 9.7 -> 6.2 ms
 *
 * The listener now builds the local delete-set once per new fold (0 -> 1)
 * instead of walking the dead structs. Fresh boots are unchanged within
 * noise (~0.2-0.8 s); in isolation the check right after the snapshot
 * costs 8.1 ms where the apply cost 11.1 ms at 14,400 events.
 *
 * Run with:
 *   npx vitest run --config benchmarks/vitest.config.ts benchmarks/fingerprint-reapply.bench.ts
 */
import { describe, it, expect, vi } from 'vitest';

const probe = vi.hoisted(() => ({
    armed: false,
    calls: [] as { update: Uint8Array; ms: number }[],
    /** Y.createDeleteSetFromStructStore calls (O(structs) each) */
    dsBuilds: 0,
}));

vi.mock('yjs', async (importOriginal) => {
    const actual = await importOriginal<typeof import('yjs')>();
    return {
        ...actual,
        applyUpdate: (doc: InstanceType<typeof actual.Doc>, update: Uint8Array, origin?: unknown) => {
            if (!probe.armed) return actual.applyUpdate(doc, update, origin);
            const t0 = performance.now();
            try {
                return actual.applyUpdate(doc, update, origin);
            } finally {
                probe.calls.push({ update, ms: performance.now() - t0 });
            }
        },
        createDeleteSetFromStructStore: (store: Parameters<typeof actual.createDeleteSetFromStructStore>[0]) => {
            if (probe.armed) probe.dsBuilds++;
            return actual.createDeleteSetFromStructStore(store);
        },
    };
});

const server = vi.hoisted(() => ({
    main: null as Record<string, any> | null,
    storage: new Map<string, Uint8Array>(),
    downloads: 0,
    added: 0,
    snapshotListeners: [] as ((snap: any) => unknown)[],
}));

vi.mock('@firebase/firestore', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/firestore')>();
    const join = (parts: unknown[]) => parts.filter(p => typeof p === 'string').join('/');
    const empty = { docs: [], empty: true, size: 0, forEach: () => { } };
    return {
        ...actual,
        collection: (_db: unknown, ...parts: unknown[]) => ({ kind: 'collection', path: join(parts) }),
        doc: (_db: unknown, ...parts: unknown[]) => ({ kind: 'doc', path: join(parts) }),
        query: (ref: any, ...constraints: any[]) => ({ ...ref, constraints }),
        orderBy: (field: string) => ({ orderBy: field }),
        startAfter: (cursor: any) => ({ startAfter: cursor }),
        limit: (n: number) => ({ limit: n }),
        serverTimestamp: () => ({ serverTimestamp: true }),
        // A warm start right after a fold: no update or history documents.
        getDocs: async () => empty,
        getDoc: async () => ({
            exists: () => server.main !== null,
            data: () => (server.main ? { ...server.main } : undefined),
        }),
        addDoc: async () => {
            server.added++;
            return { id: 'added' };
        },
        onSnapshot: (_target: unknown, next: (snap: any) => unknown) => {
            server.snapshotListeners.push(next);
            return () => {
                server.snapshotListeners = server.snapshotListeners.filter(l => l !== next);
            };
        },
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
            server.downloads++;
            return blob.slice().buffer;
        },
        uploadBytes: async () => undefined,
    };
});

import * as Y from 'yjs';
import { Bytes } from '@firebase/firestore';
import { toBase64 } from 'lib0/buffer';
import { performInitialSync, createSnapshotListener, SyncContext } from '../src/sync';
import { createSim, runSession, clientIdForSession, docStructStats } from './versicle-workload';
import { fmtBytes, fmtMs } from './helpers';

const SEED = 20260820; // same seed as the versicle aging suites
const SAMPLE_AT = [60, 120, 240];
const RUNS = 7;
const FRESH_RUNS = 5;
const PATH = 'docs/fingerprint-reapply';

type DeleteSet = ReturnType<typeof Y.decodeUpdate>['ds'];

function median(xs: number[]): number {
    const s = [...xs].sort((a, b) => a - b);
    return s[Math.floor(s.length / 2)];
}

function dsRangeCount(ds: DeleteSet): number {
    let n = 0;
    ds.clients.forEach(items => { n += items.length; });
    return n;
}

/** Inline Firestore bytes as the SDK delivers them (decoded from base64). */
function inlineBytes(u: Uint8Array): Bytes {
    return Bytes.fromBase64String(toBase64(u));
}

/**
 * Exact range containment: is every range of `sub` inside a range of `sup`?
 * Both are sorted and merged (as decoded / built from a struct store).
 */
function dsContained(sub: DeleteSet, sup: DeleteSet): boolean {
    for (const [client, items] of sub.clients) {
        const local = sup.clients.get(client);
        if (!local) return false;
        let j = 0;
        for (const { clock, len } of items) {
            while (j < local.length && local[j].clock + local[j].len <= clock) j++;
            if (j === local.length || local[j].clock > clock || local[j].clock + local[j].len < clock + len) return false;
        }
    }
    return true;
}

function ctxFor(doc: Y.Doc): SyncContext {
    return {
        db: {} as any,
        storage: {} as any,
        path: PATH,
        doc,
        uid: 'bench-client',
        maxUpdatesThreshold: 50,
        isDestroyed: () => false,
    };
}

interface Row {
    sessions: number;
    events: number;
    snapshotBytes: number;
    fpBytes: number;
    fpRanges: number;
    structsWalked: number;
    bootMs: number;
    bootFpApplyMs: number;
    bootFpApplies: number;
    bootRangesReapplied: number;
    bootDsBuilds: number;
    bootUpdateEvents: number;
    bootPushes: number;
    freshBootMs: number;
    freshFpApplyMs: number;
    listenerFpApplyMs: number;
    listenerDeliveryMs: number;
    listenerFpApplies: number;
    listenerDsBuilds: number;
    listenerDownloads: number;
    localDsMs: number;
    containMs: number;
}

describe('delete-set fingerprint re-applied on every start / fold delivery', () => {
    it('measures the fingerprint re-apply against a fully synced client', async () => {
        const quiet = [vi.spyOn(console, 'log').mockImplementation(() => { }),
            vi.spyOn(console, 'warn').mockImplementation(() => { })];

        const sim = createSim({ seed: SEED });
        const live = new Y.Doc();
        const rows: Row[] = [];
        let version = 1;

        for (let s = 0; s < SAMPLE_AT[SAMPLE_AT.length - 1]; s++) {
            live.clientID = clientIdForSession(SEED, s); // fresh client per session
            runSession(sim, live);
            if (!SAMPLE_AT.includes(s + 1)) continue;

            // ---- The fold compaction would write now ----
            const snapshot = Y.encodeStateAsUpdate(live);
            const svBytes = Y.encodeStateVector(live);
            // merge-core's GC path: fingerprint = structs-empty DS update
            const fingerprint = Y.encodeStateAsUpdate(live, svBytes);
            const fpDs = Y.decodeUpdate(fingerprint).ds;
            const fpRanges = dsRangeCount(fpDs);
            version++;
            server.storage.clear();
            server.storage.set(`snapshots/v${version}`, snapshot);
            server.main = {
                version,
                epoch: 0,
                stateVector: toBase64(svBytes),
                snapshotStoragePath: `snapshots/v${version}`,
                deleteSet: inlineBytes(fingerprint),
            };

            // ---- A fully synced client (e.g. hydrated from y-idb) ----
            const synced = new Y.Doc();
            Y.applyUpdate(synced, snapshot);
            const stats = docStructStats(synced);
            let updateEvents = 0;
            synced.on('update', () => { updateEvents++; });

            // ---- 1. Warm start: real performInitialSync ----
            const bootMs: number[] = [];
            const bootFpMs: number[] = [];
            let bootFpApplies = 0;
            let bootRanges = 0;
            let bootDsBuilds = 0;
            await performInitialSync(ctxFor(synced)); // warm-up (JIT), unmeasured
            server.downloads = 0;
            server.added = 0;
            for (let r = 0; r < RUNS; r++) {
                probe.calls = [];
                probe.dsBuilds = 0;
                probe.armed = true;
                const t0 = performance.now();
                const res = await performInitialSync(ctxFor(synced));
                bootMs.push(performance.now() - t0);
                probe.armed = false;
                expect(res.success).toBe(true);
                const fpCalls = probe.calls.filter(c => c.update.byteLength === fingerprint.byteLength);
                bootFpMs.push(fpCalls.reduce((a, c) => a + c.ms, 0));
                bootFpApplies = fpCalls.length;
                bootRanges = fpCalls.reduce((a, c) => a + dsRangeCount(Y.decodeUpdate(c.update).ds), 0);
                bootDsBuilds = probe.dsBuilds;
            }
            expect(server.downloads).toBe(0); // snapshot covered: never downloaded
            const bootPushes = server.added;

            // ---- 1b. Fresh client (context): the fingerprint is applied
            // right after the snapshot that already carries the same DS ----
            const freshMs: number[] = [];
            const freshFpMs: number[] = [];
            for (let r = 0; r < FRESH_RUNS; r++) {
                const fresh = new Y.Doc();
                probe.calls = [];
                probe.armed = true;
                const t0 = performance.now();
                const res = await performInitialSync(ctxFor(fresh));
                freshMs.push(performance.now() - t0);
                probe.armed = false;
                expect(res.success).toBe(true);
                // the snapshot, then the fingerprint unless proven redundant
                expect(probe.calls[0].update.byteLength).toBe(snapshot.byteLength);
                expect(probe.calls.length).toBeLessThanOrEqual(2);
                freshFpMs.push(probe.calls
                    .filter(c => c.update.byteLength === fingerprint.byteLength)
                    .reduce((a, c) => a + c.ms, 0));
                fresh.destroy();
            }

            // ---- 2. Snapshot listener: a NEW fold of data already held ----
            server.snapshotListeners = [];
            const unsub = createSnapshotListener(ctxFor(synced), version);
            expect(server.snapshotListeners.length).toBe(1);
            const listenerMs: number[] = [];
            const deliveryMs: number[] = [];
            let listenerApplies = 0;
            let listenerDsBuilds = 0;
            // warm-up delivery (JIT), unmeasured
            await server.snapshotListeners[0]({ exists: () => true, data: () => ({ ...server.main!, version: ++version }) });
            server.downloads = 0;
            probe.dsBuilds = 0;
            for (let r = 0; r < RUNS; r++) {
                const next = { ...server.main!, version: version + 1 + r };
                probe.calls = [];
                probe.armed = true;
                const t0 = performance.now();
                await server.snapshotListeners[0]({ exists: () => true, data: () => ({ ...next }) });
                deliveryMs.push(performance.now() - t0);
                probe.armed = false;
                listenerMs.push(probe.calls.reduce((a, c) => a + c.ms, 0));
                listenerApplies += probe.calls.length;
            }
            listenerDsBuilds = probe.dsBuilds;
            probe.dsBuilds = 0;
            unsub();
            version += RUNS;

            // ---- Reference: cost of the containment test a fix needs ----
            const localDsMs: number[] = [];
            const containMs: number[] = [];
            for (let r = -1; r < RUNS; r++) { // r = -1: warm-up
                const t0 = performance.now();
                const localDs = Y.createDeleteSetFromStructStore((synced as any).store);
                const t1 = performance.now();
                const contained = dsContained(Y.decodeUpdate(fingerprint).ds, localDs);
                const t2 = performance.now();
                expect(contained).toBe(true);
                if (r < 0) continue;
                localDsMs.push(t1 - t0);
                containMs.push(t2 - t1);
            }

            expect(updateEvents).toBe(0); // every re-apply was a no-op
            rows.push({
                sessions: s + 1,
                events: sim.totalEvents,
                snapshotBytes: snapshot.byteLength,
                fpBytes: fingerprint.byteLength,
                fpRanges,
                structsWalked: stats.deletedItems + stats.gcStructs,
                bootMs: median(bootMs),
                bootFpApplyMs: median(bootFpMs),
                bootFpApplies,
                bootRangesReapplied: bootRanges,
                bootDsBuilds,
                bootUpdateEvents: updateEvents,
                bootPushes,
                freshBootMs: median(freshMs),
                freshFpApplyMs: median(freshFpMs),
                listenerFpApplyMs: median(listenerMs),
                listenerDeliveryMs: median(deliveryMs),
                listenerFpApplies: listenerApplies / RUNS,
                listenerDsBuilds: listenerDsBuilds / RUNS,
                listenerDownloads: server.downloads,
                localDsMs: median(localDsMs),
                containMs: median(containMs),
            });
            synced.destroy();
        }
        quiet.forEach(q => q.mockRestore());

        console.log('\n=== Delete-set fingerprint re-applied to a fully synced client ===');
        console.log(`medians of ${RUNS} runs; "dead" = deleted items + GC structs in the doc (what the DS walk visits)`);
        console.log('sessions | events | snapshot | fingerprint (ranges) | dead structs | warm boot | fp apply in warm boot (calls, ranges) | localDs builds/boot | pushes | fresh boot | fp apply in fresh boot | listener delivery/fold | listener fp apply/fold (calls, localDs builds) | localDs | containment');
        for (const r of rows) {
            console.log([
                r.sessions,
                r.events,
                fmtBytes(r.snapshotBytes),
                `${fmtBytes(r.fpBytes)} (${r.fpRanges})`,
                r.structsWalked,
                fmtMs(r.bootMs),
                `${fmtMs(r.bootFpApplyMs)} (${r.bootFpApplies}, ${r.bootRangesReapplied})`,
                r.bootDsBuilds,
                r.bootPushes,
                fmtMs(r.freshBootMs),
                fmtMs(r.freshFpApplyMs),
                fmtMs(r.listenerDeliveryMs),
                `${fmtMs(r.listenerFpApplyMs)} (${r.listenerFpApplies}, ${r.listenerDsBuilds})`,
                fmtMs(r.localDsMs),
                fmtMs(r.containMs),
            ].join(' | '));
        }
        const first = rows[0], last = rows[rows.length - 1];
        console.log(`\nfp apply in boot: ${fmtMs(first.bootFpApplyMs)} -> ${fmtMs(last.bootFpApplyMs)} ` +
            `(${first.bootFpApplyMs > 0 ? (last.bootFpApplyMs / first.bootFpApplyMs).toFixed(1) : '-'}x for ${(last.events / first.events).toFixed(1)}x events), ` +
            `${(100 * last.bootFpApplyMs / last.bootMs).toFixed(0)}% of the warm boot at ${last.events} events`);
        console.log(`warm boot: ${fmtMs(first.bootMs)} -> ${fmtMs(last.bootMs)}; ` +
            `listener delivery/fold: ${fmtMs(first.listenerDeliveryMs)} -> ${fmtMs(last.listenerDeliveryMs)}`);

        // Sanity pins (the numbers above are the point): nothing changed,
        // and the push guard still proves coverage (no push).
        for (const r of rows) {
            expect(r.bootUpdateEvents).toBe(0);
            expect(r.bootPushes).toBe(0);
            expect(r.listenerDownloads).toBe(0);
        }
        live.destroy();
    }, 600_000);
});
