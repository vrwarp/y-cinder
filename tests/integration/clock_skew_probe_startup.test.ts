/**
 * Clock-skew probe on the startup critical path: Firestore op counts and
 * serial round trips
 *
 * `FireProvider.sync()` awaits `measureClockSkew` — `setDoc` of a
 * `maintenance/skew_*` document (resolves on the server ack), `getDoc` of
 * it, then a fire-and-forget `deleteDoc` — BEFORE `performInitialSync`
 * issues its first read. Initial sync never uses the offset: it only feeds
 * `acquireLock` (compaction, squash), which a short session may never run.
 *
 * Costs measured here:
 *  - 2 serial round trips added to construction → `synced` on every launch;
 *  - 3 billed ops (2 writes + 1 read) per root provider: skew is a property
 *    of the client, yet every root provider on the same Firestore instance
 *    measures it again (versicle mirrors 4 stores into 4 root docs);
 *  - 3 more per subdoc that already exists when its parent is constructed:
 *    the constructor runs `handleSubdocs` before `sync()` has measured, so
 *    those children get `cachedClockOffset: undefined` and probe themselves.
 *    Only subdocs added after the parent's probe inherit its offset.
 *
 * The SDK entry points y-cinder uses (`setDoc`, `getDoc`, `getDocs`,
 * `deleteDoc`, `addDoc`, `onSnapshot`, `runTransaction` / `transaction.get`)
 * are wrapped at the module boundary. Every call is logged with its target
 * path and a global sequence number at issue and at completion, so "ops that
 * COMPLETED before the provider's first initial-sync read was ISSUED" is a
 * deterministic count of the serial round trips ahead of initial sync.
 * Probe ops are those that target a `maintenance/skew_*` document.
 * Billing model: setDoc/deleteDoc/addDoc = 1 write, getDoc = 1 read,
 * getDocs = max(1, documents returned) reads; listener reads are reported
 * separately (attachments only).
 *
 * Two parts:
 *  - `bench:` scenarios report the numbers (always pass): op logs, critical
 *    path, billed ops per launch, and construction → synced time with an
 *    injected per-op latency (A/B against a provider given a pre-measured
 *    `cachedClockOffset`, which skips the probe; 5 interleaved runs,
 *    medians). Run just those with `-t bench`.
 *  - `regression:` tests pin the cost with counters: no probe round trip
 *    completes before the first initial-sync read, and one client measures
 *    skew at most once per Firestore instance, across root providers and
 *    pre-existing subdocs. A `guard:` test (passes today) pins what a fix
 *    must keep: lock decisions still use a MEASURED offset, measured once.
 *    Probe reads see a server clock running `ctl.probeSkewMs` ahead, so the
 *    offset handed to `acquireLock` is recognizably the measured one.
 *
 * Run (needs the Firestore + Storage emulator):
 *   bash scripts/test.sh tests/integration/clock_skew_probe_startup.test.ts          # all
 *   bash scripts/test.sh tests/integration/clock_skew_probe_startup.test.ts -t bench # report only
 *
 * @file clock_skew_probe_startup.test.ts
 */

import { describe, it, expect, beforeAll, afterEach, vi } from 'vitest';

const { ctl } = vi.hoisted(() => {
    interface OpRecord {
        kind: string;
        path: string;
        /** global sequence number when the call was issued */
        start: number;
        /** global sequence number when it settled (null while in flight) */
        end: number | null;
        /** documents returned (getDocs only) */
        docs: number;
    }
    return {
        ctl: {
            /** Artificial per-op latency (ms) added before each one-shot op */
            latencyMs: 0,
            seq: 0,
            log: [] as OpRecord[],
            /** Probe setDoc calls since the module loaded (never reset) */
            probesEver: 0,
            /** How far the server clock runs ahead, as probe reads see it */
            probeSkewMs: 5000,
            /** cachedClockOffset of every acquireLock call, by provider path */
            lockOffsets: [] as Array<{ path: string; offset: number | undefined }>,
        },
    };
});

const isProbePath = (p: string) => /\/maintenance\/skew_[^/]+$/.test(p);

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
    const pathOf = (target: any): string =>
        target?._query?.path?.canonicalString?.() ?? target?.path ?? '?';

    /** Logs issue/settle of one one-shot SDK call, after the injected latency. */
    const record = async <T>(kind: string, target: any, run: () => Promise<T>): Promise<T> => {
        const rec = { kind, path: pathOf(target), start: ctl.seq++, end: null as number | null, docs: 0 };
        ctl.log.push(rec);
        if (kind === 'setDoc' && isProbePath(rec.path)) ctl.probesEver++;
        try {
            if (ctl.latencyMs > 0) await sleep(ctl.latencyMs);
            const out: any = await run();
            if (kind === 'getDocs') rec.docs = out?.size ?? 0;
            return out;
        } finally {
            rec.end = ctl.seq++;
        }
    };

    /** A probe read whose server time runs ctl.probeSkewMs ahead of this machine. */
    const withServerSkew = (snap: any) => {
        const data = snap.data();
        if (!data?.t) return snap;
        const serverMs = data.t.toMillis() + ctl.probeSkewMs;
        return Object.assign(Object.create(snap), { data: () => ({ ...data, t: { toMillis: () => serverMs } }) });
    };

    return {
        ...actual,
        setDoc: (ref: any, ...a: any[]) => record('setDoc', ref, () => actual.setDoc(ref, ...a)),
        getDoc: (ref: any) => record('getDoc', ref, async () => {
            const snap = await actual.getDoc(ref);
            return isProbePath(pathOf(ref)) ? withServerSkew(snap) : snap;
        }),
        getDocs: (q: any) => record('getDocs', q, () => actual.getDocs(q)),
        deleteDoc: (ref: any) => record('deleteDoc', ref, () => actual.deleteDoc(ref)),
        addDoc: (ref: any, data: any) => record('addDoc', ref, () => actual.addDoc(ref, data)),
        onSnapshot: (target: any, ...args: any[]) => {
            const s = ctl.seq++;
            ctl.log.push({ kind: 'onSnapshot', path: pathOf(target), start: s, end: s, docs: 0 });
            return actual.onSnapshot(target, ...args);
        },
        runTransaction: (db: any, updateFn: (tx: any) => Promise<any>, options?: any) =>
            actual.runTransaction(db, (tx: any) => {
                const proxy: any = {
                    get: (ref: any) => record('tx.get', ref, () => tx.get(ref)),
                    set: (...a: any[]) => { tx.set(...a); return proxy; },
                    update: (...a: any[]) => { tx.update(...a); return proxy; },
                    delete: (...a: any[]) => { tx.delete(...a); return proxy; },
                };
                return updateFn(proxy);
            }, options),
    };
});

vi.mock('../../src/locking', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        acquireLock: (config: any) => {
            ctl.lockOffsets.push({ path: config.path, offset: config.cachedClockOffset });
            return actual.acquireLock(config);
        },
    };
});

import * as Y from 'yjs';
import { FireProvider } from '../../src/provider';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

type OpRecord = (typeof ctl.log)[number];

/**
 * The provider path an op belongs to: strips the tier suffix
 * (`/updates`, `/history`, `/maintenance/<id>`, `/metadata/<id>`, ...).
 * Subdoc providers live at `<parent>/subdocs/<guid>` and own their paths.
 */
const ownerOf = (p: string) => p.replace(/\/(updates|history|maintenance|metadata)(\/[^/]+)?$/, '');

const ONE_SHOT = new Set(['setDoc', 'getDoc', 'getDocs', 'deleteDoc', 'addDoc', 'tx.get']);

/** Billing model documented in the header (listeners excluded). */
function billed(ops: OpRecord[]): { reads: number; writes: number } {
    let reads = 0;
    let writes = 0;
    for (const o of ops) {
        if (o.kind === 'getDoc' || o.kind === 'tx.get') reads++;
        else if (o.kind === 'getDocs') reads += Math.max(1, o.docs);
        else if (o.kind === 'setDoc' || o.kind === 'deleteDoc' || o.kind === 'addDoc') writes++;
    }
    return { reads, writes };
}

/** Critical-path analysis of one provider's launch, from the op log. */
function launchProfile(log: OpRecord[], providerPath: string) {
    const own = log.filter(o => ownerOf(o.path) === providerPath && ONE_SHOT.has(o.kind));
    const firstRead = own.find(o =>
        (o.kind === 'getDocs' || o.kind === 'getDoc') && !isProbePath(o.path));
    const before = firstRead
        ? own.filter(o => o.end !== null && o.end < firstRead.start)
        : [];
    return {
        firstRead,
        /** serial round trips completed before initial sync's first read */
        opsBeforeFirstRead: before.length,
        probeOpsBeforeFirstRead: before.filter(o => isProbePath(o.path)).length,
        probeOps: own.filter(o => isProbePath(o.path)).length,
        sequence: own.map(o =>
            `${o.kind}(${o.path.slice(providerPath.length).replace(/skew_[^/]+$/, 'skew_*') || '<main>'})`),
    };
}

const probeOpsUnder = (log: OpRecord[], prefix: string) =>
    log.filter(o => o.path.startsWith(prefix) && isProbePath(o.path));
const probesUnder = (log: OpRecord[], prefix: string) =>
    probeOpsUnder(log, prefix).filter(o => o.kind === 'setDoc').length;

const median = (xs: number[]) => {
    const s = [...xs].sort((a, b) => a - b);
    const m = s.length >> 1;
    return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};

describe('Clock-skew probe on the startup critical path', () => {
    let app: any;
    let counter = 0;
    const providers: FireProvider[] = [];

    const newPath = (tag: string) =>
        `integration-tests/skew-probe-${tag}-${getStableDate()}-${counter++}-${Date.now()}`;

    const track = (p: FireProvider) => { providers.push(p); return p; };

    /** Resolves on the provider's 'sync' event (no polling jitter in timings). */
    const syncedEvent = (p: FireProvider, timeoutMs = 30000) => new Promise<void>((resolve, reject) => {
        if (p.synced) return resolve();
        const timer = setTimeout(() => reject(new Error(`provider ${p.path} did not sync`)), timeoutMs);
        p.on('sync', (s: boolean) => { if (s) { clearTimeout(timer); resolve(); } });
    });

    const newProvider = (path: string, ydoc: Y.Doc, extra: Record<string, any> = {}) =>
        track(new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxWaitTime: 50,
            // keep compaction out of launch measurements
            maxUpdatesThreshold: 1000,
            ...extra,
        }));

    /**
     * Constructs one provider per path at the same instant (an app launch)
     * and returns construction → all-synced time.
     */
    async function launch(paths: string[], extra: Record<string, any> = {}) {
        const t0 = performance.now();
        const ps = paths.map(p => newProvider(p, new Y.Doc(), extra));
        await Promise.all(ps.map(p => syncedEvent(p)));
        return { ms: performance.now() - t0, providers: ps };
    }

    beforeAll(async () => {
        ({ app } = await setupEmulator());
        // Warm the gRPC channel so the first measured launch is not penalized.
        const { providers: ps } = await launch([newPath('warmup')], { cachedClockOffset: 0 });
        await Promise.all(ps.map(p => p.destroy()));
    }, 60000);

    afterEach(async () => {
        ctl.latencyMs = 0;
        await Promise.allSettled(providers.map(p => p.destroy()));
        providers.length = 0;
    });

    // ------------------------------------------------------------------
    // bench: report numbers (always pass)
    // ------------------------------------------------------------------

    it('bench: op log and critical path of one root-provider launch (probe vs pre-measured offset)', async () => {
        const rows: string[] = [];
        for (const [label, extra] of [
            ['default (probe)', {}],
            ['cachedClockOffset: 0', { cachedClockOffset: 0 }],
        ] as const) {
            const path = newPath('oplog');
            ctl.log.length = 0;
            await launch([path], extra);
            const prof = launchProfile(ctl.log, path);
            const ops = ctl.log.filter(o => ownerOf(o.path) === path && ONE_SHOT.has(o.kind));
            const b = billed(ops);
            const listeners = ctl.log.filter(o => o.kind === 'onSnapshot' && ownerOf(o.path) === path).length;
            rows.push(
                `  ${label}:\n` +
                `    ops to synced:   ${prof.sequence.join(', ')}\n` +
                `    serial ops completed before first initial-sync read: ${prof.opsBeforeFirstRead}` +
                ` (probe: ${prof.probeOpsBeforeFirstRead})\n` +
                `    billed one-shot ops: ${b.reads} reads + ${b.writes} writes = ${b.reads + b.writes}` +
                ` (probe: ${prof.probeOps}); listeners attached: ${listeners}`
            );
        }
        console.log(`[bench] clock-skew probe, one root launch on an empty path\n${rows.join('\n')}`);
    }, 60000);

    it('bench: construction → synced with injected per-op latency, 5 interleaved A/B runs', async () => {
        const RUNS = 5;
        const out: string[] = [];
        for (const latency of [100, 40]) {
            for (const roots of [1, 4]) {
                const a: number[] = [];
                const b: number[] = [];
                for (let i = 0; i < RUNS; i++) {
                    // alternate order so drift does not favour one arm
                    const order = i % 2 === 0 ? ['A', 'B'] : ['B', 'A'];
                    for (const arm of order) {
                        const paths = Array.from({ length: roots }, (_, k) => newPath(`lat${latency}-${arm}-${k}`));
                        ctl.latencyMs = latency;
                        const { ms, providers: ps } = await launch(paths, arm === 'A' ? {} : { cachedClockOffset: 0 });
                        ctl.latencyMs = 0;
                        (arm === 'A' ? a : b).push(ms);
                        await Promise.all(ps.map(p => p.destroy()));
                    }
                }
                const ma = median(a);
                const mb = median(b);
                out.push(
                    `  ${latency} ms/op, ${roots} root provider(s): ` +
                    `probe median ${ma.toFixed(0)} ms [${a.map(x => x.toFixed(0)).join(', ')}] vs ` +
                    `pre-measured median ${mb.toFixed(0)} ms [${b.map(x => x.toFixed(0)).join(', ')}] → ` +
                    `+${(ma - mb).toFixed(0)} ms (+${((ma / mb - 1) * 100).toFixed(0)}%)`
                );
            }
        }
        console.log(`[bench] clock-skew probe, time to synced (medians of ${RUNS})\n${out.join('\n')}`);
    }, 240000);

    it('bench: versicle launch (4 root providers) and a parent with 10 pre-existing subdocs', async () => {
        const out: string[] = [];

        // 4 root docs on one Firestore instance (library, progress, annotations, settings)
        for (const [label, extra] of [
            ['default (probe)', {}],
            ['cachedClockOffset: 0', { cachedClockOffset: 0 }],
        ] as const) {
            const base = newPath('versicle');
            const paths = ['library', 'progress', 'annotations', 'settings'].map(s => `${base}-${s}`);
            ctl.log.length = 0;
            await launch(paths, extra);
            const ops = ctl.log.filter(o => o.path.startsWith(base) && ONE_SHOT.has(o.kind));
            const b = billed(ops);
            out.push(
                `  versicle, 4 roots, ${label}: probes ${probesUnder(ctl.log, base)}, ` +
                `probe ops ${probeOpsUnder(ctl.log, base).length}, ` +
                `billed one-shot ops to synced ${b.reads + b.writes} (${b.reads} reads + ${b.writes} writes)`
            );
        }

        // Parent whose 10 subdocs exist before it is constructed (local-first /
        // hydrated) vs the same 10 subdocs added after the parent synced.
        for (const when of ['pre-existing', 'added after parent synced'] as const) {
            const path = newPath('subdocs');
            const parent = new Y.Doc();
            const addSubdocs = () => {
                for (let i = 0; i < 10; i++) {
                    const sub = new Y.Doc();
                    parent.getMap('subs').set(`s${i}`, sub);
                    sub.getText('t').insert(0, `sub ${i}`);
                }
            };
            ctl.log.length = 0;
            if (when === 'pre-existing') addSubdocs();
            const p = newProvider(path, parent);
            await syncedEvent(p);
            if (when !== 'pre-existing') addSubdocs();
            const subs = () => [...((p as any).subProviders as Map<string, FireProvider>).values()];
            await waitForConditionTruthy(() => subs().length === 10 && subs().every(s => s.synced), {
                timeout: 30000, message: 'all 10 subdoc providers synced',
            });
            out.push(
                `  parent + 10 subdocs (${when}): providers 11, probes ${probesUnder(ctl.log, path)}, ` +
                `probe ops ${probeOpsUnder(ctl.log, path).length}`
            );
        }
        console.log(`[bench] clock-skew probe, ops per launch\n${out.join('\n')}`);
    }, 120000);

    // ------------------------------------------------------------------
    // regression: pin the cost (fail on the unfixed code)
    // ------------------------------------------------------------------

    it('regression: no clock-skew probe round trip completes before the first initial-sync read', async () => {
        const path = newPath('critical');
        ctl.log.length = 0;
        await launch([path]);
        const prof = launchProfile(ctl.log, path);
        expect(prof.firstRead, 'initial sync issued a read').toBeDefined();
        // Unfixed: setDoc(skew) is awaited to the server ack, then getDoc(skew),
        // and only then does initial sync read — 2 serial round trips.
        expect(
            prof.probeOpsBeforeFirstRead,
            `probe ops completed before initial sync's first read (sequence: ${prof.sequence.join(', ')})`
        ).toBe(0);
    }, 60000);

    it('regression: four root providers on one Firestore instance measure clock skew at most once', async () => {
        const base = newPath('roots');
        const paths = ['library', 'progress', 'annotations', 'settings'].map(s => `${base}-${s}`);
        ctl.log.length = 0;
        await launch(paths);
        // Skew is a property of the client: one measurement (3 ops) per Firestore
        // instance at most — possibly zero if one is already cached or the
        // measurement is deferred until a lock is needed. Unfixed: 4 probes / 12 ops.
        expect(probesUnder(ctl.log, base), 'clock-skew probes across 4 root providers').toBeLessThanOrEqual(1);
        expect(probeOpsUnder(ctl.log, base).length, 'probe ops across 4 root providers').toBeLessThanOrEqual(3);
    }, 60000);

    it('regression: subdocs present before the parent is constructed do not each run their own probe', async () => {
        const path = newPath('presubs');
        const parent = new Y.Doc();
        for (let i = 0; i < 10; i++) {
            const sub = new Y.Doc();
            parent.getMap('subs').set(`s${i}`, sub);
            sub.getText('t').insert(0, `sub ${i}`);
        }
        ctl.log.length = 0;
        const p = newProvider(path, parent);
        await syncedEvent(p);
        const subs = () => [...((p as any).subProviders as Map<string, FireProvider>).values()];
        await waitForConditionTruthy(() => subs().length === 10 && subs().every(s => s.synced), {
            timeout: 30000, message: 'all 10 subdoc providers synced',
        });
        // Unfixed: the constructor starts the subdoc providers before the
        // parent's sync() has measured, so 1 + 10 probes (33 ops).
        expect(probesUnder(ctl.log, path), 'clock-skew probes for a parent with 10 pre-existing subdocs')
            .toBeLessThanOrEqual(1);
    }, 60000);

    // ------------------------------------------------------------------
    // guard: what a fix must keep (passes on the unfixed code too)
    // ------------------------------------------------------------------

    it('guard: lock decisions use a measured offset, measured once per session (not per lock)', async () => {
        const path = newPath('guard');
        ctl.log.length = 0;
        ctl.lockOffsets.length = 0;
        const p = newProvider(path, new Y.Doc());
        await syncedEvent(p);
        p.doc.getText('t').insert(0, 'x');
        await new Promise<void>(resolve => p.on('saved', () => resolve()));
        await p.compact();
        await p.compact();

        const lockReads = ctl.log.filter(o =>
            o.kind === 'tx.get' && o.path === `${path}/metadata/lock_compaction`).length;
        expect(lockReads, 'both compactions consulted the distributed lock').toBeGreaterThanOrEqual(2);
        // Passing `cachedClockOffset: undefined` into acquireLock re-measures on
        // EVERY call: a deferred/shared measurement must be awaited and passed.
        expect(probesUnder(ctl.log, path), 'probes during a session with two compactions').toBeLessThanOrEqual(1);
        // ...and must actually happen (dropping the probe would make lock
        // expiry use the raw client clock): by the time a lock was taken, this
        // process has measured skew on this Firestore instance at least once.
        expect(ctl.probesEver, 'clock-skew measurements in this process').toBeGreaterThanOrEqual(1);
        // Every lock decision got the measured offset: probe reads see the
        // server clock ctl.probeSkewMs ahead, so neither `undefined` nor an
        // unmeasured 0 passes.
        const offsets = ctl.lockOffsets.filter(l => l.path === path).map(l => l.offset);
        expect(offsets.length, 'acquireLock calls').toBeGreaterThanOrEqual(2);
        for (const offset of offsets) {
            expect(offset, `offset passed to acquireLock (all: ${offsets.join(', ')})`).toBeTypeOf('number');
            expect(Math.abs(offset! - ctl.probeSkewMs), `offset ${offset} vs measured skew ${ctl.probeSkewMs}`)
                .toBeLessThan(1000);
        }
    }, 60000);
});
