/**
 * Performance regression + benchmark: late lock winners in multi-client
 * automatic compaction.
 *
 * Every online client runs the compaction trigger on its own update
 * listener (sync.ts createUpdateListener + shouldTriggerCompaction), so
 * when the updates backlog crosses `maxUpdatesThreshold` ALL online
 * clients call compact() on the same delivery. Their lock transactions
 * race. The loser's transaction is retried by the Firestore SDK after its
 * backoff (~0.5-1.5 s); when the winner's compaction is shorter than that,
 * the retry finds the lock FREE again and the loser compacts whatever has
 * arrived since. (Natural, ungated emulator runs: writer pausing after
 * the crossing, separate Firestore clients — 2 of 12 crossings, peer's
 * lock acquired 562 / 959 ms after its trigger, each time a fold of
 * base + history with 0 updates; writer typing on, threshold 50 — 5 of 9
 * crossings, 8-17 leftovers each.)
 *
 * Once it holds the lock, compact() has no minimum batch:
 *
 * - NOTHING left (the writer paused after the crossing — the usual shape
 *   for a reading app): shouldUseDelta() requires updateCount > 0, so the
 *   late winner FOLDS base + history into a new snapshot. That is an
 *   O(snapshot) Storage download + upload + merge, a new snapshot version
 *   delivered to every client, for zero new updates — long before
 *   historyFoldThreshold is reached.
 * - A FEW leftovers: it delta-compacts them into a tiny history segment
 *   (lock ops, history + main reads, a segment write) that uses up one of
 *   the historyFoldThreshold slots, so the O(snapshot) fold comes sooner.
 *
 * The race outcome is made deterministic here: the peer's lock
 * acquisition is held (a gate around acquireLock) until the writer's
 * compaction has committed — exactly the interleaving the SDK's
 * transaction retry produces when it lands after the winner's release.
 * Each device has its own Firebase app (own Firestore client and cache),
 * so the peer sees the writer's updates only through the server.
 *
 * Costs are counted, never timed: Firestore reads / writes / deletes and
 * transactions issued by each compaction (attributed through
 * AsyncLocalStorage), Cloud Storage bytes moved, segments and folds
 * written, and listener deliveries.
 *
 * Run just the regression tests / just the benchmark (through the
 * emulator isolation wrapper on shared machines):
 *   bash scripts/test.sh tests/integration/multi_client_mini_compactions.test.ts -t regression
 *   bash scripts/test.sh tests/integration/multi_client_mini_compactions.test.ts -t bench
 *
 * @file multi_client_mini_compactions.test.ts
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

interface CompactionRecord {
    uid: string;
    round: string;
    reads: number;
    writes: number;
    deletes: number;
    transactions: number;
    storageDown: number;
    storageUp: number;
    /** Byte size of each history segment this compaction wrote */
    segmentBytes: number[];
    lockAcquired: boolean;
    result?: { type?: string; updatesCompacted: number; historySegmentsMerged: number; success: boolean };
}

const instr = await vi.hoisted(async () => {
    const { AsyncLocalStorage } = await import('node:async_hooks');
    return {
        /** Attributes Firestore / Storage operations to the compaction that issued them */
        als: new AsyncLocalStorage<any>(),
        records: [] as any[],
        /** Tag stamped on each compaction record */
        round: '',
        /** Overrides DEFAULTS.COMPACTION_TRIGGER_COOLDOWN_MS when set */
        cooldownMs: null as number | null,
        /** Runs before acquireLock (used to order the lock race) */
        beforeAcquire: null as null | ((uid: string) => Promise<void>),
        /** Runs after acquireLock with its outcome */
        afterAcquire: null as null | ((uid: string, ok: boolean) => void),
        /** Listener deliveries and listener-side Storage downloads (all clients) */
        listener: { historyDocs: 0, updateDocs: 0, mainDoc: 0, storageDown: 0 },
    };
});

// The trigger cooldown is read on every delivery; the benchmark shortens it
// so consecutive rounds need not wait 10 s each. Regression tests keep the
// real value.
vi.mock('../../src/types', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    const defaults = { ...actual.DEFAULTS };
    const realCooldown = actual.DEFAULTS.COMPACTION_TRIGGER_COOLDOWN_MS;
    Object.defineProperty(defaults, 'COMPACTION_TRIGGER_COOLDOWN_MS', {
        get: () => instr.cooldownMs ?? realCooldown,
        enumerable: true,
    });
    return { ...actual, DEFAULTS: defaults };
});

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getBytes: async (r: any, ...rest: any[]) => {
            const rec = instr.als.getStore();
            const buf: ArrayBuffer = await actual.getBytes(r, ...rest);
            if (rec) rec.storageDown += buf.byteLength;
            else instr.listener.storageDown += buf.byteLength;
            return buf;
        },
        uploadBytes: async (r: any, data: Uint8Array, ...rest: any[]) => {
            const rec = instr.als.getStore();
            if (rec) rec.storageUp += data.byteLength;
            return actual.uploadBytes(r, data, ...rest);
        },
    };
});

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    // Billing model: one read per document returned (an empty query result
    // still bills one), one per transaction.get / getDoc.
    const wrapTransaction = (tx: any, rec: any) => {
        const proxy: any = {
            get: async (ref: any) => {
                if (rec) rec.reads += 1;
                return tx.get(ref);
            },
            set: (ref: any, data: any, options?: any) => {
                if (rec) {
                    rec.writes += 1;
                    if (String(ref.path).includes('/history/') && data?.segment) {
                        rec.segmentBytes.push(data.segment.toUint8Array().byteLength);
                    }
                }
                if (options === undefined) tx.set(ref, data);
                else tx.set(ref, data, options);
                return proxy;
            },
            update: (...args: any[]) => {
                if (rec) rec.writes += 1;
                tx.update(...args);
                return proxy;
            },
            delete: (ref: any) => {
                if (rec) rec.deletes += 1;
                tx.delete(ref);
                return proxy;
            },
        };
        return proxy;
    };
    const countDelivery = (snapshot: any) => {
        if (typeof snapshot?.docChanges === 'function') {
            for (const change of snapshot.docChanges()) {
                if (change.type === 'removed') continue;
                const p = String(change.doc.ref.path);
                if (p.includes('/history/')) instr.listener.historyDocs += 1;
                else if (p.includes('/updates/')) instr.listener.updateDocs += 1;
            }
        } else if (snapshot?.exists?.()) {
            instr.listener.mainDoc += 1;
        }
    };
    return {
        ...actual,
        getDocs: async (q: any) => {
            const rec = instr.als.getStore();
            const snap = await actual.getDocs(q);
            if (rec) rec.reads += Math.max(1, snap.size);
            return snap;
        },
        getDoc: async (r: any) => {
            const rec = instr.als.getStore();
            const snap = await actual.getDoc(r);
            if (rec) rec.reads += 1;
            return snap;
        },
        runTransaction: (db: any, updateFunction: any, options?: any) => {
            const rec = instr.als.getStore();
            if (rec) rec.transactions += 1;
            return actual.runTransaction(db, (tx: any) => updateFunction(wrapTransaction(tx, rec)), options);
        },
        onSnapshot: (target: any, ...args: any[]) => {
            const idx = args.findIndex((arg) => typeof arg === 'function');
            if (idx >= 0) {
                const next = args[idx];
                args[idx] = (snapshot: any) => {
                    countDelivery(snapshot);
                    return next(snapshot);
                };
            }
            return actual.onSnapshot(target, ...args);
        },
    };
});

vi.mock('../../src/locking', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        acquireLock: async (config: any) => {
            if (instr.beforeAcquire) await instr.beforeAcquire(config.uid);
            const ok = await actual.acquireLock(config);
            const rec = instr.als.getStore();
            if (rec) rec.lockAcquired = ok;
            instr.afterAcquire?.(config.uid, ok);
            return ok;
        },
    };
});

vi.mock('../../src/compaction', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        compact: async (ctx: any, attempt?: number) => {
            const rec: CompactionRecord = {
                uid: ctx.uid,
                round: instr.round,
                reads: 0,
                writes: 0,
                deletes: 0,
                transactions: 0,
                storageDown: 0,
                storageUp: 0,
                segmentBytes: [],
                lockAcquired: false,
            };
            const result = await instr.als.run(rec, () => actual.compact(ctx, attempt));
            rec.result = result;
            instr.records.push(rec);
            return result;
        },
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { initializeApp, getApps, FirebaseApp } from 'firebase/app';
import { getFirestore, connectFirestoreEmulator, collection, getDocs, getDoc, doc } from 'firebase/firestore';
import { getStorage, connectStorageEmulator } from 'firebase/storage';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** maxUpdatesThreshold for every provider in this file */
const THRESHOLD = 10;
/** The minimum a deferred automatic compaction should wait for (issue proposal: threshold / 2) */
const MIN_AUTOMATIC_BATCH = THRESHOLD / 2;
/** Live text in the base snapshot, so a fold's O(snapshot) transfer is visible */
const BASE_TEXT_CHARS = 50_000;
/** How long the writer's lock waits for the peer's trigger before giving up on it */
const PEER_TRIGGER_WAIT_MS = 3000;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** A Firebase app of its own per device: own Firestore client, own cache. */
function deviceApp(name: string): FirebaseApp {
    const existing = getApps().find((app) => app.name === name);
    if (existing) return existing;
    const app = initializeApp({
        projectId: 'demo-test-project',
        apiKey: 'fake-api-key',
        storageBucket: 'demo-test-project.appspot.com',
    }, name);
    connectFirestoreEmulator(getFirestore(app), '127.0.0.1', 8080);
    connectStorageEmulator(getStorage(app), '127.0.0.1', 9199);
    return app;
}

interface Device {
    provider: FireProvider;
    ydoc: Y.Doc;
}

interface Env {
    path: string;
    writer: Device;
    peer: Device;
    editCount: number;
    lastCrossingAt: number;
}

interface RoundMetrics {
    round: string;
    /** compact() calls (lock attempts) */
    compactions: number;
    /** Compactions that won the lock */
    lockWins: number;
    folds: number;
    segments: number;
    /** Segments made from fewer than MIN_AUTOMATIC_BATCH updates */
    tinySegments: number;
    reads: number;
    writes: number;
    deletes: number;
    transactions: number;
    storageBytes: number;
    /** What the peer's (late) lock-winning compaction did */
    peer: string;
    peerReads: number;
    peerWrites: number;
    peerDeletes: number;
    peerStorageBytes: number;
    peerSegmentBytes: string;
    listenerHistoryDocs: number;
    listenerMainDoc: number;
    listenerStorageDown: number;
}

describe('multi-client automatic compaction: late lock winners', () => {
    const live: FireProvider[] = [];
    let counter = 0;
    let db: any;
    let mainApp: FirebaseApp;

    beforeEach(async () => {
        const setup = await setupEmulator();
        db = setup.db;
        mainApp = setup.app;
        instr.records.length = 0;
        instr.round = '';
        instr.cooldownMs = null;
        instr.beforeAcquire = null;
        instr.afterAcquire = null;
    });

    afterEach(async () => {
        instr.beforeAcquire = null;
        instr.afterAcquire = null;
        instr.cooldownMs = null;
        while (live.length > 0) {
            await live.pop()!.destroy();
        }
    });

    function open(app: FirebaseApp, path: string, ydoc: Y.Doc): FireProvider {
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: THRESHOLD,
            maxWaitTime: 20,
            // Skip the clock-skew probe (3 ops per provider, not under test).
            cachedClockOffset: 0,
        });
        live.push(provider);
        return provider;
    }

    /** One edit on the writer, resolved once its update document is committed. */
    async function writerEdit(env: Env): Promise<void> {
        const { provider, ydoc } = env.writer;
        const saved = new Promise<void>((resolve) => {
            const onSaved = () => {
                (provider as any).off('saved', onSaved);
                resolve();
            };
            (provider as any).on('saved', onSaved);
        });
        const i = env.editCount++;
        ydoc.getMap('m').set(`k${i % 40}`, `value-${i}`);
        await saved;
    }

    /**
     * Two devices online on one document with a base snapshot already in
     * Storage (the first compaction of a document always folds).
     */
    async function setupTwoDevices(name: string): Promise<Env> {
        const path = `tests/multi-client-mini-${name}-${getStableDate()}-${Date.now()}-${counter++}`;
        const writerDoc = new Y.Doc();
        const peerDoc = new Y.Doc();
        const writer = open(deviceApp('device-writer'), path, writerDoc);
        const peer = open(deviceApp('device-peer'), path, peerDoc);
        await waitForConditionTruthy(() => writer.synced && peer.synced, { timeout: 30000, message: 'both devices sync' });

        const env: Env = { path, writer: { provider: writer, ydoc: writerDoc }, peer: { provider: peer, ydoc: peerDoc }, editCount: 0, lastCrossingAt: 0 };

        instr.round = `${name}:setup`;
        const saved = new Promise<void>((resolve) => {
            const onSaved = () => {
                (writer as any).off('saved', onSaved);
                resolve();
            };
            (writer as any).on('saved', onSaved);
        });
        writerDoc.getText('t').insert(0, 'x'.repeat(BASE_TEXT_CHARS));
        await saved;
        await writer.compact();
        const main = await getDoc(doc(db, path));
        expect(main.data()?.snapshotStoragePath).toBeTruthy();
        await waitForConditionTruthy(() => peerDoc.getText('t').length === BASE_TEXT_CHARS, { timeout: 20000, message: 'peer has the base content' });
        return env;
    }

    /**
     * Drives the writer across the threshold with both devices online and
     * orders the lock race:
     * - 'busy': the peer's lock attempt lands while the writer holds the
     *   lock (the intended outcome: the peer backs off with 'none');
     * - 'late': the peer's attempt lands after the writer's compaction has
     *   committed and `leftovers` more updates have been written (what the
     *   SDK's transaction retry produces when it outlasts the winner).
     */
    async function runRound(env: Env, tag: string, peerTiming: 'busy' | 'late', leftovers: number): Promise<RoundMetrics> {
        const writerUid = env.writer.provider.uid;
        const peerUid = env.peer.provider.uid;

        // The trigger cooldown is per listener: start the round only once
        // both listeners may trigger again.
        const cooldown = instr.cooldownMs ?? 10_000;
        const ready = env.lastCrossingAt + cooldown + 250;
        if (env.lastCrossingAt > 0 && Date.now() < ready) await sleep(ready - Date.now());

        const listenerBefore = { ...instr.listener };
        const firstRecord = instr.records.length;
        let peerEntered = false;
        let writerEntered = false;
        let signalPeerEntered!: () => void;
        const peerEnteredP = new Promise<void>((resolve) => { signalPeerEntered = resolve; });
        let openPeer!: () => void;
        const peerGate = new Promise<void>((resolve) => { openPeer = resolve; });
        let signalWriterLocked!: () => void;
        const writerLocked = new Promise<void>((resolve) => { signalWriterLocked = resolve; });

        instr.round = tag;
        instr.beforeAcquire = async (uid) => {
            if (uid === peerUid) {
                peerEntered = true;
                signalPeerEntered();
                await peerGate;
            } else if (uid === writerUid) {
                writerEntered = true;
                // Both devices trigger on the same crossing: let the peer's
                // listener see it before the writer's compaction removes it.
                await Promise.race([peerEnteredP, sleep(PEER_TRIGGER_WAIT_MS)]);
            }
        };
        instr.afterAcquire = (uid, ok) => {
            if (uid === writerUid && ok) signalWriterLocked();
        };

        try {
            // 1. Writer edits until its own listener triggers a compaction.
            for (let i = 0; i < THRESHOLD + 5 && !writerEntered; i++) {
                await writerEdit(env);
            }
            await waitForConditionTruthy(() => writerEntered, { timeout: 5000, message: `${tag}: writer triggers compaction` });
            env.lastCrossingAt = Date.now();

            if (peerTiming === 'busy') {
                await writerLocked;
                openPeer();
            }

            // 2. The writer's compaction commits and releases the lock.
            await waitForConditionTruthy(
                () => instr.records.slice(firstRecord).some((r: CompactionRecord) => r.uid === writerUid),
                { timeout: 30000, message: `${tag}: writer compaction finishes` },
            );

            // 3. Updates that arrive before the peer's retry lands.
            for (let i = 0; i < leftovers; i++) {
                await writerEdit(env);
            }

            // 4. The peer's lock attempt proceeds.
            openPeer();
            if (peerEntered) {
                await waitForConditionTruthy(
                    () => instr.records.slice(firstRecord).some((r: CompactionRecord) => r.uid === peerUid),
                    { timeout: 30000, message: `${tag}: peer compaction finishes` },
                );
            }
            await waitForConditionTruthy(
                () => !env.writer.provider.isCompacting && !env.peer.provider.isCompacting,
                { timeout: 30000, message: `${tag}: compactions settle` },
            );
        } finally {
            openPeer();
            instr.beforeAcquire = null;
            instr.afterAcquire = null;
        }

        // Peer converges with the writer (data integrity under the race).
        await waitForConditionTruthy(
            () => JSON.stringify(env.peer.ydoc.getMap('m').toJSON()) === JSON.stringify(env.writer.ydoc.getMap('m').toJSON()),
            { timeout: 20000, message: `${tag}: peer converges` },
        );

        const recs: CompactionRecord[] = instr.records.slice(firstRecord);
        const won = recs.filter((r) => r.lockAcquired);
        const peerWins = won.filter((r) => r.uid === peerUid);
        const sum = (list: CompactionRecord[], f: (r: CompactionRecord) => number) => list.reduce((s, r) => s + f(r), 0);
        return {
            round: tag,
            compactions: recs.length,
            lockWins: won.length,
            folds: won.filter((r) => r.result?.type === 'snapshot').length,
            segments: sum(won, (r) => r.segmentBytes.length),
            tinySegments: sum(won.filter((r) => (r.result?.updatesCompacted ?? 0) < MIN_AUTOMATIC_BATCH), (r) => r.segmentBytes.length),
            reads: sum(recs, (r) => r.reads),
            writes: sum(recs, (r) => r.writes),
            deletes: sum(recs, (r) => r.deletes),
            transactions: sum(recs, (r) => r.transactions),
            storageBytes: sum(recs, (r) => r.storageDown + r.storageUp),
            peer: peerWins.map((r) => `${r.result?.type}(u=${r.result?.updatesCompacted},h=${r.result?.historySegmentsMerged})`).join(' ') || 'none',
            peerReads: sum(peerWins, (r) => r.reads),
            peerWrites: sum(peerWins, (r) => r.writes),
            peerDeletes: sum(peerWins, (r) => r.deletes),
            peerStorageBytes: sum(peerWins, (r) => r.storageDown + r.storageUp),
            peerSegmentBytes: peerWins.flatMap((r) => r.segmentBytes).join(','),
            listenerHistoryDocs: instr.listener.historyDocs - listenerBefore.historyDocs,
            listenerMainDoc: instr.listener.mainDoc - listenerBefore.mainDoc,
            listenerStorageDown: instr.listener.storageDown - listenerBefore.storageDown,
        };
    }

    async function serverCounts(path: string): Promise<{ updates: number; history: number; version: number }> {
        const [updates, history, main] = await Promise.all([
            getDocs(collection(db, path, 'updates')),
            getDocs(collection(db, path, 'history')),
            getDoc(doc(db, path)),
        ]);
        return { updates: updates.size, history: history.size, version: main.data()?.version ?? 0 };
    }

    /** A brand-new device sees everything the writer wrote. */
    async function expectFreshClientConverges(env: Env): Promise<void> {
        const freshDoc = new Y.Doc();
        const fresh = open(mainApp, env.path, freshDoc);
        await waitForConditionTruthy(() => fresh.synced, { timeout: 30000, message: 'fresh client syncs' });
        await waitForConditionTruthy(
            () => JSON.stringify(freshDoc.getMap('m').toJSON()) === JSON.stringify(env.writer.ydoc.getMap('m').toJSON()),
            { timeout: 20000, message: 'fresh client converges' },
        );
        expect(freshDoc.getText('t').length).toBe(BASE_TEXT_CHARS);
    }

    describe('regression', () => {
        it('a late lock winner with nothing left to compact does not fold the history into a new snapshot', { timeout: 120000 }, async () => {
            const env = await setupTwoDevices('late-empty');
            const before = await serverCounts(env.path);
            expect(before.history).toBe(0);

            const m = await runRound(env, 'late-empty', 'late', 0);
            const after = await serverCounts(env.path);
            console.log('[late lock winner, 0 leftovers]', JSON.stringify(m), 'server', JSON.stringify(after));

            await expectFreshClientConverges(env);

            // The crossing was compacted by the writer into one delta segment;
            // history then held 1 segment against a fold threshold of 8, so no
            // fold was due and the peer had nothing new to compact. A fold here
            // is pure O(snapshot) overhead: Storage download + upload + merge
            // and a snapshot delivery to every client, for zero updates.
            expect(m.folds, 'folds run by the round\'s automatic compactions').toBe(0);
            expect(m.storageBytes, 'Cloud Storage bytes moved by the round\'s automatic compactions').toBe(0);
            expect(after.version, 'snapshot version after the round').toBe(before.version);
            expect(after.history, 'history segments after the round').toBe(1);
        });

        it('a late lock winner does not compact a few leftover updates into their own history segment', { timeout: 120000 }, async () => {
            const env = await setupTwoDevices('late-few');
            const leftovers = 3;

            const m = await runRound(env, 'late-few', 'late', leftovers);
            const after = await serverCounts(env.path);
            console.log(`[late lock winner, ${leftovers} leftovers]`, JSON.stringify(m), 'server', JSON.stringify(after));

            await expectFreshClientConverges(env);

            // One threshold crossing should cost one history segment. A second,
            // tiny segment uses up one of the historyFoldThreshold slots (the
            // O(snapshot) fold arrives sooner) and costs lock ops, history +
            // main reads and a segment write for a handful of updates; the
            // leftovers can wait for the next regular compaction.
            expect(m.tinySegments, `segments built from fewer than ${MIN_AUTOMATIC_BATCH} updates`).toBe(0);
            expect(m.segments, 'history segments written for one threshold crossing').toBe(1);
            expect(after.history, 'history segments after the round').toBe(1);
            expect(after.updates, 'leftover update documents waiting for the next compaction').toBe(leftovers);
        });
    });

    describe('bench: per-crossing compaction cost with two devices online', () => {
        const ROUNDS = 4;
        const scenarios: Array<{ name: string; peerTiming: 'busy' | 'late'; leftovers: number }> = [
            // Intended outcome: the peer's lock attempt lands while the writer holds the lock.
            { name: 'peer-busy', peerTiming: 'busy', leftovers: 0 },
            // The peer's retried lock attempt lands after the writer's release.
            { name: 'late-0-leftovers', peerTiming: 'late', leftovers: 0 },
            { name: 'late-3-leftovers', peerTiming: 'late', leftovers: 3 },
        ];

        for (const scenario of scenarios) {
            it(`${scenario.name}: ${ROUNDS} threshold crossings`, { timeout: 180000 }, async () => {
                instr.cooldownMs = 1500;
                const env = await setupTwoDevices(scenario.name);
                const rows: RoundMetrics[] = [];
                for (let r = 0; r < ROUNDS; r++) {
                    rows.push(await runRound(env, `${scenario.name}#${r + 1}`, scenario.peerTiming, scenario.leftovers));
                }
                const total = (key: keyof RoundMetrics) => rows.reduce((s, row) => s + (row[key] as number), 0);
                const after = await serverCounts(env.path);
                console.table(rows.map(({ round, compactions, lockWins, folds, segments, tinySegments, reads, writes, deletes, transactions, storageBytes, peer, listenerHistoryDocs, listenerMainDoc }) => ({
                    round, compactions, lockWins, folds, segments, tinySegments, reads, writes, deletes, transactions, storageBytes, peer, listenerHistoryDocs, listenerMainDoc,
                })));
                console.log(`[bench ${scenario.name}] totals`, JSON.stringify({
                    rounds: ROUNDS,
                    lockWins: total('lockWins'),
                    folds: total('folds'),
                    segments: total('segments'),
                    tinySegments: total('tinySegments'),
                    reads: total('reads'),
                    writes: total('writes'),
                    deletes: total('deletes'),
                    transactions: total('transactions'),
                    storageBytes: total('storageBytes'),
                    peerReads: total('peerReads'),
                    peerWrites: total('peerWrites'),
                    peerDeletes: total('peerDeletes'),
                    peerStorageBytes: total('peerStorageBytes'),
                    listenerHistoryDocs: total('listenerHistoryDocs'),
                    listenerMainDoc: total('listenerMainDoc'),
                    listenerStorageDown: total('listenerStorageDown'),
                    // historyFoldThreshold slots used per threshold crossing
                    // (folds reset the count; 1.0 is the single-writer cadence)
                    segmentsPerCrossing: total('segments') / ROUNDS,
                    foldsPerCrossing: total('folds') / ROUNDS,
                    server: after,
                }));
                await expectFreshClientConverges(env);
            });
        }
    });
});
