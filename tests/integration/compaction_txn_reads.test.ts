/**
 * Compaction transaction re-reads: Firestore read counts
 *
 * Every compaction already holds the pending update documents (and, for a
 * fold, the history segments) from its `getDocs` queries. The commit
 * transactions — delta (`tryDeltaCompaction`), fold
 * (`performCompactionTransaction`) and squash (`squashDocument`) — then
 * call `transaction.get` on EVERY one of those refs again, only to filter
 * on `exists()` before deleting it. Each of those gets is:
 *
 *  - a billed Firestore read returning the document's full payload
 *    (update blobs; history segments can be up to ~1 MB each),
 *  - its own BatchGetDocuments RPC (the web SDK's `Transaction.lookup` is
 *    called with one key per `transaction.get`),
 *  - repeated on every SDK transaction retry.
 *
 * The re-reads protect nothing: the merged candidate is fixed before the
 * transaction (a vanished document is already merged in), deleting a
 * missing document is a no-op, and every deleter of update/history docs
 * checks LOCK_COMPACTION inside its own transaction.
 *
 * The SDK entry points y-cinder uses (`getDocs`, `getDoc`,
 * `runTransaction` and the transaction's `get`/`set`/`delete`) are wrapped
 * at the module boundary and counted while the measured operation runs.
 * Billing model: a query costs one read per returned document (minimum
 * one), `getDoc` and each `transaction.get` cost one read.
 *
 * Two parts:
 *  - `bench:` scenarios report the counts (always pass; numbers are the
 *    deliverable). Run just those with `-t bench`.
 *  - `regression:` tests pin the cost: zero in-transaction reads of the
 *    update/history documents being deleted, and a marginal cost of ONE
 *    read per pending update document (the query read), not two.
 *
 * Run (needs the Firestore + Storage emulator):
 *   bash scripts/test.sh tests/integration/compaction_txn_reads.test.ts          # all
 *   bash scripts/test.sh tests/integration/compaction_txn_reads.test.ts -t bench # report only
 *
 * @file compaction_txn_reads.test.ts
 */

import { describe, it, expect, beforeAll, vi } from 'vitest';

const { ctl } = vi.hoisted(() => {
    const freshCounters = () => ({
        /** getDocs calls */
        queries: 0,
        /** getDocs calls that returned no documents (still billed 1 read) */
        emptyQueries: 0,
        /** documents returned by getDocs */
        queryDocs: 0,
        /** getDoc calls */
        getDoc: 0,
        /** runTransaction calls */
        transactions: 0,
        /** transaction body invocations (> transactions when the SDK retries) */
        txAttempts: 0,
        /** transaction.get calls by target category */
        txGets: { lock: 0, main: 0, update: 0, history: 0, other: 0 } as Record<string, number>,
        /** binary payload bytes returned by in-transaction gets of update/history docs */
        txRereadBytes: 0,
        /** transaction.delete calls by target category (counted per attempt) */
        txDeletes: { lock: 0, main: 0, update: 0, history: 0, other: 0 } as Record<string, number>,
        /** onSnapshot server-confirmed document deliveries (added/modified) */
        listenerDocs: 0,
        /** RPCs: one per getDocs/getDoc/transaction.get, one commit per attempt */
        rpcs: 0,
    });
    return {
        ctl: {
            counting: false,
            countListeners: false,
            /** Main document path of the scenario being measured */
            basePath: '',
            /** When set, the next work-deleting transaction is forced to retry once */
            forceRetryOnce: false,
            forcedRetries: 0,
            freshCounters,
            c: freshCounters(),
        },
    };
});

/** Classifies a document path relative to the scenario's main document. */
function categoryOf(refPath: string): 'lock' | 'main' | 'update' | 'history' | 'other' {
    if (refPath === ctl.basePath) return 'main';
    if (!refPath.startsWith(ctl.basePath + '/')) return 'other';
    const rest = refPath.slice(ctl.basePath.length + 1);
    if (rest === 'metadata/lock_compaction') return 'lock';
    if (/^updates\/[^/]+$/.test(rest)) return 'update';
    if (/^history\/[^/]+$/.test(rest)) return 'history';
    return 'other';
}

/** Sum of the binary (Bytes) field sizes of a document snapshot. */
function binaryPayloadBytes(snap: any): number {
    if (!snap?.exists?.()) return 0;
    let total = 0;
    for (const v of Object.values(snap.data() as Record<string, unknown>)) {
        if (v && typeof (v as any).toUint8Array === 'function') {
            total += (v as any).toUint8Array().byteLength;
        }
    }
    return total;
}

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();

    const countSnapshotDelivery = (snap: any) => {
        if (!ctl.counting || !ctl.countListeners || !snap || snap.metadata?.fromCache) return;
        if (typeof snap.docChanges === 'function') {
            for (const ch of snap.docChanges()) {
                if (ch.type !== 'removed' && !ch.doc.metadata.hasPendingWrites) ctl.c.listenerDocs++;
            }
        } else if (!snap.metadata?.hasPendingWrites) {
            ctl.c.listenerDocs++;
        }
    };

    return {
        ...actual,
        getDocs: async (q: any) => {
            const snap = await actual.getDocs(q);
            if (ctl.counting) {
                ctl.c.queries++;
                ctl.c.rpcs++;
                ctl.c.queryDocs += snap.size;
                if (snap.size === 0) ctl.c.emptyQueries++;
            }
            return snap;
        },
        getDoc: async (r: any) => {
            const snap = await actual.getDoc(r);
            if (ctl.counting) {
                ctl.c.getDoc++;
                ctl.c.rpcs++;
            }
            return snap;
        },
        onSnapshot: (target: any, ...args: any[]) => {
            const wrapped = args.map(a => (typeof a === 'function' && a === args.find(x => typeof x === 'function'))
                ? (snap: any) => { countSnapshotDelivery(snap); return a(snap); }
                : a);
            return actual.onSnapshot(target, ...wrapped);
        },
        runTransaction: (db: any, updateFn: (tx: any) => Promise<any>, options?: any) => {
            if (!ctl.counting) return actual.runTransaction(db, updateFn, options);
            ctl.c.transactions++;
            let attempt = 0;
            return actual.runTransaction(db, async (tx: any) => {
                attempt++;
                ctl.c.txAttempts++;
                ctl.c.rpcs++; // the commit
                let deletesWork = 0;
                let lockRef: any = null;
                const proxy: any = {
                    get: async (ref: any) => {
                        const snap = await tx.get(ref);
                        const cat = categoryOf(ref.path);
                        ctl.c.txGets[cat]++;
                        ctl.c.rpcs++;
                        if (cat === 'update' || cat === 'history') {
                            ctl.c.txRereadBytes += binaryPayloadBytes(snap);
                        }
                        if (cat === 'lock') lockRef = ref;
                        return snap;
                    },
                    set: (...a: any[]) => { tx.set(...a); return proxy; },
                    update: (...a: any[]) => { tx.update(...a); return proxy; },
                    delete: (ref: any) => {
                        const cat = categoryOf(ref.path);
                        ctl.c.txDeletes[cat]++;
                        if (cat === 'update' || cat === 'history') deletesWork++;
                        tx.delete(ref);
                        return proxy;
                    },
                };
                const out = await updateFn(proxy);
                if (ctl.forceRetryOnce && attempt === 1 && deletesWork > 0 && lockRef) {
                    // Touch the lock document outside the transaction (owner
                    // unchanged): the commit's precondition on the lock read
                    // fails and the SDK re-runs the whole body — every
                    // transaction.get included. Models any contention retry.
                    ctl.forceRetryOnce = false;
                    ctl.forcedRetries++;
                    await actual.setDoc(lockRef, { benchTouch: Date.now() }, { merge: true });
                }
                return out;
            }, options);
        },
    };
});

import * as Y from 'yjs';
import {
    collection,
    doc,
    getDocs,
    writeBatch,
    serverTimestamp,
    Bytes,
    Firestore,
} from 'firebase/firestore';
import { FirebaseStorage, ref as storageRef, uploadBytes } from 'firebase/storage';
import { compact, CompactionContext } from '../../src/compaction';
import { squashDocument } from '../../src/squash';
import { FireProvider } from '../../src/provider';
import { extractClockEnds, aggregateClockEnds } from '../../src/update-metadata';
import { createSim, runSession, clientIdForSession, VersicleSimState } from '../../benchmarks/versicle-workload';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

const SEED = 20261002;
const UID = 'txn-reads-compactor';

type Counters = ReturnType<typeof ctl.freshCounters>;

/** Billed reads: query docs (min 1 per query) + getDoc + transaction.get. */
function billedReads(c: Counters): number {
    const txGets = Object.values(c.txGets).reduce((a, b) => a + b, 0);
    return c.queryDocs + c.emptyQueries + c.getDoc + txGets + c.listenerDocs;
}

/** In-transaction reads of the update/history documents being deleted. */
function workRereads(c: Counters): number {
    return c.txGets.update + c.txGets.history;
}

function fmtKB(n: number): string {
    return `${(n / 1024).toFixed(1)} KB`;
}

function summarize(label: string, c: Counters): string {
    const txGets = Object.values(c.txGets).reduce((a, b) => a + b, 0);
    return [
        `[txn-reads] ${label}`,
        `  billed reads ${billedReads(c)} = query reads ${c.queryDocs + c.emptyQueries} + getDoc ${c.getDoc} + tx.get ${txGets}` +
            (c.listenerDocs ? ` + listener docs ${c.listenerDocs}` : ''),
        `  tx.get by target: lock ${c.txGets.lock}, main ${c.txGets.main}, update ${c.txGets.update}, history ${c.txGets.history}` +
            ` -> re-reads of deleted docs ${workRereads(c)} (${fmtKB(c.txRereadBytes)} payload), ` +
            `${(100 * workRereads(c) / Math.max(1, billedReads(c))).toFixed(1)}% of billed reads`,
        `  transactions ${c.transactions}, attempts ${c.txAttempts}, RPCs ${c.rpcs}`,
    ].join('\n');
}

/**
 * Deterministic versicle-shaped update stream (fresh clientID per session,
 * one blob per debounced save) — the same model as the aging benchmarks.
 */
class VersicleStream {
    private sim: VersicleSimState;
    private session = 0;
    private queue: Uint8Array[] = [];
    /** Holds every blob generated so far (a fully synced client). */
    readonly full = new Y.Doc();

    constructor(private readonly seed: number) {
        this.sim = createSim({ seed });
    }

    private runOne(): Uint8Array[] {
        const d = new Y.Doc();
        d.clientID = clientIdForSession(this.seed, this.session++);
        Y.applyUpdate(d, Y.encodeStateAsUpdate(this.full));
        const { blobs } = runSession(this.sim, d);
        d.destroy();
        for (const b of blobs) Y.applyUpdate(this.full, b);
        return blobs;
    }

    /** All blobs of the next `n` sessions. */
    sessions(n: number): Uint8Array[] {
        const out = this.queue.splice(0);
        for (let i = 0; i < n; i++) out.push(...this.runOne());
        return out;
    }

    /** The next `n` save blobs. */
    take(n: number): Uint8Array[] {
        while (this.queue.length < n) this.queue.push(...this.runOne());
        return this.queue.splice(0, n);
    }
}

describe('Compaction transaction re-reads (Firestore op counts)', () => {
    let app: any;
    let db: Firestore;
    let storage: FirebaseStorage;
    let counter = 0;

    beforeAll(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
    });

    const newPath = (label: string) => `tests/txn-reads-${label}-${getStableDate()}-${Date.now()}-${counter++}`;

    const ctxFor = (path: string, historyFoldThreshold: number): CompactionContext => ({
        db,
        path,
        uid: UID,
        lockTTL: 60_000,
        compactionLimit: 500,
        isDestroyed: () => false,
        storage,
        cachedClockOffset: 0, // the provider's steady state (offset already measured)
        historyFoldThreshold,
    });

    /** Writes update documents exactly as the provider's save path does. */
    async function writeUpdates(path: string, blobs: Uint8Array[]): Promise<void> {
        for (let i = 0; i < blobs.length; i += 400) {
            const batch = writeBatch(db);
            for (const b of blobs.slice(i, i + 400)) {
                batch.set(doc(collection(db, path, 'updates')), {
                    createdAt: serverTimestamp(),
                    createdBy: 'writer',
                    update: Bytes.fromUint8Array(b),
                    ...aggregateClockEnds(extractClockEnds(b)),
                });
            }
            await batch.commit();
        }
    }

    /**
     * Base snapshot through the real fold path: the base content goes in as
     * one storage-backed update document (it can exceed the inline limit),
     * and the first compaction folds it into snapshot v1.
     */
    async function seedBase(path: string, stream: VersicleStream, sessions: number): Promise<void> {
        const merged = Y.mergeUpdates(stream.sessions(sessions));
        const blobPath = `${path}/large_updates/seed_base.bin`;
        await uploadBytes(storageRef(storage, blobPath), merged);
        const batch = writeBatch(db);
        batch.set(doc(collection(db, path, 'updates')), {
            createdAt: serverTimestamp(),
            createdBy: 'writer',
            updateStoragePath: blobPath,
        });
        await batch.commit();
        const r = await compact(ctxFor(path, 8));
        expect(r.type).toBe('snapshot');
    }

    /** `count` delta segments of `perSegment` updates each. */
    async function seedHistory(path: string, stream: VersicleStream, count: number, perSegment: number): Promise<void> {
        for (let i = 0; i < count; i++) {
            await writeUpdates(path, stream.take(perSegment));
            const r = await compact(ctxFor(path, 100));
            expect(r.type).toBe('history');
        }
    }

    /** Runs `fn` with counting on and returns its result plus the counters. */
    async function measure<T>(path: string, fn: () => Promise<T>, opts: { listeners?: boolean } = {}): Promise<{ result: T; c: Counters }> {
        ctl.basePath = path;
        ctl.c = ctl.freshCounters();
        ctl.countListeners = !!opts.listeners;
        ctl.counting = true;
        try {
            const result = await fn();
            return { result, c: ctl.c };
        } finally {
            ctl.counting = false;
            ctl.countListeners = false;
        }
    }

    async function remaining(path: string): Promise<{ updates: number; history: number }> {
        const [u, h] = await Promise.all([
            getDocs(collection(db, path, 'updates')),
            getDocs(collection(db, path, 'history')),
        ]);
        return { updates: u.size, history: h.size };
    }

    /** A delta cycle of `n` updates on a doc with `h` existing segments. */
    async function measureDelta(label: string, baseSessions: number, h: number, n: number) {
        const path = newPath(label);
        const stream = new VersicleStream(SEED);
        await seedBase(path, stream, baseSessions);
        await seedHistory(path, stream, h, 20);
        await writeUpdates(path, stream.take(n));
        const { result, c } = await measure(path, () => compact(ctxFor(path, 100)));
        expect(result.type).toBe('history');
        expect(result.updatesCompacted).toBe(n);
        expect(await remaining(path)).toEqual({ updates: 0, history: h + 1 });
        return { result, c, path };
    }

    /** A fold of `h` segments + `n` updates. */
    async function measureFold(label: string, baseSessions: number, h: number, perSegment: number, n: number) {
        const path = newPath(label);
        const stream = new VersicleStream(SEED);
        await seedBase(path, stream, baseSessions);
        await seedHistory(path, stream, h, perSegment);
        await writeUpdates(path, stream.take(n));
        // threshold h + 1: the cycle that would add segment h+1 folds instead
        const { result, c } = await measure(path, () => compact(ctxFor(path, h + 1)));
        expect(result.type).toBe('snapshot');
        expect(result.updatesCompacted).toBe(n);
        expect(result.historySegmentsMerged).toBe(h);
        expect(await remaining(path)).toEqual({ updates: 0, history: 0 });
        return { result, c, path };
    }

    /**
     * A squash of base + `h` segments + `n` pending updates, the shape
     * provider.squash() meets: it runs compact() first, so the squash sees
     * up to historyFoldThreshold - 1 segments plus whatever saves landed
     * since. Delete-only saves carry no clock metadata and make the squash
     * refuse ('local-behind'), so the pending updates are drawn from saves
     * that insert something.
     */
    async function measureSquash(label: string, baseSessions: number, h: number, perSegment: number, n: number) {
        const path = newPath(label);
        const stream = new VersicleStream(SEED);
        await seedBase(path, stream, baseSessions);
        await seedHistory(path, stream, h, perSegment);
        const pending = stream.take(n * 3).filter(b => extractClockEnds(b).size > 0).slice(0, n);
        expect(pending.length).toBe(n);
        await writeUpdates(path, pending);
        // The app reads its roots through typed accessors; the squash
        // rebuild needs every root share to have its concrete type.
        for (const key of Array.from(stream.full.share.keys())) stream.full.getMap(key);
        const { result, c } = await measure(path, () => squashDocument({
            db,
            path,
            uid: UID,
            lockTTL: 60_000,
            cachedClockOffset: 0,
            storage,
            isDestroyed: () => false,
            doc: stream.full,
        }));
        expect(result.success, `squash: ${result.skippedReason ?? ''} ${result.error ?? ''}`).toBe(true);
        expect(await remaining(path)).toEqual({ updates: 0, history: 0 });
        stream.full.destroy();
        return { result, c, path };
    }

    describe('bench: what one compaction pays for the re-reads', () => {
        it('delta cycle: 52 updates on a 60-session versicle document', { timeout: 180_000 }, async () => {
            const { c } = await measureDelta('bench-delta', 60, 1, 52);
            console.log(summarize('delta cycle, 60-session doc, 1 existing segment, N=52 updates', c));
        });

        it('delta scaling: in-transaction reads grow 1:1 with the pending updates', { timeout: 300_000 }, async () => {
            const rows: string[] = [];
            for (const n of [25, 100, 400]) {
                const { c } = await measureDelta(`bench-delta-n${n}`, 4, 0, n);
                rows.push(`  N=${String(n).padStart(3)}: billed reads ${String(billedReads(c)).padStart(4)}, ` +
                    `re-reads of deleted docs ${String(workRereads(c)).padStart(3)} (${fmtKB(c.txRereadBytes)}), ` +
                    `RPCs ${c.rpcs}`);
            }
            console.log(['[txn-reads] delta scaling (no existing history)', ...rows].join('\n'));
        });

        it('fold: 7 segments + 50 updates (the default historyFoldThreshold cycle)', { timeout: 240_000 }, async () => {
            const { c } = await measureFold('bench-fold', 60, 7, 50, 50);
            console.log(summarize('fold, 60-session doc, H=7 segments x 50 updates, N=50 updates', c));
        });

        it('squash: base + 7 segments + 5 pending updates (provider.squash after its compact)', { timeout: 240_000 }, async () => {
            const { c } = await measureSquash('bench-squash', 30, 7, 50, 5);
            console.log(summarize('squash, 30-session doc, H=7 segments x 50 updates, N=5 updates', c));
        });

        it('one SDK transaction retry repeats every re-read', { timeout: 180_000 }, async () => {
            const path = newPath('bench-retry');
            const stream = new VersicleStream(SEED);
            await seedBase(path, stream, 4);
            await writeUpdates(path, stream.take(52));
            ctl.forcedRetries = 0;
            ctl.forceRetryOnce = true;
            const { result, c } = await measure(path, () => compact(ctxFor(path, 100)));
            ctl.forceRetryOnce = false;
            expect(ctl.forcedRetries).toBe(1);
            expect(result.type).toBe('history');
            console.log(summarize('delta cycle N=52 with one forced SDK retry of the commit transaction', c));
        });

        it('end-to-end: 400-save single-client provider session (share of all reads)', { timeout: 300_000 }, async () => {
            const path = newPath('bench-e2e');
            const stream = new VersicleStream(SEED);
            const blobs = stream.take(400);
            const ydoc = new Y.Doc();
            const { result: counts, c } = await measure(path, async () => {
                const provider = new FireProvider({ firebaseApp: app, ydoc, path, maxWaitTime: 10 });
                await waitForConditionTruthy(() => provider.synced, { timeout: 30_000, message: 'initial sync' });
                let saves = 0;
                provider.on('saved', () => { saves++; });
                for (let i = 0; i < blobs.length; i++) {
                    const target = saves + 1;
                    Y.applyUpdate(ydoc, blobs[i], 'bench-edit');
                    await waitForConditionTruthy(() => saves >= target, { timeout: 20_000, interval: 5, message: `save ${i}` });
                    await new Promise(r => setTimeout(r, 25)); // page-turn pacing
                }
                // Compact the tail too, so every save is compacted exactly once
                // whatever the trigger timing was.
                const idle = () => waitForConditionTruthy(() => !(provider as any)._inflightCompaction, { timeout: 60_000, interval: 50, message: 'compaction idle' });
                await idle();
                await provider.compact();
                await idle();
                await provider.destroy();
                return { saves };
            }, { listeners: true });
            ydoc.destroy();
            expect(counts.saves).toBe(400);
            console.log(summarize(`end-to-end session, ${counts.saves} saves, 1 client (listener deliveries approximated)`, c));
            // Not asserted: the known emulator flake (a desynced gRPC stream
            // logging RESOURCE_EXHAUSTED) can fail a compaction attempt, and
            // the retry re-reads everything again — numbers from such a run
            // are inflated, so flag them instead of failing the bench.
            const left = await remaining(path);
            if (left.updates > 0) {
                console.log(`[txn-reads]   WARNING: ${left.updates} updates left uncompacted (emulator stream flake?) — re-run for clean numbers`);
            }
            console.log(`[txn-reads]   reads per save: ${(billedReads(c) / counts.saves).toFixed(2)}` +
                ` -> ${((billedReads(c) - workRereads(c)) / counts.saves).toFixed(2)} without the re-reads`);
        });
    });

    describe('regression: commit transactions must not re-read the documents they delete', () => {
        it('delta: zero in-transaction update reads; one billed read per pending update', { timeout: 180_000 }, async () => {
            const small = await measureDelta('reg-delta-10', 3, 1, 10);
            const large = await measureDelta('reg-delta-40', 3, 1, 40);
            console.log(summarize('regression delta N=10', small.c));
            console.log(summarize('regression delta N=40', large.c));

            expect(workRereads(small.c), 'transaction.get calls on update docs (delta, N=10)').toBe(0);
            expect(workRereads(large.c), 'transaction.get calls on update docs (delta, N=40)').toBe(0);
            // 30 more pending updates must cost 30 more reads (the query
            // returning them), not 60.
            expect(billedReads(large.c) - billedReads(small.c), 'marginal billed reads for 30 extra updates').toBe(30);
        });

        it('fold: zero in-transaction update/history reads; one billed read per merged document', { timeout: 240_000 }, async () => {
            const small = await measureFold('reg-fold-10', 3, 2, 10, 10);
            const large = await measureFold('reg-fold-40', 3, 2, 10, 40);
            console.log(summarize('regression fold H=2 N=10', small.c));
            console.log(summarize('regression fold H=2 N=40', large.c));

            expect(workRereads(small.c), 'transaction.get calls on update/history docs (fold, N=10)').toBe(0);
            expect(workRereads(large.c), 'transaction.get calls on update/history docs (fold, N=40)').toBe(0);
            expect(billedReads(large.c) - billedReads(small.c), 'marginal billed reads for 30 extra updates').toBe(30);
        });

        it('squash: zero in-transaction update/history reads', { timeout: 240_000 }, async () => {
            const { c } = await measureSquash('reg-squash', 3, 2, 10, 20);
            console.log(summarize('regression squash H=2 N=20', c));

            expect(workRereads(c), 'transaction.get calls on update/history docs (squash)').toBe(0);
            // The squash still reads the lock and the main document version.
            expect(c.txGets.lock).toBeGreaterThanOrEqual(1);
        });
    });
});
