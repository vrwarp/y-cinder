/**
 * Poison document compaction storm: Firestore/Storage operation counts
 *
 * Compaction merges every pending update document it reads. When one of
 * them can never be merged, compaction fails the same way on every attempt:
 *
 *  - an undecodable inline payload (the "poison pill" the sync layer
 *    explicitly quarantines) makes the merge throw;
 *  - a storage-backed update whose blob is gone (e.g. removed by a
 *    lifecycle rule) makes the download throw `storage/object-not-found`;
 *  - a fold whose snapshot upload is rejected (Storage rules / quota)
 *    throws after downloading the base and merging everything.
 *
 * `handleCompactionError` gives up on these non-retryable errors, but
 * nothing remembers the failure. The sync layer quarantines the poison for
 * APPLYING, while compaction keeps including it. Every client re-triggers
 * compaction once per 10 s cooldown — or on EVERY listener delivery once
 * the pending collection reaches REALTIME_LIMIT (200), which it does
 * because nothing is ever compacted — and every attempt takes the lock,
 * re-reads min(backlog, compactionLimit) update documents plus history and
 * the main document (on the fold path also downloads the snapshot and
 * merges it) before failing again. The backlog never shrinks, so every
 * cold start reads all of it too.
 *
 * Measurement: the SDK entry points y-cinder uses are wrapped at the module
 * boundary (`@firebase/firestore`, `@firebase/storage`) and every operation
 * issued from inside `compact()` is attributed to compaction via an
 * AsyncLocalStorage scope opened by a wrapper around the exported
 * `compact` (which is what FireProvider calls). Billing model: a query
 * costs one read per returned document (minimum one), `getDoc` and each
 * `transaction.get` cost one read.
 *
 * Two parts:
 *  - `bench:` scenarios report the counts (always pass; the numbers are the
 *    deliverable). Run just those with `-t bench`.
 *  - `regression:` tests drive a provider into the hard-cap regime with a
 *    deterministic, permanent compaction failure, then deliver N more
 *    single remote writes, waiting for each delivery and for any
 *    compaction it triggered to settle before the next. They pin that
 *    those N deliveries cost at most ONE more full backlog read by
 *    compaction (not one per delivery) — whether the fix backs off after
 *    a non-retryable failure or isolates the poison and drains the
 *    backlog. They fail on the unfixed code (N + 1 full reads).
 *
 * Run (needs the Firestore + Storage emulator, through the isolation
 * wrapper on shared machines):
 *   bash scripts/test.sh tests/integration/poison_doc_compaction_storm.test.ts               # all
 *   bash scripts/test.sh tests/integration/poison_doc_compaction_storm.test.ts -t bench      # report only
 *   bash scripts/test.sh tests/integration/poison_doc_compaction_storm.test.ts -t regression # pins only
 *
 * @file poison_doc_compaction_storm.test.ts
 */

import { describe, it, expect, beforeAll, afterEach, vi } from 'vitest';

const { ctl } = vi.hoisted(() => {
    const freshCounters = () => ({
        /** compact() entries (provider triggers + direct calls) */
        compactCalls: 0,
        /** compact() results with success === false */
        failedCompactions: 0,
        /** compact() results that actually compacted something */
        successfulCompactions: 0,
        /** update-doc queries issued by compaction (one per lock-holding attempt) */
        compUpdateQueries: 0,
        /** update documents returned to compaction queries */
        compUpdateDocs: 0,
        /** binary payload bytes of those documents */
        compUpdateBytes: 0,
        /** history queries / documents returned to compaction */
        compHistoryQueries: 0,
        compHistoryDocs: 0,
        /** compaction queries that returned nothing (billed one read each) */
        compEmptyQueries: 0,
        /** getDoc calls issued by compaction (main document) */
        compGetDoc: 0,
        /** runTransaction calls issued by compaction (lock acquire/release + commits) */
        compTransactions: 0,
        /** transaction.get calls issued by compaction transactions */
        compTxGets: 0,
        /** Storage downloads issued by compaction (attempted / bytes / failed) */
        compDownloads: 0,
        compDownloadBytes: 0,
        compDownloadErrors: 0,
        /** Storage uploads issued by compaction (attempted / bytes / rejected) */
        compUploads: 0,
        compUploadBytes: 0,
        compUploadsRejected: 0,
        /** merges issued by compaction and their total input bytes */
        compMerges: 0,
        compMergeInputBytes: 0,
        /** update documents returned to queries OUTSIDE compaction (initial sync) */
        syncUpdateDocs: 0,
        syncUpdateQueries: 0,
        /** listener attaches outside compaction, by target */
        listenerAttaches: { updates: 0, history: 0, main: 0, other: 0 } as Record<string, number>,
        /** server-confirmed documents delivered to listeners (added/modified), by target */
        listenerDocs: { updates: 0, history: 0, main: 0, other: 0 } as Record<string, number>,
    });
    return {
        ctl: {
            counting: false,
            /** Reject compaction's fold-snapshot uploads like a Storage rule / quota would */
            rejectFoldUploads: false,
            freshCounters,
            c: freshCounters(),
            als: null as any,
        },
    };
});

/** Lazily creates the AsyncLocalStorage shared by the mock factories. */
async function compactionScope(): Promise<any> {
    if (!ctl.als) {
        const { AsyncLocalStorage } = await import('node:async_hooks');
        ctl.als = new AsyncLocalStorage();
    }
    return ctl.als;
}

/** Category of a Firestore query / collection / document reference. */
function targetCategory(target: any): 'updates' | 'history' | 'main' | 'other' {
    let p = '';
    try {
        p = target?._query?.path?.canonicalString?.() ?? target?.path ?? '';
    } catch {
        p = '';
    }
    if (target?.type === 'document') {
        if (p.endsWith('/metadata/lock_compaction')) return 'other';
        // Main documents in this file live at integration-tests/<id>
        return p.split('/').length === 2 ? 'main' : 'other';
    }
    if (p.endsWith('/updates')) return 'updates';
    if (p.endsWith('/history')) return 'history';
    return 'other';
}

/** Sum of the binary (Bytes) field sizes of a document snapshot. */
function binaryPayloadBytes(snap: any): number {
    let total = 0;
    for (const v of Object.values((snap?.data?.() ?? {}) as Record<string, unknown>)) {
        if (v && typeof (v as any).toUint8Array === 'function') {
            total += (v as any).toUint8Array().byteLength;
        }
    }
    return total;
}

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    const als = await compactionScope();

    return {
        ...actual,
        getDocs: async (q: any) => {
            const inCompaction = als.getStore() !== undefined;
            const snap = await actual.getDocs(q);
            if (ctl.counting) {
                const cat = targetCategory(q);
                if (inCompaction) {
                    if (snap.size === 0) ctl.c.compEmptyQueries++;
                    if (cat === 'updates') {
                        ctl.c.compUpdateQueries++;
                        ctl.c.compUpdateDocs += snap.size;
                        snap.docs.forEach((d: any) => { ctl.c.compUpdateBytes += binaryPayloadBytes(d); });
                    } else if (cat === 'history') {
                        ctl.c.compHistoryQueries++;
                        ctl.c.compHistoryDocs += snap.size;
                    }
                } else if (cat === 'updates') {
                    ctl.c.syncUpdateQueries++;
                    ctl.c.syncUpdateDocs += snap.size;
                }
            }
            return snap;
        },
        getDoc: async (r: any) => {
            const inCompaction = als.getStore() !== undefined;
            const snap = await actual.getDoc(r);
            if (ctl.counting && inCompaction) ctl.c.compGetDoc++;
            return snap;
        },
        onSnapshot: (target: any, ...args: any[]) => {
            const cat = targetCategory(target);
            if (ctl.counting) ctl.c.listenerAttaches[cat]++;
            const onNextIndex = args.findIndex(a => typeof a === 'function');
            if (onNextIndex >= 0) {
                const onNext = args[onNextIndex];
                args = [...args];
                args[onNextIndex] = (snap: any) => {
                    if (ctl.counting && snap && !snap.metadata?.fromCache) {
                        if (typeof snap.docChanges === 'function') {
                            for (const ch of snap.docChanges()) {
                                if (ch.type !== 'removed' && !ch.doc.metadata.hasPendingWrites) ctl.c.listenerDocs[cat]++;
                            }
                        } else if (!snap.metadata?.hasPendingWrites) {
                            ctl.c.listenerDocs[cat]++;
                        }
                    }
                    return onNext(snap);
                };
            }
            return actual.onSnapshot(target, ...args);
        },
        runTransaction: (db: any, updateFn: (tx: any) => Promise<any>, options?: any) => {
            // Captured at entry: the body runs on the SDK's async queue.
            const inCompaction = als.getStore() !== undefined;
            if (!ctl.counting || !inCompaction) return actual.runTransaction(db, updateFn, options);
            ctl.c.compTransactions++;
            return actual.runTransaction(db, async (tx: any) => {
                const proxy: any = {
                    get: async (ref: any) => {
                        ctl.c.compTxGets++;
                        return tx.get(ref);
                    },
                    set: (...a: any[]) => { tx.set(...a); return proxy; },
                    update: (...a: any[]) => { tx.update(...a); return proxy; },
                    delete: (ref: any) => { tx.delete(ref); return proxy; },
                };
                return updateFn(proxy);
            }, options);
        },
    };
});

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    const als = await compactionScope();

    return {
        ...actual,
        getBytes: async (storageRef: any, maxDownloadSizeBytes?: number) => {
            const inCompaction = ctl.counting && als.getStore() !== undefined;
            if (inCompaction) ctl.c.compDownloads++;
            try {
                const buf = await actual.getBytes(storageRef, maxDownloadSizeBytes);
                if (inCompaction) ctl.c.compDownloadBytes += buf.byteLength;
                return buf;
            } catch (e) {
                if (inCompaction) ctl.c.compDownloadErrors++;
                throw e;
            }
        },
        uploadBytes: async (storageRef: any, data: Uint8Array, metadata?: any) => {
            const inCompaction = ctl.counting && als.getStore() !== undefined;
            const fullPath: string = storageRef?.fullPath ?? '';
            if (inCompaction) {
                ctl.c.compUploads++;
                ctl.c.compUploadBytes += data.byteLength;
            }
            if (ctl.rejectFoldUploads && /\/snapshot_v\d+_[^/]+\.bin$/.test(fullPath)) {
                if (inCompaction) ctl.c.compUploadsRejected++;
                // What the SDK surfaces when Storage security rules (or a
                // quota) reject the write: permanent, not retryable.
                throw new actual.StorageError(
                    actual.StorageErrorCode.UNAUTHORIZED,
                    `User does not have permission to access '${fullPath}'.`,
                    403,
                );
            }
            return actual.uploadBytes(storageRef, data, metadata);
        },
    };
});

vi.mock('../../src/merge-utils', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    const als = await compactionScope();
    return {
        ...actual,
        mergeUpdatesWithMetaAsync: (updates: Uint8Array[], options?: any) => {
            if (ctl.counting && als.getStore() !== undefined) {
                ctl.c.compMerges++;
                ctl.c.compMergeInputBytes += updates.reduce((n, u) => n + u.byteLength, 0);
            }
            return actual.mergeUpdatesWithMetaAsync(updates, options);
        },
    };
});

vi.mock('../../src/compaction', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    const als = await compactionScope();
    return {
        ...actual,
        // FireProvider imports this export; the scope tags every SDK call
        // issued while the compaction runs (lock, queries, transactions,
        // Storage, merge).
        compact: (ctx: any, attempt?: number) => als.run({ uid: ctx.uid }, async () => {
            if (ctl.counting) ctl.c.compactCalls++;
            const result = await actual.compact(ctx, attempt);
            if (ctl.counting) {
                if (!result.success) ctl.c.failedCompactions++;
                else if (result.type && result.type !== 'none') ctl.c.successfulCompactions++;
            }
            return result;
        }),
    };
});

import * as Y from 'yjs';
import { initializeApp, getApps, type FirebaseApp } from 'firebase/app';
import {
    collection,
    doc,
    getDocs,
    addDoc,
    writeBatch,
    serverTimestamp,
    Timestamp,
    Bytes,
    Firestore,
    getFirestore,
    connectFirestoreEmulator,
} from 'firebase/firestore';
import { FirebaseStorage, getStorage, connectStorageEmulator } from 'firebase/storage';
import { FireProvider } from '../../src/provider';
import { compact, CompactionContext } from '../../src/compaction';
import { DEFAULTS, FIRESTORE_PATHS } from '../../src/types';
import { setupEmulator } from '../utils/emulator';
import { waitFor, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

type Counters = ReturnType<typeof ctl.freshCounters>;

const GARBAGE = new Uint8Array([255, 254, 253, 252, 251, 250, 249, 248]);
/** Default provider compactionLimit: the most update docs one attempt reads. */
const COMPACTION_LIMIT = DEFAULTS.COMPACTION_LIMIT;
/** Payload of each writer update (~ a small app edit). */
const VALUE = 'x'.repeat(80);

/** Firebase apps connected to the emulator, one per simulated device. */
const deviceApps = new Map<string, FirebaseApp>();

/** A separate Firebase app (own Firestore client) per simulated device. */
function deviceApp(name: string): FirebaseApp {
    let app = deviceApps.get(name);
    if (app) return app;
    app = getApps().find(a => a.name === name) ?? initializeApp({
        projectId: 'demo-test-project',
        apiKey: 'fake-api-key',
        storageBucket: 'demo-test-project.appspot.com',
    }, name);
    connectFirestoreEmulator(getFirestore(app), '127.0.0.1', 8080);
    connectStorageEmulator(getStorage(app), '127.0.0.1', 9199);
    deviceApps.set(name, app);
    return app;
}

function fmtKB(n: number): string {
    return `${(n / 1024).toFixed(1)} KB`;
}

function summarize(label: string, c: Counters, extra: string[] = []): string {
    return [
        `[poison-storm] ${label}`,
        `  compact() calls ${c.compactCalls}: lock-holding attempts ${c.compUpdateQueries}, failed ${c.failedCompactions}, succeeded ${c.successfulCompactions}`,
        `  compaction reads: update docs ${c.compUpdateDocs} (${fmtKB(c.compUpdateBytes)}), history docs ${c.compHistoryDocs}, ` +
            `main getDoc ${c.compGetDoc}, tx.get ${c.compTxGets} -> ${compactionReads(c)} billed reads`,
        `  compaction transactions ${c.compTransactions} (lock acquire/release + commits)`,
        `  compaction Storage: downloads ${c.compDownloads} (${fmtKB(c.compDownloadBytes)}, ${c.compDownloadErrors} failed), ` +
            `uploads ${c.compUploads} (${fmtKB(c.compUploadBytes)}, ${c.compUploadsRejected} rejected)`,
        `  compaction merges ${c.compMerges} (${fmtKB(c.compMergeInputBytes)} input)`,
        `  non-compaction: update-doc query reads ${c.syncUpdateDocs} in ${c.syncUpdateQueries} queries; ` +
            `listener attaches updates/history/main ${c.listenerAttaches.updates}/${c.listenerAttaches.history}/${c.listenerAttaches.main}`,
        ...extra.map(l => `  ${l}`),
    ].join('\n');
}

/**
 * Billed reads issued by compaction: returned documents (an empty query
 * still costs one read) + getDoc + transaction.get.
 */
function compactionReads(c: Counters): number {
    return c.compUpdateDocs + c.compHistoryDocs + c.compGetDoc + c.compTxGets + c.compEmptyQueries;
}

async function pendingUpdateCount(db: Firestore, path: string): Promise<number> {
    return (await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES))).size;
}

async function historyCount(db: Firestore, path: string): Promise<number> {
    return (await getDocs(collection(db, path, FIRESTORE_PATHS.HISTORY))).size;
}

/** A remote writer emitting small incremental updates (one per edit). */
class Writer {
    readonly doc = new Y.Doc();
    private n = 0;

    /** Next incremental update: sets map key `w<n>`. Returns [key, update]. */
    next(): [string, Uint8Array] {
        const sv = Y.encodeStateVector(this.doc);
        const key = `w${this.n++}`;
        this.doc.getMap('m').set(key, VALUE);
        return [key, Y.encodeStateAsUpdate(this.doc, sv)];
    }

    /** One bulk update (e.g. imported book content) of ~`bytes` bytes. */
    bulk(bytes: number): Uint8Array {
        const sv = Y.encodeStateVector(this.doc);
        this.doc.getText('bulk').insert(0, 'b'.repeat(bytes));
        return Y.encodeStateAsUpdate(this.doc, sv);
    }
}

/**
 * Strictly increasing explicit createdAt values, in the past relative to
 * any later serverTimestamp() write. Seeded documents get a deterministic
 * order (a single batch would otherwise tie on one commit timestamp).
 */
function timestampSequence(): () => Timestamp {
    const base = Date.now() - 3_600_000;
    let seq = 0;
    return () => Timestamp.fromMillis(base + seq++);
}

async function seedUpdates(db: Firestore, path: string, items: Uint8Array[], nextTs: () => Timestamp, createdBy: string) {
    for (let i = 0; i < items.length; i += 450) {
        const batch = writeBatch(db);
        for (const u of items.slice(i, i + 450)) {
            batch.set(doc(collection(db, path, FIRESTORE_PATHS.UPDATES)), {
                update: Bytes.fromUint8Array(u),
                createdAt: nextTs(),
                createdBy,
            });
        }
        await batch.commit();
    }
}

type PoisonKind = 'inline-garbage' | 'missing-blob' | 'none';

async function writePoison(db: Firestore, path: string, kind: PoisonKind, nextTs: () => Timestamp): Promise<void> {
    if (kind === 'inline-garbage') {
        await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
            update: Bytes.fromUint8Array(GARBAGE),
            createdAt: nextTs(),
            createdBy: 'poison-writer',
        });
    } else if (kind === 'missing-blob') {
        // A storage-backed update whose blob no longer exists (lifecycle
        // rule, manual cleanup, partial restore). Written by a one-off
        // client nobody else builds on, so the rest of the document stays
        // appliable everywhere.
        await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
            updateStoragePath: `${path}/large_updates/ghost-client_${Date.now()}.bin`,
            createdAt: nextTs(),
            createdBy: 'ghost-client',
        });
    }
}

interface StormOptions {
    label: string;
    poison: PoisonKind;
    /** Seed a base snapshot in Storage + (fold threshold - 1) history segments, so attempts take the fold path */
    foldDue?: boolean;
    /** Approximate base snapshot payload (bytes) when foldDue */
    baseBytes?: number;
    rejectFoldUploads?: boolean;
    /** Valid update docs seeded in one commit after the poison */
    backlog: number;
    /** Single remote writes delivered after the backlog */
    liveWrites: number;
}

interface StormResult {
    c: Counters;
    /** Pending update docs when counting started (after the backlog commit) */
    pendingAtArm: number;
    pendingAtEnd: number;
    historyAtEnd: number;
    /** min(pending, compactionLimit): update docs one lock-holding attempt reads */
    fullAttemptReads: number;
    /** compaction update-doc reads caused by the live writes alone */
    liveWriteReads: number;
    liveWriteAttempts: number;
}

/**
 * Drives one provider into the hard-cap regime on a document with a
 * permanent compaction failure, then delivers `liveWrites` single remote
 * writes, each one waited for (delivered and any compaction it triggered
 * settled) before the next.
 */
async function runHardCapStorm(db: Firestore, storage: FirebaseStorage, path: string, opts: StormOptions): Promise<StormResult> {
    const nextTs = timestampSequence();
    const writer = new Writer();

    if (opts.foldDue) {
        // Aged document: base snapshot in Cloud Storage + one history
        // segment short of the fold threshold, built by real compactions.
        const seederCtx: CompactionContext = {
            db, path, uid: 'seeder', lockTTL: 60_000, compactionLimit: COMPACTION_LIMIT,
            isDestroyed: () => false, storage, cachedClockOffset: 0,
        };
        await seedUpdates(db, path, [writer.bulk(opts.baseBytes ?? 64 * 1024)], nextTs, 'writer');
        const fold = await compact(seederCtx);
        expect(fold.type, 'seed fold').toBe('snapshot');
        for (let s = 0; s < DEFAULTS.HISTORY_FOLD_THRESHOLD - 1; s++) {
            await seedUpdates(db, path, [writer.next()[1], writer.next()[1]], nextTs, 'writer');
            const delta = await compact(seederCtx);
            expect(delta.type, 'seed delta').toBe('history');
        }
        expect(await historyCount(db, path)).toBe(DEFAULTS.HISTORY_FOLD_THRESHOLD - 1);
    }

    const docA = new Y.Doc();
    const providerA = new FireProvider({
        firebaseApp: deviceApp('poison-storm-device-a'),
        ydoc: docA,
        path,
        maxUpdatesThreshold: 10,
        maxWaitTime: 50,
    });
    const quarantined: string[] = [];
    providerA.on('corrupted-document', (e: any) => quarantined.push(e.docId));
    try {
        await waitForConditionTruthy(() => providerA.synced, { timeout: 30_000, interval: 50, message: 'provider A initial sync' });

        ctl.rejectFoldUploads = !!opts.rejectFoldUploads;
        ctl.c = ctl.freshCounters();
        ctl.counting = true;

        await writePoison(db, path, opts.poison, nextTs);
        if (opts.poison !== 'none') {
            await waitForConditionTruthy(() => quarantined.length > 0, {
                timeout: 20_000, interval: 50, message: 'provider A should quarantine the poison document',
            });
        }

        // The backlog arrives as ONE delivery that crosses REALTIME_LIMIT:
        // from here on every delivery triggers a compaction.
        const keys: string[] = [];
        const backlog: Uint8Array[] = [];
        for (let i = 0; i < opts.backlog; i++) {
            const [k, u] = writer.next();
            keys.push(k);
            backlog.push(u);
        }
        await seedUpdates(db, path, backlog, nextTs, 'writer');
        const lastKey = keys[keys.length - 1];
        await waitForConditionTruthy(() => docA.getMap('m').has(lastKey), {
            timeout: 30_000, interval: 25, message: 'provider A should apply the backlog',
        });
        await waitForConditionTruthy(() => !providerA.isCompacting, { timeout: 30_000, interval: 25, message: 'first compaction settles' });
        const pendingAtArm = opts.backlog + (opts.poison !== 'none' ? 1 : 0);
        const before = { reads: ctl.c.compUpdateDocs, attempts: ctl.c.compUpdateQueries };

        for (let i = 0; i < opts.liveWrites; i++) {
            const [key, update] = writer.next();
            await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
                update: Bytes.fromUint8Array(update),
                createdAt: serverTimestamp(),
                createdBy: 'writer',
            });
            await waitForConditionTruthy(() => docA.getMap('m').has(key), {
                timeout: 20_000, interval: 10, message: `provider A should receive live write ${i}`,
            });
            // The delivery's trigger decision runs before its updates are
            // applied, so an attempt it started is already in flight here.
            await waitForConditionTruthy(() => !providerA.isCompacting, { timeout: 30_000, interval: 10, message: 'compaction settles' });
        }

        ctl.counting = false;
        const c = ctl.c;
        return {
            c,
            pendingAtArm,
            pendingAtEnd: await pendingUpdateCount(db, path),
            historyAtEnd: await historyCount(db, path),
            fullAttemptReads: Math.min(pendingAtArm, COMPACTION_LIMIT),
            liveWriteReads: c.compUpdateDocs - before.reads,
            liveWriteAttempts: c.compUpdateQueries - before.attempts,
        };
    } finally {
        ctl.counting = false;
        ctl.rejectFoldUploads = false;
        await providerA.destroy();
        docA.destroy();
        writer.doc.destroy();
    }
}

function stormReport(opts: StormOptions, r: StormResult): string {
    return summarize(`${opts.label}: backlog ${r.pendingAtArm} pending (hard cap ${DEFAULTS.REALTIME_LIMIT}), ${opts.liveWrites} live writes`, r.c, [
        `live writes alone: ${r.liveWriteAttempts} lock-holding attempts, ${r.liveWriteReads} compaction update-doc reads ` +
            `(${(r.liveWriteReads / Math.max(1, opts.liveWrites)).toFixed(0)} per delivered write)`,
        `update docs pending: ${r.pendingAtArm} at arm -> ${r.pendingAtEnd} at end; history segments at end ${r.historyAtEnd}`,
        `compaction update-doc reads = ${(r.c.compUpdateDocs / r.fullAttemptReads).toFixed(1)} x one full backlog read (${r.fullAttemptReads})`,
    ]);
}

describe('Poison document compaction storm', () => {
    let db: Firestore;
    let storage: FirebaseStorage;
    let counter = 0;

    const newPath = (label: string) => `integration-tests/poison-storm-${label}-${getStableDate()}-${counter++}`;

    beforeAll(async () => {
        const setup = await setupEmulator();
        db = setup.db;
        storage = setup.storage;
    });

    afterEach(() => {
        ctl.counting = false;
        ctl.rejectFoldUploads = false;
    });

    // ------------------------------------------------------------------
    // bench: report-only scenarios
    // ------------------------------------------------------------------

    it('bench: every attempt re-reads the whole backlog and nothing drains (direct compact(), scaling with backlog)', { timeout: 120_000 }, async () => {
        const lines: string[] = [];
        const rows: { n: number; perAttempt: number; coldStart: number; pendingAfter: number }[] = [];
        for (const n of [25, 100]) {
            const path = newPath(`scaling-${n}`);
            const nextTs = timestampSequence();
            const writer = new Writer();
            await writePoison(db, path, 'inline-garbage', nextTs);
            await seedUpdates(db, path, Array.from({ length: n }, () => writer.next()[1]), nextTs, 'writer');

            const ctx: CompactionContext = {
                db, path, uid: `bench-compactor-${n}`, lockTTL: 60_000, compactionLimit: COMPACTION_LIMIT,
                isDestroyed: () => false, storage, cachedClockOffset: 0,
            };
            const attempts = 4;
            ctl.c = ctl.freshCounters();
            ctl.counting = true;
            for (let i = 0; i < attempts; i++) {
                const res = await compact(ctx);
                expect(res.success).toBe(false);
            }
            ctl.counting = false;
            const perAttempt = ctl.c.compUpdateDocs / attempts;
            lines.push(summarize(`direct compact() x${attempts}, ${n} valid + 1 poison`, ctl.c));

            // Cold start: initial sync reads everything still pending.
            ctl.c = ctl.freshCounters();
            ctl.counting = true;
            const coldDoc = new Y.Doc();
            const cold = new FireProvider({
                firebaseApp: deviceApp('poison-storm-cold'),
                ydoc: coldDoc,
                path,
                maxUpdatesThreshold: 100_000, // isolate initial sync from compaction
            });
            await waitForConditionTruthy(() => cold.synced, { timeout: 30_000, interval: 50, message: 'cold start sync' });
            ctl.counting = false;
            const coldStart = ctl.c.syncUpdateDocs;
            await cold.destroy();
            coldDoc.destroy();
            writer.doc.destroy();

            rows.push({ n, perAttempt, coldStart, pendingAfter: await pendingUpdateCount(db, path) });
        }
        const [a, b] = rows;
        console.log([
            ...lines,
            '[poison-storm] scaling (inline poison, direct compact()):',
            ...rows.map(r => `  backlog ${r.n} + 1: ${r.perAttempt} update-doc reads per failed attempt; ` +
                `${r.pendingAfter} still pending after 4 attempts; cold-start initial sync reads ${r.coldStart} update docs`),
            `  ratio backlog x${(b.n / a.n).toFixed(0)}: per-attempt reads x${(b.perAttempt / a.perAttempt).toFixed(2)}, ` +
                `cold-start reads x${(b.coldStart / a.coldStart).toFixed(2)}`,
        ].join('\n'));
        expect(a.pendingAfter).toBe(a.n + 1);
        expect(b.pendingAfter).toBe(b.n + 1);
    });

    it('bench: two active writers on a poisoned document (issue replica) vs healthy control', { timeout: 300_000 }, async () => {
        const edits = Number(process.env.POISON_STORM_EDITS ?? 260);
        const intervalMs = Number(process.env.POISON_STORM_INTERVAL_MS ?? 110);
        const out: string[] = [];
        const summary: Record<string, { reads: number; billed: number; attempts: number; failed: number; left: number; cold: number; coldComp: number; tx: number }> = {};

        for (const variant of ['poisoned', 'healthy'] as const) {
            const path = newPath(`replica-${variant}`);
            if (variant === 'poisoned') await writePoison(db, path, 'inline-garbage', timestampSequence());

            const docs = [new Y.Doc(), new Y.Doc()];
            const providers = docs.map((d, i) => new FireProvider({
                firebaseApp: deviceApp(`poison-storm-replica-${i}`),
                ydoc: d,
                path,
                maxUpdatesThreshold: 10,
                maxWaitTime: 50,
            }));
            await Promise.all(providers.map(p => waitForConditionTruthy(() => p.synced, { timeout: 30_000, interval: 50, message: 'replica sync' })));

            ctl.c = ctl.freshCounters();
            ctl.counting = true;
            const t0 = Date.now();
            for (let i = 0; i < edits; i++) {
                const d = docs[i % 2];
                d.getMap('m').set(`e${i}`, `${VALUE}-${i}`);
                await new Promise(r => setTimeout(r, intervalMs));
            }
            const wallS = (Date.now() - t0) / 1000;
            // Let the last saves and any running compaction settle.
            await new Promise(r => setTimeout(r, 1500));
            await waitFor(() => providers.every(p => !p.isCompacting), ok => ok, { timeout: 60_000, interval: 50 });
            ctl.counting = false;
            const storm = ctl.c;
            await Promise.all(providers.map(p => p.destroy()));
            docs.forEach(d => d.destroy());
            const left = await pendingUpdateCount(db, path);

            // Cold start of a third device after the storm.
            ctl.c = ctl.freshCounters();
            ctl.counting = true;
            const coldDoc = new Y.Doc();
            const cold = new FireProvider({
                firebaseApp: deviceApp('poison-storm-replica-cold'),
                ydoc: coldDoc,
                path,
                maxUpdatesThreshold: 10,
                maxWaitTime: 50,
            });
            await waitForConditionTruthy(() => cold.synced, { timeout: 30_000, interval: 50, message: 'cold start sync' });
            await new Promise(r => setTimeout(r, 500));
            await waitFor(() => !cold.isCompacting, ok => ok, { timeout: 60_000, interval: 50 });
            ctl.counting = false;
            const coldC = ctl.c;
            await cold.destroy();
            coldDoc.destroy();

            summary[variant] = {
                reads: storm.compUpdateDocs,
                billed: compactionReads(storm),
                attempts: storm.compUpdateQueries,
                failed: storm.failedCompactions,
                left,
                cold: coldC.syncUpdateDocs,
                coldComp: coldC.compUpdateDocs,
                tx: storm.compTransactions,
            };
            out.push(summarize(`${variant}: 2 providers, threshold 10, ${edits} edits over ${wallS.toFixed(1)} s`, storm, [
                `per edit: ${(storm.compUpdateDocs / edits).toFixed(2)} compaction update-doc reads, ` +
                    `${(compactionReads(storm) / edits).toFixed(2)} billed compaction reads`,
                `update docs left at end: ${left}`,
                `cold start (3rd device): initial-sync update-doc reads ${coldC.syncUpdateDocs}; ` +
                    `its first compaction read ${coldC.compUpdateDocs} update docs (${coldC.failedCompactions} failed)`,
            ]));
        }
        const p = summary.poisoned, h = summary.healthy;
        out.push(
            '[poison-storm] replica poisoned vs healthy:',
            `  lock-holding attempts ${p.attempts} vs ${h.attempts} (failed ${p.failed} vs ${h.failed}); lock/commit transactions ${p.tx} vs ${h.tx}`,
            `  compaction update-doc reads ${p.reads} vs ${h.reads} (${(p.reads / Math.max(1, h.reads)).toFixed(1)}x); ` +
                `billed compaction reads per edit ${(p.billed / edits).toFixed(2)} vs ${(h.billed / edits).toFixed(2)}`,
            `  update docs left ${p.left} vs ${h.left}; cold-start initial-sync update-doc reads ${p.cold} vs ${h.cold}`,
        );
        console.log(out.join('\n'));
    });

    it('bench: fold-due document whose snapshot upload is rejected (per failed attempt: snapshot download + full merge)', { timeout: 120_000 }, async () => {
        const opts: StormOptions = {
            label: 'fold upload rejected', poison: 'none', foldDue: true, baseBytes: 256 * 1024,
            rejectFoldUploads: true, backlog: DEFAULTS.REALTIME_LIMIT, liveWrites: 4,
        };
        const r = await runHardCapStorm(db, storage, newPath('fold-rejected-bench'), opts);
        const attempts = Math.max(1, r.c.compUpdateQueries);
        console.log([
            stormReport(opts, r),
            `  per failed fold attempt: ${fmtKB(r.c.compDownloadBytes / attempts)} snapshot downloaded, ` +
                `${fmtKB(r.c.compMergeInputBytes / attempts)} merged, ${fmtKB(r.c.compUploadBytes / attempts)} upload rejected`,
        ].join('\n'));
    });

    // ------------------------------------------------------------------
    // regression: N deliveries after a permanent failure must not cost
    // N full backlog reads
    // ------------------------------------------------------------------

    const LIVE_WRITES = 8;

    const regressionCases: StormOptions[] = [
        {
            label: 'inline poison pill',
            poison: 'inline-garbage',
            backlog: DEFAULTS.REALTIME_LIMIT - 1, // + poison = exactly the hard cap, all within one attempt's read
            liveWrites: LIVE_WRITES,
        },
        {
            label: 'storage-backed update with missing blob',
            poison: 'missing-blob',
            backlog: DEFAULTS.REALTIME_LIMIT - 1,
            liveWrites: LIVE_WRITES,
        },
        {
            label: 'fold upload rejected by Storage',
            poison: 'none',
            foldDue: true,
            baseBytes: 64 * 1024,
            rejectFoldUploads: true,
            backlog: DEFAULTS.REALTIME_LIMIT,
            liveWrites: LIVE_WRITES,
        },
    ];

    for (const opts of regressionCases) {
        it(`regression: ${opts.label} — ${LIVE_WRITES} later deliveries cost at most one more full backlog read`, { timeout: 180_000 }, async () => {
            const r = await runHardCapStorm(db, storage, newPath(opts.label.replace(/[^a-z]+/gi, '-')), opts);
            const report = stormReport(opts, r);
            console.log(report);

            // Sanity: the scenario really is a permanent failure in the
            // hard-cap regime, and the first attempt read the full backlog.
            expect(r.pendingAtArm).toBeGreaterThanOrEqual(DEFAULTS.REALTIME_LIMIT);
            expect(r.c.compUpdateQueries, `no compaction ran at all\n${report}`).toBeGreaterThanOrEqual(1);

            // The pin: whether a fix backs off after a non-retryable
            // failure or isolates the poison and drains the backlog, the
            // N deliveries that follow cost at most ONE more full read of
            // the backlog by compaction — not one per delivery. Unfixed:
            // every delivery re-reads min(backlog, compactionLimit) docs,
            // i.e. (N + 1) x.
            expect(
                r.c.compUpdateDocs,
                `compaction re-read the poisoned backlog on every delivery\n${report}`,
            ).toBeLessThanOrEqual(2 * r.fullAttemptReads);
            expect(
                r.c.compUpdateQueries,
                `every delivery started another lock-holding compaction attempt\n${report}`,
            ).toBeLessThanOrEqual(2);
        });
    }
});
