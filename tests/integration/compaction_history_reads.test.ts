/**
 * Delta compaction downloads every existing history segment to count it
 *
 * Before choosing between DELTA and FOLD, `compact()` runs `getDocs` on up
 * to MAX_COMPACTION_HISTORY + 1 history documents, segment bytes included.
 * The delta path only uses the number of segments (`shouldUseDelta` looks
 * at `historyToMerge.length`) and never touches the payloads, so the k-th
 * delta cycle after a fold re-downloads all k segments the previous delta
 * cycles wrote — data every client already holds. That is O(k) billed
 * reads and segment bytes per delta cycle, ~T²/2 per fold period of
 * `historyFoldThreshold` T; raising T (the only lever on the recurring
 * O(document) fold transfer) makes it quadratically worse.
 *
 * This file drives `compact()` directly against the emulator with a
 * versicle-shaped workload (`benchmarks/versicle-workload.ts`, 50 debounced
 * saves per cycle = DEFAULTS.MAX_UPDATES_THRESHOLD) and counts the
 * compactor's own Firestore reads by wrapping `getDocs` / `getDoc` /
 * `getCountFromServer` / `getAggregateFromServer` in `@firebase/firestore`
 * (the module `src/compaction.ts` imports). Everything is counted, nothing
 * is timed:
 *
 *  - `bench:` logs a per-cycle table for one fold period at the default
 *    threshold (8) and at 4× that (32): history documents and bytes the
 *    compactor downloaded, and the billed reads its history query cost.
 *  - The regression test pins the contract: a delta cycle's history reads
 *    do not depend on how many segments already exist. It also checks that
 *    the fold still merges every segment and that a fresh client
 *    reconstructs the source document from snapshot + history + updates.
 *
 * Billing model (Firestore): a query costs one read per document returned,
 * minimum one; a count aggregation costs one read per 1000 index entries
 * matched, minimum one; a single-document get costs one read. Byte sizes
 * use Firestore's documented storage-size formula
 * (https://firebase.google.com/docs/firestore/storage-size), a stable,
 * deterministic proxy for the transferred document size.
 *
 * Run (emulator, through the isolation wrapper):
 *   bash scripts/test.sh tests/integration/compaction_history_reads.test.ts
 *   bash scripts/test.sh tests/integration/compaction_history_reads.test.ts -t bench:
 *
 * @file compaction_history_reads.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

/** One compactor-side read, as seen through the wrapped Firestore API. */
interface ReadEvent {
    api: 'getDocs' | 'getDoc' | 'count';
    /** Last path segment of the queried collection / the doc's collection. */
    collection: string;
    /** Documents returned (0 for an aggregation). */
    docs: number;
    /** Sum of Bytes-field payload sizes (segment / update / content). */
    payloadBytes: number;
    /** Firestore storage-size estimate of the returned documents. */
    docBytes: number;
    /** Billed document reads. */
    billed: number;
}

const { io } = vi.hoisted(() => ({
    io: {
        active: false,
        events: [] as ReadEvent[],
    },
}));

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();

    const utf8 = (s: string) => new TextEncoder().encode(s).byteLength;

    /** Firestore storage size of one field value. */
    const valueSize = (v: any): number => {
        if (v === null || v === undefined) return 1;
        if (typeof v === 'boolean') return 1;
        if (typeof v === 'number') return 8;
        if (typeof v === 'string') return utf8(v) + 1;
        if (v instanceof actual.Bytes) return v.toUint8Array().byteLength;
        if (v instanceof actual.Timestamp) return 8;
        if (v instanceof actual.GeoPoint) return 16;
        if (v instanceof actual.DocumentReference) return utf8(v.path) + 16;
        if (Array.isArray(v)) return v.reduce((n: number, x: any) => n + valueSize(x), 0);
        if (typeof v === 'object') {
            return Object.entries(v).reduce((n, [k, x]) => n + utf8(k) + 1 + valueSize(x), 0);
        }
        return 0;
    };

    const payloadSize = (data: Record<string, any>): number => {
        let n = 0;
        for (const x of Object.values(data)) {
            if (x instanceof actual.Bytes) n += x.toUint8Array().byteLength;
        }
        return n;
    };

    /** Document size = name + fields + 32 (Firestore storage-size formula). */
    const docSize = (snap: any): number => {
        const name = snap.ref.path.split('/').reduce((n: number, seg: string) => n + utf8(seg) + 1, 0) + 16;
        return name + valueSize(snap.data() ?? {}) + 32;
    };

    const queryCollection = (q: any, snap: any): string => {
        try {
            const seg = q?._query?.path?.lastSegment?.();
            if (typeof seg === 'string' && seg.length > 0) return seg;
        } catch { /* fall through */ }
        return snap?.docs?.[0]?.ref?.parent?.id ?? q?.id ?? '?';
    };

    const recordAggregate = (q: any, snap: any) => {
        if (!io.active) return;
        const data = snap.data?.() ?? {};
        const count = typeof data.count === 'number' ? data.count : 0;
        io.events.push({
            api: 'count',
            collection: queryCollection(q, null),
            docs: 0,
            payloadBytes: 0,
            docBytes: 0,
            billed: Math.max(1, Math.ceil(count / 1000)),
        });
    };

    return {
        ...actual,
        getDocs: async (q: any) => {
            const snap = await actual.getDocs(q);
            if (io.active) {
                io.events.push({
                    api: 'getDocs',
                    collection: queryCollection(q, snap),
                    docs: snap.docs.length,
                    payloadBytes: snap.docs.reduce((n: number, d: any) => n + payloadSize(d.data()), 0),
                    docBytes: snap.docs.reduce((n: number, d: any) => n + docSize(d), 0),
                    billed: Math.max(1, snap.docs.length),
                });
            }
            return snap;
        },
        getDoc: async (r: any) => {
            const snap = await actual.getDoc(r);
            if (io.active) {
                io.events.push({
                    api: 'getDoc',
                    collection: r?.parent?.id ?? '?',
                    docs: snap.exists() ? 1 : 0,
                    payloadBytes: snap.exists() ? payloadSize(snap.data()) : 0,
                    docBytes: snap.exists() ? docSize(snap) : 0,
                    billed: 1,
                });
            }
            return snap;
        },
        getCountFromServer: async (q: any) => {
            const snap = await actual.getCountFromServer(q);
            recordAggregate(q, snap);
            return snap;
        },
        getAggregateFromServer: async (q: any, spec: any) => {
            const snap = await actual.getAggregateFromServer(q, spec);
            recordAggregate(q, snap);
            return snap;
        },
    };
});

import * as Y from 'yjs';
import {
    collection,
    doc,
    getDoc,
    getDocs,
    writeBatch,
    serverTimestamp,
    Bytes,
    Firestore,
} from 'firebase/firestore';
import { FirebaseStorage, ref, getBytes } from 'firebase/storage';
import { compact, CompactionContext, CompactionResult } from '../../src/compaction';
import { DEFAULTS, FIRESTORE_PATHS } from '../../src/types';
import { setupEmulator } from '../utils/emulator';
import { getStableDate } from '../unit/prng';
import {
    createSim,
    runSession,
    clientIdForSession,
    materializeVersicleDoc,
    VersicleSimState,
} from '../../benchmarks/versicle-workload';

const SEED = 20261002;
/** Debounced saves per compaction cycle (the provider's default trigger). */
const UPDATES_PER_CYCLE = DEFAULTS.MAX_UPDATES_THRESHOLD;

/** What one compaction cycle cost, from the compactor's point of view. */
interface CycleRow {
    cycle: number;
    type: CompactionResult['type'];
    /** History segments that existed when the cycle started. */
    existingSegments: number;
    historySegmentsMerged: number;
    /** History documents the compactor downloaded (getDocs). */
    historyDocs: number;
    historyPayloadBytes: number;
    historyDocBytes: number;
    /** Billed reads attributable to the history collection. */
    historyBilled: number;
    /** All billed reads outside transactions (updates + history + main doc). */
    totalBilled: number;
    /** All document bytes downloaded outside transactions. */
    totalDocBytes: number;
}

/**
 * A versicle-shaped author: a fresh Y.Doc (and clientID) per session,
 * hydrated from the previous session's state, one blob per debounced save.
 */
class VersicleAuthor {
    private sim: VersicleSimState = createSim({ seed: SEED });
    private session = 0;
    private queue: Uint8Array[] = [];
    doc: Y.Doc | null = null;

    take(n: number): Uint8Array[] {
        while (this.queue.length < n) {
            const next = new Y.Doc();
            next.clientID = clientIdForSession(SEED, this.session++);
            if (this.doc) {
                Y.applyUpdate(next, Y.encodeStateAsUpdate(this.doc));
                this.doc.destroy();
            }
            this.doc = next;
            this.queue.push(...runSession(this.sim, next).blobs);
        }
        return this.queue.splice(0, n);
    }
}

describe('Delta compaction history reads', () => {
    let db: Firestore;
    let storage: FirebaseStorage;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        db = setup.db as Firestore;
        storage = setup.storage as FirebaseStorage;
        path = `tests/compaction-history-reads-${getStableDate()}-${Date.now()}-${counter++}`;
        io.active = false;
        io.events.length = 0;
    });

    async function seedUpdates(blobs: Uint8Array[]): Promise<void> {
        // One batch per cycle (<= 500 writes). The provider's update documents
        // carry more metadata; compaction only consumes `update` + `createdAt`.
        const batch = writeBatch(db);
        for (const blob of blobs) {
            batch.set(doc(collection(db, path, FIRESTORE_PATHS.UPDATES)), {
                update: Bytes.fromUint8Array(blob),
                createdAt: serverTimestamp(),
            });
        }
        await batch.commit();
    }

    async function historyCount(): Promise<number> {
        return (await getDocs(collection(db, path, FIRESTORE_PATHS.HISTORY))).size;
    }

    /**
     * Runs `cycles` compaction cycles of UPDATES_PER_CYCLE versicle saves each
     * and returns what each cycle read, plus every blob written (for the
     * convergence check).
     */
    async function runCycles(historyFoldThreshold: number, cycles: number): Promise<{ rows: CycleRow[]; written: Uint8Array[] }> {
        const author = new VersicleAuthor();
        const written: Uint8Array[] = [];
        const rows: CycleRow[] = [];
        const ctx: CompactionContext = {
            db,
            path,
            uid: 'history-reads-compactor',
            lockTTL: DEFAULTS.LOCK_TTL,
            compactionLimit: DEFAULTS.COMPACTION_LIMIT,
            isDestroyed: () => false,
            storage,
            historyFoldThreshold,
        };

        for (let cycle = 0; cycle < cycles; cycle++) {
            const blobs = author.take(UPDATES_PER_CYCLE);
            written.push(...blobs);
            await seedUpdates(blobs);
            const existingSegments = await historyCount();

            io.events.length = 0;
            io.active = true;
            let result: CompactionResult;
            try {
                result = await compact(ctx);
            } finally {
                io.active = false;
            }
            expect(result.success).toBe(true);

            const history = io.events.filter(e => e.collection === FIRESTORE_PATHS.HISTORY);
            rows.push({
                cycle,
                type: result.type,
                existingSegments,
                historySegmentsMerged: result.historySegmentsMerged,
                historyDocs: history.reduce((n, e) => n + e.docs, 0),
                historyPayloadBytes: history.reduce((n, e) => n + e.payloadBytes, 0),
                historyDocBytes: history.reduce((n, e) => n + e.docBytes, 0),
                historyBilled: history.reduce((n, e) => n + e.billed, 0),
                totalBilled: io.events.reduce((n, e) => n + e.billed, 0),
                totalDocBytes: io.events.reduce((n, e) => n + e.docBytes, 0),
            });
        }
        author.doc?.destroy();
        return { rows, written };
    }

    /** A fresh client's view: snapshot (Storage) + history segments + updates. */
    async function materializeServer(): Promise<string> {
        const main = await getDoc(doc(db, path));
        const data = main.data() ?? {};
        let snapshot: Uint8Array | null = null;
        if (data.snapshotStoragePath) {
            snapshot = new Uint8Array(await getBytes(ref(storage, data.snapshotStoragePath)));
        }
        const pending: Uint8Array[] = [];
        for (const h of (await getDocs(collection(db, path, FIRESTORE_PATHS.HISTORY))).docs) {
            pending.push((h.data().segment as Bytes).toUint8Array());
        }
        for (const u of (await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES))).docs) {
            pending.push((u.data().update as Bytes).toUint8Array());
        }
        return materializeVersicleDoc(snapshot, pending);
    }

    const kb = (n: number) => `${(n / 1024).toFixed(1)} KB`;

    function printTable(label: string, rows: CycleRow[]): void {
        const lines = rows.map(r =>
            `  ${String(r.cycle).padStart(3)}  ${String(r.type).padEnd(8)}  ` +
            `${String(r.existingSegments).padStart(4)}  ${String(r.historyDocs).padStart(5)}  ` +
            `${kb(r.historyPayloadBytes).padStart(9)}  ${kb(r.historyDocBytes).padStart(9)}  ` +
            `${String(r.historyBilled).padStart(6)}  ${String(r.totalBilled).padStart(6)}  ${kb(r.totalDocBytes).padStart(9)}`
        );
        const delta = rows.filter(r => r.type === 'history');
        const sum = (f: (r: CycleRow) => number, rs: CycleRow[]) => rs.reduce((n, r) => n + f(r), 0);
        console.log([
            `[history reads] ${label}`,
            `  cyc  type      segs  hDocs   hPayload    hDocSz  hBill   bill    allDocSz`,
            ...lines,
            `  delta cycles: ${delta.length}; history docs downloaded ${sum(r => r.historyDocs, delta)}, ` +
            `payload ${kb(sum(r => r.historyPayloadBytes, delta))}, doc bytes ${kb(sum(r => r.historyDocBytes, delta))}, ` +
            `history billed reads ${sum(r => r.historyBilled, delta)} ` +
            `(of ${sum(r => r.totalBilled, delta)} billed / ${kb(sum(r => r.totalDocBytes, delta))} downloaded by delta cycles)`,
        ].join('\n'));
    }

    it('bench: history reads per compaction cycle across one fold period (T=8 and T=32)', { timeout: 300_000 }, async () => {
        for (const threshold of [DEFAULTS.HISTORY_FOLD_THRESHOLD, DEFAULTS.HISTORY_FOLD_THRESHOLD * 4]) {
            path = `tests/compaction-history-reads-${getStableDate()}-${Date.now()}-${counter++}`;
            // Initial fold (no base yet), T-1 delta cycles, then the fold.
            const { rows } = await runCycles(threshold, threshold + 1);
            printTable(`historyFoldThreshold=${threshold}, ${UPDATES_PER_CYCLE} versicle saves/cycle`, rows);
            expect(rows.map(r => r.type)).toEqual([
                'snapshot',
                ...Array(threshold - 1).fill('history'),
                'snapshot',
            ]);
        }
    });

    it('a delta cycle does not download the history segments it only needs to count', { timeout: 180_000 }, async () => {
        const threshold = DEFAULTS.HISTORY_FOLD_THRESHOLD;
        const { rows, written } = await runCycles(threshold, threshold + 1);
        printTable(`historyFoldThreshold=${threshold} (regression)`, rows);

        // Mode sequence: initial fold, T-1 deltas, fold of all T-1 segments.
        expect(rows.map(r => r.type)).toEqual([
            'snapshot',
            ...Array(threshold - 1).fill('history'),
            'snapshot',
        ]);
        const delta = rows.filter(r => r.type === 'history');
        expect(delta.map(r => r.existingSegments)).toEqual(delta.map((_, k) => k));
        expect(rows[rows.length - 1].historySegmentsMerged).toBe(threshold - 1);

        // Nothing was lost: a fresh client rebuilds the author's document.
        // (Parsed: Y.Map key order depends on integration order, so the JSON
        // strings of equal documents may differ.)
        expect(JSON.parse(await materializeServer())).toEqual(JSON.parse(materializeVersicleDoc(null, written)));

        // The contract: a delta cycle's history reads are independent of
        // how many segments already exist. The delta with T-2 segments in
        // place must not download (or be billed for) more than the delta
        // that started from an empty history.
        const first = delta[0];
        const last = delta[delta.length - 1];
        expect(last.existingSegments).toBe(threshold - 2);
        expect({
            historyDocs: delta.map(r => r.historyDocs),
            historyPayloadBytes: delta.map(r => r.historyPayloadBytes),
            historyBilled: delta.map(r => r.historyBilled),
        }).toEqual({
            historyDocs: delta.map(() => first.historyDocs),
            historyPayloadBytes: delta.map(() => first.historyPayloadBytes),
            historyBilled: delta.map(() => first.historyBilled),
        });
        expect(last.historyDocBytes).toBeLessThanOrEqual(first.historyDocBytes);
    });
});
