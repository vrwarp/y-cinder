/**
 * Perf regression: initial sync must not round-trip blobs through
 * Firestore `Bytes`.
 *
 * `performInitialSync` downloads the base snapshot (and storage-backed
 * updates, and an offloaded delete-set fingerprint) from Cloud Storage as
 * an ArrayBuffer, then wraps it with `Bytes.fromUint8Array` only so it can
 * sit in `data.content` / `data.update`. In @firebase/firestore that call
 * builds a binary string one `String.fromCharCode` concatenation per
 * byte — a cons-string rope ~32x the blob size, superlinear in time — and
 * every reader on the initial-sync path then copies the whole blob back
 * out with `toUint8Array()` (a charCodeAt loop): `buildServerCoverage`,
 * `applyItem`, `collectServerBlobs` (eagerly, for EVERY blob including the
 * snapshot, before the push guard's smallest-first early exit), and
 * `processUpdateMetadata` for the metadata-less fingerprint. On an aged
 * 3.5 MB snapshot this is ~0.4 s of main thread and >100 MB of transient
 * heap per snapshot-downloading cold start (see
 * benchmarks/initial-sync-bytes.bench.ts).
 *
 * Firestore and Storage are faked at the SDK boundary (queries, getDoc,
 * getBytes); the REAL `Bytes` class is kept and instrumented, so the
 * counters see exactly the conversions the production code performs. The
 * server state has the shape the real provider/compaction code writes.
 *
 * Contract pinned (operation counters, no timing):
 *  - bytes downloaded from Storage are never wrapped into `Bytes`;
 *  - `toUint8Array()` never copies more than one pass over the inline
 *    (Firestore-delivered) blobs — Storage bytes are never copied back out;
 *  - each inline `Bytes` value is converted at most once.
 */
import { describe, it, expect, vi, beforeAll, afterEach } from 'vitest';
import * as Y from 'yjs';
import { toBase64 } from 'lib0/buffer';
import { Bytes } from '@firebase/firestore';
import { performInitialSync } from '../../src/sync';
import { mergeUpdatesWithMeta } from '../../src/merge-core';
import { aggregateClockEnds, extractClockEnds, updateHasDeletions } from '../../src/update-metadata';
import { SeededRandom } from './prng';

// ---------------------------------------------------------------------------
// Firestore / Storage fake (SDK boundary only — Bytes stays real)
// ---------------------------------------------------------------------------

const server = vi.hoisted(() => ({
    /** Update documents, in createdAt order. */
    updates: [] as Record<string, any>[],
    /** History segment documents, in startTime order. */
    history: [] as Record<string, any>[],
    /** Main document (null = does not exist). */
    main: null as Record<string, any> | null,
    /** Cloud Storage objects by path. */
    storage: new Map<string, Uint8Array>(),
    /** Storage downloads served (getBytes calls / bytes). */
    downloads: 0,
    downloadedBytes: 0,
    /** Documents written by the client (initial-sync push). */
    added: 0,
}));

vi.mock('@firebase/firestore', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/firestore')>();
    const join = (parts: unknown[]) => parts.filter(p => typeof p === 'string').join('/');
    const asSnapshot = (rows: Record<string, any>[]) => {
        // A fresh data object per call, like the SDK: performInitialSync
        // writes into it (content, _decodedSV).
        const docs = rows.map((row, i) => ({
            id: `doc-${i}`,
            ref: { path: `doc-${i}` },
            metadata: { hasPendingWrites: false },
            data: () => ({ ...row }),
        }));
        return {
            docs,
            empty: docs.length === 0,
            size: docs.length,
            forEach: (fn: (d: unknown) => void) => docs.forEach(fn),
        };
    };
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
            const page = asSnapshot(rows);
            const docs = page.docs.slice(start, start + max);
            return { ...page, docs, empty: docs.length === 0, size: docs.length, forEach: (fn: (d: unknown) => void) => docs.forEach(fn) };
        },
        getDoc: async () => ({
            exists: () => server.main !== null,
            data: () => (server.main ? { ...server.main } : undefined),
        }),
        addDoc: async () => {
            server.added++;
            return { id: 'added' };
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
            if (!blob) {
                throw Object.assign(new Error(`object-not-found: ${r.fullPath}`), { code: 'storage/object-not-found' });
            }
            server.downloads++;
            server.downloadedBytes += blob.byteLength;
            return blob.slice().buffer; // a fresh ArrayBuffer, like a download
        },
        uploadBytes: async () => undefined,
        deleteObject: async () => undefined,
    };
});

// ---------------------------------------------------------------------------
// Bytes instrumentation
// ---------------------------------------------------------------------------

interface ConversionCounters {
    wrapCalls: number;
    wrapBytes: number;
    unwrapCalls: number;
    unwrapBytes: number;
    /** toUint8Array calls per Bytes instance */
    unwrapsPerInstance: Map<object, number>;
}

function instrumentBytes(): { counters: ConversionCounters; restore: () => void } {
    const origFrom = Bytes.fromUint8Array;
    const origTo = Bytes.prototype.toUint8Array;
    const counters: ConversionCounters = {
        wrapCalls: 0, wrapBytes: 0, unwrapCalls: 0, unwrapBytes: 0,
        unwrapsPerInstance: new Map(),
    };
    Bytes.fromUint8Array = function (array: Uint8Array): Bytes {
        counters.wrapCalls++;
        counters.wrapBytes += array.byteLength;
        return origFrom.call(Bytes, array);
    };
    Bytes.prototype.toUint8Array = function (this: Bytes): Uint8Array {
        const out = origTo.call(this);
        counters.unwrapCalls++;
        counters.unwrapBytes += out.byteLength;
        counters.unwrapsPerInstance.set(this, (counters.unwrapsPerInstance.get(this) ?? 0) + 1);
        return out;
    };
    return {
        counters,
        restore: () => {
            Bytes.fromUint8Array = origFrom;
            Bytes.prototype.toUint8Array = origTo;
        },
    };
}

/**
 * Inline Firestore bytes as the browser SDK delivers them: decoded from the
 * wire's base64 (a flat string — no rope to flatten on first read).
 */
function inlineBytes(u: Uint8Array): Bytes {
    return Bytes.fromBase64String(toBase64(u));
}

// ---------------------------------------------------------------------------
// Server fixture: an aged document in the provider's tiered storage shape
// ---------------------------------------------------------------------------

const PATH = 'docs/initial-sync-bytes';
const ROOTS = ['settings', 'progress', 'annotations'];

/** One editing session with a fresh clientID; returns one blob per save. */
function editSession(doc: Y.Doc, rng: SeededRandom, clientID: number, saves: number): Uint8Array[] {
    doc.clientID = clientID;
    const blobs: Uint8Array[] = [];
    const onUpdate = (u: Uint8Array) => blobs.push(u);
    doc.on('update', onUpdate);
    for (let s = 0; s < saves; s++) {
        doc.transact(() => {
            // Same-key overwrites (page turns, settings) leave tombstones…
            doc.getMap('progress').set(`book-${rng.int(0, 7)}`, rng.string(24));
            doc.getMap('settings').set(`k-${rng.int(0, 3)}`, rng.int(0, 1_000_000));
            // …and text churn leaves real deletions in the delete-set.
            const note = doc.getText('note');
            note.insert(rng.int(0, note.length), rng.string(rng.int(20, 60)) + ' ');
            if (note.length > 1_500) note.delete(rng.int(0, note.length - 300), rng.int(50, 250));
            if (s % 5 === 0) doc.getMap('annotations').set(`a-${s}-${clientID}`, rng.string(40));
        });
    }
    doc.off('update', onUpdate);
    return blobs;
}

interface Fixture {
    /** The server's full state (snapshot + history + updates). */
    expected: Y.Doc;
    snapshotBytes: number;
    /** Sum of the blobs delivered inline as Firestore Bytes. */
    inlineBytes: number;
    /** Sum of the blobs that must be downloaded from Storage. */
    storageBytes: number;
}

function buildServer(fingerprint: 'inline' | 'offloaded'): Fixture {
    server.updates = [];
    server.history = [];
    server.main = null;
    server.storage.clear();
    server.downloads = 0;
    server.downloadedBytes = 0;
    server.added = 0;

    const rng = new SeededRandom(fingerprint === 'inline' ? 4242 : 2424);
    const doc = new Y.Doc();
    for (const r of ROOTS) doc.getMap(r);
    for (let session = 0; session < 40; session++) {
        editSession(doc, rng, 10_000 + session, 30);
    }

    // Tier 1: a fold — snapshot in Storage, delete-set fingerprint inline
    // or offloaded, exactly the fields compaction writes.
    const fold = mergeUpdatesWithMeta([Y.encodeStateAsUpdate(doc)], { gc: true });
    server.storage.set(`${PATH}/snapshots/fold-1.bin`, fold.result);
    server.main = {
        snapshotStoragePath: `${PATH}/snapshots/fold-1.bin`,
        stateVector: toBase64(fold.stateVector),
        version: 1,
    };
    let inline = 0;
    let storage = fold.result.byteLength;
    if (fingerprint === 'inline') {
        server.main.deleteSet = inlineBytes(fold.dsUpdate);
        inline += fold.dsUpdate.byteLength;
    } else {
        server.main.deleteSetStoragePath = `${PATH}/snapshots/fold-1.ds.bin`;
        server.storage.set(server.main.deleteSetStoragePath, fold.dsUpdate);
        storage += fold.dsUpdate.byteLength;
    }

    // Tier 2: one delta-compaction history segment (another device).
    const segBlobs = editSession(doc, rng, 20_001, 20);
    const seg = mergeUpdatesWithMeta(segBlobs, { gc: false });
    server.history.push({
        stateVector: toBase64(seg.stateVector),
        ...(updateHasDeletions(seg.dsUpdate) ? { hasDeletions: true } : {}),
        createdBy: 'device-b',
        segment: inlineBytes(seg.result),
    });
    inline += seg.result.byteLength;

    // Tier 3: per-save update documents, one of them storage-backed
    // (as provider._save writes oversized batches).
    const saves = editSession(doc, rng, 20_002, 12);
    saves.forEach((u, i) => {
        const meta = aggregateClockEnds(extractClockEnds(u));
        if (i === 6) {
            const p = `${PATH}/large_updates/device-c_${i}.bin`;
            server.storage.set(p, u);
            server.updates.push({ createdBy: 'device-c', updateStoragePath: p, ...meta });
            storage += u.byteLength;
        } else {
            server.updates.push({ createdBy: 'device-c', update: inlineBytes(u), ...meta });
            inline += u.byteLength;
        }
    });

    return { expected: doc, snapshotBytes: fold.result.byteLength, inlineBytes: inline, storageBytes: storage };
}

function contentOf(doc: Y.Doc): Record<string, unknown> {
    return {
        ...Object.fromEntries(ROOTS.map(r => [r, doc.getMap(r).toJSON()])),
        note: doc.getText('note').toString(),
    };
}

// ---------------------------------------------------------------------------

describe('initial sync: Storage downloads are not round-tripped through Firestore Bytes', () => {
    let instrumentation: ReturnType<typeof instrumentBytes> | null = null;

    beforeAll(() => {
        vi.spyOn(console, 'log').mockImplementation(() => { });
    });

    afterEach(() => {
        instrumentation?.restore();
        instrumentation = null;
    });

    for (const fingerprint of ['inline', 'offloaded'] as const) {
        it(`fresh client, ${fingerprint} delete-set fingerprint: no wrap, at most one copy per inline blob`, async () => {
            const fx = buildServer(fingerprint);
            const client = new Y.Doc();

            instrumentation = instrumentBytes();
            const result = await performInitialSync({
                db: {} as any,
                storage: {} as any,
                path: PATH,
                doc: client,
                uid: 'fresh-client',
                maxUpdatesThreshold: 50,
                isDestroyed: () => false,
            });
            const c = instrumentation.counters;
            instrumentation.restore();
            instrumentation = null;

            // The scenario is the real one: a cold start that downloads
            // the snapshot and converges with nothing to push.
            expect(result.success).toBe(true);
            expect(result.localUpdatesPushed).toBe(false);
            expect(server.added).toBe(0);
            expect(server.downloadedBytes).toBe(fx.storageBytes);
            expect(contentOf(client)).toEqual(contentOf(fx.expected));
            expect(Y.encodeStateVector(client)).toEqual(Y.encodeStateVector(fx.expected));

            const maxUnwrapsPerBlob = Math.max(0, ...c.unwrapsPerInstance.values());
            const report =
                `snapshot ${fx.snapshotBytes} B, storage ${fx.storageBytes} B, inline ${fx.inlineBytes} B | ` +
                `wrapped ${c.wrapCalls}x/${c.wrapBytes} B, unwrapped ${c.unwrapCalls}x/${c.unwrapBytes} B ` +
                `(${(c.unwrapBytes / fx.inlineBytes).toFixed(2)} passes over inline), ` +
                `max ${maxUnwrapsPerBlob} conversions of one Bytes value`;

            // 1. Bytes downloaded from Storage are already a Uint8Array:
            //    wrapping them costs one string concat per byte and a rope
            //    ~32x the blob size. Nothing is pushed here, so no wrap at all.
            expect(c.wrapBytes, `Storage downloads wrapped in Bytes — ${report}`).toBe(0);

            // 2. No copy of Storage bytes back out of Bytes: at most one
            //    pass over the inline blobs in total.
            expect(c.unwrapBytes, `toUint8Array copied more than the inline blobs — ${report}`)
                .toBeLessThanOrEqual(fx.inlineBytes);

            // 3. Each inline Firestore Bytes value is converted once and
            //    reused by coverage, apply, metadata and the push guard.
            expect(maxUnwrapsPerBlob, `an inline blob was converted repeatedly — ${report}`)
                .toBeLessThanOrEqual(1);
        });
    }
});
