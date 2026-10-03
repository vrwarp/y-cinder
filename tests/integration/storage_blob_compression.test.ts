/**
 * Performance regression test: Cloud Storage blobs travel uncompressed.
 *
 * Every blob y-cinder keeps in Cloud Storage — fold snapshots, squash
 * snapshots, offloaded large updates, offloaded delete-set fingerprints —
 * is uploaded with `uploadBytes(ref, rawV1)` and read back with
 * `getBytes(ref)` as raw Yjs V1. Storage egress is billed per byte (and on
 * mobile it is the user's bandwidth). Yjs V1 updates of map-heavy documents
 * compress about 3x with plain gzip (repeated key strings and client ids,
 * varint clocks), so every snapshot transfer — a fresh or returning
 * device's cold start, every fold's base download and snapshot upload —
 * moves about 3x more bytes than it needs to.
 *
 * This test drives the real provider against the emulator with an aged,
 * versicle-shaped document (the seeded workload of
 * benchmarks/versicle-workload.ts, ~1.2 MB of GC'd V1 state) and counts the
 * bytes each Storage call actually carries — the payload handed to
 * uploadBytes / uploadBytesResumable, and for getBytes / getBlob the
 * object's stored size (what the server sends; see `Transfer`, which also
 * keeps the decoded payload so a contentEncoding:'gzip' upload that the
 * HTTP stack inflates transparently is still counted by its wire size) —
 * and compares each with the raw V1 size of the same blob, computed
 * independently from the client's own document:
 *
 *   - large update offload: provider save upload, update-listener download
 *     on a live client, compaction download;
 *   - fold: snapshot upload, a fresh client's cold-start download, and the
 *     next fold's base download + upload;
 *   - oversized delete-set fingerprint offload (forced with
 *     maxDeleteSetFieldBytes: 0): fold upload and squash readBlob download;
 *   - squash: snapshot upload and the post-squash cold-start download.
 *
 * Measured on the uncompressed code, every transfer is exactly its raw size
 * (wire/raw = 1.000; 8.60 MB and 5.68 MB moved by the two scenarios). gzip
 * of the same blobs is ~0.33-0.37 for the snapshot / update / squash blobs
 * and ~0.63 for the fingerprint (see benchmarks/storage-compression.bench.ts),
 * so the bounds below (0.5 and 0.8) pass with any reasonable compression and
 * fail without one. Both fix routes were checked against this test with
 * throwaway prototypes: client-side gzip with explicit decompression after
 * every download, and gzip uploaded with contentEncoding 'gzip' and the
 * readers left unchanged (the emulator serves such an object with
 * Content-Encoding: gzip and the SDK's HTTP stack inflates it, so getBytes
 * returns raw V1). Each scenario also checks that the receiving client
 * converges to the writer's content, so a reader that is handed compressed
 * bytes it does not decode fails here too.
 *
 * Byte counts are deterministic (seeded workload, fixed client ids, no
 * timing in any assertion). Run through the isolation wrapper:
 *   isolated.sh bash scripts/test.sh tests/integration/storage_blob_compression.test.ts
 *
 * @file storage_blob_compression.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach, afterEach, vi } from 'vitest';

type TransferOp = 'upload' | 'download';
/**
 * `bytes` is what crosses the network: the payload handed to an upload, and
 * for a download the object's stored size (the Content-Length the server
 * sends). `payload` is what the API call returned. The two differ for an
 * object uploaded with contentEncoding 'gzip': the emulator (like GCS for a
 * client sending Accept-Encoding: gzip) serves it compressed and the HTTP
 * stack inflates it transparently, so getBytes returns the decoded size
 * although only the compressed bytes travelled.
 */
interface Transfer { op: TransferOp; path: string; bytes: number; payload: number }

const { recorder } = vi.hoisted(() => ({
    recorder: { transfers: [] as Transfer[] },
}));

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    const payloadSize = (data: any): number =>
        typeof data?.byteLength === 'number' ? data.byteLength
            : typeof data?.size === 'number' ? data.size
                : 0;
    const storedSize = async (storageRef: any): Promise<number | undefined> => {
        try {
            return (await actual.getMetadata(storageRef)).size;
        } catch {
            return undefined;
        }
    };
    const record = (op: TransferOp, storageRef: any, payload: number, wire: number | undefined) => {
        recorder.transfers.push({
            op,
            path: storageRef?.fullPath ?? String(storageRef),
            bytes: wire ?? payload,
            payload,
        });
    };
    return {
        ...actual,
        uploadBytes: async (storageRef: any, data: any, metadata?: any) => {
            record('upload', storageRef, payloadSize(data), undefined);
            return actual.uploadBytes(storageRef, data, metadata);
        },
        uploadBytesResumable: (storageRef: any, data: any, metadata?: any) => {
            record('upload', storageRef, payloadSize(data), undefined);
            return actual.uploadBytesResumable(storageRef, data, metadata);
        },
        getBytes: async (storageRef: any, maxDownloadSizeBytes?: number) => {
            const wire = await storedSize(storageRef);
            const buffer = await actual.getBytes(storageRef, maxDownloadSizeBytes);
            record('download', storageRef, payloadSize(buffer), wire);
            return buffer;
        },
        getBlob: async (storageRef: any, maxDownloadSizeBytes?: number) => {
            const wire = await storedSize(storageRef);
            const blob = await actual.getBlob(storageRef, maxDownloadSizeBytes);
            record('download', storageRef, payloadSize(blob), wire);
            return blob;
        },
    };
});

import * as Y from 'yjs';
import { doc as fsDoc, getDoc } from '@firebase/firestore';
import { FireProvider } from '../../src/provider';
import { compact } from '../../src/compaction';
import { DEFAULTS } from '../../src/types';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';
import { createSim, runSession, clientIdForSession } from '../../benchmarks/versicle-workload';

/** Wire bytes / raw V1 bytes allowed for snapshot, large-update and squash blobs. */
const MAX_RATIO = 0.5;
/** Fingerprints (structs-empty delete-set updates) compress less well. */
const MAX_FINGERPRINT_RATIO = 0.8;

const SEED = 20260820;
/** ~1.22 MB of GC'd V1 state: past INLINE_UPDATE_LIMIT, so its save offloads. */
const FIXTURE_SESSIONS = 80;

const VERSICLE_MAPS = ['library', 'progress', 'annotations', 'reading-list', 'vocabulary', 'lexicon', 'contentAnalysis', 'devices', 'searchHistory', 'meta'];

type BlobKind = 'snapshot' | 'large-update' | 'fingerprint' | 'other';

function blobKind(path: string): BlobKind {
    if (path.includes('/large_updates/')) return 'large-update';
    if (path.includes('/ds_')) return 'fingerprint';
    if (path.includes('/snapshot_')) return 'snapshot';
    return 'other';
}

function versicleJson(doc: Y.Doc): string {
    return JSON.stringify(VERSICLE_MAPS.map(name => doc.getMap(name).toJSON()));
}

/** Structs-empty update carrying the full delete set (the fold's fingerprint). */
function fingerprintOf(doc: Y.Doc): Uint8Array {
    return Y.encodeStateAsUpdate(doc, Y.encodeStateVector(doc));
}

interface Row { op: TransferOp; kind: BlobKind; path: string; bytes: number; payload: number; raw: number; ratio: number }

/**
 * Pairs every recorded transfer with the raw V1 size of the blob at its
 * path. Uploads are attributed when the operation that wrote them returns;
 * downloads are matched by path, so a listener download that lands late is
 * still compared with the right raw size.
 */
function buildRows(rawByPath: Map<string, number>): Row[] {
    return recorder.transfers
        .filter(t => blobKind(t.path) !== 'other')
        .map(t => {
            const raw = rawByPath.get(t.path) ?? NaN;
            return { op: t.op, kind: blobKind(t.path), path: t.path, bytes: t.bytes, payload: t.payload, raw, ratio: t.bytes / raw };
        });
}

function describeRow(r: Row): string {
    const decoded = r.payload !== r.bytes ? ` (API payload ${r.payload} B)` : '';
    return `${r.op} ${r.kind}: ${r.bytes} B on the wire for ${r.raw} B of raw V1 (ratio ${r.ratio.toFixed(3)})${decoded} ${r.path.split('/').pop()}`;
}

function report(title: string, rows: Row[]): void {
    const wire = rows.reduce((a, r) => a + r.bytes, 0);
    const raw = rows.reduce((a, r) => a + r.raw, 0);
    console.log(`\n=== ${title} ===\n${rows.map(describeRow).join('\n')}\n` +
        `total Storage transfer: ${wire} B on the wire for ${raw} B of raw V1 (ratio ${(wire / raw).toFixed(3)})`);
}

/** Records the raw size of every blob uploaded since `from` whose kind matches. */
function attributeUploads(rawByPath: Map<string, number>, from: number, kind: BlobKind, raw: number): number {
    let n = 0;
    for (const t of recorder.transfers.slice(from)) {
        if (t.op === 'upload' && blobKind(t.path) === kind) {
            rawByPath.set(t.path, raw);
            n++;
        }
    }
    return n;
}

describe('Storage blob transfers are compressed', () => {
    let app: any;
    let db: any;
    let storage: any;
    let counter = 0;
    let fixture: Uint8Array;
    const providers: FireProvider[] = [];

    const createProvider = (ydoc: Y.Doc, path: string, extra: Record<string, unknown> = {}) => {
        const p = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxWaitTime: 50,
            maxUpdatesThreshold: 100_000, // compaction only via explicit compact()
            ...extra,
        });
        providers.push(p);
        return p;
    };

    /** Applies `update` as a local edit and resolves with its raw size once the save commits. */
    const applyAndSave = async (ydoc: Y.Doc, provider: FireProvider, apply: () => void): Promise<number> => {
        let updateBytes = 0;
        const capture = (u: Uint8Array) => { updateBytes += u.byteLength; };
        const saved = new Promise<void>(resolve => provider.once('saved', () => resolve()));
        ydoc.on('update', capture);
        apply();
        ydoc.off('update', capture);
        await saved;
        return updateBytes;
    };

    beforeAll(() => {
        // Aged versicle-shaped document: one Y.Doc, a fresh client per
        // session, ten top-level maps (same model as the aging benchmarks).
        const sim = createSim({ seed: SEED });
        const doc = new Y.Doc();
        for (let s = 0; s < FIXTURE_SESSIONS; s++) {
            doc.clientID = clientIdForSession(SEED, s);
            runSession(sim, doc);
        }
        fixture = Y.encodeStateAsUpdate(doc);
        doc.destroy();
    });

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
        recorder.transfers = [];
    });

    afterEach(async () => {
        for (const p of providers.splice(0)) {
            try { await p.destroy(); } catch { /* already fenced/destroyed */ }
        }
    });

    it('fixture is large enough to exercise the large-update offload', () => {
        expect(fixture.byteLength).toBeGreaterThan(DEFAULTS.INLINE_UPDATE_LIMIT);
    });

    it('large-update offload, fold snapshot upload, cold-start download and the next fold move compressed bytes', async () => {
        const path = `integration-tests/storage-compression-fold-${getStableDate()}-${counter++}`;
        const rawByPath = new Map<string, number>();

        // A live client that receives the large update through the update listener
        const docLive = new Y.Doc();
        docLive.clientID = 101;
        const pLive = createProvider(docLive, path);
        await waitForConditionTruthy(() => pLive.synced, { timeout: 30000 });

        // Writer; historyFoldThreshold 1 makes every compaction a fold
        const doc1 = new Y.Doc();
        doc1.clientID = 102;
        const p1 = createProvider(doc1, path, { historyFoldThreshold: 1 });
        await waitForConditionTruthy(() => p1.synced, { timeout: 30000 });

        // 1. Oversized local update → offloaded to Storage by the save path
        let mark = recorder.transfers.length;
        const updateRaw = await applyAndSave(doc1, p1, () => Y.applyUpdate(doc1, fixture));
        expect(updateRaw).toBeGreaterThan(DEFAULTS.INLINE_UPDATE_LIMIT);
        expect(attributeUploads(rawByPath, mark, 'large-update', updateRaw)).toBe(1);

        // 2. The live client downloads it through the update listener
        const expected1 = versicleJson(doc1);
        await waitForConditionEquals(() => versicleJson(docLive), expected1, { timeout: 60000, interval: 200 });

        // 3. Fold: compaction downloads the large update, uploads the snapshot
        const snapshot1Raw = Y.encodeStateAsUpdate(doc1).byteLength;
        mark = recorder.transfers.length;
        await p1.compact();
        const main1 = (await getDoc(fsDoc(db, path))).data();
        expect(typeof main1?.snapshotStoragePath).toBe('string');
        expect(attributeUploads(rawByPath, mark, 'snapshot', snapshot1Raw)).toBe(1);

        // 4. Fresh device cold start downloads the snapshot
        const doc2 = new Y.Doc();
        doc2.clientID = 103;
        createProvider(doc2, path);
        await waitForConditionEquals(() => versicleJson(doc2), expected1, { timeout: 60000, interval: 200 });

        // 5. A small edit, delivered everywhere, then the next fold
        //    downloads the base snapshot and uploads the new one
        await applyAndSave(doc1, p1, () => doc1.getMap('meta').set('compressionProbe', 1));
        const expected2 = versicleJson(doc1);
        await waitForConditionEquals(() => versicleJson(doc2), expected2, { timeout: 30000, interval: 100 });
        await waitForConditionEquals(() => versicleJson(docLive), expected2, { timeout: 30000, interval: 100 });
        const snapshot2Raw = Y.encodeStateAsUpdate(doc1).byteLength;
        mark = recorder.transfers.length;
        await p1.compact();
        const main2 = (await getDoc(fsDoc(db, path))).data();
        expect(main2?.snapshotStoragePath).not.toBe(main1?.snapshotStoragePath);
        expect(attributeUploads(rawByPath, mark, 'snapshot', snapshot2Raw)).toBe(1);

        const rows = buildRows(rawByPath);
        report('fold pipeline Storage transfers', rows);

        // Every path was exercised: save upload; listener + compaction
        // downloads of the large update; two fold uploads; cold-start and
        // fold-base downloads of the first snapshot.
        const count = (op: TransferOp, kind: BlobKind) => rows.filter(r => r.op === op && r.kind === kind).length;
        expect({
            largeUpdateUploads: count('upload', 'large-update'),
            largeUpdateDownloads: count('download', 'large-update') >= 2,
            snapshotUploads: count('upload', 'snapshot'),
            snapshotDownloads: count('download', 'snapshot') >= 2,
        }).toEqual({ largeUpdateUploads: 1, largeUpdateDownloads: true, snapshotUploads: 2, snapshotDownloads: true });
        expect(rows.filter(r => Number.isNaN(r.raw)).map(describeRow)).toEqual([]);

        // The cost: every one of these transfers carries the raw V1 bytes.
        expect(rows.filter(r => r.ratio > MAX_RATIO).map(describeRow)).toEqual([]);
    }, 180_000);

    it('offloaded delete-set fingerprint, squash snapshot upload and the post-squash cold start move compressed bytes', async () => {
        const path = `integration-tests/storage-compression-squash-${getStableDate()}-${counter++}`;
        const rawByPath = new Map<string, number>();

        const doc3 = new Y.Doc();
        doc3.clientID = 201;
        const p3 = createProvider(doc3, path);
        await waitForConditionTruthy(() => p3.synced, { timeout: 30000 });

        let mark = recorder.transfers.length;
        const updateRaw = await applyAndSave(doc3, p3, () => Y.applyUpdate(doc3, fixture));
        attributeUploads(rawByPath, mark, 'large-update', updateRaw);

        // Fold with the fingerprint forced past its inline cap → offloaded
        const fingerprintRaw = fingerprintOf(doc3).byteLength;
        const snapshotRaw = Y.encodeStateAsUpdate(doc3).byteLength;
        mark = recorder.transfers.length;
        const folded = await compact({
            db,
            path,
            uid: 'fingerprint-offload-compactor',
            lockTTL: 60000,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
            maxDeleteSetFieldBytes: 0,
        });
        expect(folded.success).toBe(true);
        expect(folded.type).toBe('snapshot');
        const mainFolded = (await getDoc(fsDoc(db, path))).data();
        expect(typeof mainFolded?.deleteSetStoragePath).toBe('string');
        expect(attributeUploads(rawByPath, mark, 'fingerprint', fingerprintRaw)).toBe(1);
        expect(attributeUploads(rawByPath, mark, 'snapshot', snapshotRaw)).toBe(1);

        // Squash: reads the offloaded fingerprint (readBlob), uploads the
        // new epoch's snapshot
        const expected = versicleJson(doc3);
        mark = recorder.transfers.length;
        const squashed = await p3.squash();
        expect(squashed.success).toBe(true);
        expect(squashed.epoch).toBe(1);
        const squashUploads = recorder.transfers.slice(mark).filter(t => t.op === 'upload' && blobKind(t.path) === 'snapshot');
        expect(squashUploads.length).toBe(1);

        // Post-squash cold start downloads the squash snapshot; its raw V1
        // size is the fresh client's own full-state encoding
        const doc4 = new Y.Doc();
        doc4.clientID = 202;
        const p4 = createProvider(doc4, path);
        await waitForConditionEquals(() => versicleJson(doc4), expected, { timeout: 60000, interval: 200 });
        expect(p4.epoch).toBe(1);
        rawByPath.set(squashUploads[0].path, Y.encodeStateAsUpdate(doc4).byteLength);

        const rows = buildRows(rawByPath);
        report('fingerprint offload + squash Storage transfers', rows);

        const count = (op: TransferOp, kind: BlobKind) => rows.filter(r => r.op === op && r.kind === kind).length;
        const squashPath = squashUploads[0].path;
        expect({
            fingerprintUploads: count('upload', 'fingerprint'),
            fingerprintDownloads: count('download', 'fingerprint') >= 1,
            squashSnapshotDownloads: rows.filter(r => r.op === 'download' && r.path === squashPath).length >= 1,
        }).toEqual({ fingerprintUploads: 1, fingerprintDownloads: true, squashSnapshotDownloads: true });
        expect(rows.filter(r => Number.isNaN(r.raw)).map(describeRow)).toEqual([]);

        const over = rows.filter(r => r.ratio > (r.kind === 'fingerprint' ? MAX_FINGERPRINT_RATIO : MAX_RATIO));
        expect(over.map(describeRow)).toEqual([]);
    }, 180_000);
});
