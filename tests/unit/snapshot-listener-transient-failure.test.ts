/**
 * Regression test: a transient Cloud Storage failure while the snapshot
 * listener downloads a new fold must not quarantine that fold.
 *
 * Bug: when a new fold arrives on the main document and the local doc does
 * not cover it (a device that was offline across the fold, with no usable
 * fold tail), `createSnapshotListener` downloads `snapshotStoragePath`. ANY
 * rejection of that download — including `storage/retry-limit-exceeded`,
 * which is what the Storage SDK surfaces once its own retries on network
 * errors / 5xx / 429 run out — lands in the catch that adds
 * `snapshot:<path>` to `corruptedDocIds` and emits 'corrupted-document'.
 * The listener's version cursor is not advanced, and every later delivery
 * of the same main document returns at the quarantine check; only a new
 * fold or squash (a new path) or a re-sync clears it. The fold already
 * deleted the update and history documents it merged, so for this device
 * the snapshot is the only remaining source of those edits: the device
 * stays diverged for the session, and the app sees a false corruption
 * event. 1d5a2bf fixed exactly this for storage-backed update blobs
 * (retry transient failures, quarantine only a missing blob) but left the
 * snapshot download alone.
 *
 * Contract asserted here: after one transient download failure, the
 * listener still brings the fold's content into the local doc (whether by
 * retrying on its own or on a later delivery of the same main document),
 * and a network error is not reported as a corrupted document.
 *
 * Firestore and Storage are faked at the SDK boundary; time is faked, so a
 * retry backoff is driven explicitly with `advanceTimersByTimeAsync`.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { toBase64 } from 'lib0/buffer';

const rec = vi.hoisted(() => ({
    listeners: [] as ((snapshot: unknown) => unknown)[],
    blobs: new Map<string, Uint8Array>(),
    /** storage path -> number of upcoming getBytes calls to fail transiently */
    transientFailures: new Map<string, number>(),
    /** every getBytes call, by path */
    downloads: [] as string[],
}));

vi.mock('@firebase/firestore', () => {
    class FakeBytes {
        constructor(private readonly bytes: Uint8Array) { }
        static fromUint8Array(bytes: Uint8Array) { return new FakeBytes(bytes); }
        toUint8Array() { return this.bytes; }
    }
    const refPath = (parts: any[]) =>
        parts.map(p => (typeof p === 'string' ? p : p?.path)).filter(Boolean).join('/');
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
        Bytes: FakeBytes,
        onSnapshot: vi.fn((_target: unknown, next: (s: unknown) => unknown) => {
            rec.listeners.push(next);
            return () => undefined;
        }),
    };
});

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        getStorage: vi.fn(() => ({})),
        ref: vi.fn((_s: unknown, path: string) => ({ path, fullPath: path })),
        uploadBytes: vi.fn(async () => undefined),
        deleteObject: vi.fn(async () => undefined),
        getBytes: vi.fn(async (r: { path: string }) => {
            rec.downloads.push(r.path);
            const remaining = rec.transientFailures.get(r.path) ?? 0;
            if (remaining > 0) {
                rec.transientFailures.set(r.path, remaining - 1);
                // Exactly what the Storage SDK surfaces once its own internal
                // retries on network errors / 5xx / 429 are exhausted.
                throw new actual.StorageError(
                    actual.StorageErrorCode.RETRY_LIMIT_EXCEEDED,
                    'Max retry time for operation exceeded, please try again.',
                    503,
                );
            }
            const blob = rec.blobs.get(r.path);
            if (!blob) {
                throw new actual.StorageError(actual.StorageErrorCode.OBJECT_NOT_FOUND, `Object '${r.path}' does not exist.`, 404);
            }
            return blob.slice().buffer;
        }),
    };
});

import * as Y from 'yjs';
import { createSnapshotListener, type SyncContext } from '../../src/sync';

const PATH = 'docs/returning-device';
const SNAPSHOT_PATH = `${PATH}/snapshots/snap_v2.bin`;
const FOLD_VERSION = 2;

const CLIENT_LOCAL = 101;
const CLIENT_OTHER = 202;

beforeEach(() => {
    rec.listeners = [];
    rec.blobs.clear();
    rec.transientFailures.clear();
    rec.downloads = [];
    vi.useFakeTimers();
});

afterEach(() => {
    vi.useRealTimers();
});

describe('snapshot listener: transient Storage failure on a new fold', () => {
    it('retries a transiently failed snapshot download instead of quarantining the fold', async () => {
        // The returning device: it holds {a} (version 1 of the doc).
        const local = new Y.Doc();
        local.clientID = CLIENT_LOCAL;
        local.getMap('m').set('a', 1);

        // While it was offline, another device wrote b and a fold merged
        // {a, b} into the snapshot (deleting the update docs that held b).
        const server = new Y.Doc();
        server.clientID = CLIENT_OTHER;
        Y.applyUpdate(server, Y.encodeStateAsUpdate(local));
        server.getMap('m').set('b', 2);
        const fold = Y.encodeStateAsUpdate(server);
        const foldSV = Y.encodeStateVector(server);
        server.destroy();

        rec.blobs.set(SNAPSHOT_PATH, fold);
        // The first download fails transiently (network drop outlasting the
        // SDK's retry window); the network then recovers.
        rec.transientFailures.set(SNAPSHOT_PATH, 1);

        // No fold tail: the device has to download the snapshot itself.
        const mainDoc = {
            snapshotStoragePath: SNAPSHOT_PATH,
            stateVector: toBase64(foldSV),
            version: FOLD_VERSION,
            origin: 'other-device',
            epoch: 0,
        };
        const deliver = () => rec.listeners[0]({ exists: () => true, data: () => ({ ...mainDoc }) });

        const corrupted: string[] = [];
        const ctx: SyncContext = {
            db: {} as any,
            storage: {} as any,
            path: PATH,
            doc: local,
            uid: 'returning-device',
            maxUpdatesThreshold: 50,
            isDestroyed: () => false,
            corruptedDocIds: new Set<string>(),
            onCorruptedDocument: (docId) => { corrupted.push(docId); },
            getEpoch: () => 0,
        };

        // Initial sync processed version 1; the fold (version 2) arrives.
        const unsubscribe = createSnapshotListener(ctx, FOLD_VERSION - 1);
        expect(rec.listeners).toHaveLength(1);

        void deliver();
        // Let any retry backoff elapse (well past the capped backoff).
        await vi.advanceTimersByTimeAsync(60_000);

        // The same main document is delivered again (e.g. a reconnect or a
        // metadata change); a quarantined fold is skipped here.
        void deliver();
        await vi.advanceTimersByTimeAsync(60_000);

        const content = local.getMap('m').toJSON();
        unsubscribe();

        expect(rec.downloads.filter(p => p === SNAPSHOT_PATH).length).toBeGreaterThanOrEqual(1);
        // The device converged: it holds the edit only the fold carries
        expect(content, 'the fold was never applied after the transient failure').toEqual({ a: 1, b: 2 });
        // A network error is not a corrupted document
        expect(corrupted, 'a transient download failure was reported as a corrupted document').toEqual([]);
        expect(ctx.corruptedDocIds!.has(`snapshot:${SNAPSHOT_PATH}`)).toBe(false);

        local.destroy();
    });

    // --- Companion cases ---
    // Real Firestore does not deliver an unchanged main document again, so
    // the retry has to happen inside the listener; a newer fold supersedes
    // the download; and a missing or undecodable snapshot is still
    // quarantined (only network failures are retried).

    /** The returning device ({a}) and a fold ({a, b}) made while it was away. */
    function returningDevice() {
        const local = new Y.Doc();
        local.clientID = CLIENT_LOCAL;
        local.getMap('m').set('a', 1);
        const server = new Y.Doc();
        server.clientID = CLIENT_OTHER;
        Y.applyUpdate(server, Y.encodeStateAsUpdate(local));
        server.getMap('m').set('b', 2);
        return { local, server };
    }

    function contextFor(local: Y.Doc, corrupted: string[]): SyncContext {
        return {
            db: {} as any,
            storage: {} as any,
            path: PATH,
            doc: local,
            uid: 'returning-device',
            maxUpdatesThreshold: 50,
            isDestroyed: () => false,
            corruptedDocIds: new Set<string>(),
            onCorruptedDocument: (docId) => { corrupted.push(docId); },
            getEpoch: () => 0,
        };
    }

    const foldDoc = (server: Y.Doc, snapshotPath: string, version: number, extra: object = {}) => {
        rec.blobs.set(snapshotPath, Y.encodeStateAsUpdate(server));
        return {
            snapshotStoragePath: snapshotPath,
            stateVector: toBase64(Y.encodeStateVector(server)),
            version,
            origin: 'other-device',
            epoch: 0,
            ...extra,
        };
    };
    const deliverDoc = (data: object) => rec.listeners[0]({ exists: () => true, data: () => ({ ...data }) });

    it('retries within the listener when the main document is not delivered again', async () => {
        const { local, server } = returningDevice();
        const mainDoc = foldDoc(server, SNAPSHOT_PATH, FOLD_VERSION);
        server.destroy();
        rec.transientFailures.set(SNAPSHOT_PATH, 3);

        const corrupted: string[] = [];
        const ctx = contextFor(local, corrupted);
        const unsubscribe = createSnapshotListener(ctx, FOLD_VERSION - 1);

        // Real Firestore delivers an unchanged document once
        void deliverDoc(mainDoc);
        await vi.advanceTimersByTimeAsync(60_000);
        const content = local.getMap('m').toJSON();
        unsubscribe();

        expect(rec.downloads.filter(p => p === SNAPSHOT_PATH)).toHaveLength(4);
        expect(content).toEqual({ a: 1, b: 2 });
        expect(corrupted).toEqual([]);
        local.destroy();
    });

    it('retries the snapshot when the fold tail fallback failed transiently too', async () => {
        const { local, server } = returningDevice();
        const tailPath = `${PATH}/snapshots/tail_v2.bin`;
        // The tail: what the fold merged on top of the replaced snapshot ({a})
        rec.blobs.set(tailPath, Y.encodeStateAsUpdate(server, Y.encodeStateVector(local)));
        const mainDoc = foldDoc(server, SNAPSHOT_PATH, FOLD_VERSION, {
            foldTailStoragePath: tailPath,
            foldTailBaseClocks: toBase64(Y.encodeStateVector(new Map([[CLIENT_OTHER, 0]]))),
            foldTailVersion: FOLD_VERSION,
        });
        server.destroy();
        rec.transientFailures.set(tailPath, 1);
        rec.transientFailures.set(SNAPSHOT_PATH, 1);

        const corrupted: string[] = [];
        const ctx = contextFor(local, corrupted);
        const unsubscribe = createSnapshotListener(ctx, FOLD_VERSION - 1);

        void deliverDoc(mainDoc);
        await vi.advanceTimersByTimeAsync(60_000);
        const content = local.getMap('m').toJSON();
        unsubscribe();

        expect(rec.downloads).toEqual([tailPath, SNAPSHOT_PATH, SNAPSHOT_PATH]);
        expect(content).toEqual({ a: 1, b: 2 });
        expect(corrupted).toEqual([]);
        local.destroy();
    });

    it('a newer fold takes over from a download still retrying for the previous one', async () => {
        const { local, server } = returningDevice();
        const mainDocV2 = foldDoc(server, SNAPSHOT_PATH, FOLD_VERSION);
        // The network stays down for the v2 download
        rec.transientFailures.set(SNAPSHOT_PATH, 1_000);

        const corrupted: string[] = [];
        const ctx = contextFor(local, corrupted);
        const unsubscribe = createSnapshotListener(ctx, FOLD_VERSION - 1);

        void deliverDoc(mainDocV2);
        await vi.advanceTimersByTimeAsync(10_000);

        // Meanwhile another fold (v3) committed and garbage-collected the v2
        // blob; the network recovers and the v3 main document arrives.
        server.getMap('m').set('c', 3);
        const v3Path = `${PATH}/snapshots/snap_v3.bin`;
        const mainDocV3 = foldDoc(server, v3Path, FOLD_VERSION + 1);
        server.destroy();
        rec.blobs.delete(SNAPSHOT_PATH);
        rec.transientFailures.clear();

        void deliverDoc(mainDocV3);
        await vi.advanceTimersByTimeAsync(60_000);
        const content = local.getMap('m').toJSON();
        unsubscribe();

        expect(content).toEqual({ a: 1, b: 2, c: 3 });
        // The deleted v2 blob is the newer fold's doing, not corruption
        expect(corrupted).toEqual([]);
        expect([...ctx.corruptedDocIds!]).toEqual([]);
        local.destroy();
    });

    it('still quarantines a snapshot that is missing, without retrying it', async () => {
        const { local, server } = returningDevice();
        const mainDoc = foldDoc(server, SNAPSHOT_PATH, FOLD_VERSION);
        server.destroy();
        rec.blobs.delete(SNAPSHOT_PATH);

        const corrupted: string[] = [];
        const ctx = contextFor(local, corrupted);
        const unsubscribe = createSnapshotListener(ctx, FOLD_VERSION - 1);

        void deliverDoc(mainDoc);
        await vi.advanceTimersByTimeAsync(60_000);
        unsubscribe();

        expect(rec.downloads).toEqual([SNAPSHOT_PATH]);
        expect(corrupted).toEqual([`snapshot:${SNAPSHOT_PATH}`]);
        expect(ctx.corruptedDocIds!.has(`snapshot:${SNAPSHOT_PATH}`)).toBe(true);
        local.destroy();
    });

    it('still quarantines a snapshot that cannot be decoded', async () => {
        const { local, server } = returningDevice();
        const mainDoc = foldDoc(server, SNAPSHOT_PATH, FOLD_VERSION);
        server.destroy();
        rec.blobs.set(SNAPSHOT_PATH, new Uint8Array([1, 2, 3]));

        const corrupted: string[] = [];
        const ctx = contextFor(local, corrupted);
        const unsubscribe = createSnapshotListener(ctx, FOLD_VERSION - 1);

        void deliverDoc(mainDoc);
        await vi.advanceTimersByTimeAsync(60_000);
        unsubscribe();

        expect(rec.downloads).toEqual([SNAPSHOT_PATH]);
        expect(corrupted).toEqual([`snapshot:${SNAPSHOT_PATH}`]);
        local.destroy();
    });
});
