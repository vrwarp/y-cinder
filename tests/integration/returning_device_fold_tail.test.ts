/**
 * Perf regression: a device returning after a fold must download what it
 * lacks, not the whole snapshot.
 *
 * versicle shape: device B is warm (hydrated from IndexedDB) and was fully
 * synced; it is then away while device A keeps reading, and A's saves cross
 * a FOLD. The fold merges base + history + updates into a new snapshot and
 * deletes the sources, so the few sessions B lacks exist only inside the
 * new multi-hundred-KB/multi-MB snapshot. Today both catch-up paths fall
 * back to `getBytes(snapshot)` + a full `Y.applyUpdate` whenever
 * `localCoversSnapshot` fails:
 *
 *  - initial sync (warm start of a returning device), src/sync.ts
 *  - the snapshot listener (a device that was offline across the fold)
 *
 * so the Cloud Storage transfer is O(snapshot) — growing with document age —
 * although B covers the replaced snapshot and lacks only the fold's tail.
 *
 * Contract pinned here (counters, no wall clock): the Storage bytes B
 * downloads to catch up stay within a small multiple of B's real diff
 * (`Y.encodeStateAsUpdate(serverState, B's state vector)`), whatever the
 * snapshot size.
 *
 * Folds are driven explicitly (maxUpdatesThreshold 1000,
 * historyFoldThreshold 2: compact #1 = delta segment, compact #2 = fold),
 * the same mechanism the default 50 x 8 cadence reaches every 400 saves.
 *
 * @file returning_device_fold_tail.test.ts
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

const { storageLog } = vi.hoisted(() => ({
    storageLog: {
        recording: false,
        downloads: [] as { path: string; bytes: number }[],
    },
}));

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getBytes: async (storageRef: any, maxDownloadSizeBytes?: number) => {
            const buffer: ArrayBuffer = await actual.getBytes(storageRef, maxDownloadSizeBytes);
            if (storageLog.recording) {
                storageLog.downloads.push({
                    path: storageRef?.fullPath ?? String(storageRef),
                    bytes: buffer.byteLength,
                });
            }
            return buffer;
        },
    };
});

import * as Y from 'yjs';
import { doc as fsDoc, getDoc, collection, getDocs } from '@firebase/firestore';
import { getMetadata, ref } from '@firebase/storage';
import { FireProvider } from '../../src/provider';
import { createSnapshotListener, SyncContext } from '../../src/sync';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';
import { createSim, runSession, clientIdForSession, VersicleSimState } from '../../benchmarks/versicle-workload';

const SEED = 20260820;
/** Sessions of history in the document before B goes away (~0.6 MB snapshot) */
const BASE_SESSIONS = 40;
/**
 * Allowed Storage download per byte of B's real diff. The fold's tail (what
 * B lacks from the fold, un-GC'd) is about 1x; the snapshot is 11-16x here
 * and grows with document age.
 */
const MAX_AMPLIFICATION = 3;

const ROOTS = ['library', 'progress', 'annotations', 'reading-list', 'vocabulary', 'lexicon', 'contentAnalysis', 'devices', 'searchHistory', 'meta'];

/** Materialized content (compare with toEqual: map key order is integration order). */
function docContent(d: Y.Doc): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const name of ROOTS) out[name] = d.getMap(name).toJSON();
    return out;
}

function svCovers(local: Y.Doc, remote: Y.Doc): boolean {
    const l = Y.decodeStateVector(Y.encodeStateVector(local));
    for (const [client, clock] of Y.decodeStateVector(Y.encodeStateVector(remote))) {
        if ((l.get(client) || 0) < clock) return false;
    }
    return true;
}

function storageBytes(downloads: { bytes: number }[]): number {
    return downloads.reduce((n, d) => n + d.bytes, 0);
}

describe('Returning device after a missed fold (Storage transfer)', () => {
    let app: any;
    let db: any;
    let storage: any;
    let counter = 0;
    const cleanup: Array<() => Promise<void> | void> = [];

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
        storageLog.recording = false;
        storageLog.downloads = [];
    });

    afterEach(async () => {
        storageLog.recording = false;
        for (const fn of cleanup.splice(0).reverse()) {
            try { await fn(); } catch { /* best effort */ }
        }
    });

    const createProvider = (ydoc: Y.Doc, path: string) => {
        const p = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxWaitTime: 50,
            maxUpdatesThreshold: 1000, // compaction only via explicit compact()
            historyFoldThreshold: 2, // compact #1 -> delta segment, #2 -> fold
        });
        cleanup.push(() => p.destroy());
        return p;
    };

    /**
     * Device A: an aged versicle document folded into a Storage snapshot.
     * Returns B's warm local copy (fully synced at this point).
     */
    const setupAgedDocument = async (path: string) => {
        const sim: VersicleSimState = createSim({ seed: SEED });
        const docA = new Y.Doc();
        for (let s = 0; s < BASE_SESSIONS; s++) {
            docA.clientID = clientIdForSession(SEED, s);
            runSession(sim, docA);
        }
        // A reopens the app: fresh clientID for everything it writes next
        docA.clientID = clientIdForSession(SEED, BASE_SESSIONS);

        const providerA = createProvider(docA, path);
        await waitForConditionTruthy(() => providerA.synced, { timeout: 60000 });
        // Initial sync pushed the whole document; fold it into the base
        await providerA.compact();
        const main = (await getDoc(fsDoc(db, path))).data();
        expect(main?.snapshotStoragePath).toContain('snapshot_v');

        const docB = new Y.Doc();
        Y.applyUpdate(docB, Y.encodeStateAsUpdate(docA));

        /**
         * One reading session on A, persisted as one debounced save.
         * Returns the session's merged update (what the save wrote).
         */
        const readOneSession = async (): Promise<Uint8Array> => {
            const saved = new Promise<void>(resolve => providerA.once('saved', () => resolve()));
            const { blobs } = runSession(sim, docA);
            await saved;
            return Y.mergeUpdates(blobs);
        };

        return { docA, docB, providerA, readOneSession };
    };

    /** Current snapshot blob (size via metadata: not a download). */
    const currentSnapshot = async (path: string): Promise<{ bytes: number; version: number; storagePath: string }> => {
        const main = (await getDoc(fsDoc(db, path))).data()!;
        const meta = await getMetadata(ref(storage, main.snapshotStoragePath));
        return { bytes: meta.size, version: main.version, storagePath: main.snapshotStoragePath };
    };

    it('warm start: Storage download is O(B\'s diff), not O(snapshot)', async () => {
        const path = `integration-tests/returning-device-warm-${getStableDate()}-${Date.now()}-${counter++}`;
        const { docA, docB, providerA, readOneSession } = await setupAgedDocument(path);
        const base = await currentSnapshot(path);

        // B goes away. A reads: session -> delta segment, session -> FOLD,
        // one more session (stays as an update document).
        const s1 = await readOneSession();
        await providerA.compact();
        expect((await getDocs(collection(db, path, 'history'))).size).toBe(1);
        const s2 = await readOneSession();
        await providerA.compact();
        await readOneSession();
        await providerA.destroy();
        // The fold's tail: mergeUpdates(history + updates it folded), gc off
        const tailBytes = Y.mergeUpdates([s1, s2]).byteLength;

        const folded = await currentSnapshot(path);
        expect(folded.version).toBe(base.version + 1);
        expect((await getDocs(collection(db, path, 'history'))).size).toBe(0);

        // What B actually lacks
        const diffBytes = Y.encodeStateAsUpdate(docA, Y.encodeStateVector(docB)).byteLength;
        expect(svCovers(docB, docA)).toBe(false);

        // B comes back (warm from IndexedDB)
        storageLog.downloads = [];
        storageLog.recording = true;
        const providerB = createProvider(docB, path);
        await waitForConditionTruthy(() => providerB.synced && svCovers(docB, docA), { timeout: 60000 });
        // Let the listeners' attach deliveries settle
        await new Promise(r => setTimeout(r, 1500));
        storageLog.recording = false;

        expect(docContent(docB)).toEqual(docContent(docA));

        // Device A is gone: every recorded download is B's
        const downloaded = storageBytes(storageLog.downloads);
        console.log(
            `[returning-device warm start] snapshot=${folded.bytes} B (base was ${base.bytes} B), ` +
            `B real diff=${diffBytes} B, fold tail=${tailBytes} B, Storage downloaded=${downloaded} B ` +
            `(${(downloaded / diffBytes).toFixed(1)}x diff) via ${JSON.stringify(storageLog.downloads.map(d => d.path.split('/').pop()))}`
        );
        // Premise: the bound is reachable — the fold's own tail fits in it
        expect(tailBytes).toBeLessThanOrEqual(MAX_AMPLIFICATION * diffBytes);
        expect(downloaded).toBeLessThanOrEqual(MAX_AMPLIFICATION * diffBytes);
    }, 180000);

    it('snapshot listener: a device offline across the fold downloads O(its diff), not O(snapshot)', async () => {
        const path = `integration-tests/returning-device-listener-${getStableDate()}-${Date.now()}-${counter++}`;
        const { docA, docB, providerA, readOneSession } = await setupAgedDocument(path);
        const base = await currentSnapshot(path);

        // B's snapshot listener is attached at the version it synced; its
        // update/history listeners are not (B is offline for those writes —
        // a resumed listener sees the folded sources already deleted).
        let destroyed = false;
        const ctxB: SyncContext = {
            db,
            path,
            doc: docB,
            uid: 'returning-device-b',
            maxUpdatesThreshold: 1000,
            isDestroyed: () => destroyed,
            storage,
            corruptedDocIds: new Set(),
            getEpoch: () => 0,
        };
        storageLog.downloads = [];
        storageLog.recording = true;
        const unsubscribe = createSnapshotListener(ctxB, base.version);
        cleanup.push(() => { destroyed = true; unsubscribe(); });

        const s1 = await readOneSession();
        await providerA.compact();
        const s2 = await readOneSession();
        const tailBytes = Y.mergeUpdates([s1, s2]).byteLength;
        // Everything A has is persisted: this is exactly what the fold holds
        const diffBytes = Y.encodeStateAsUpdate(docA, Y.encodeStateVector(docB)).byteLength;
        expect(svCovers(docB, docA)).toBe(false);
        await providerA.compact();

        const folded = await currentSnapshot(path);
        expect(folded.version).toBe(base.version + 1);

        await waitForConditionTruthy(() => svCovers(docB, docA), { timeout: 60000 });
        await new Promise(r => setTimeout(r, 500));
        storageLog.recording = false;

        expect(docContent(docB)).toEqual(docContent(docA));

        // A's fold downloads the base snapshot it replaces (B covers that
        // one and never needs it); everything else recorded is B's.
        const bDownloads = storageLog.downloads.filter(d => d.path !== base.storagePath);
        const downloaded = storageBytes(bDownloads);
        console.log(
            `[returning-device listener] snapshot=${folded.bytes} B (base was ${base.bytes} B), ` +
            `B real diff=${diffBytes} B, fold tail=${tailBytes} B, Storage downloaded=${downloaded} B ` +
            `(${(downloaded / diffBytes).toFixed(1)}x diff) via ${JSON.stringify(bDownloads.map(d => d.path.split('/').pop()))}`
        );
        // Premise: the bound is reachable — the fold's own tail fits in it
        expect(tailBytes).toBeLessThanOrEqual(MAX_AMPLIFICATION * diffBytes);
        expect(downloaded).toBeLessThanOrEqual(MAX_AMPLIFICATION * diffBytes);
    }, 180000);
});
