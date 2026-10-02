/**
 * Performance regression test (Firestore/Storage I/O): local-persistence
 * hydration, and edits buffered before initial sync, must not be uploaded
 * a second time on every cold start.
 *
 * Cost: FireProvider buffers every update whose origin is not one of its
 * own three Firebase origins. y-idb applies all of its stored rows in ONE
 * transaction whose origin is the IndexeddbPersistence instance. When the
 * provider is constructed first (IndexedDB open is always async, and
 * versicle builds a fresh Y.Doc + provider on every page load), that
 * transaction's 'update' event is the WHOLE document: handleUpdate buffers
 * it as a local edit and the debounced save writes it as a brand-new
 * update document, although the server already holds all of it and the
 * initial-sync push already decided what (if anything) the server lacks.
 * Offline edits from the previous session and edits typed while initial
 * sync runs are written twice the same way (once inside the push diff,
 * once by the debounced save).
 *
 * Knock-on costs, all measured below:
 *  - every online peer downloads the echo (above MAX_METADATA_CLIENTS=50
 *    clients it carries no clientIDs/clientClocks, so peers must run a full
 *    Y.applyUpdate on it — see benchmarks/hydration-echo.bench.ts);
 *  - the next compaction folds the echo into an O(document) history segment
 *    flagged hasDeletions, or forces a full fold when it does not fit;
 *  - above 1 MB the echo becomes a Cloud Storage blob plus pointer document.
 *
 * Determinism: the cold-start client's first server read is held until the
 * persisted state has been applied (the "IndexedDB hydration lands before
 * server content" ordering), and every counter is read after destroy() —
 * which flushes whatever is still buffered — so the counts do not depend
 * on when the debounce timer happens to fire. Peer-side deliveries are
 * read after a sentinel update written later has reached the peer
 * (the listener delivers in createdAt order).
 *
 * Every assertion is an operation/byte count, never a wall-clock bound.
 *
 * @file hydration_echo_upload.test.ts
 */

import { describe, it, expect, beforeAll, afterEach, vi } from 'vitest';

interface WriteRecord {
    collectionPath: string;
    createdBy?: string;
    /** Inline update payload, or the bytes uploaded for a storage pointer */
    payload: Uint8Array | null;
    storagePath?: string;
    hasMetadata: boolean;
}

interface DeliveryRecord {
    listenerId: number;
    createdBy?: string;
    inlineBytes: number;
    storagePath?: string;
    hasMetadata: boolean;
}

const { io } = vi.hoisted(() => ({
    io: {
        /** While set, every getDocs (initial-sync reads) waits on it. */
        getDocsGate: null as Promise<void> | null,
        writes: [] as WriteRecord[],
        uploads: [] as { path: string; bytes: Uint8Array }[],
        downloads: [] as { path: string; bytes: number }[],
        deliveries: [] as DeliveryRecord[],
        listenerSeq: 0,
        reset() {
            this.writes = [];
            this.uploads = [];
            this.downloads = [];
            this.deliveries = [];
        },
    },
}));

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        addDoc: async (collectionRef: any, data: any) => {
            io.writes.push({
                collectionPath: collectionRef.path,
                createdBy: data?.createdBy,
                payload: data?.update?.toUint8Array?.() ?? null,
                storagePath: data?.updateStoragePath,
                hasMetadata: Array.isArray(data?.clientIDs) && data.clientIDs.length > 0,
            });
            return actual.addDoc(collectionRef, data);
        },
        getDocs: async (q: any) => {
            if (io.getDocsGate) await io.getDocsGate;
            return actual.getDocs(q);
        },
        onSnapshot: (target: any, ...rest: any[]) => {
            const listenerId = ++io.listenerSeq;
            const next = rest[0];
            if (typeof next !== 'function') return actual.onSnapshot(target, ...rest);
            const wrapped = (snap: any) => {
                if (typeof snap?.docChanges === 'function') {
                    for (const change of snap.docChanges()) {
                        if (change.type !== 'added' || !change.doc.ref.path.includes('/updates/')) continue;
                        const data = change.doc.data();
                        io.deliveries.push({
                            listenerId,
                            createdBy: data?.createdBy,
                            inlineBytes: data?.update?.toUint8Array?.().byteLength ?? 0,
                            storagePath: data?.updateStoragePath,
                            hasMetadata: Array.isArray(data?.clientIDs) && data.clientIDs.length > 0,
                        });
                    }
                }
                return next(snap);
            };
            return actual.onSnapshot(target, wrapped, ...rest.slice(1));
        },
    };
});

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        uploadBytes: async (storageRef: any, data: Uint8Array, ...rest: any[]) => {
            io.uploads.push({ path: storageRef.fullPath, bytes: data });
            return actual.uploadBytes(storageRef, data, ...rest);
        },
        getBytes: async (storageRef: any, ...rest: any[]) => {
            const buffer = await actual.getBytes(storageRef, ...rest);
            io.downloads.push({ path: storageRef.fullPath, bytes: buffer.byteLength });
            return buffer;
        },
    };
});

import * as Y from 'yjs';
import { addDoc, Bytes, collection, deleteDoc, getDocs, serverTimestamp } from 'firebase/firestore';
import { FireProvider } from '../../src/provider';
import { compact, CompactionContext } from '../../src/compaction';
import { setupEmulator } from '../utils/emulator';
import { waitFor, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';
import { createSim, runSession, clientIdForSession, materializeVersicleDoc, VersicleSimState } from '../../benchmarks/versicle-workload';

/** Stand-in for the IndexeddbPersistence instance y-idb uses as origin. */
const IDB_ORIGIN = { name: 'y-idb persistence (test stand-in)' };

const SEED = 20260820;

/**
 * A versicle document aged through `sessions` sessions, one fresh
 * clientID per session (versicle mints one per page load).
 */
function buildAgedDoc(sessions: number, seed: number = SEED): { sim: VersicleSimState; doc: Y.Doc } {
    const sim = createSim({ seed });
    const doc = new Y.Doc();
    for (let s = 0; s < sessions; s++) {
        doc.clientID = clientIdForSession(seed, s);
        runSession(sim, doc);
    }
    return { sim, doc };
}

/** Exactly what y-idb's applyStoredUpdates does on load. */
function hydrate(doc: Y.Doc, persisted: Uint8Array): void {
    Y.transact(doc, () => Y.applyUpdate(doc, persisted), IDB_ORIGIN, false);
}

const sum = (xs: number[]) => xs.reduce((a, b) => a + b, 0);
const fmt = (n: number) => n.toLocaleString('en-US');

describe('Cold-start uploads of hydrated / pre-sync local state', () => {
    let app: any;
    let db: any;
    let storage: any;
    let counter = 0;
    const live: FireProvider[] = [];

    beforeAll(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
    });

    afterEach(async () => {
        io.getDocsGate = null;
        while (live.length > 0) {
            await live.pop()!.destroy();
        }
    });

    const newPath = (tag: string) => `tests/hydration-echo-${tag}-${getStableDate()}-${Date.now()}-${counter++}`;

    const createProvider = (ydoc: Y.Doc, path: string, extra: Record<string, unknown> = {}) => {
        const p = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            // Skip the clock-skew probe (unrelated I/O; initial sync starts
            // inside the constructor, so the getDocs gate covers it).
            cachedClockOffset: 0,
            // Keep automatic compaction out of the picture.
            maxUpdatesThreshold: 1000,
            ...extra,
        });
        live.push(p);
        return p;
    };

    const release = async (p: FireProvider) => {
        const i = live.indexOf(p);
        if (i >= 0) live.splice(i, 1);
        await p.destroy();
    };

    const waitSynced = (p: FireProvider, message: string) =>
        waitForConditionTruthy(() => p.synced, { timeout: 60000, message });

    /** No buffered update, no save in flight, no save scheduled. */
    const waitSaveIdle = (p: FireProvider) => {
        const a = p as any;
        return waitForConditionTruthy(
            () => (a._pendingUpdates?.length ?? 0) === 0 && !a._inflightSave && !a._debounceTimerId,
            { timeout: 30000, interval: 20, message: 'provider save path idle' },
        );
    };

    /**
     * Puts `state` on the server the normal way: a provider pushes it and
     * the first compaction folds it into a snapshot (no updates left).
     */
    const seedServer = async (state: Uint8Array, tag: string): Promise<string> => {
        const path = newPath(tag);
        const seedDoc = new Y.Doc();
        Y.applyUpdate(seedDoc, state);
        const seeder = createProvider(seedDoc, path);
        await waitSynced(seeder, 'seeder synced');
        await waitForConditionTruthy(
            async () => (await getDocs(collection(db, path, 'updates'))).size >= 1,
            { timeout: 30000, message: 'seed state pushed' });
        await compact(compactionCtx(path, 'seed-compactor'));
        await waitForConditionTruthy(
            async () => (await getDocs(collection(db, path, 'updates'))).size === 0,
            { timeout: 30000, message: 'seed state folded into the snapshot' });
        await release(seeder);
        seedDoc.destroy();
        return path;
    };

    const compactionCtx = (path: string, uid: string): CompactionContext => ({
        db,
        path,
        uid,
        lockTTL: 60000,
        compactionLimit: 500,
        isDestroyed: () => false,
        storage,
        cachedClockOffset: 0,
    });

    interface ColdStartIO {
        uid: string;
        clientID: number;
        updateDocs: number;
        /** Inline payload bytes + Storage-uploaded bytes */
        bytes: number;
        storageUploads: number;
        withoutMetadata: number;
        /** Update documents started before the provider reported 'sync' */
        startedDuringInitialSync: number;
        payloads: Uint8Array[];
    }

    /**
     * One page load: fresh Y.Doc, provider, local persistence hydration.
     *
     * 'hydrate-after-construct': the provider exists first and the
     * persisted state lands before any server read returns (IndexedDB
     * open is async; Firestore is a network round trip away).
     * 'hydrate-before-construct': the control ordering.
     *
     * `duringSync` runs while initial sync's first read is still held
     * (e.g. the app writing to its stores at startup). `holdMs` keeps the
     * read held that long after hydration, so the debounced save timer
     * fires while initial sync is still in flight.
     */
    const coldStart = async (
        path: string,
        persisted: Uint8Array,
        ordering: 'hydrate-after-construct' | 'hydrate-before-construct',
        opts: { duringSync?: (doc: Y.Doc) => void; holdMs?: number; maxWaitTime?: number } = {},
    ): Promise<ColdStartIO> => {
        const doc = new Y.Doc();
        if (ordering === 'hydrate-before-construct') hydrate(doc, persisted);

        let open!: () => void;
        io.getDocsGate = new Promise<void>(resolve => { open = resolve; });
        const provider = createProvider(doc, path, { maxWaitTime: opts.maxWaitTime ?? 500 });
        const writesBy = (uid: string) =>
            io.writes.filter(w => w.createdBy === uid && w.collectionPath.endsWith('/updates'));
        let startedDuringInitialSync = -1;
        provider.on('sync', () => {
            if (startedDuringInitialSync < 0) startedDuringInitialSync = writesBy(provider.uid).length;
        });

        if (ordering === 'hydrate-after-construct') hydrate(doc, persisted);
        opts.duringSync?.(doc);
        if (opts.holdMs) await new Promise(r => setTimeout(r, opts.holdMs));
        io.getDocsGate = null;
        open();

        await waitSynced(provider, 'cold-start client synced');
        await waitSaveIdle(provider);
        // destroy() flushes anything still buffered, so the count below is
        // independent of debounce timing.
        await release(provider);

        const mine = writesBy(provider.uid);
        const uploads = io.uploads.filter(u => u.path.includes('/large_updates/') && u.path.includes(provider.uid));
        const payloads = [
            ...mine.filter(w => w.payload).map(w => w.payload!),
            ...uploads.map(u => u.bytes),
        ];
        const result: ColdStartIO = {
            uid: provider.uid,
            clientID: doc.clientID,
            updateDocs: mine.length,
            bytes: sum(payloads.map(p => p.byteLength)),
            storageUploads: uploads.length,
            withoutMetadata: mine.filter(w => !w.hasMetadata).length,
            startedDuringInitialSync,
            payloads,
        };
        doc.destroy();
        return result;
    };

    /** Compaction run right after the cold start: what the echo costs it. */
    const measureCompaction = async (path: string) => {
        io.reset();
        const histBefore = (await getDocs(collection(db, path, 'history'))).docs.map(d => d.id);
        const result = await compact(compactionCtx(path, 'measuring-compactor'));
        const added = (await getDocs(collection(db, path, 'history'))).docs.filter(d => !histBefore.includes(d.id));
        return {
            type: result.type,
            updatesCompacted: result.updatesCompacted,
            segmentBytes: sum(added.map(d => d.data().segment?.toUint8Array().byteLength ?? 0)),
            segmentsWithDeletions: added.filter(d => d.data().hasDeletions === true).length,
            storageDownloadBytes: sum(io.downloads.map(d => d.bytes)),
            storageUploadBytes: sum(io.uploads.map(u => u.bytes.byteLength)),
        };
    };

    /** Reads `path` from a brand-new client and returns its content. */
    const readFromFreshClient = async (path: string): Promise<string> => {
        const fresh = new Y.Doc();
        const reader = createProvider(fresh, path);
        await waitSynced(reader, 'fresh reader synced');
        expect((fresh.store as any).pendingStructs).toBeNull();
        const content = materializeVersicleDoc(Y.encodeStateAsUpdate(fresh), []);
        await release(reader);
        fresh.destroy();
        return content;
    };

    it('20 sessions: hydration landing after construction is not re-uploaded, and leaves compaction nothing to do', { timeout: 180000 }, async () => {
        const { doc: aged } = buildAgedDoc(20);
        const state = Y.encodeStateAsUpdate(aged);
        const path = await seedServer(state, 's20');

        io.reset();
        const control = await coldStart(path, state, 'hydrate-before-construct');
        io.reset();
        // Debounce (50 ms) fires while initial sync's reads are still held.
        const echo = await coldStart(path, state, 'hydrate-after-construct', { maxWaitTime: 50, holdMs: 300 });
        const compaction = await measureCompaction(path);

        console.log(
            `[hydration-echo] 20 sessions, persisted state ${fmt(state.byteLength)} B, server already holds all of it\n` +
            `  hydrate before construct: update docs ${control.updateDocs}, ${fmt(control.bytes)} B\n` +
            `  hydrate after construct:  update docs ${echo.updateDocs}, ${fmt(echo.bytes)} B ` +
            `(${echo.startedDuringInitialSync} started during initial sync, ` +
            `${echo.withoutMetadata} without redundancy metadata, ${echo.storageUploads} Storage uploads)\n` +
            `  next compaction: ${compaction.type}, ${compaction.updatesCompacted} update docs, ` +
            `history segment ${fmt(compaction.segmentBytes)} B (${compaction.segmentsWithDeletions} flagged hasDeletions), ` +
            `Storage down ${fmt(compaction.storageDownloadBytes)} B / up ${fmt(compaction.storageUploadBytes)} B`,
        );

        expect({ updateDocs: control.updateDocs, bytes: control.bytes }).toEqual({ updateDocs: 0, bytes: 0 });
        // The server already holds every byte of the hydrated state.
        expect({ updateDocs: echo.updateDocs, bytes: echo.bytes, storageUploads: echo.storageUploads })
            .toEqual({ updateDocs: 0, bytes: 0, storageUploads: 0 });
        expect({ updatesCompacted: compaction.updatesCompacted, segmentBytes: compaction.segmentBytes })
            .toEqual({ updatesCompacted: 0, segmentBytes: 0 });
    });

    for (const sessions of [60, 100]) {
        it(`${sessions} sessions: an online peer receives nothing from a hydrate-after-construct cold start`, { timeout: 300000 }, async () => {
            const { doc: aged } = buildAgedDoc(sessions);
            const state = Y.encodeStateAsUpdate(aged);
            const path = await seedServer(state, `s${sessions}`);

            // An online peer (another device with the doc open).
            const peerDoc = new Y.Doc();
            Y.applyUpdate(peerDoc, state);
            const firstPeerListener = io.listenerSeq + 1;
            const peer = createProvider(peerDoc, path);
            await waitSynced(peer, 'peer synced');
            const lastPeerListener = io.listenerSeq;
            const isPeerListener = (id: number) => id >= firstPeerListener && id <= lastPeerListener;

            io.reset();
            const echo = await coldStart(path, state, 'hydrate-after-construct');
            const echoBlobs = io.uploads.filter(u => u.path.includes('/large_updates/') && u.path.includes(echo.uid)).map(u => u.path);

            // Sentinel written after the cold start settled: once the peer's
            // listener delivered it, it has delivered everything before it.
            const sentinelDoc = new Y.Doc();
            sentinelDoc.getMap('meta').set('sentinel', true);
            const sentinelRef = await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(sentinelDoc)),
                createdAt: serverTimestamp(),
                createdBy: 'sentinel',
                clientIDs: [sentinelDoc.clientID],
                clientClocks: [1],
            });
            await waitForConditionTruthy(
                () => io.deliveries.some(d => isPeerListener(d.listenerId) && d.createdBy === 'sentinel'),
                { timeout: 30000, message: 'peer listener delivered the sentinel' });
            // Storage-backed echoes are downloaded by the peer after delivery.
            for (const blob of echoBlobs) {
                await waitFor(() => io.downloads.some(d => d.path === blob), Boolean, { timeout: 30000, interval: 50 })
                    .catch(() => undefined);
            }
            await deleteDoc(sentinelRef);

            const fromColdStart = io.deliveries.filter(d => isPeerListener(d.listenerId) && d.createdBy === echo.uid);
            const peerBlobDownloads = io.downloads.filter(d => echoBlobs.includes(d.path));
            const peerReceived = {
                docs: fromColdStart.length,
                inlineBytes: sum(fromColdStart.map(d => d.inlineBytes)),
                withoutMetadata: fromColdStart.filter(d => !d.hasMetadata).length,
                storageDownloadBytes: sum(peerBlobDownloads.map(d => d.bytes)),
            };
            await release(peer);
            peerDoc.destroy();

            const compaction = await measureCompaction(path);

            console.log(
                `[hydration-echo] ${sessions} sessions, persisted state ${fmt(state.byteLength)} B, server already holds all of it\n` +
                `  hydrate after construct: update docs ${echo.updateDocs}, ${fmt(echo.bytes)} B ` +
                `(${echo.startedDuringInitialSync} started during initial sync, ` +
                `${echo.withoutMetadata} without redundancy metadata, ${echo.storageUploads} Storage uploads)\n` +
                `  online peer received: ${peerReceived.docs} docs, ${fmt(peerReceived.inlineBytes)} B inline + ` +
                `${fmt(peerReceived.storageDownloadBytes)} B Storage download ` +
                `(${peerReceived.withoutMetadata} without metadata -> full Y.applyUpdate on the peer)\n` +
                `  next compaction: ${compaction.type}, ${compaction.updatesCompacted} update docs, ` +
                `history segment ${fmt(compaction.segmentBytes)} B (${compaction.segmentsWithDeletions} flagged hasDeletions), ` +
                `Storage down ${fmt(compaction.storageDownloadBytes)} B / up ${fmt(compaction.storageUploadBytes)} B`,
            );

            expect({ updateDocs: echo.updateDocs, bytes: echo.bytes, storageUploads: echo.storageUploads })
                .toEqual({ updateDocs: 0, bytes: 0, storageUploads: 0 });
            expect(peerReceived).toEqual({ docs: 0, inlineBytes: 0, withoutMetadata: 0, storageDownloadBytes: 0 });
            expect({ updatesCompacted: compaction.updatesCompacted, storageUploadBytes: compaction.storageUploadBytes })
                .toEqual({ updatesCompacted: 0, storageUploadBytes: 0 });
        });
    }

    it('offline edits from the previous session are uploaded once, not once in the push and again as the whole doc', { timeout: 180000 }, async () => {
        const { sim, doc: aged } = buildAgedDoc(20);
        const serverState = Y.encodeStateAsUpdate(aged);
        const path = await seedServer(serverState, 'offline');

        // Previous session edited offline: local persistence holds
        // server state + one more versicle session the server never saw.
        aged.clientID = clientIdForSession(SEED, 20);
        runSession(sim, aged);
        const persisted = Y.encodeStateAsUpdate(aged);
        // What the server lacks, encoded exactly like the initial-sync push.
        const serverLacks = Y.encodeStateAsUpdate(aged, Y.encodeStateVectorFromUpdate(serverState));
        const expected = materializeVersicleDoc(persisted, []);

        io.reset();
        const cold = await coldStart(path, persisted, 'hydrate-after-construct');
        const freshView = await readFromFreshClient(path);

        console.log(
            `[hydration-echo] offline edits: persisted ${fmt(persisted.byteLength)} B, server lacks ${fmt(serverLacks.byteLength)} B\n` +
            `  hydrate after construct: update docs ${cold.updateDocs}, ` +
            `${cold.payloads.map(p => fmt(p.byteLength)).join(' + ')} B = ${fmt(cold.bytes)} B ` +
            `(${(cold.bytes / serverLacks.byteLength).toFixed(1)}x what the server lacked; ` +
            `${cold.startedDuringInitialSync} written before 'sync', the initial-sync push included)`,
        );

        // No data lost: a fresh device sees the offline session.
        expect(freshView === expected).toBe(true);
        expect({
            updateDocs: cold.updateDocs,
            bytesWithinTwiceWhatServerLacked: cold.bytes <= 2 * serverLacks.byteLength,
        }).toEqual({ updateDocs: 1, bytesWithinTwiceWhatServerLacked: true });
    });

    it('an edit made while initial sync runs is uploaded once', { timeout: 180000 }, async () => {
        const { doc: aged } = buildAgedDoc(20);
        const state = Y.encodeStateAsUpdate(aged);
        const path = await seedServer(state, 'during-sync');

        io.reset();
        // Control ordering (hydrated before construction: no echo); the app
        // writes to one of its stores before the server reads return.
        const cold = await coldStart(path, state, 'hydrate-before-construct', {
            duringSync: (doc) => doc.getMap('meta').set('lastOpened', 'cold-start-edit'),
        });
        const carryingEdit = cold.payloads.filter(p => (Y.parseUpdateMeta(p).to.get(cold.clientID) ?? 0) > 0);
        const freshView = await readFromFreshClient(path);

        console.log(
            `[hydration-echo] edit during initial sync: update docs ${cold.updateDocs} ` +
            `(${cold.payloads.map(p => fmt(p.byteLength)).join(' + ')} B), ${carryingEdit.length} of them carry the edit ` +
            `(${cold.startedDuringInitialSync} written before 'sync', the initial-sync push included)`,
        );

        expect(JSON.parse(freshView).meta.lastOpened).toBe('cold-start-edit');
        expect({ updateDocs: cold.updateDocs, docsCarryingTheEdit: carryingEdit.length })
            .toEqual({ updateDocs: 1, docsCarryingTheEdit: 1 });
    });
});
