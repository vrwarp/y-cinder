/**
 * Perf regression: the delete-set fingerprint must not be re-applied to a
 * document that already holds every deletion it carries.
 *
 * Compaction writes the snapshot's full delete-set as a structs-empty update
 * on the main document (`deleteSet` / `deleteSetStoragePath`). Applying it is
 * O(delete-set): Yjs binary-searches every range and walks every struct
 * inside it — on an aged versicle document ~12k ranges / ~85k dead structs,
 * 7-12 ms of main thread that finds nothing to delete (see
 * benchmarks/fingerprint-reapply.bench.ts). Two paths pay it
 * unconditionally today:
 *
 *  - `performInitialSync` queues it as an `update` item without
 *    clientIDs/clientClocks, so `isItemRedundant` cannot judge it and every
 *    start (cold, warm, listener-error re-sync) applies it — also on a fully
 *    synced, y-idb-hydrated client;
 *  - `createSnapshotListener` applies it before `localCoversSnapshot` on
 *    every NEW fold version, even when the fold merged only data the client
 *    already holds.
 *
 * Firestore and Storage are faked at the SDK boundary (the real `Bytes` is
 * kept); the real initial sync and snapshot listener run. `Y.applyUpdate`
 * is wrapped pass-through to count the delete-set ranges handed to Yjs —
 * an implementation-agnostic measure of the walk: any correct optimization
 * hands it 0 ranges when every range is already deleted locally.
 *
 * The contract tests pin what a fix must keep: a deletion the client missed
 * (a delete-only change, invisible to state vectors) still arrives through
 * the fingerprint, and the fingerprint still counts as server evidence for
 * the push guard (no spurious push).
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

const probe = vi.hoisted(() => ({
    armed: false,
    applies: 0,
    /** Delete-set ranges carried by the updates handed to Y.applyUpdate */
    dsRanges: 0,
}));

vi.mock('yjs', async (importOriginal) => {
    const actual = await importOriginal<typeof import('yjs')>();
    return {
        ...actual,
        applyUpdate: (doc: InstanceType<typeof actual.Doc>, update: Uint8Array, origin?: unknown) => {
            if (probe.armed) {
                probe.applies++;
                actual.decodeUpdate(update).ds.clients.forEach(items => { probe.dsRanges += items.length; });
            }
            return actual.applyUpdate(doc, update, origin);
        },
    };
});

const server = vi.hoisted(() => ({
    main: null as Record<string, any> | null,
    storage: new Map<string, Uint8Array>(),
    downloads: 0,
    added: 0,
    pushed: [] as Record<string, any>[],
    snapshotListeners: [] as ((snap: any) => unknown)[],
}));

vi.mock('@firebase/firestore', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/firestore')>();
    const join = (parts: unknown[]) => parts.filter(p => typeof p === 'string').join('/');
    const empty = { docs: [], empty: true, size: 0, forEach: () => { } };
    return {
        ...actual,
        collection: (_db: unknown, ...parts: unknown[]) => ({ kind: 'collection', path: join(parts) }),
        doc: (_db: unknown, ...parts: unknown[]) => ({ kind: 'doc', path: join(parts) }),
        query: (ref: any, ...constraints: any[]) => ({ ...ref, constraints }),
        orderBy: (field: string) => ({ orderBy: field }),
        startAfter: (cursor: any) => ({ startAfter: cursor }),
        limit: (n: number) => ({ limit: n }),
        serverTimestamp: () => ({ serverTimestamp: true }),
        // Right after a fold: no update or history documents.
        getDocs: async () => empty,
        getDoc: async () => ({
            exists: () => server.main !== null,
            data: () => (server.main ? { ...server.main } : undefined),
        }),
        addDoc: async (_ref: unknown, pkg: Record<string, any>) => {
            server.added++;
            server.pushed.push(pkg);
            return { id: 'added' };
        },
        onSnapshot: (_target: unknown, next: (snap: any) => unknown) => {
            server.snapshotListeners.push(next);
            return () => {
                server.snapshotListeners = server.snapshotListeners.filter(l => l !== next);
            };
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
            if (!blob) throw Object.assign(new Error('object-not-found'), { code: 'storage/object-not-found' });
            server.downloads++;
            return blob.slice().buffer;
        },
        uploadBytes: async () => undefined,
    };
});

import * as Y from 'yjs';
import { Bytes } from '@firebase/firestore';
import { toBase64 } from 'lib0/buffer';
import { performInitialSync, createSnapshotListener, SyncContext } from '../../src/sync';
import { mergeUpdatesWithMeta } from '../../src/merge-core';
import { SeededRandom } from './prng';

const PATH = 'docs/fingerprint-reapply';
const SESSIONS = 12;
const SAVES_PER_SESSION = 40;

function ctxFor(doc: Y.Doc): SyncContext {
    return {
        db: {} as any,
        storage: {} as any,
        path: PATH,
        doc,
        uid: 'reader',
        maxUpdatesThreshold: 50,
        isDestroyed: () => false,
    };
}

function rangeCount(update: Uint8Array): number {
    let n = 0;
    Y.decodeUpdate(update).ds.clients.forEach(items => { n += items.length; });
    return n;
}

/**
 * Publishes a fold of `updates` the way compaction writes it: GC'd snapshot
 * in Storage, its state vector and inline delete-set fingerprint on the
 * main document, and a bumped version.
 */
function publishFold(updates: Uint8Array[], version: number): { snapshot: Uint8Array; fingerprint: Uint8Array } {
    const { result: snapshot, stateVector, dsUpdate: fingerprint } = mergeUpdatesWithMeta(updates, { gc: true });
    const storagePath = `snapshots/v${version}`;
    server.storage.set(storagePath, snapshot);
    server.main = {
        version,
        epoch: 0,
        stateVector: toBase64(stateVector),
        // Current folds mark their vector as contiguous from clock 0 (see
        // snapshotStateVectorIsContiguous); an unmarked one is legacy.
        stateVectorContiguous: true,
        snapshotStoragePath: storagePath,
        deleteSet: Bytes.fromBase64String(toBase64(fingerprint)),
    };
    return { snapshot, fingerprint };
}

/**
 * An aged versicle-like document: a fresh clientID per session, same-key
 * overwrites (page turns, settings) and annotation add/remove — every save
 * leaves deletions behind.
 */
function ageDocument(writer: Y.Doc): Uint8Array[] {
    const updates: Uint8Array[] = [];
    writer.on('update', (u: Uint8Array) => updates.push(u));
    const rng = new SeededRandom(20261002);
    for (let s = 0; s < SESSIONS; s++) {
        writer.clientID = 7_000 + s;
        for (let e = 0; e < SAVES_PER_SESSION; e++) {
            writer.transact(() => {
                writer.getMap('progress').set(`book-${rng.int(0, 5)}`, `cfi-${s}-${e}`);
                writer.getMap('settings').set(`k-${rng.int(0, 3)}`, rng.int(0, 1_000_000));
                const notes = writer.getMap('annotations');
                notes.set(`a-${s}-${e}`, rng.string(16));
                if (e % 3 === 0 && notes.size > 10) notes.delete(rng.choice([...notes.keys()]));
            });
        }
    }
    return updates;
}

/** Delivers the current main document to the snapshot listener. */
async function deliverMain(): Promise<void> {
    expect(server.snapshotListeners).toHaveLength(1);
    const data = { ...server.main! };
    await server.snapshotListeners[0]({ exists: () => true, data: () => ({ ...data }) });
}

function measure<T>(fn: () => Promise<T>): Promise<T> {
    probe.applies = 0;
    probe.dsRanges = 0;
    probe.armed = true;
    return fn().finally(() => { probe.armed = false; });
}

describe('delete-set fingerprint re-apply', () => {
    let writer: Y.Doc;
    let updates: Uint8Array[];

    beforeEach(() => {
        server.main = null;
        server.storage.clear();
        server.downloads = 0;
        server.added = 0;
        server.pushed = [];
        server.snapshotListeners = [];
        vi.spyOn(console, 'log').mockImplementation(() => { });
        vi.spyOn(console, 'warn').mockImplementation(() => { });
        writer = new Y.Doc();
        updates = ageDocument(writer);
    });

    afterEach(() => {
        writer.destroy();
        vi.restoreAllMocks();
    });

    it('a fully synced client\'s warm start hands Yjs no delete-set range', async () => {
        const { snapshot, fingerprint } = publishFold(updates, 3);
        const fpRanges = rangeCount(fingerprint);
        expect(fpRanges).toBeGreaterThan(300); // the fixture really is aged

        // Hydrated from local persistence: holds every struct and deletion.
        const reader = new Y.Doc();
        Y.applyUpdate(reader, snapshot);
        let updateEvents = 0;
        reader.on('update', () => { updateEvents++; });

        const result = await measure(() => performInitialSync(ctxFor(reader)));

        expect(result.success).toBe(true);
        expect(server.downloads).toBe(0); // snapshot covered: not downloaded
        expect(server.added).toBe(0); // fingerprint still proves coverage: no push
        expect(updateEvents).toBe(0); // nothing changed...
        // ...yet today the whole fingerprint (every range) is re-applied.
        expect(probe.dsRanges, `re-applied ${probe.dsRanges} of ${fpRanges} fingerprint ranges`).toBe(0);
        reader.destroy();
    });

    it("a legacy (unmarked) snapshot is downloaded once to verify coverage, and a client holding it all pushes nothing", async () => {
        const { snapshot } = publishFold(updates, 3);
        // As a pre-marker fold left it: same vector, no marker.
        delete (server.main as Record<string, unknown>).stateVectorContiguous;

        const reader = new Y.Doc();
        Y.applyUpdate(reader, snapshot);
        let updateEvents = 0;
        reader.on('update', () => { updateEvents++; });

        const result = await measure(() => performInitialSync(ctxFor(reader)));

        expect(result.success).toBe(true);
        // The stored vector is not trusted for push coverage: the blob is
        // fetched (although the local doc covers it) to derive coverage...
        expect(server.downloads).toBe(1);
        // ...which proves the server holds everything: no push, no change.
        expect(server.added).toBe(0);
        expect(updateEvents).toBe(0);
        reader.destroy();
    });

    it('a new fold of data the client already holds hands Yjs no delete-set range', async () => {
        const { snapshot, fingerprint } = publishFold(updates, 3);
        const reader = new Y.Doc();
        Y.applyUpdate(reader, snapshot);
        const unsubscribe = createSnapshotListener(ctxFor(reader), 3);

        // Another client folds again (new version) without new data — e.g.
        // its fold merged the updates this client already received live.
        publishFold(updates, 4);
        await measure(deliverMain);

        expect(server.downloads).toBe(0);
        expect(probe.dsRanges, `re-applied ${probe.dsRanges} of ${rangeCount(fingerprint)} fingerprint ranges`).toBe(0);
        unsubscribe();
        reader.destroy();
    });

    it('a deletion the client missed still arrives through the fingerprint at start', async () => {
        const { snapshot } = publishFold(updates, 3);
        const reader = new Y.Doc();
        Y.applyUpdate(reader, snapshot);

        // A delete-only change (no structs: the state vector does not move)
        // folded on the server while the reader was offline.
        const victim = [...writer.getMap('annotations').keys()][0];
        writer.getMap('annotations').delete(victim);
        publishFold(updates, 4);
        expect(reader.getMap('annotations').has(victim)).toBe(true);

        const result = await measure(() => performInitialSync(ctxFor(reader)));

        expect(result.success).toBe(true);
        expect(server.downloads).toBe(0); // state vector covered: only the fingerprint can carry it
        expect(reader.getMap('annotations').has(victim)).toBe(false);
        expect(reader.getMap('annotations').toJSON()).toEqual(writer.getMap('annotations').toJSON());
        expect(server.added).toBe(0);
        reader.destroy();
    });

    it('a deletion an observer makes in reaction to the start\'s apply is still pushed', async () => {
        // The reader lacks the last save: the snapshot is downloaded and
        // applied, and the fingerprint is checked right after it.
        const { snapshot } = publishFold(updates, 3);
        const reader = new Y.Doc();
        updates.slice(0, -1).forEach(u => Y.applyUpdate(reader, u));
        // An app observer answers the remote change with a delete-only
        // local edit. It runs when the apply transaction ends, after the
        // fingerprint check and before the push guard.
        const notes = reader.getMap('annotations');
        const victim = [...notes.keys()][0];
        reader.getMap('progress').observe(() => {
            if (notes.has(victim)) notes.delete(victim);
        });

        const result = await performInitialSync(ctxFor(reader));

        expect(result.success).toBe(true);
        expect(server.downloads).toBe(1);
        expect(notes.has(victim)).toBe(false);
        // The guard judged the doc as it is now, not as it was at the check
        expect(server.added).toBe(1);
        const check = new Y.Doc();
        Y.applyUpdate(check, snapshot);
        Y.applyUpdate(check, (server.pushed[0].update as Bytes).toUint8Array());
        expect(check.getMap('annotations').has(victim)).toBe(false);
        check.destroy();
        reader.destroy();
    });

    it('a deletion the client missed still arrives through the fingerprint on a new fold', async () => {
        const { snapshot } = publishFold(updates, 3);
        const reader = new Y.Doc();
        Y.applyUpdate(reader, snapshot);
        const unsubscribe = createSnapshotListener(ctxFor(reader), 3);

        const victim = [...writer.getMap('annotations').keys()][0];
        writer.getMap('annotations').delete(victim);
        publishFold(updates, 4);
        await measure(deliverMain);

        expect(server.downloads).toBe(0);
        expect(reader.getMap('annotations').has(victim)).toBe(false);
        expect(reader.getMap('annotations').toJSON()).toEqual(writer.getMap('annotations').toJSON());
        unsubscribe();
        reader.destroy();
    });
});
