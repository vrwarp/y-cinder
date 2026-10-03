/**
 * Regression test: a delta history segment stored WITHOUT `hasDeletions`
 * may still carry deletions, and a returning device must apply them.
 *
 * febdebd made delta compaction flag segments whose merged delete-set is
 * non-empty (`hasDeletions: true`, omitted when false) and made readers
 * skip a segment whose state vector they cover unless it is flagged (or its
 * vector is empty). But the flag is new: clients still running the older
 * code (4297ea9) keep delta-compacting without it, and every segment
 * already in Firestore at upgrade time lacks it — whether or not it holds
 * deletions. An absent flag therefore cannot mean "no deletions".
 *
 * Scenario (a mixed fleet, or a pre-upgrade segment):
 *  - A folds 'base ' into the base snapshot;
 *  - device B inserts 'XYZ'; A receives it;
 *  - B goes away keeping its local doc (provider destroyed, or y-idb);
 *  - A deletes 'Y' and an OLD-code compactor merges B's insert and A's
 *    deletion into one delta segment: state vector {B: 3}, no flag;
 *  - B returns. Its doc covers the segment's state vector, so the segment
 *    is judged redundant and skipped — at initial sync, and in the history
 *    listener alike. B keeps showing 'base XYZ' (and writes it back into
 *    its local persistence) until a fold's delete-set fingerprint repairs
 *    it, up to historyFoldThreshold x maxUpdatesThreshold updates later.
 *
 * Firestore and Storage are faked at the SDK boundary (the real `Bytes` is
 * kept); the real `performInitialSync` / `createHistoryListener` run. The
 * legacy segment has exactly the fields 4297ea9's tryDeltaCompaction
 * writes: stateVector, createdBy, segment, startTime (epoch omitted at 0).
 *
 * Contract asserted: after the returning device syncs, it shows the same
 * content as the device that made the deletion.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

const server = vi.hoisted(() => ({
    /** History segment documents, in startTime order. */
    history: [] as Record<string, any>[],
    /** Main document (null = does not exist). */
    main: null as Record<string, any> | null,
    /** Cloud Storage objects by path. */
    storage: new Map<string, Uint8Array>(),
    /** Live onSnapshot callbacks, by queried path. */
    listeners: [] as { path: string; next: (snap: any) => unknown }[],
}));

vi.mock('@firebase/firestore', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/firestore')>();
    const join = (parts: unknown[]) => parts.filter(p => typeof p === 'string').join('/');
    const docSnap = (row: Record<string, any>, i: number) => ({
        id: `history-${i}`,
        ref: { path: `history-${i}` },
        metadata: { hasPendingWrites: false, fromCache: false },
        data: () => ({ ...row }),
    });
    const querySnap = (docs: ReturnType<typeof docSnap>[]) => ({
        docs,
        empty: docs.length === 0,
        size: docs.length,
        metadata: { hasPendingWrites: false, fromCache: false },
        forEach: (fn: (d: unknown) => void) => docs.forEach(fn),
    });
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
            // Compaction already deleted the update documents it merged.
            const rows = q.path.endsWith('/history') ? server.history : [];
            const after = q.constraints?.find((c: any) => c.startAfter)?.startAfter;
            const start = after ? Number(after.id.slice('history-'.length)) + 1 : 0;
            const max = q.constraints?.find((c: any) => c.limit)?.limit ?? rows.length;
            return querySnap(rows.map(docSnap).slice(start, start + max));
        },
        getDoc: async () => ({
            exists: () => server.main !== null,
            metadata: { hasPendingWrites: false, fromCache: false },
            data: () => (server.main ? { ...server.main } : undefined),
        }),
        addDoc: async () => ({ id: 'added' }),
        onSnapshot: (target: any, next: (snap: any) => unknown) => {
            const entry = { path: target.path as string, next };
            server.listeners.push(entry);
            return () => {
                server.listeners = server.listeners.filter(l => l !== entry);
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
            if (!blob) {
                throw Object.assign(new Error(`object-not-found: ${r.fullPath}`), { code: 'storage/object-not-found' });
            }
            return blob.slice().buffer;
        },
        uploadBytes: async () => undefined,
        deleteObject: async () => undefined,
    };
});

import * as Y from 'yjs';
import { Bytes } from '@firebase/firestore';
import { toBase64 } from 'lib0/buffer';
import { performInitialSync, createHistoryListener, SyncContext } from '../../src/sync';
import { mergeUpdatesWithMeta } from '../../src/merge-core';

const PATH = 'docs/legacy-segment-deletions';
const CLIENT_A = 101;
const CLIENT_B = 202;

function ctxFor(doc: Y.Doc): SyncContext {
    return {
        db: {} as any,
        storage: {} as any,
        path: PATH,
        doc,
        uid: 'device-b',
        maxUpdatesThreshold: 50,
        isDestroyed: () => false,
    };
}

/** Records the update blobs `doc` emits while `edit` runs. */
function capture(doc: Y.Doc, edit: () => void): Uint8Array[] {
    const blobs: Uint8Array[] = [];
    const onUpdate = (u: Uint8Array) => blobs.push(u);
    doc.on('update', onUpdate);
    try {
        edit();
    } finally {
        doc.off('update', onUpdate);
    }
    return blobs;
}

/**
 * A writes 'base ' and folds it into the base snapshot (snapshot in
 * Storage, state vector and delete-set fingerprint inline, as compaction
 * writes them); B starts from that snapshot.
 */
function seedBaseFold(): { docA: Y.Doc; docB: Y.Doc } {
    const docA = new Y.Doc();
    docA.clientID = CLIENT_A;
    const docB = new Y.Doc();
    docB.clientID = CLIENT_B;

    const baseUpdates = capture(docA, () => docA.getText('note').insert(0, 'base '));
    const fold = mergeUpdatesWithMeta(baseUpdates, { gc: true });
    server.storage.set(`${PATH}/snapshots/v1`, fold.result);
    server.main = {
        version: 1,
        stateVector: toBase64(fold.stateVector),
        snapshotStoragePath: `${PATH}/snapshots/v1`,
        deleteSet: Bytes.fromUint8Array(fold.dsUpdate),
    };
    Y.applyUpdate(docB, fold.result);
    return { docA, docB };
}

/** A history segment in the format clients without the hasDeletions flag write. */
function legacySegmentDoc(updates: Uint8Array[]): Record<string, any> {
    const merged = mergeUpdatesWithMeta(updates, { gc: false });
    return {
        stateVector: toBase64(merged.stateVector),
        createdBy: 'device-a-old-client',
        segment: Bytes.fromUint8Array(merged.result),
        startTime: { seconds: 1_790_000_000, nanoseconds: 0 },
    };
}

/**
 * Builds the server state and the returning device's local doc:
 *
 *  - main document: a fold of A's 'base ' (snapshot in Storage, state
 *    vector and delete-set fingerprint inline), as compaction writes it;
 *  - history: ONE delta segment merging B's 'XYZ' insert with A's deletion
 *    of 'Y', in the format clients without the hasDeletions flag write;
 *  - docB: the returning device — base + its own 'XYZ', no deletion.
 */
function buildMixedFleetState(): { docA: Y.Doc; docB: Y.Doc } {
    // 1. A writes 'base ' and folds it into the base snapshot.
    const { docA, docB } = seedBaseFold();

    // 2. B inserts 'XYZ'; A receives it.
    const insertB = capture(docB, () => docB.getText('note').insert(5, 'XYZ'));
    insertB.forEach(u => Y.applyUpdate(docA, u));

    // 3. B goes away (its doc is kept). A deletes 'Y'.
    const deleteA = capture(docA, () => docA.getText('note').delete(6, 1));
    expect(docA.getText('note').toString()).toBe('base XZ');

    // 4. An old-code compactor delta-compacts both pending update documents
    //    into one history segment. 4297ea9's tryDeltaCompaction writes
    //    { stateVector, createdBy, segment, startTime } — no hasDeletions.
    server.history = [legacySegmentDoc([...insertB, ...deleteA])];

    // The returning device still shows the deleted 'Y'.
    expect(docB.getText('note').toString()).toBe('base XYZ');
    return { docA, docB };
}

describe('delta segments stored without hasDeletions (old clients / pre-upgrade)', () => {
    beforeEach(() => {
        server.history = [];
        server.main = null;
        server.storage.clear();
        server.listeners = [];
        vi.spyOn(console, 'log').mockImplementation(() => { });
        vi.spyOn(console, 'warn').mockImplementation(() => { });
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('a returning device applies the deletions of a legacy segment at initial sync', async () => {
        const { docA, docB } = buildMixedFleetState();

        const result = await performInitialSync(ctxFor(docB));

        expect(result.success).toBe(true);
        // The 'Y' that A deleted must not survive on the returning device.
        expect(docB.getText('note').toString()).toBe('base XZ');
        expect(docB.getText('note').toString()).toBe(docA.getText('note').toString());

        docA.destroy();
        docB.destroy();
    });

    it('a returning device applies the deletions of a legacy segment whose structs it received from another device', async () => {
        // The covered structs need not be the returning device's own: A
        // inserts 'abc' and B receives it before going away; A then deletes
        // 'a' and an old-code compactor merges both (state vector {A: n}).
        const { docA, docB } = seedBaseFold();
        const insertA = capture(docA, () => docA.getText('note').insert(5, 'abc'));
        insertA.forEach(u => Y.applyUpdate(docB, u));
        const deleteA = capture(docA, () => docA.getText('note').delete(5, 1));
        server.history = [legacySegmentDoc([...insertA, ...deleteA])];
        expect(docA.getText('note').toString()).toBe('base bc');
        expect(docB.getText('note').toString()).toBe('base abc');

        const result = await performInitialSync(ctxFor(docB));

        expect(result.success).toBe(true);
        expect(docB.getText('note').toString()).toBe('base bc');

        docA.destroy();
        docB.destroy();
    });

    it('a returning device applies the deletions of a legacy segment delivered by the history listener', async () => {
        // B's provider is already running (its doc held 'XYZ' when the
        // listener started) and went briefly offline; meanwhile A deleted
        // 'Y' and an old-code compactor wrote the segment. When B is back
        // online, the history listener delivers it.
        const { docA, docB } = buildMixedFleetState();
        const segment = server.history[0];
        server.history = [];

        const unsubscribe = createHistoryListener(ctxFor(docB), null);
        const listener = server.listeners.find(l => l.path.endsWith('/history'));
        expect(listener).toBeDefined();

        await listener!.next({
            docChanges: () => [{
                type: 'added',
                doc: { id: 'history-0', data: () => ({ ...segment }) },
            }],
        });

        expect(docB.getText('note').toString()).toBe('base XZ');
        expect(docB.getText('note').toString()).toBe(docA.getText('note').toString());

        unsubscribe();
        docA.destroy();
        docB.destroy();
    });
});
