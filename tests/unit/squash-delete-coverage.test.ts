/**
 * Regression: squashDocument must not publish a new epoch from a local
 * document that is missing a deletion the server already holds.
 *
 * squashDocument guards "the squasher must hold everything the server
 * holds" with state-vector checks only (main stateVector, pending docs'
 * clientClocks / stateVector). Deletions do not advance a state vector, so
 * a squasher that never received a server-side deletion passes every
 * check, clones its stale content into epoch N+1, and the transaction
 * deletes the old-epoch documents that carried the deletion. Every client
 * that rebuilds from the new epoch then sees the deleted content again.
 *
 * Two ways the server holds a deletion the squasher lacks:
 *  - another client (or the squasher's own pre-squash compact()) folded a
 *    delete-only update into the main snapshot: the main stateVector does
 *    not move, only the deleteSet fingerprint does;
 *  - a delta-compaction history segment carries the deletion: a
 *    delete-only segment's stateVector is empty ('AA=='), and a mixed one
 *    reflects only its inserts.
 *
 * Firestore, Storage and the lock are replaced by a small in-memory fake
 * at the module boundary, so the server state is exactly the one the real
 * compaction code writes (same merge helper, same field shapes). The
 * contract checked is user-visible: after the squasher calls squash, a
 * fresh client rebuilding from the server must still see the deletion —
 * whether the squash refused (local-behind) or published an epoch that
 * reflects it.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import * as Y from 'yjs';
import { toBase64 } from 'lib0/buffer';

// ---------------------------------------------------------------------------
// In-memory Firestore / Storage fake
// ---------------------------------------------------------------------------

const server = vi.hoisted(() => {
    const DELETE = Symbol('deleteField');
    const TIMESTAMP = Symbol('serverTimestamp');

    class FakeBytes {
        constructor(private readonly bytes: Uint8Array) { }
        static fromUint8Array(u: Uint8Array): FakeBytes {
            return new FakeBytes(u.slice());
        }
        toUint8Array(): Uint8Array {
            return this.bytes.slice();
        }
    }

    /** Firestore documents keyed by full path, in insertion order. */
    const docs = new Map<string, Record<string, any>>();
    /** Cloud Storage objects keyed by full path. */
    const blobs = new Map<string, Uint8Array>();
    let autoId = 0;

    const join = (parts: string[]) => parts.filter(Boolean).join('/');

    function snapshotOf(path: string) {
        const data = docs.get(path);
        const ref = { path, id: path.split('/').pop() as string };
        return {
            id: ref.id,
            ref,
            exists: () => data !== undefined,
            data: () => (data === undefined ? undefined : { ...data }),
        };
    }

    function write(path: string, data: Record<string, any>, merge: boolean) {
        const next: Record<string, any> = merge ? { ...(docs.get(path) ?? {}) } : {};
        for (const [k, v] of Object.entries(data)) {
            if (v === DELETE) delete next[k];
            else if (v === TIMESTAMP) next[k] = { toMillis: () => Date.now() };
            else next[k] = v;
        }
        docs.set(path, next);
    }

    function childrenOf(collectionPath: string) {
        const prefix = collectionPath + '/';
        return [...docs.keys()].filter(
            (p) => p.startsWith(prefix) && !p.slice(prefix.length).includes('/'),
        );
    }

    return {
        DELETE,
        TIMESTAMP,
        FakeBytes,
        docs,
        blobs,
        join,
        snapshotOf,
        write,
        childrenOf,
        nextId: () => `auto${++autoId}`,
        reset() {
            docs.clear();
            blobs.clear();
            autoId = 0;
        },
    };
});

vi.mock('@firebase/firestore', () => {
    const doc = (base: any, ...segments: string[]) => {
        const prefix = base && typeof base.path === 'string' ? base.path : '';
        const path = server.join([prefix, ...segments]);
        if (segments.length === 0) {
            // doc(collectionRef) -> auto id
            const full = server.join([prefix, server.nextId()]);
            return { path: full, id: full.split('/').pop() };
        }
        return { path, id: path.split('/').pop() };
    };
    const collection = (base: any, ...segments: string[]) => {
        const prefix = base && typeof base.path === 'string' ? base.path : '';
        return { path: server.join([prefix, ...segments]) };
    };
    return {
        Bytes: server.FakeBytes,
        doc,
        collection,
        query: (coll: { path: string }) => ({ path: coll.path }),
        orderBy: () => ({}),
        limit: () => ({}),
        where: () => ({}),
        serverTimestamp: () => server.TIMESTAMP,
        deleteField: () => server.DELETE,
        getDoc: async (ref: { path: string }) => server.snapshotOf(ref.path),
        getDocs: async (q: { path: string }) => {
            const docs = server.childrenOf(q.path).map((p) => server.snapshotOf(p));
            return { docs, empty: docs.length === 0, size: docs.length };
        },
        setDoc: async (ref: { path: string }, data: any, opts?: { merge?: boolean }) => {
            server.write(ref.path, data, Boolean(opts?.merge));
        },
        deleteDoc: async (ref: { path: string }) => {
            server.docs.delete(ref.path);
        },
        runTransaction: async (_db: unknown, fn: (tx: any) => Promise<any>) => {
            const writes: Array<() => void> = [];
            const tx = {
                get: async (ref: { path: string }) => server.snapshotOf(ref.path),
                set: (ref: { path: string }, data: any, opts?: { merge?: boolean }) => {
                    writes.push(() => server.write(ref.path, data, Boolean(opts?.merge)));
                    return tx;
                },
                update: (ref: { path: string }, data: any) => {
                    writes.push(() => server.write(ref.path, data, true));
                    return tx;
                },
                delete: (ref: { path: string }) => {
                    writes.push(() => { server.docs.delete(ref.path); });
                    return tx;
                },
            };
            const result = await fn(tx);
            writes.forEach((w) => w());
            return result;
        },
    };
});

vi.mock('@firebase/storage', () => ({
    ref: (_storage: unknown, path: string) => ({ fullPath: path }),
    uploadBytes: async (r: { fullPath: string }, bytes: Uint8Array) => {
        server.blobs.set(r.fullPath, bytes.slice());
        return {};
    },
    getBytes: async (r: { fullPath: string }) => {
        const b = server.blobs.get(r.fullPath);
        if (!b) throw new Error(`storage/object-not-found: ${r.fullPath}`);
        return b.slice().buffer;
    },
    deleteObject: async (r: { fullPath: string }) => {
        server.blobs.delete(r.fullPath);
    },
}));

// The squasher always wins the distributed lock; the commit transaction's
// own lock re-check reads the lock document seeded below.
vi.mock('../../src/locking', () => ({
    acquireLock: async () => true,
    releaseLock: async () => undefined,
}));

import { squashDocument, SquashResult } from '../../src/squash';
import { mergeUpdatesWithMeta } from '../../src/merge-core';
import { FIRESTORE_PATHS } from '../../src/types';

// ---------------------------------------------------------------------------
// Scenario helpers
// ---------------------------------------------------------------------------

const PATH = 'docs/squash-delete-coverage';
const SQUASHER_UID = 'uid-B';
const WRITER_CLIENT = 101;
const SQUASHER_CLIENT = 202;

/** Writer A's edits: initial content, then a delete-only follow-up. */
function writerEdits(): { initial: Uint8Array; deletion: Uint8Array } {
    const a = new Y.Doc();
    a.clientID = WRITER_CLIENT;
    a.getText('text').insert(0, 'hello world');
    a.getMap('library').set('book1', { title: 'Book One' });
    a.getMap('library').set('book2', { title: 'Book Two' });
    const initial = Y.encodeStateAsUpdate(a);

    const before = Y.encodeStateVector(a);
    a.transact(() => {
        a.getText('text').delete(5, 6); // ' world'
        a.getMap('library').delete('book1');
    });
    const deletion = Y.encodeStateAsUpdate(a, before);
    a.destroy();
    return { initial, deletion };
}

/** Writes a full fold of `updates` to the main document, as compaction does. */
function seedFoldedSnapshot(updates: Uint8Array[], version: number) {
    const merged = mergeUpdatesWithMeta(updates, { gc: true });
    const storagePath = `${PATH}/snapshot_v${version}.bin`;
    server.blobs.set(storagePath, merged.result);
    server.write(PATH, {
        snapshotStoragePath: storagePath,
        stateVector: toBase64(merged.stateVector),
        deleteSet: server.FakeBytes.fromUint8Array(merged.dsUpdate),
        version,
        updatedAt: server.TIMESTAMP,
        origin: 'uid-A',
    }, true);
}

/** Writes a delta-compaction history segment, as tryDeltaCompaction does. */
function seedDeltaSegment(updates: Uint8Array[]) {
    const merged = mergeUpdatesWithMeta(updates, { gc: false });
    server.write(`${PATH}/${FIRESTORE_PATHS.HISTORY}/seg1`, {
        stateVector: toBase64(merged.stateVector),
        createdBy: 'uid-A',
        segment: server.FakeBytes.fromUint8Array(merged.result),
        startTime: server.TIMESTAMP,
    }, false);
}

/** Squasher B's live document, hydrated from the given updates and typed. */
function squasherDoc(updates: Uint8Array[]): Y.Doc {
    const b = new Y.Doc();
    b.clientID = SQUASHER_CLIENT;
    for (const u of updates) Y.applyUpdate(b, u);
    b.getText('text');
    b.getMap('library');
    return b;
}

async function runSquash(ydoc: Y.Doc): Promise<SquashResult> {
    server.write(`${PATH}/${FIRESTORE_PATHS.LOCK_COMPACTION}`, {
        owner: SQUASHER_UID,
        createdAt: server.TIMESTAMP,
    }, false);
    return squashDocument({
        db: {} as any,
        storage: {} as any,
        path: PATH,
        uid: SQUASHER_UID,
        lockTTL: 60_000,
        cachedClockOffset: 0,
        isDestroyed: () => false,
        doc: ydoc,
    });
}

/**
 * What a brand-new client sees when it loads the document from the server
 * after the squash: the main snapshot plus every history segment and update
 * of the current epoch.
 */
function freshClientView(): { epoch: number; text: string; library: Record<string, unknown> } {
    const main = server.docs.get(PATH);
    if (!main) throw new Error('main document missing');
    const epoch = typeof main.epoch === 'number' ? main.epoch : 0;
    const epochOf = (d: Record<string, any>) => (typeof d.epoch === 'number' ? d.epoch : 0);

    const d = new Y.Doc();
    if (typeof main.snapshotStoragePath === 'string') {
        const blob = server.blobs.get(main.snapshotStoragePath);
        if (!blob) throw new Error(`snapshot blob missing: ${main.snapshotStoragePath}`);
        Y.applyUpdate(d, blob);
    } else if (main.content) {
        Y.applyUpdate(d, main.content.toUint8Array());
    }
    for (const coll of [FIRESTORE_PATHS.HISTORY, FIRESTORE_PATHS.UPDATES]) {
        for (const p of server.childrenOf(`${PATH}/${coll}`)) {
            const data = server.docs.get(p)!;
            if (epochOf(data) !== epoch) continue;
            const bytes = data.segment ?? data.update;
            if (bytes) Y.applyUpdate(d, bytes.toUint8Array());
        }
    }
    const view = {
        epoch,
        text: d.getText('text').toString(),
        library: d.getMap('library').toJSON(),
    };
    d.destroy();
    return view;
}

const EXPECTED_AFTER_DELETION = {
    text: 'hello',
    library: { book2: { title: 'Book Two' } },
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('squashDocument: deletions the squasher has not received', () => {
    beforeEach(() => {
        server.reset();
        vi.spyOn(console, 'log').mockImplementation(() => { });
        vi.spyOn(console, 'warn').mockImplementation(() => { });
    });

    it('control: a squasher that holds the deletion publishes an epoch that reflects it', async () => {
        const { initial, deletion } = writerEdits();
        seedFoldedSnapshot([initial, deletion], 2);
        const b = squasherDoc([initial, deletion]);

        const result = await runSquash(b);

        expect(result.error).toBeUndefined();
        expect(result).toMatchObject({ success: true, epoch: 1 });
        expect(freshClientView()).toEqual({ epoch: 1, ...EXPECTED_AFTER_DELETION });
        b.destroy();
    });

    it('does not resurrect content deleted by a fold the squasher never received', async () => {
        // Server: A folded 'hello world' + book1/book2, then folded the
        // delete-only update. The main stateVector is unchanged by the
        // deletion; only the deleteSet fingerprint carries it.
        const { initial, deletion } = writerEdits();
        seedFoldedSnapshot([initial, deletion], 2);
        expect(freshClientView()).toEqual({ epoch: 0, ...EXPECTED_AFTER_DELETION });

        // B synced the first fold but has not applied the deletion yet
        // (its listener has not delivered it, or its own pre-squash
        // compact() folded it and the listener skipped the own-origin write).
        const b = squasherDoc([initial]);
        expect(b.getText('text').toString()).toBe('hello world');

        const result = await runSquash(b);

        // Either refusing (local-behind) or publishing a deletion-aware
        // epoch is acceptable; resurrecting ' world' and book1 is not.
        expect(result.error).toBeUndefined();
        const view = freshClientView();
        expect(
            { text: view.text, library: view.library },
            `squash returned ${JSON.stringify(result)}; a fresh client of epoch ${view.epoch} sees deleted content`,
        ).toEqual(EXPECTED_AFTER_DELETION);
        b.destroy();
    });

    it('does not resurrect content deleted by a pending delta segment the squasher never applied', async () => {
        // Server: fold v1 holds 'hello world' + book1/book2; the delete-only
        // update was delta-compacted into a history segment whose
        // stateVector is empty (a deletion advances no clock).
        const { initial, deletion } = writerEdits();
        seedFoldedSnapshot([initial], 1);
        seedDeltaSegment([deletion]);
        expect(server.docs.get(`${PATH}/${FIRESTORE_PATHS.HISTORY}/seg1`)!.stateVector).toBe('AA==');
        expect(freshClientView()).toEqual({ epoch: 0, ...EXPECTED_AFTER_DELETION });

        const b = squasherDoc([initial]);
        expect(b.getText('text').toString()).toBe('hello world');

        const result = await runSquash(b);

        expect(result.error).toBeUndefined();
        const view = freshClientView();
        expect(
            { text: view.text, library: view.library },
            `squash returned ${JSON.stringify(result)}; a fresh client of epoch ${view.epoch} sees deleted content`,
        ).toEqual(EXPECTED_AFTER_DELETION);
        b.destroy();
    });

    it('does not resurrect content deleted by a delta segment whose state vector reflects only its inserts', async () => {
        // Server: fold v1 holds 'hello world' + book1/book2; another
        // client's insert and A's delete-only update were delta-compacted
        // into one segment. Its stateVector carries the insert's clock and
        // nothing for the deletion.
        const { initial, deletion } = writerEdits();
        const c = new Y.Doc();
        c.clientID = 303;
        Y.applyUpdate(c, initial);
        const beforeInsert = Y.encodeStateVector(c);
        c.getMap('library').set('book3', { title: 'Book Three' });
        const insert = Y.encodeStateAsUpdate(c, beforeInsert);
        c.destroy();
        seedFoldedSnapshot([initial], 1);
        seedDeltaSegment([insert, deletion]);
        const expected = {
            text: 'hello',
            library: { book2: { title: 'Book Two' }, book3: { title: 'Book Three' } },
        };
        expect(freshClientView()).toEqual({ epoch: 0, ...expected });

        // B holds the insert, so it covers the segment's state vector, but
        // never applied the deletion.
        const b = squasherDoc([initial, insert]);
        expect(b.getText('text').toString()).toBe('hello world');

        const result = await runSquash(b);

        expect(result.error).toBeUndefined();
        const view = freshClientView();
        expect(
            { text: view.text, library: view.library },
            `squash returned ${JSON.stringify(result)}; a fresh client of epoch ${view.epoch} sees deleted content`,
        ).toEqual(expected);
        b.destroy();
    });
});
