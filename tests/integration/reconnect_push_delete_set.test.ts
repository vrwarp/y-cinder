/**
 * Performance regression: the initial-sync push of a few offline edits must
 * upload what the server lacks, not the document's whole delete-set.
 *
 * When the local doc holds structs the server does not (offline reading, or
 * a save lost when the previous session ended), performInitialSync pushes
 * `Y.encodeStateAsUpdate(ydoc, serverSV)`. Yjs embeds the COMPLETE local
 * delete-set in every such diff, so one inserted character is written to
 * Firestore as an update document whose size is linear in the document's
 * historical churn. Every online peer integrates that whole delete-set, and
 * the next delta compaction copies it into a history segment flagged
 * `hasDeletions`, which fresh and returning clients always apply until the
 * next fold.
 *
 * Setup: an aged document (one fresh client per session, each adding about
 * RUN_DELETIONS - OLD_DELETIONS delete-set ranges) is folded on the
 * server into a snapshot plus its delete-set fingerprint, so the server
 * PROVABLY holds every deletion. A client hydrated with that same state (as
 * a local IndexedDB copy would hold it) makes one offline edit and runs the
 * real performInitialSync.
 *
 * Contract pinned here (counted in bytes and delete-set ranges written to
 * Firestore, never wall-clock):
 *  - the pushed update carries no deletion the server already proves it
 *    holds: its delete-set ranges are at most those of the offline edit
 *    itself, and its size is O(offline edit), not O(document age);
 *  - the delta segment built from it does not inherit the delete-set;
 *  - pushed bytes stay flat when the document's history is 4x longer;
 *  - the same holds for a deletion-only edit (the push guard's structs-
 *    empty branch) and for a stale client that downloads the snapshot;
 *  - correctness: genuine offline deletions still reach the server, and a
 *    fresh client converges with the reconnecting one.
 *
 * @file reconnect_push_delete_set.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import * as Y from 'yjs';
import {
    Firestore,
    collection,
    addDoc,
    getDoc,
    getDocs,
    doc,
    serverTimestamp,
    Bytes,
} from 'firebase/firestore';
import { FirebaseStorage, ref, getBytes } from 'firebase/storage';
import { setupEmulator } from '../utils/emulator';
import { getStableDate, SeededRandom } from '../unit/prng';
import { compact, CompactionContext } from '../../src/compaction';
import { performInitialSync } from '../../src/sync';
import { extractClockEnds, aggregateClockEnds } from '../../src/update-metadata';
import { FIRESTORE_PATHS } from '../../src/types';

const SEED = 20261002;
/** Characters deleted from each session's own typed run (non-adjacent). */
const RUN_DELETIONS = 16;
/**
 * Older characters deleted per session. Each usually sits between two
 * already-deleted characters and merges their ranges, so a session adds
 * about RUN_DELETIONS - OLD_DELETIONS delete-set ranges.
 */
const OLD_DELETIONS = 4;
const MIN_RANGES_PER_SESSION = 10;
const SMALL_AGE = 12;
const LARGE_AGE = 4 * SMALL_AGE;
const RECONNECT_CLIENT_ID = 77_000_001;

/**
 * The full state of a document edited across `sessions` sessions, each by a
 * fresh client: type a run of text, delete every other character of it
 * (RUN_DELETIONS separate delete-set ranges), and delete a few older
 * characters. Deterministic for a given session count.
 */
function buildAgedState(sessions: number): Uint8Array {
    const rng = new SeededRandom(SEED);
    let state: Uint8Array | null = null;
    for (let s = 0; s < sessions; s++) {
        const d = new Y.Doc();
        d.clientID = 1_000 + s;
        if (state) Y.applyUpdate(d, state);
        const text = d.getText('content');
        const pos = rng.int(0, text.length);
        d.transact(() => text.insert(pos, rng.string(2 * RUN_DELETIONS)));
        d.transact(() => {
            // Deleting at pos+i after i earlier deletions removes the
            // original characters pos, pos+2, pos+4, ...
            for (let i = 0; i < RUN_DELETIONS; i++) text.delete(pos + i, 1);
        });
        for (let i = 0; i < OLD_DELETIONS && text.length > 1; i++) {
            d.transact(() => text.delete(rng.int(0, text.length - 1), 1));
        }
        state = Y.encodeStateAsUpdate(d);
        d.destroy();
    }
    return state!;
}

function dsRangesOf(update: Uint8Array): number {
    let n = 0;
    Y.decodeUpdate(update).ds.clients.forEach((items) => { n += items.length; });
    return n;
}

function docDsRanges(d: Y.Doc): number {
    let n = 0;
    Y.createDeleteSetFromStructStore((d as any).store).clients.forEach((items: unknown[]) => { n += items.length; });
    return n;
}

type OfflineEdit = (text: Y.Text) => void;

/** One inserted run: one struct, no deletion. */
const insertOnly: OfflineEdit = (text) => text.insert(0, 'offline ');

/** One inserted character and one deleted live character. */
const insertAndDelete: OfflineEdit = (text) => {
    text.delete(Math.floor(text.length / 2), 1);
    text.insert(0, 'x');
};

/** One deleted live character: no struct, so the server covers every local struct. */
const deleteOnly: OfflineEdit = (text) => {
    text.delete(Math.floor(text.length / 2), 1);
};

describe('Initial-sync push of offline edits on an aged document', () => {
    let db: Firestore;
    let storage: FirebaseStorage;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        db = setup.db as unknown as Firestore;
        storage = setup.storage as unknown as FirebaseStorage;
    });

    const newPath = () => `tests/reconnect-push-ds-${getStableDate()}-${Date.now()}-${counter++}`;

    const compactionCtx = (path: string): CompactionContext => ({
        db,
        path,
        uid: 'compactor',
        lockTTL: 60000,
        compactionLimit: 500,
        isDestroyed: () => false,
        storage,
        historyFoldThreshold: 8,
    });

    /** Server holds `state` folded into a snapshot plus its delete-set fingerprint. */
    async function seedCompactedServer(path: string, state: Uint8Array): Promise<void> {
        await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
            update: Bytes.fromUint8Array(state),
            createdAt: serverTimestamp(),
            createdBy: 'seed',
            ...aggregateClockEnds(extractClockEnds(state)),
        });
        const result = await compact(compactionCtx(path));
        expect(result.type).toBe('snapshot');
        const main = (await getDoc(doc(db, path))).data();
        // The inline fingerprint is what proves server delete-set coverage.
        expect(main?.deleteSet).toBeDefined();
        expect((await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES))).size).toBe(0);
    }

    /**
     * A client hydrated with `state` makes `edit` offline, then completes
     * initial sync. Returns the update document initial sync wrote, and the
     * offline edit's own update (the O(new data) floor).
     */
    async function reconnectWithOfflineEdit(path: string, state: Uint8Array, edit: OfflineEdit) {
        const local = new Y.Doc();
        local.clientID = RECONNECT_CLIENT_ID;
        Y.applyUpdate(local, state);
        const offline: Uint8Array[] = [];
        const capture = (u: Uint8Array) => offline.push(u);
        local.on('update', capture);
        local.transact(() => edit(local.getText('content')));
        local.off('update', capture);

        const result = await performInitialSync({
            db,
            path,
            doc: local,
            uid: 'reconnecting-client',
            maxUpdatesThreshold: 1000,
            isDestroyed: () => false,
            storage,
        });
        expect(result.success).toBe(true);
        expect(result.localUpdatesPushed).toBe(true);

        const updates = await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES));
        expect(updates.size).toBe(1);
        const data = updates.docs[0].data();
        const pushed = data.updateStoragePath
            ? new Uint8Array(await getBytes(ref(storage, data.updateStoragePath)))
            : (data.update as Bytes).toUint8Array();

        return {
            pushed,
            floor: Y.mergeUpdates(offline),
            localText: local.getText('content').toString(),
            localDsRanges: docDsRanges(local),
        };
    }

    async function freshClientText(path: string): Promise<string> {
        const fresh = new Y.Doc();
        try {
            const result = await performInitialSync({
                db,
                path,
                doc: fresh,
                uid: 'fresh-reader',
                maxUpdatesThreshold: 1000,
                isDestroyed: () => false,
                storage,
            });
            expect(result.success).toBe(true);
            expect((fresh.store as any).pendingStructs).toBeNull();
            return fresh.getText('content').toString();
        } finally {
            fresh.destroy();
        }
    }

    it('an insert-only offline edit pushes no delete-set ranges, and the next segment inherits none', async () => {
        const path = newPath();
        const state = buildAgedState(LARGE_AGE);
        await seedCompactedServer(path, state);

        const r = await reconnectWithOfflineEdit(path, state, insertOnly);
        console.log(
            `[reconnect push, insert-only, ${LARGE_AGE} sessions] local delete-set ${r.localDsRanges} ranges; ` +
            `pushed ${r.pushed.byteLength} B (${dsRangesOf(r.pushed)} ranges) for an offline edit of ` +
            `${r.floor.byteLength} B (${dsRangesOf(r.floor)} ranges)`
        );

        // Correctness first: the edit reached the server.
        expect(await freshClientText(path)).toBe(r.localText);

        expect(r.localDsRanges).toBeGreaterThanOrEqual(LARGE_AGE * MIN_RANGES_PER_SESSION);
        expect(dsRangesOf(r.floor)).toBe(0);
        // Every local deletion is proven by the server's fingerprint. (Soft,
        // so the segment below is still measured when this fails.)
        expect.soft(dsRangesOf(r.pushed), 'pushed update re-uploads deletions the server already holds').toBe(0);
        expect.soft(r.pushed.byteLength, 'pushed update is not O(offline edit)')
            .toBeLessThanOrEqual(2 * r.floor.byteLength + 16);

        // The next delta compaction must not copy a delete-set into history.
        const result = await compact(compactionCtx(path));
        expect(result.type).toBe('history');
        const history = await getDocs(collection(db, path, FIRESTORE_PATHS.HISTORY));
        expect(history.size).toBe(1);
        const seg = history.docs[0].data();
        const segment = (seg.segment as Bytes).toUint8Array();
        console.log(
            `[next delta segment] ${segment.byteLength} B, ${dsRangesOf(segment)} ranges, hasDeletions=${seg.hasDeletions === true}`
        );
        expect.soft(seg.hasDeletions, 'segment flagged hasDeletions: every fresh client must apply it').not.toBe(true);
        expect.soft(dsRangesOf(segment), 'segment inherited the delete-set').toBe(0);
        expect(await freshClientText(path)).toBe(r.localText);
    }, 90000);

    it('a genuine offline deletion is pushed, and no deletion the server already holds', async () => {
        const path = newPath();
        const state = buildAgedState(LARGE_AGE);
        await seedCompactedServer(path, state);

        const r = await reconnectWithOfflineEdit(path, state, insertAndDelete);
        console.log(
            `[reconnect push, insert+delete, ${LARGE_AGE} sessions] local delete-set ${r.localDsRanges} ranges; ` +
            `pushed ${r.pushed.byteLength} B (${dsRangesOf(r.pushed)} ranges) for an offline edit of ` +
            `${r.floor.byteLength} B (${dsRangesOf(r.floor)} ranges)`
        );

        // The offline deletion must reach the server: a fresh client sees
        // the document exactly as the reconnecting client does.
        expect(await freshClientText(path)).toBe(r.localText);

        expect(dsRangesOf(r.floor)).toBe(1);
        expect(dsRangesOf(r.pushed), 'pushed update re-uploads deletions the server already holds')
            .toBeLessThanOrEqual(dsRangesOf(r.floor));
        expect(r.pushed.byteLength, 'pushed update is not O(offline edit)')
            .toBeLessThanOrEqual(2 * r.floor.byteLength + 16);
    }, 90000);

    it('a deletion-only offline edit pushes that deletion, and no deletion the server already holds', async () => {
        const path = newPath();
        const state = buildAgedState(LARGE_AGE);
        await seedCompactedServer(path, state);

        const r = await reconnectWithOfflineEdit(path, state, deleteOnly);
        console.log(
            `[reconnect push, delete-only, ${LARGE_AGE} sessions] local delete-set ${r.localDsRanges} ranges; ` +
            `pushed ${r.pushed.byteLength} B (${dsRangesOf(r.pushed)} ranges) for an offline edit of ` +
            `${r.floor.byteLength} B (${dsRangesOf(r.floor)} ranges)`
        );

        expect(await freshClientText(path)).toBe(r.localText);

        // The deletions-only push: structs-empty by construction
        expect(Y.decodeUpdate(r.pushed).structs.length).toBe(0);
        expect(dsRangesOf(r.floor)).toBe(1);
        expect(dsRangesOf(r.pushed), 'pushed update re-uploads deletions the server already holds')
            .toBeLessThanOrEqual(dsRangesOf(r.floor));
        expect(r.pushed.byteLength, 'pushed update is not O(offline edit)')
            .toBeLessThanOrEqual(2 * r.floor.byteLength + 16);
    }, 90000);

    it('a stale client that downloads the snapshot still pushes only its offline edit', async () => {
        const path = newPath();
        await seedCompactedServer(path, buildAgedState(LARGE_AGE));
        // The local copy predates the last two sessions, so initial sync
        // downloads and applies the snapshot; the fingerprint, not the
        // snapshot content, proves the server's deletions.
        const stale = buildAgedState(LARGE_AGE - 2);
        expect(Y.decodeStateVector(Y.encodeStateVectorFromUpdate(stale)).has(1_000 + LARGE_AGE - 1)).toBe(false);

        const r = await reconnectWithOfflineEdit(path, stale, insertOnly);
        console.log(
            `[reconnect push, stale client, ${LARGE_AGE} sessions] local delete-set ${r.localDsRanges} ranges; ` +
            `pushed ${r.pushed.byteLength} B (${dsRangesOf(r.pushed)} ranges) for an offline edit of ` +
            `${r.floor.byteLength} B (${dsRangesOf(r.floor)} ranges)`
        );

        expect(await freshClientText(path)).toBe(r.localText);
        expect(r.localDsRanges).toBeGreaterThanOrEqual(LARGE_AGE * MIN_RANGES_PER_SESSION);
        expect(dsRangesOf(r.pushed), 'pushed update re-uploads deletions the server already holds').toBe(0);
        expect(r.pushed.byteLength, 'pushed update is not O(offline edit)')
            .toBeLessThanOrEqual(2 * r.floor.byteLength + 16);
    }, 90000);

    it('pushed bytes stay flat when the document history is 4x longer', async () => {
        const measure = async (sessions: number) => {
            const path = newPath();
            const state = buildAgedState(sessions);
            await seedCompactedServer(path, state);
            return reconnectWithOfflineEdit(path, state, insertOnly);
        };
        const small = await measure(SMALL_AGE);
        const large = await measure(LARGE_AGE);

        const ageRatio = large.localDsRanges / small.localDsRanges;
        const pushRatio = large.pushed.byteLength / small.pushed.byteLength;
        console.log(
            `[reconnect push scaling] delete-set ${small.localDsRanges} -> ${large.localDsRanges} ranges (${ageRatio.toFixed(2)}x); ` +
            `pushed ${small.pushed.byteLength} -> ${large.pushed.byteLength} B (${pushRatio.toFixed(2)}x); ` +
            `offline edit ${small.floor.byteLength} -> ${large.floor.byteLength} B`
        );

        expect(ageRatio).toBeGreaterThan(3.5);
        // Same offline edit at both ages (origin ids may differ by a varint byte).
        expect(Math.abs(large.floor.byteLength - small.floor.byteLength)).toBeLessThanOrEqual(4);
        expect(pushRatio, 'pushed bytes grow with document age').toBeLessThanOrEqual(1.25);
    }, 120000);
});
