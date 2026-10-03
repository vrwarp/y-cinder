/**
 * Regression test: a same-client update that fills a clock gap must be
 * applied even when it is delivered/ordered AFTER the later range.
 *
 * Client X (clientID 1111) types "hello" (clocks 0..5) and then " world"
 * (clocks 5..11). If the document carrying the later range is ordered before
 * the one carrying the earlier range (by createdAt for update documents, by
 * startTime for history segments), Yjs parks the later structs in
 * pendingStructs until the earlier range arrives. Sources of such inversions
 * in production: a debounced save / destroy flush committing before initial
 * sync's push of pre-existing local state, a storage-backed initial-sync push
 * (upload first, addDoc later), gapped snapshots/segments, delta compaction
 * carrying the inverted order into history.
 *
 * Contract: once both ranges are on the server, every client — a fresh one
 * running initial sync, and a live one receiving them through the realtime
 * listeners — must converge to "hello world".
 *
 * Bug: initial sync and the update/history listeners fold the later item's
 * CLAIMED clock end (X=11) into their cached local state vector right after
 * Y.applyUpdate, even though the structs stayed pending and the real state
 * vector did not move. The earlier-range item (X=5) is then classified as
 * redundant and skipped, so the gap never closes and X's content never
 * appears.
 *
 * Ordering is controlled explicitly: createdAt / startTime are fixed
 * Timestamps, and listener progress is observed through independent
 * "sentinel" updates from another client that are written after the items
 * under test (once the sentinel is visible, the earlier deliveries have been
 * processed).
 *
 * @file pending_gap_fill.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as Y from 'yjs';
import { toBase64 } from 'lib0/buffer';
import {
    Firestore,
    Timestamp,
    Bytes,
    addDoc,
    collection,
} from '@firebase/firestore';
import { ref, uploadBytes, type FirebaseStorage } from '@firebase/storage';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionEquals } from '../utils/wait';
import { getStableDate } from '../unit/prng';
import {
    performInitialSync,
    createUpdateListener,
    createHistoryListener,
    SyncContext,
} from '../../src/sync';
import { FireProvider } from '../../src/provider';
import { FIRESTORE_PATHS } from '../../src/types';
import { extractClockEnds, aggregateClockEnds } from '../../src/update-metadata';

const WRITER_CLIENT = 1111;
const SENTINEL_CLIENT = 2222;

/** Two consecutive same-client ranges: u1 = "hello" (0..5), u2 = " world" (5..11). */
function makeSameClientRanges(): { u1: Uint8Array; u2: Uint8Array } {
    const src = new Y.Doc();
    src.clientID = WRITER_CLIENT;
    const updates: Uint8Array[] = [];
    src.on('update', (u: Uint8Array) => updates.push(u));
    src.getText('t').insert(0, 'hello');
    src.getText('t').insert(5, ' world');
    expect(updates).toHaveLength(2);
    // Sanity: the ranges are contiguous and u2 depends on u1.
    expect(extractClockEnds(updates[0]).get(WRITER_CLIENT)).toBe(5);
    expect(extractClockEnds(updates[1]).get(WRITER_CLIENT)).toBe(11);
    return { u1: updates[0], u2: updates[1] };
}

/** Independent updates from another client, used to observe listener progress. */
function makeSentinels(count: number): Uint8Array[] {
    const src = new Y.Doc();
    src.clientID = SENTINEL_CLIENT;
    const updates: Uint8Array[] = [];
    src.on('update', (u: Uint8Array) => updates.push(u));
    for (let i = 0; i < count; i++) {
        src.getText('sentinel').insert(i, String(i));
    }
    return updates;
}

/** An update document exactly as the provider writes it, with a fixed createdAt. */
function updateDoc(update: Uint8Array, createdAtMs: number, createdBy = 'client-A') {
    return {
        update: Bytes.fromUint8Array(update),
        createdAt: Timestamp.fromMillis(createdAtMs),
        createdBy,
        ...aggregateClockEnds(extractClockEnds(update)),
    };
}

/** A storage-backed update document as an oversized push writes it, with a fixed createdAt. */
function storageUpdateDoc(update: Uint8Array, storagePath: string, createdAtMs: number, createdBy = 'client-A') {
    return {
        updateStoragePath: storagePath,
        createdAt: Timestamp.fromMillis(createdAtMs),
        createdBy,
        ...aggregateClockEnds(extractClockEnds(update)),
    };
}

/** A history segment as delta compaction writes it, with a fixed startTime. */
function segmentDoc(segment: Uint8Array, startTimeMs: number) {
    return {
        // Same derivation as merge-core's partial merge: clock ends per client.
        stateVector: toBase64(Y.encodeStateVector(Y.parseUpdateMeta(segment).to)),
        // Inserts only. An unflagged segment is always applied, which
        // would hide a wrong redundancy skip.
        hasDeletions: false,
        createdBy: 'compactor',
        segment: Bytes.fromUint8Array(segment),
        startTime: Timestamp.fromMillis(startTimeMs),
    };
}

describe('Same-client gap fill delivered after the later range', () => {
    let app: any;
    let db: Firestore;
    let storage: FirebaseStorage;
    let path: string;
    let counter = 0;
    const cleanups: (() => void | Promise<void>)[] = [];

    const makeCtx = (ydoc: Y.Doc, uid = 'client-B'): SyncContext => ({
        db,
        path,
        doc: ydoc,
        uid,
        maxUpdatesThreshold: 1000, // keep compaction out of the picture
        isDestroyed: () => false,
        storage,
    });

    const updatesCol = () => collection(db, path, FIRESTORE_PATHS.UPDATES);
    const historyCol = () => collection(db, path, FIRESTORE_PATHS.HISTORY);

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db as Firestore;
        storage = setup.storage as FirebaseStorage;
        path = `integration-tests/pending-gap-fill-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    afterEach(async () => {
        while (cleanups.length > 0) {
            await cleanups.pop()!();
        }
    });

    it('initial sync applies an earlier-range update document ordered after the later range', async () => {
        const { u1, u2 } = makeSameClientRanges();
        // Later range first by createdAt, then the earlier range.
        await addDoc(updatesCol(), updateDoc(u2, 1_000));
        await addDoc(updatesCol(), updateDoc(u1, 2_000));

        const fresh = new Y.Doc();
        const result = await performInitialSync(makeCtx(fresh));

        expect(result.success).toBe(true);
        expect(fresh.getText('t').toString()).toBe('hello world');
        expect((fresh as any).store.pendingStructs).toBeNull();
    });

    it('initial sync applies an earlier-range history segment ordered after the later range', async () => {
        const { u1, u2 } = makeSameClientRanges();
        // Later range first by startTime, then the earlier range.
        await addDoc(historyCol(), segmentDoc(u2, 1_000));
        await addDoc(historyCol(), segmentDoc(u1, 2_000));

        const fresh = new Y.Doc();
        const result = await performInitialSync(makeCtx(fresh));

        expect(result.success).toBe(true);
        expect(fresh.getText('t').toString()).toBe('hello world');
        expect((fresh as any).store.pendingStructs).toBeNull();
    });

    it('update listener applies the gap filler when both ranges arrive in one delivery', async () => {
        const { u1, u2 } = makeSameClientRanges();
        const [sentinel] = makeSentinels(1);

        const live = new Y.Doc();
        const ctx = makeCtx(live);
        const sync = await performInitialSync(ctx); // empty path
        expect(sync.success).toBe(true);

        // Written before the listener attaches: its first delivery carries
        // [u2, u1, sentinel] in createdAt order.
        await addDoc(updatesCol(), updateDoc(u2, 1_000));
        await addDoc(updatesCol(), updateDoc(u1, 2_000));
        await addDoc(updatesCol(), updateDoc(sentinel, 3_000, 'client-Z'));

        const unsub = createUpdateListener(ctx, sync.lastSyncedDoc);
        cleanups.push(unsub);

        await waitForConditionEquals(() => live.getText('sentinel').toString(), '0', 10_000);
        await waitForConditionEquals(() => live.getText('t').toString(), 'hello world', 3_000);
    });

    it('update listener applies the gap filler when it arrives in a later delivery', async () => {
        const { u1, u2 } = makeSameClientRanges();
        const [sentinel0, sentinel1] = makeSentinels(2);

        const live = new Y.Doc();
        const ctx = makeCtx(live);
        const sync = await performInitialSync(ctx); // empty path
        expect(sync.success).toBe(true);

        const unsub = createUpdateListener(ctx, sync.lastSyncedDoc);
        cleanups.push(unsub);

        // Delivery 1: the later range (structs stay pending on the client).
        await addDoc(updatesCol(), updateDoc(u2, 1_000));
        await addDoc(updatesCol(), updateDoc(sentinel0, 1_500, 'client-Z'));
        await waitForConditionEquals(() => live.getText('sentinel').toString(), '0', 10_000);

        // Delivery 2: the earlier range that fills the gap.
        await addDoc(updatesCol(), updateDoc(u1, 2_000));
        await addDoc(updatesCol(), updateDoc(sentinel1, 2_500, 'client-Z'));
        await waitForConditionEquals(() => live.getText('sentinel').toString(), '01', 10_000);

        await waitForConditionEquals(() => live.getText('t').toString(), 'hello world', 3_000);
    });

    it('update listener applies the gap filler after downloading a storage-backed later range', async () => {
        const { u1, u2 } = makeSameClientRanges();
        const [sentinel] = makeSentinels(1);

        const live = new Y.Doc();
        const ctx = makeCtx(live);
        const sync = await performInitialSync(ctx); // empty path
        expect(sync.success).toBe(true);

        const unsub = createUpdateListener(ctx, sync.lastSyncedDoc);
        cleanups.push(unsub);

        // Delivery 1: the later range, offloaded to Cloud Storage. It is
        // downloaded and applied asynchronously; its structs stay pending.
        const storagePath = `${path}/large_updates/client-A_1000.bin`;
        await uploadBytes(ref(storage, storagePath), u2);
        await addDoc(updatesCol(), storageUpdateDoc(u2, storagePath, 1_000));
        await waitForConditionEquals(() => (live as any).store.pendingStructs !== null, true, 10_000);

        // Delivery 2: the earlier range that fills the gap, inline.
        await addDoc(updatesCol(), updateDoc(u1, 2_000));
        await addDoc(updatesCol(), updateDoc(sentinel, 2_500, 'client-Z'));
        await waitForConditionEquals(() => live.getText('sentinel').toString(), '0', 10_000);

        await waitForConditionEquals(() => live.getText('t').toString(), 'hello world', 3_000);
    });

    it('update listener applies the gap filler after our own push carried the pending later range', async () => {
        const { u1, u2 } = makeSameClientRanges();
        const [sentinel] = makeSentinels(1);

        // Local state that predates the provider (e.g. loaded from
        // IndexedDB): our own edit plus X's later range, still pending.
        const live = new Y.Doc();
        live.getText('own').insert(0, 'abc');
        Y.applyUpdate(live, u2);
        expect((live as any).store.pendingStructs).not.toBeNull();

        // The push carries the pending range too (encodeStateAsUpdate
        // includes pendingStructs), so its metadata claims X=11.
        const ctx = makeCtx(live);
        const sync = await performInitialSync(ctx); // empty path
        expect(sync.success).toBe(true);
        expect(sync.localUpdatesPushed).toBe(true);

        const unsub = createUpdateListener(ctx, sync.lastSyncedDoc);
        cleanups.push(unsub);

        // Ordered right after our own push (skipped as own), with no other
        // apply in between: the earlier range that fills the gap.
        await addDoc(updatesCol(), updateDoc(u1, Date.now()));
        await addDoc(updatesCol(), updateDoc(sentinel, Date.now() + 1, 'client-Z'));
        await waitForConditionEquals(() => live.getText('sentinel').toString(), '0', 10_000);

        await waitForConditionEquals(() => live.getText('t').toString(), 'hello world', 3_000);
    });

    it('history listener applies a gap-filling segment ordered after the later range', async () => {
        const { u1, u2 } = makeSameClientRanges();
        const [sentinel] = makeSentinels(1);

        const live = new Y.Doc();
        const ctx = makeCtx(live);
        const sync = await performInitialSync(ctx); // empty path
        expect(sync.success).toBe(true);

        await addDoc(historyCol(), segmentDoc(u2, 1_000));
        await addDoc(historyCol(), segmentDoc(u1, 2_000));
        await addDoc(historyCol(), segmentDoc(sentinel, 3_000));

        const unsub = createHistoryListener(ctx, sync.lastHistoryDoc);
        cleanups.push(unsub);

        await waitForConditionEquals(() => live.getText('sentinel').toString(), '0', 10_000);
        await waitForConditionEquals(() => live.getText('t').toString(), 'hello world', 3_000);
    });

    it('live and fresh FireProviders both show a remote client\'s edits written in inverted order', async () => {
        const { u1, u2 } = makeSameClientRanges();
        const [sentinel0, sentinel1] = makeSentinels(2);

        const createProvider = (ydoc: Y.Doc) => {
            const provider = new FireProvider({
                firebaseApp: app,
                ydoc,
                path,
                maxWaitTime: 50,
                maxUpdatesThreshold: 1000, // no automatic compaction
            });
            cleanups.push(() => provider.destroy());
            return provider;
        };

        // A connected peer that receives the documents live.
        const liveDoc = new Y.Doc();
        const liveProvider = createProvider(liveDoc);
        await waitForConditionEquals(() => liveProvider.synced, true, 30_000);

        await addDoc(updatesCol(), updateDoc(u2, Date.now()));
        await addDoc(updatesCol(), updateDoc(sentinel0, Date.now() + 1, 'client-Z'));
        await waitForConditionEquals(() => liveDoc.getText('sentinel').toString(), '0', 10_000);

        await addDoc(updatesCol(), updateDoc(u1, Date.now() + 2));
        await addDoc(updatesCol(), updateDoc(sentinel1, Date.now() + 3, 'client-Z'));
        await waitForConditionEquals(() => liveDoc.getText('sentinel').toString(), '01', 10_000);

        // A client that connects afterwards and loads everything via initial sync.
        const freshDoc = new Y.Doc();
        const freshProvider = createProvider(freshDoc);
        await waitForConditionEquals(() => freshProvider.synced, true, 30_000);
        expect(freshDoc.getText('sentinel').toString()).toBe('01');

        await waitForConditionEquals(() => liveDoc.getText('t').toString(), 'hello world', 3_000);
        expect(freshDoc.getText('t').toString()).toBe('hello world');
    });
});
