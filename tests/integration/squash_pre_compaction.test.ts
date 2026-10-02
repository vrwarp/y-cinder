/**
 * squash()'s preparatory compaction: the jobs it keeps after it stopped
 * folding.
 *
 * squash() compacts before it squashes so its transaction stays within
 * Firestore's write budget. That cycle runs with `beforeSquash`: a fold
 * that is merely due is skipped (the squash snapshot supersedes it moments
 * later), and history may hold one segment past historyFoldThreshold. It
 * still does two jobs squashDocument cannot do itself, both of which used
 * to ride along on the fold:
 *
 *  - delete stale old-epoch update documents: squashDocument does not
 *    epoch-filter its pending documents, and one from another id space can
 *    never be covered by the local doc, so the squash would return
 *    'local-behind';
 *  - drain update documents that carry no redundancy metadata (no
 *    clientIDs/clientClocks/stateVector, as older clients wrote them) into
 *    a delta segment that carries a stateVector: squash refuses a payload
 *    it cannot verify.
 *
 * And the bound that keeps a failing squash from growing history: once it
 * holds historyFoldThreshold segments, the preparatory cycle folds.
 *
 * @file squash_pre_compaction.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import * as Y from 'yjs';
import { collection, getDocs, addDoc, Bytes, serverTimestamp, Firestore } from 'firebase/firestore';
import { FirebaseStorage } from 'firebase/storage';
import { FireProvider } from '../../src/provider';
import { compact, CompactionContext } from '../../src/compaction';
import { DEFAULTS } from '../../src/types';
import { extractClockEnds, aggregateClockEnds } from '../../src/update-metadata';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe("squash()'s preparatory compaction", () => {
    let app: any;
    let db: Firestore;
    let storage: FirebaseStorage;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
        path = `tests/squash-pre-compaction-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    const countDocs = async (tier: 'updates' | 'history') => (await getDocs(collection(db, path, tier))).size;

    async function connect(name: string): Promise<{ ydoc: Y.Doc; provider: FireProvider }> {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({ firebaseApp: app, ydoc, path, maxUpdatesThreshold: 1000, maxWaitTime: 50 });
        await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: `${name} synced` });
        return { ydoc, provider };
    }

    /** Builds epoch 1: one edit, squashed. */
    async function squashOnce(): Promise<void> {
        const { ydoc, provider } = await connect('A');
        ydoc.getMap('data').set('k', 'v');
        await waitForConditionTruthy(async () => (await countDocs('updates')) >= 1,
            { timeout: 20000, message: 'update persisted' });
        const res = await provider.squash();
        expect(res.success).toBe(true);
        await provider.destroy();
        ydoc.destroy();
    }

    /** A late flush from a client still on epoch 0, with provider-shaped metadata. */
    async function addStaleOldEpochUpdate(): Promise<void> {
        const staleDoc = new Y.Doc();
        staleDoc.getMap('data').set('stale', 'poison');
        const blob = Y.encodeStateAsUpdate(staleDoc);
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(blob),
            createdAt: serverTimestamp(),
            createdBy: 'old-epoch-client',
            ...aggregateClockEnds(extractClockEnds(blob)),
            // epoch field absent = epoch 0
        });
        staleDoc.destroy();
    }

    it('deletes a stale old-epoch update document', { timeout: 120000 }, async () => {
        await squashOnce();
        await addStaleOldEpochUpdate();

        const { ydoc, provider } = await connect('B');
        ydoc.getMap('data');
        const res = await provider.squash();

        expect(res.error).toBeUndefined();
        expect(res.skippedReason).toBeUndefined();
        expect(res.success).toBe(true);
        expect(await countDocs('updates')).toBe(0);
        await provider.destroy();
        ydoc.destroy();
    });

    /*
     * No pending updates and history below the threshold: the cycle ends
     * without folding, and must still delete the stale document.
     */
    it('deletes a stale old-epoch update document beside a segment it does not fold', { timeout: 120000 }, async () => {
        await squashOnce();

        // One current-epoch delta segment, no update documents left.
        const { ydoc, provider } = await connect('B');
        ydoc.getMap('data').set('b', 'from B');
        await waitForConditionTruthy(async () => (await countDocs('updates')) >= 1,
            { timeout: 20000, message: 'B update persisted' });
        await provider.compact();
        expect(await countDocs('history')).toBe(1);
        expect(await countDocs('updates')).toBe(0);

        await addStaleOldEpochUpdate();
        const res = await provider.squash();

        expect(res.error).toBeUndefined();
        expect(res.skippedReason).toBeUndefined();
        expect(res.success).toBe(true);
        expect(await countDocs('updates')).toBe(0);
        expect(await countDocs('history')).toBe(0);
        await provider.destroy();
        ydoc.destroy();
    });

    it('drains a current-epoch update document that carries no redundancy metadata', { timeout: 120000 }, async () => {
        await squashOnce();

        const { ydoc, provider } = await connect('B');
        const other = new Y.Doc();
        Y.applyUpdate(other, Y.encodeStateAsUpdate(ydoc));
        other.getMap('data').set('legacy', 'x');
        const diff = Y.encodeStateAsUpdate(other, Y.encodeStateVector(ydoc));
        other.destroy();
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(diff),
            createdAt: serverTimestamp(),
            createdBy: 'legacy-client',
            epoch: 1,
            // no clientIDs / clientClocks / stateVector
        });
        await waitForConditionTruthy(() => ydoc.getMap('data').get('legacy') === 'x',
            { timeout: 20000, message: 'B received the legacy update' });

        const res = await provider.squash();

        expect(res.error).toBeUndefined();
        expect(res.skippedReason).toBeUndefined();
        expect(res.success).toBe(true);
        await provider.destroy();
        ydoc.destroy();
    });

    /*
     * A squash that keeps failing after its preparatory cycle (e.g.
     * 'local-changed' on a busy client) must not grow history without
     * bound: one segment past the threshold, then the cycle folds.
     */
    it('tolerates one segment past historyFoldThreshold, then folds', { timeout: 120000 }, async () => {
        const historyFoldThreshold = 3;
        const ctx = (beforeSquash: boolean): CompactionContext => ({
            db,
            path,
            uid: 'squasher',
            lockTTL: 60000,
            compactionLimit: DEFAULTS.COMPACTION_LIMIT,
            isDestroyed: () => false,
            storage,
            historyFoldThreshold,
            beforeSquash,
        });
        const writer = new Y.Doc();
        const pushUpdate = async (key: string) => {
            const sv = Y.encodeStateVector(writer);
            writer.getMap('m').set(key, key);
            const blob = Y.encodeStateAsUpdate(writer, sv);
            await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(blob),
                createdAt: serverTimestamp(),
                createdBy: 'writer',
                ...aggregateClockEnds(extractClockEnds(blob)),
            });
        };

        // Base, then history at the normal maximum (threshold - 1).
        await pushUpdate('k0');
        expect((await compact(ctx(false))).type).toBe('snapshot');
        for (let i = 1; i < historyFoldThreshold; i++) {
            await pushUpdate('k' + i);
            expect((await compact(ctx(false))).type).toBe('history');
        }
        expect(await countDocs('history')).toBe(historyFoldThreshold - 1);

        // Nothing pending: no fold, the squash supersedes it.
        expect((await compact(ctx(true))).type).toBe('none');

        // The fold a normal cycle would run is skipped: one more segment.
        await pushUpdate('k' + historyFoldThreshold);
        expect((await compact(ctx(true))).type).toBe('history');
        expect(await countDocs('history')).toBe(historyFoldThreshold);

        // No further: the next preparatory cycle folds.
        await pushUpdate('k' + (historyFoldThreshold + 1));
        expect((await compact(ctx(true))).type).toBe('snapshot');
        expect(await countDocs('history')).toBe(0);
        expect(await countDocs('updates')).toBe(0);

        writer.destroy();
    });
});
