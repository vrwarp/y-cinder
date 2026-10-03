/**
 * Threshold-triggered compactions skip small leftovers; manual ones drain.
 *
 * Every online client triggers on the same threshold crossing, so a
 * triggered compaction carries a minimum batch (CompactionContext
 * .minUpdates, half the threshold) and releases the lock untouched when
 * it finds fewer updates and no fold is due (see shouldDeferCompaction
 * and multi_client_mini_compactions.test.ts for the race itself). This
 * file pins the wiring around that skip:
 *
 *  - manual compact() passes no minimum: it compacts a backlog the
 *    trigger's minimum would leave for the next crossing (with nothing
 *    pending and no fold due, both end without merging; see
 *    zero_update_compaction.test.ts);
 *  - the minimum counts every update document read, stale-epoch ones
 *    included, so a stale-only backlog the trigger counted gets cleaned;
 *  - the minimum is clamped to what one cycle reads, so a threshold far
 *    above compactionLimit cannot defer every cycle forever.
 *
 * @file triggered_compaction_minimum.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import * as Y from 'yjs';
import { toBase64 } from 'lib0/buffer';
import { collection, addDoc, getDocs, getDoc, doc, setDoc, serverTimestamp, Bytes } from 'firebase/firestore';
import { compact, CompactionContext } from '../../src/compaction';
import { FireProvider } from '../../src/provider';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('Triggered compaction minimum batch', () => {
    let app: any;
    let db: any;
    let storage: any;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
        path = `tests/triggered-compaction-minimum-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    const ctx = (overrides: Partial<CompactionContext> = {}): CompactionContext => ({
        db,
        path,
        uid: 'compactor',
        lockTTL: 60_000,
        cachedClockOffset: 0,
        compactionLimit: 200,
        isDestroyed: () => false,
        storage,
        ...overrides,
    });

    /** A writer whose edits are pushed as update documents. */
    function writer() {
        const ydoc = new Y.Doc();
        const map = ydoc.getMap('m');
        return {
            ydoc,
            async push(key: string, extra: Record<string, unknown> = {}) {
                const sv = Y.encodeStateVector(ydoc);
                map.set(key, key);
                await addDoc(collection(db, path, 'updates'), {
                    update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(ydoc, sv)),
                    createdAt: serverTimestamp(),
                    createdBy: 'writer',
                    ...extra,
                });
            },
        };
    }

    /**
     * Base snapshot (inline) + one history segment, no pending updates,
     * each carrying the state vector compaction would store with it.
     */
    async function seedBaseAndSegment(): Promise<void> {
        const ydoc = new Y.Doc();
        ydoc.getMap('m').set('base', 'base');
        const baseSv = Y.encodeStateVector(ydoc);
        await setDoc(doc(db, path), {
            content: Bytes.fromUint8Array(Y.encodeStateAsUpdate(ydoc)),
            stateVector: toBase64(baseSv),
            version: 1,
        });
        ydoc.getMap('m').set('segment', 'segment');
        const segment = Y.encodeStateAsUpdate(ydoc, baseSv);
        await setDoc(doc(collection(db, path, 'history'), 'h1'), {
            segment: Bytes.fromUint8Array(segment),
            stateVector: toBase64(Y.encodeStateVector(Y.parseUpdateMeta(segment).to)),
            createdBy: 'seed',
            startTime: serverTimestamp(),
        });
    }

    const count = async (sub: string) => (await getDocs(collection(db, path, sub))).size;

    it('a triggered cycle leaves a small backlog and history alone; a manual one compacts it', { timeout: 60000 }, async () => {
        await seedBaseAndSegment();

        const idle = await compact(ctx({ minUpdates: 5 }));
        expect(idle.type).toBe('none');
        expect(await count('history')).toBe(1);

        const w = writer();
        await w.push('k0');
        await w.push('k1');

        const skipped = await compact(ctx({ minUpdates: 5 }));
        expect(skipped.type).toBe('none');
        expect(skipped.updatesCompacted).toBe(0);
        expect(await count('updates')).toBe(2);
        expect(await count('history')).toBe(1);
        expect((await getDoc(doc(db, path))).data()?.version).toBe(1);

        const drained = await compact(ctx());
        expect(drained.type).toBe('history');
        expect(drained.updatesCompacted).toBe(2);
        expect(await count('updates')).toBe(0);
        expect(await count('history')).toBe(2);
    });

    it('provider.compact() compacts a backlog below the trigger minimum', { timeout: 60000 }, async () => {
        await seedBaseAndSegment();
        const w = writer();
        await w.push('k0');
        await w.push('k1');
        const ydoc = new Y.Doc();
        // Threshold 10: the trigger would ask for 5 updates and never fires
        // for these 2.
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 10,
            cachedClockOffset: 0,
        });
        try {
            await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: 'initial sync' });
            expect(ydoc.getMap('m').toJSON()).toEqual({ base: 'base', segment: 'segment', k0: 'k0', k1: 'k1' });

            await provider.compact();

            expect(await count('updates')).toBe(0);
            expect(await count('history')).toBe(2);
        } finally {
            await provider.destroy();
        }
    });

    it('counts stale-epoch update documents toward the minimum', { timeout: 60000 }, async () => {
        // Squashed to epoch 1; the update documents are from epoch 0.
        await setDoc(doc(db, path), { epoch: 1, version: 1 });
        const old = writer();
        for (let i = 0; i < 5; i++) {
            await old.push(`old${i}`);
        }

        const result = await compact(ctx({ minUpdates: 5 }));
        expect(result.type).toBe('none');
        expect(result.updatesCompacted).toBe(5);
        expect(await count('updates')).toBe(0);
    });

    it('clamps the minimum to what one cycle reads', { timeout: 60000 }, async () => {
        await seedBaseAndSegment();
        const w = writer();
        for (let i = 0; i < 4; i++) {
            await w.push(`k${i}`);
        }

        // A threshold of 100 asks for 50, but one cycle reads only 3.
        const result = await compact(ctx({ minUpdates: 50, compactionLimit: 3 }));
        expect(result.type).toBe('history');
        expect(result.updatesCompacted).toBe(3);
        expect(await count('updates')).toBe(1);
        expect(await count('history')).toBe(2);
    });
});
