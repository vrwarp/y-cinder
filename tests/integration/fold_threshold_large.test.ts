/**
 * historyFoldThreshold above what compaction can observe
 *
 * `historyFoldThreshold` is a documented option: history segments
 * accumulated before compaction folds everything back into the base
 * snapshot. Compaction reads at most DEFAULTS.MAX_COMPACTION_HISTORY (99)
 * history segments per cycle, though, and decides DELTA vs FOLD from the
 * size of that capped read. With a threshold of 101 or more the
 * "history has reached the threshold" condition can therefore never be
 * met: every compaction that has pending updates (which is every
 * listener-triggered compaction) writes yet another delta segment, the
 * history collection grows without bound and the base snapshot (its GC
 * and delete-set fingerprint) is never refreshed.
 *
 * Contract: a threshold above the observable window is either rejected at
 * construction, or honored — once history has reached the configured
 * number of segments, the next compaction folds into a new base snapshot.
 * That fold must not lose anything: when history is longer than one fold
 * can merge, a fresh client must still sync the full document.
 *
 * @file fold_threshold_large.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { toBase64 } from 'lib0/buffer';
import {
    collection,
    getDocs,
    getDoc,
    doc,
    writeBatch,
    Bytes,
    Timestamp,
} from 'firebase/firestore';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

const FOLD_THRESHOLD = 150;
/** More segments than the configured threshold (and than one compaction can read). */
const SEEDED_SEGMENTS = 160;

describe('historyFoldThreshold above the compaction history window', () => {
    let app: any;
    let db: any;
    let path: string;
    let counter = 0;
    const cleanup: Array<() => Promise<void> | void> = [];

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        path = `tests/fold-threshold-large-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    afterEach(async () => {
        for (const fn of cleanup.splice(0).reverse()) {
            await fn();
        }
    });

    /** Edits the doc, waits until the update document is in Firestore, then compacts. */
    async function persistAndCompact(provider: FireProvider, ydoc: Y.Doc, value: string): Promise<void> {
        ydoc.getMap('data').set('k', value);
        await waitForConditionTruthy(async () => {
            const snap = await getDocs(collection(db, path, 'updates'));
            return snap.size >= 1;
        }, { timeout: 20000, message: `update '${value}' persisted` });
        await provider.compact();
    }

    it('folds into a new base snapshot once history reaches a threshold above 100 (or rejects the threshold)', { timeout: 120000 }, async () => {
        // --- A first client establishes the base snapshot (the first
        // compaction of a document always folds).
        const baseDoc = new Y.Doc();
        const baseProvider = new FireProvider({
            firebaseApp: app,
            ydoc: baseDoc,
            path,
            maxUpdatesThreshold: 1000, // no listener-triggered compaction; we drive it
            maxWaitTime: 50,
        });
        await waitForConditionTruthy(() => baseProvider.synced, { timeout: 30000, message: 'base client sync' });
        await persistAndCompact(baseProvider, baseDoc, 'base');
        expect((await getDoc(doc(db, path))).data()!.version).toBe(1);
        expect((await getDocs(collection(db, path, 'history'))).size).toBe(0);
        await baseProvider.destroy();
        baseDoc.destroy();

        // --- History has since grown past the threshold, every segment
        // written by the client under test (its local state survived, e.g.
        // in IndexedDB). Compaction reads at most 99 segments per cycle, so
        // seeding them directly is equivalent to organic growth: every
        // decision past segment 99 sees the same capped read. Segments are
        // shaped like the ones delta compaction writes, each continuing the
        // same client's clock range with a strictly increasing startTime.
        // (Seeded while no provider is listening, to keep the emulator's
        // stream quiet.)
        const ydoc = new Y.Doc();
        cleanup.push(() => ydoc.destroy());
        const segments: Uint8Array[] = [];
        const collect = (update: Uint8Array) => segments.push(update);
        ydoc.on('update', collect);
        for (let i = 0; i < SEEDED_SEGMENTS; i++) {
            ydoc.getMap('seed').set('s' + i, i);
        }
        ydoc.off('update', collect);
        expect(segments.length).toBe(SEEDED_SEGMENTS);

        const t0 = Date.now();
        const batch = writeBatch(db);
        segments.forEach((update, i) => {
            batch.set(doc(collection(db, path, 'history')), {
                segment: Bytes.fromUint8Array(update),
                // Clock ends, as compaction stores them (encodeStateVectorFromUpdate
                // is empty for updates that do not start at clock 0).
                stateVector: toBase64(Y.encodeStateVector(Y.parseUpdateMeta(update).to)),
                createdBy: 'seed',
                startTime: Timestamp.fromMillis(t0 + i),
            });
        });
        await batch.commit();
        expect((await getDocs(collection(db, path, 'history'))).size).toBe(SEEDED_SEGMENTS);

        // --- The client under test is configured to fold every 150 segments.
        let provider: FireProvider;
        try {
            provider = new FireProvider({
                firebaseApp: app,
                ydoc,
                path,
                maxUpdatesThreshold: 1000,
                maxWaitTime: 50,
                historyFoldThreshold: FOLD_THRESHOLD,
            });
        } catch (err: any) {
            // Rejecting a threshold compaction cannot honor is an acceptable fix.
            expect(String(err?.message ?? err)).toMatch(/historyFoldThreshold/i);
            return;
        }
        cleanup.push(() => provider.destroy());

        await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: 'client under test sync' });
        expect(ydoc.getMap('data').get('k')).toBe('base');

        // A compaction cycle with pending updates now has
        // SEEDED_SEGMENTS >= historyFoldThreshold segments behind it: it must
        // fold, not append segment number SEEDED_SEGMENTS + 1. The update
        // continues the same client's clock range as the newest segments.
        await persistAndCompact(provider, ydoc, 'after-threshold');

        const versionAfter = (await getDoc(doc(db, path))).data()!.version;
        const historyAfter = (await getDocs(collection(db, path, 'history'))).size;
        const updatesAfter = (await getDocs(collection(db, path, 'updates'))).size;
        const observed = `snapshot version ${versionAfter}, ${historyAfter} history segments, ${updatesAfter} pending updates`;

        // History shrank instead of growing...
        expect(historyAfter, `fold expected; observed ${observed}`).toBeLessThan(SEEDED_SEGMENTS);
        // ...and the base snapshot was rebuilt.
        expect(versionAfter, `fold expected; observed ${observed}`).toBeGreaterThan(1);

        // A fresh client must still assemble the whole document from the
        // new snapshot plus whatever history and updates remain.
        const freshDoc = new Y.Doc();
        const fresh = new FireProvider({
            firebaseApp: app,
            ydoc: freshDoc,
            path,
            maxUpdatesThreshold: 1000,
            maxWaitTime: 50,
        });
        try {
            await waitForConditionTruthy(() => fresh.synced, { timeout: 30000, message: 'fresh client sync' });
            const seed = freshDoc.getMap('seed');
            const missing = Array.from({ length: SEEDED_SEGMENTS }, (_, i) => 's' + i).filter((key) => !seed.has(key));
            expect(missing, `fresh client after fold (${observed})`).toEqual([]);
            expect(freshDoc.getMap('data').get('k'), `fresh client after fold (${observed})`).toBe('after-threshold');
        } finally {
            await fresh.destroy();
            freshDoc.destroy();
        }

        // A backlog longer than one fold drains across cycles: the pending
        // update is compacted by the next one at the latest.
        if (updatesAfter > 0) {
            await provider.compact();
        }
        expect((await getDocs(collection(db, path, 'updates'))).size, 'pending update compacted').toBe(0);
    });
});
