/**
 * Delta Compaction Tests
 *
 * The aged-document bandwidth fix: between folds, compaction merges only
 * the pending update documents into ONE history segment (O(new data)),
 * leaving the multi-MB base snapshot untouched. Folding (snapshot +
 * history + updates -> new snapshot) happens once per historyFoldThreshold
 * cycles.
 *
 * Verifies:
 *  - first compaction (no base) folds into a snapshot
 *  - subsequent compactions run in delta mode: history segment written,
 *    updates deleted, main document version/snapshot UNCHANGED
 *  - at the fold threshold everything folds back into the snapshot
 *  - a fresh client converges from base + segments at every stage
 *
 * @file delta_compaction.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { collection, getDocs, getDoc, doc } from 'firebase/firestore';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('FireProvider Delta Compaction', () => {
    let app: any;
    let db: any;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        path = `tests/delta-compaction-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    it('keeps the snapshot untouched during delta cycles and folds at the threshold', { timeout: 90000 }, async () => {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 1000, // no auto-compaction; we drive it
            maxWaitTime: 50,
            historyFoldThreshold: 3,
        });
        await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: 'initial sync' });

        const map = ydoc.getMap('data');
        const writeBatch = async (round: number) => {
            for (let i = 0; i < 5; i++) {
                map.set('k' + i, `round${round}-${i}`);
                await new Promise(r => setTimeout(r, 120)); // separate debounce windows
            }
            // Wait until the updates actually landed in Firestore
            await waitForConditionTruthy(async () => {
                const snap = await getDocs(collection(db, path, 'updates'));
                return snap.size >= 1;
            }, { timeout: 20000, message: `round ${round} updates persisted` });
        };

        // --- Round 0: no base yet -> first compaction folds to snapshot v1
        await writeBatch(0);
        await provider.compact();
        let mainSnap = await getDoc(doc(db, path));
        expect(mainSnap.exists()).toBe(true);
        expect(mainSnap.data()!.version).toBe(1);
        expect((await getDocs(collection(db, path, 'history'))).size).toBe(0);
        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);
        const snapshotPathV1 = mainSnap.data()!.snapshotStoragePath;

        // --- Round 1: delta cycle -> history segment, snapshot untouched
        await writeBatch(1);
        await provider.compact();
        mainSnap = await getDoc(doc(db, path));
        expect(mainSnap.data()!.version).toBe(1); // NOT bumped
        expect(mainSnap.data()!.snapshotStoragePath).toBe(snapshotPathV1); // NOT rewritten
        let historySnap = await getDocs(collection(db, path, 'history'));
        expect(historySnap.size).toBe(1);
        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);
        const segData = historySnap.docs[0].data();
        expect(segData.segment).toBeTruthy();
        expect(typeof segData.stateVector).toBe('string');

        // Fresh client must converge from base + segment
        const ydocB = new Y.Doc();
        const providerB = new FireProvider({ firebaseApp: app, ydoc: ydocB, path, maxUpdatesThreshold: 1000 });
        await waitForConditionTruthy(
            () => ydocB.getMap('data').get('k4') === 'round1-4',
            { timeout: 30000, message: 'fresh client sees delta-compacted state' }
        );
        await providerB.destroy();
        ydocB.destroy();

        // --- Round 2: second delta cycle -> two segments
        await writeBatch(2);
        await provider.compact();
        historySnap = await getDocs(collection(db, path, 'history'));
        expect(historySnap.size).toBe(2);
        mainSnap = await getDoc(doc(db, path));
        expect(mainSnap.data()!.version).toBe(1);

        // --- Round 3: threshold reached (2 segments + 1 >= 3) -> FOLD
        await writeBatch(3);
        await provider.compact();
        await waitForConditionTruthy(async () => {
            const h = await getDocs(collection(db, path, 'history'));
            const u = await getDocs(collection(db, path, 'updates'));
            return h.empty && u.empty;
        }, { timeout: 20000, message: 'fold clears history and updates' });
        mainSnap = await getDoc(doc(db, path));
        expect(mainSnap.data()!.version).toBe(2);
        expect(mainSnap.data()!.snapshotStoragePath).not.toBe(snapshotPathV1);

        // Fresh client converges from the folded snapshot alone
        const ydocC = new Y.Doc();
        const providerC = new FireProvider({ firebaseApp: app, ydoc: ydocC, path, maxUpdatesThreshold: 1000 });
        await waitForConditionTruthy(
            () => ydocC.getMap('data').get('k4') === 'round3-4',
            { timeout: 30000, message: 'fresh client sees folded state' }
        );
        expect(ydocC.getMap('data').toJSON()).toEqual(ydoc.getMap('data').toJSON());
        await providerC.destroy();
        ydocC.destroy();

        await provider.destroy();
        ydoc.destroy();
    });

    it('historyFoldThreshold: 1 restores the always-fold behavior', { timeout: 60000 }, async () => {
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 1000,
            maxWaitTime: 50,
            historyFoldThreshold: 1,
        });
        await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: 'initial sync' });

        const map = ydoc.getMap('data');
        for (let round = 0; round < 2; round++) {
            map.set('k', 'v' + round);
            await waitForConditionTruthy(async () => {
                const snap = await getDocs(collection(db, path, 'updates'));
                return snap.size >= 1;
            }, { timeout: 20000, message: 'update persisted' });
            await provider.compact();
            expect((await getDocs(collection(db, path, 'history'))).size).toBe(0);
            expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);
        }
        const mainSnap = await getDoc(doc(db, path));
        expect(mainSnap.data()!.version).toBe(2);

        await provider.destroy();
        ydoc.destroy();
    });
});
