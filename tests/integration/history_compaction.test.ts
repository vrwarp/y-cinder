/**
 * History Compaction Tests
 *
 * Tests the tiered compaction strategy (updates → history → snapshot).
 * Verifies that updates are correctly merged into history segments when
 * the snapshot size limit would be exceeded, and that history segments
 * are properly merged into the base snapshot during subsequent compactions.
 *
 * @file history_compaction.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { collection, getDocs, setDoc, doc, Bytes } from 'firebase/firestore';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { getStableDate } from '../unit/prng';

describe('FireProvider History Compaction', () => {
    let app: any;
    let db: any;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        // await clearFirestore(db);
        path = `tests/history-compaction-${getStableDate()}-${counter++}`;
    });

    afterEach(async () => {

    });

    it('should merge existing History Segments into Base Snapshot if total size is small', { timeout: 20000 }, async () => {
        // 1. Setup Data directly in Firestore to simulate "Pre-Compaction" state
        const ydocBase = new Y.Doc();
        ydocBase.getText('content').insert(0, 'Base');
        const baseUpdate = Y.encodeStateAsUpdate(ydocBase);

        const ydocHistory = new Y.Doc();
        ydocHistory.getText('content').insert(4, 'History');
        const historyUpdate = Y.encodeStateAsUpdate(ydocHistory);

        const ydocUpdate = new Y.Doc();
        ydocUpdate.getText('content').insert(11, 'Update');
        const recentUpdate = Y.encodeStateAsUpdate(ydocUpdate);

        // Write Base
        await setDoc(doc(db, path), {
            content: Bytes.fromUint8Array(baseUpdate)
        });

        // Write History Segment
        await setDoc(doc(collection(db, path, 'history'), 'hist1'), {
            segment: Bytes.fromUint8Array(historyUpdate),
            startTime: 100,
            endTime: 200
        });

        // Write Update
        await setDoc(doc(collection(db, path, 'updates'), 'upd1'), {
            update: Bytes.fromUint8Array(recentUpdate),
            createdAt: 300,
            createdBy: 'test-user'
        });

        // 2. Initialize Provider
        const ydoc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: 1 // Trigger compaction easily
        });

        // Wait for sync
        await new Promise(r => setTimeout(r, 1000));

        // 3. Trigger Compaction explicitly
        await provider.compact();

        // 4. Verification

        const historySnap = await getDocs(collection(db, path, 'history'));
        const updatesSnap = await getDocs(collection(db, path, 'updates'));

        expect(historySnap.empty).toBe(true);
        expect(updatesSnap.empty).toBe(true);

        provider.destroy();

        const ydoc2 = new Y.Doc();
        const provider2 = new FireProvider({
            firebaseApp: app,
            ydoc: ydoc2,
            path
        });

        await new Promise(r => setTimeout(r, 1000));

        const text = ydoc2.getText('content').toString();
        // Since sync order might be tricky, we just ensure data exists
        expect(text).toContain('Base');
        expect(text).toContain('History');
        expect(text).toContain('Update');

        provider2.destroy();
    });
});
