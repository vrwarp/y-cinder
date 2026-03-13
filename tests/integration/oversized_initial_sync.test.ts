/**
 * Oversized Initial Sync Integration Tests
 *
 * Tests the storage-backed update offloading for initial sync diffs
 * that exceed Firestore's 1MB document limit:
 * - Large local diff is uploaded to Cloud Storage instead of inline
 * - Pointer document is written to the updates collection
 * - A second client correctly reads and applies the storage-backed update
 *
 * @file oversized_initial_sync.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { getFirestore, collection, getDocs, query, orderBy } from 'firebase/firestore';
import { getStorage } from 'firebase/storage';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { getStableDate } from '../unit/prng';
import { performInitialSync, SyncContext, SyncResult } from '../../src/sync';
import { DEFAULTS, FIRESTORE_PATHS } from '../../src/types';

describe('Oversized Initial Sync (Emulator)', () => {
    let app: any;
    let db: any;
    let storage: any;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
        await clearFirestore(db);
    });

    it('should offload oversized initial sync diff to Cloud Storage', async () => {
        const path = `integration-tests/oversized-sync-${getStableDate()}-${counter++}`;

        // 1. Create a local Y.Doc with > 1MB of content (simulating offline work)
        const ydoc = new Y.Doc();
        const text = ydoc.getText('content');

        // Generate > 1MB of text. Yjs text encoding is ~1.5-2 bytes per char.
        const chunkSize = 100 * 1024; // 100 KB chunks
        for (let i = 0; i < 12; i++) {
            text.insert(0, 'A'.repeat(chunkSize));
        }

        // Verify local doc is large enough
        const fullState = Y.encodeStateAsUpdate(ydoc);
        expect(fullState.byteLength).toBeGreaterThan(DEFAULTS.FIRESTORE_DOC_LIMIT);

        // 2. Perform initial sync against empty Firestore
        const syncCtx: SyncContext = {
            db,
            path,
            doc: ydoc,
            uid: 'test-client-1',
            maxUpdatesThreshold: 50,
            isDestroyed: () => false,
            storage,
        };

        const result: SyncResult = await performInitialSync(syncCtx);

        // 3. Verify sync succeeded
        expect(result.success).toBe(true);
        expect(result.localUpdatesPushed).toBe(true);

        // 4. Verify the updates collection has a pointer doc (not inline blob)
        const updatesSnap = await getDocs(
            query(collection(db, path, FIRESTORE_PATHS.UPDATES), orderBy('createdAt', 'asc'))
        );
        expect(updatesSnap.size).toBe(1);

        const pointerDoc = updatesSnap.docs[0].data();
        expect(pointerDoc.updateStoragePath).toBeDefined();
        expect(pointerDoc.updateStoragePath).toContain('large_updates/');
        // Should NOT have an inline update blob
        expect(pointerDoc.update).toBeUndefined();
        // Should have metadata
        expect(pointerDoc.clientIDs).toBeDefined();
        expect(pointerDoc.clientClocks).toBeDefined();
        expect(pointerDoc.createdBy).toBe('test-client-1');
    }, 30000);

    it('should allow a second client to read storage-backed updates', async () => {
        const path = `integration-tests/oversized-sync-read-${getStableDate()}-${counter++}`;

        // 1. Client 1: create large doc and sync
        const ydoc1 = new Y.Doc();
        const text1 = ydoc1.getText('content');

        const chunkSize = 100 * 1024;
        for (let i = 0; i < 12; i++) {
            text1.insert(0, 'B'.repeat(chunkSize));
        }

        const syncCtx1: SyncContext = {
            db,
            path,
            doc: ydoc1,
            uid: 'client-1',
            maxUpdatesThreshold: 50,
            isDestroyed: () => false,
            storage,
        };

        const result1 = await performInitialSync(syncCtx1);
        expect(result1.success).toBe(true);
        expect(result1.localUpdatesPushed).toBe(true);

        // 2. Client 2: fresh doc, sync from same path
        const ydoc2 = new Y.Doc();
        const syncCtx2: SyncContext = {
            db,
            path,
            doc: ydoc2,
            uid: 'client-2',
            maxUpdatesThreshold: 50,
            isDestroyed: () => false,
            storage,
        };

        const result2 = await performInitialSync(syncCtx2);
        expect(result2.success).toBe(true);
        expect(result2.updatesApplied).toBeGreaterThan(0);

        // 3. Verify client 2's doc matches client 1's
        const text2 = ydoc2.getText('content');
        expect(text2.toString()).toBe(text1.toString());
        expect(text2.toString().length).toBe(12 * chunkSize);
    }, 30000);
});
