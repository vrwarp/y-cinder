/**
 * FireProvider Core Integration Tests
 *
 * Tests the fundamental functionality of FireProvider including:
 * - Real-time synchronization between multiple clients
 * - Data persistence across provider restarts
 * - Basic compaction behavior
 *
 * These tests require the Firestore emulator to be running on port 8080.
 *
 * @file provider.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionEquals } from '../utils/wait';

describe('FireProvider Integration (Emulator)', () => {
    let app: any;
    let db: any;

    // Helper to create provider connected to emulator
    const createProvider = (doc: Y.Doc, path: string, config: any = {}) => {
        return new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path,
            ...config
        });
    }

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        await clearFirestore(db); // Clear before each test
    });

    afterEach(async () => {
        // await clearFirestore(db);
    });

    it('should sync updates between two clients', async () => {
        const path = `integration-tests/sync-${Date.now()}`;

        const doc1 = new Y.Doc();
        const provider1 = createProvider(doc1, path, { maxWaitTime: 10 });

        const doc2 = new Y.Doc();
        const provider2 = createProvider(doc2, path, { maxWaitTime: 10 });

        // Client 1 makes a change
        doc1.getText('content').insert(0, 'Hello World');

        // Wait for sync
        await waitForConditionEquals(
            () => doc2.getText('content').toString(),
            'Hello World',
            { timeout: 5000, interval: 100, message: 'Doc2 should receive content' }
        );

        expect(doc2.getText('content').toString()).toBe('Hello World');

        provider1.destroy();
        provider2.destroy();
    });

    it('should persist data after restart', async () => {
        const path = `integration-tests/persistence-${Date.now()}`;

        const doc1 = new Y.Doc();
        const provider1 = createProvider(doc1, path, { maxWaitTime: 10 });

        doc1.getText('content').insert(0, 'Persisted Data');

        // Wait for it to verify local update is processed locally? No, waiting for nothing.
        // We wait a bit for save to firestore.
        await new Promise(r => setTimeout(r, 500));
        provider1.destroy();

        // Restart
        const doc2 = new Y.Doc();
        const provider2 = createProvider(doc2, path);

        await waitForConditionEquals(
            () => doc2.getText('content').toString(),
            'Persisted Data',
            { timeout: 5000, interval: 100, message: 'Doc2 should load persisted data' }
        );

        expect(doc2.getText('content').toString()).toBe('Persisted Data');

        provider2.destroy();
    });

    it('should perform compaction on emulator', { timeout: 15000 }, async () => {
        const path = `integration-tests/compaction-${Date.now()}`;
        const doc = new Y.Doc();
        // Low threshold to force compaction
        const provider = createProvider(doc, path, { maxUpdatesThreshold: 2, maxWaitTime: 5 });

        doc.getText('content').insert(0, 'A');
        await new Promise(r => setTimeout(r, 20));
        doc.getText('content').insert(1, 'B');
        await new Promise(r => setTimeout(r, 20));
        doc.getText('content').insert(2, 'C');

        // Wait for compaction? Hard to detect from outside without checking logs or DB.
        // But verifying a fresh client joins correctly is the goal.
        await new Promise(r => setTimeout(r, 500));

        // Verify via a fresh client
        const doc2 = new Y.Doc();
        const provider2 = createProvider(doc2, path);

        await waitForConditionEquals(
            () => doc2.getText('content').toString(),
            'ABC',
            { timeout: 10000, interval: 100, message: 'Doc2 should receive compacted data' }
        );

        expect(doc2.getText('content').toString()).toBe('ABC');

        provider.destroy();
        provider2.destroy();
    });
});
