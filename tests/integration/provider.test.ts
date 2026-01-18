import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';

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
        // await clearFirestore(db); // Clear before each test
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

        // Wait for sync (debounce + network + polling)
        // Emulator cold start might be slow
        await new Promise(r => setTimeout(r, 2500));

        expect(doc2.getText('content').toString()).toBe('Hello World');

        provider1.destroy();
        provider2.destroy();
    });

    it('should persist data after restart', async () => {
        const path = `integration-tests/persistence-${Date.now()}`;

        const doc1 = new Y.Doc();
        const provider1 = createProvider(doc1, path, { maxWaitTime: 10 });

        doc1.getText('content').insert(0, 'Persisted Data');

        await new Promise(r => setTimeout(r, 1000));
        provider1.destroy();

        // Restart
        const doc2 = new Y.Doc();
        const provider2 = createProvider(doc2, path);

        // Wait for load
        await new Promise(r => setTimeout(r, 1500));

        expect(doc2.getText('content').toString()).toBe('Persisted Data');

        provider2.destroy();
    });

    it('should perform compaction on emulator', async () => {
        const path = `integration-tests/compaction-${Date.now()}`;
        const doc = new Y.Doc();
        // Low threshold to force compaction
        const provider = createProvider(doc, path, { maxUpdatesThreshold: 2, maxWaitTime: 5 });

        doc.getText('content').insert(0, 'A');
        await new Promise(r => setTimeout(r, 20));
        doc.getText('content').insert(1, 'B');
        await new Promise(r => setTimeout(r, 20));
        doc.getText('content').insert(2, 'C');
        await new Promise(r => setTimeout(r, 500)); // Wait for compaction

        // Verify via a fresh client
        const doc2 = new Y.Doc();
        const provider2 = createProvider(doc2, path);

        await new Promise(r => setTimeout(r, 500));

        expect(doc2.getText('content').toString()).toBe('ABC');

        provider.destroy();
        provider2.destroy();
    });
});
