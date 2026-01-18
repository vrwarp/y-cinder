import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';

describe('FireProvider Sync Reproduction (Non-Empty)', () => {
    let app: any;
    let db: any;

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
    });

    it('should sync local content even if remote is not empty', async () => {
        const path = `repro-tests/mixed-sync-${Date.now()}`;

        // 1. Initialize Firestore with some data ("Hello")
        const doc1 = new Y.Doc();
        const provider1 = createProvider(doc1, path, { maxWaitTime: 10 });
        doc1.getText('content').insert(0, 'Hello');
        // Wait for sync
        await new Promise(r => setTimeout(r, 1000));
        provider1.destroy(); // Disconnect client 1

        // 2. New Client with Local Data ("World") connecting to same path
        const doc2 = new Y.Doc();
        doc2.getText('content').insert(0, 'World ');

        // Connect
        const provider2 = createProvider(doc2, path, { maxWaitTime: 10 });

        // Wait for sync
        // Expected: Pull "Hello", Push "World ". Result: "World Hello" or "Hello World" (depends on order/IDs)
        // AND Firestore should have BOTH.
        await new Promise(r => setTimeout(r, 2000));

        // 3. Verify Client 2 has merged state
        const text2 = doc2.getText('content').toString();
        expect(text2).toContain('Hello');
        expect(text2).toContain('World');

        // 4. Verify Firestore has merged state via Client 3
        const doc3 = new Y.Doc();
        const provider3 = createProvider(doc3, path);
        await new Promise(r => setTimeout(r, 1000));

        const text3 = doc3.getText('content').toString();
        // This fails if Client 2 didn't push "World" because it saw "Hello" on server and thought "not empty, do nothing".
        expect(text3).toContain('World');

        provider2.destroy();
        provider3.destroy();
    });
});
