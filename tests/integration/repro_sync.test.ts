/**
 * Basic Sync Reproduction Tests
 *
 * Simple reproduction tests for basic synchronization scenarios.
 * Used to isolate and verify specific sync behaviors in controlled conditions.
 *
 * @file repro_sync.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { getStableDate } from '../unit/prng';

describe('FireProvider Sync Reproduction', () => {
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

    it('should sync existing document content to empty firestore', async () => {
        const path = `repro-tests/repro-sync-${getStableDate()}`;

        // 1. Create a document and populate it *before* connecting
        const doc1 = new Y.Doc();
        doc1.getText('content').insert(0, 'Initial Content');

        // 2. Connect to FireProvider (empty firestore path)
        const provider1 = createProvider(doc1, path, { maxWaitTime: 50 });

        // Wait for potential sync
        await new Promise(r => setTimeout(r, 2000));

        // 3. Connect a second client to verify data exists in Firestore
        const doc2 = new Y.Doc();
        const provider2 = createProvider(doc2, path);

        // Wait for sync
        await new Promise(r => setTimeout(r, 2000));

        // 4. Expect doc2 to have the content
        // If this fails, it means the issue is reproduced
        expect(doc2.getText('content').toString()).toBe('Initial Content');

        provider1.destroy();
        provider2.destroy();
    });
});
