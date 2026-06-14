/**
 * Mixed Sync Mode Tests
 *
 * Tests synchronization behavior when clients have different starting states.
 * Verifies that clients with pre-existing local content correctly sync with
 * clients that start fresh, ensuring bidirectional merge consistency.
 *
 * @file repro_mixed_sync.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('FireProvider Sync Reproduction (Non-Empty)', () => {
    let app: any;

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
    });

    let counter = 0;

    it('should sync local content even if remote is not empty', async () => {
        const path = `repro-tests/mixed-sync-${getStableDate()}-${counter++}`;

        // 1. Initialize Firestore with some data ("Hello")
        const doc1 = new Y.Doc();
        const provider1 = createProvider(doc1, path, { maxWaitTime: 10 });
        doc1.getText('content').insert(0, 'Hello');
        // Flush 'Hello' to Firestore deterministically: destroy() awaits the
        // final save, so awaiting it guarantees the data landed before we
        // disconnect client 1.
        await provider1.destroy(); // Disconnect client 1

        // 2. New Client with Local Data ("World") connecting to same path
        const doc2 = new Y.Doc();
        doc2.getText('content').insert(0, 'World ');

        // Connect
        const provider2 = createProvider(doc2, path, { maxWaitTime: 10 });

        // Wait for sync (poll until merged)
        // Expected: Pull "Hello", Push "World ". Result: "World Hello" or "Hello World" (depends on order/IDs)
        // AND Firestore should have BOTH.
        await waitForConditionTruthy(() => {
            const t = doc2.getText('content').toString();
            return t.includes('Hello') && t.includes('World');
        }, { timeout: 30000, interval: 100, message: 'Client 2 should merge both Hello and World' });

        // 3. Verify Client 2 has merged state
        const text2 = doc2.getText('content').toString();
        expect(text2).toContain('Hello');
        expect(text2).toContain('World');

        // 4. Verify Firestore has merged state via Client 3
        const doc3 = new Y.Doc();
        const provider3 = createProvider(doc3, path);
        // This fails if Client 2 didn't push "World" because it saw "Hello" on server and thought "not empty, do nothing".
        await waitForConditionTruthy(
            () => doc3.getText('content').toString().includes('World'),
            { timeout: 30000, interval: 100, message: 'Client 3 should receive World from Firestore' }
        );

        const text3 = doc3.getText('content').toString();
        expect(text3).toContain('World');

        provider2.destroy();
        provider3.destroy();
    });
});
