/**
 * Advanced Integration Tests
 *
 * Tests complex multi-client scenarios including:
 * - Subdocument synchronization across clients
 * - Concurrent modifications to nested data structures
 * - Complex Yjs data types (arrays, maps, text)
 *
 * @file advanced.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionTruthy, waitForConditionEquals } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('FireProvider Advanced Integration (Emulator)', () => {
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
        await clearFirestore(db);
    });

    let counter = 0;

    it('should sync subdocuments recursively', async () => {
        const path = `integration-tests/subdocs-${getStableDate()}-${counter++}`;

        // Client 1 setup
        const doc1 = new Y.Doc();
        const provider1 = createProvider(doc1, path);
        const subdocs1 = doc1.getMap('my-subdocs');

        // Client 2 setup
        const doc2 = new Y.Doc();
        const provider2 = createProvider(doc2, path);
        const subdocs2 = doc2.getMap('my-subdocs');

        // Add subdoc on Client 1
        const subdoc1 = new Y.Doc();
        subdocs1.set('child', subdoc1);

        // Wait for subdoc provider to attach (it's sync in handleSubdocs, but good to be safe)
        await new Promise(r => setTimeout(r, 100));

        subdoc1.getText('inner').insert(0, 'Nested Content');

        // Wait for propagation (condition-based — fixed sleeps flake when
        // the emulator is under load from earlier tests in the batch):
        // 1. Main doc update -> Client 2
        // 2. Client 2 sees new subdoc -> instantiates child provider
        // 3. Child provider syncs 'subdocs/guid'
        const subdoc2Candidate = await waitForConditionTruthy(
            () => subdocs2.get('child'),
            { timeout: 15000, interval: 100, message: 'Client 2 should see the subdoc' }
        );

        // Load subdoc on Client 2 by "requesting" it (Yjs lazy loading)
        // Note: Yjs map.get() returns the subdoc instance if available.
        // We verify that its content eventually syncs.
        const subdoc2 = subdoc2Candidate as Y.Doc;

        await waitForConditionEquals(
            () => subdoc2.getText('inner').toString(),
            'Nested Content',
            { timeout: 15000, interval: 100, message: 'Subdoc content should sync to Client 2' }
        );

        provider1.destroy();
        provider2.destroy();
    }, 40000);

    it('should handle concurrent edits from multiple clients', async () => {
        const path = `integration-tests/concurrency-${getStableDate()}-${counter++}`;
        const numClients = 3;
        const clients = [];

        for (let i = 0; i < numClients; i++) {
            const doc = new Y.Doc();
            doc.clientID = i + 1; // Force distinct IDs
            const provider = createProvider(doc, path, { maxWaitTime: 10 });
            clients.push({ doc, provider });
        }

        // All clients insert text at same position concurrently
        clients.forEach((c, idx) => {
            c.doc.getText('content').insert(0, `Client${idx}`);
        });

        // Wait for convergence using waitForConditionTruthy
        await waitForConditionTruthy(async () => {
            const firstContent = clients[0].doc.getText('content').toString();
            if (firstContent.length < numClients * 'Client0'.length) return false;

            for (let i = 1; i < numClients; i++) {
                if (clients[i].doc.getText('content').toString() !== firstContent) {
                    return false;
                }
            }
            return true;
        }, { timeout: 30000, interval: 100, message: 'Clients did not converge' });

        const finalContent = clients[0].doc.getText('content').toString();
        expect(finalContent.length).toBe(numClients * 'Client0'.length);

        // Verify they all have the same content (redundant but explicit)
        for (let i = 1; i < numClients; i++) {
            expect(clients[i].doc.getText('content').toString()).toBe(finalContent);
        }

        clients.forEach(c => c.provider.destroy());
    }, 40000);
});
