/**
 * Fuzz Integration Tests
 *
 * Randomized stress tests that generate random document operations across
 * multiple simulated clients. Verifies that all clients eventually converge
 * to the same document state regardless of operation order or timing.
 *
 * Uses a seeded PRNG for reproducibility when debugging failures.
 *
 * @file fuzz.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { seedFromString, getStableDate } from '../unit/prng';

describe('FireProvider Fuzz Testing (Emulator)', () => {
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
        await clearFirestore(db);
    });

    it('should converge after random operations', async () => {
        // Create a deterministic seed based on date
        const seedValue = `fuzz-${getStableDate()}`;
        console.log(`Fuzz test seed: "${seedValue}"`);
        const rng = seedFromString(seedValue);

        const path = `integration-tests/${seedValue}`;
        const numClients = 3;
        const numOps = 100; // Requested high load

        interface Client {
            doc: Y.Doc;
            provider: FireProvider;
            id: number;
        }
        const clients: Client[] = [];

        // Setup clients
        for (let i = 0; i < numClients; i++) {
            const doc = new Y.Doc();
            doc.clientID = i; // Deterministic IDs for debugging if needed
            const provider = createProvider(doc, path, { maxWaitTime: 5 });
            clients.push({ doc, provider, id: i });
        }

        // Random operations
        const ops = ['insert', 'delete', 'insert']; // Bias towards insert to keep content

        for (let j = 0; j < numOps; j++) {
            for (const client of clients) {
                const text = client.doc.getText('content');
                const len = text.length;
                const op = rng.choice(ops);

                try {
                    if (op === 'insert') {
                        const pos = rng.int(0, len);
                        const char = rng.string(1);
                        text.insert(pos, char);
                    } else if (op === 'delete' && len > 0) {
                        const pos = rng.int(0, len - 1);
                        text.delete(pos, 1);
                    }
                } catch (e) {
                    // Ignore index errors if any (shouldn't happen with correct logic)
                }
            }
            // Small jitter
            await new Promise(r => setTimeout(r, rng.int(5, 25))); // Increased jitter to avoid emulator contention
        }

        // Allow settling using waitForConditionTruthy
        await waitForConditionTruthy(async () => {
            const state0 = clients[0].doc.getText('content').toString();
            for (let i = 1; i < numClients; i++) {
                if (clients[i].doc.getText('content').toString() !== state0) {
                    return false;
                }
            }
            return true;
        }, {
            timeout: 60000,
            interval: 500,
            message: 'Fuzz test did not converge',
            onFailure: async () => {
                const logs = clients.map(c => `Client ${c.id}: "${c.doc.getText('content').toString()}"`);
                return logs.join('\n');
            }
        });

        const finalState = clients[0].doc.getText('content').toString();
        console.log(`Fuzz test converged to length: ${finalState.length}`);

        await Promise.all(clients.map(c => c.provider.destroy()));
    }, 90000);
});
