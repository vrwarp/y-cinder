import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForCondition } from '../utils/wait';

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
        const path = `integration-tests/fuzz-${Date.now()}`;
        const numClients = 3;
        const numOps = 100; // Requested high load
        const clients = [];

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
                const op = ops[Math.floor(Math.random() * ops.length)];

                try {
                    if (op === 'insert') {
                        const pos = Math.floor(Math.random() * (len + 1));
                        const char = Math.random().toString(36).substring(2, 3);
                        text.insert(pos, char);
                    } else if (op === 'delete' && len > 0) {
                        const pos = Math.floor(Math.random() * len);
                        text.delete(pos, 1);
                    }
                } catch (e) {
                    // Ignore index errors if any (shouldn't happen with correct logic)
                }
            }
            // Small jitter
            await new Promise(r => setTimeout(r, Math.random() * 20 + 5)); // Increased jitter to avoid emulator contention
        }

        // Allow settling using waitForCondition
        await waitForCondition(async () => {
            const state0 = clients[0].doc.getText('content').toString();
            for (let i = 1; i < numClients; i++) {
                if (clients[i].doc.getText('content').toString() !== state0) {
                    return false;
                }
            }
            return true;
        }, 30000, 500, 'Fuzz test did not converge');

        const finalState = clients[0].doc.getText('content').toString();
        console.log(`Fuzz test converged to length: ${finalState.length}`);

        clients.forEach(c => c.provider.destroy());
    }, 60000);
});
