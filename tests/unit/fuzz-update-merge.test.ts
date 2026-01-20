import { describe, it, expect } from 'vitest';
import { SeededRandom } from './prng';
import * as Y from 'yjs';

/**
 * Fuzz tests for Yjs update merging operations.
 * 
 * These tests verify:
 * 1. Merged updates are equivalent to sequential application
 * 2. Commutativity (order of merge doesn't affect final state)
 * 3. Idempotency (applying same update twice is safe)
 * 4. Large-scale merging doesn't corrupt data
 */
describe('Fuzz: Update Merge Operations', () => {
    const SEED = 42;

    describe('merge equivalence', () => {
        it('should produce same result as sequential application', () => {
            const rng = new SeededRandom(SEED);

            for (let trial = 0; trial < 50; trial++) {
                // Create a shared doc that all "clients" contribute to
                const sharedDoc = new Y.Doc();
                sharedDoc.clientID = rng.int(1, 100);
                const text = sharedDoc.getText('content');

                const updates: Uint8Array[] = [];

                // Generate sequence of updates
                const numOps = rng.int(5, 20);
                for (let i = 0; i < numOps; i++) {
                    const svBefore = Y.encodeStateVector(sharedDoc);
                    const pos = rng.int(0, Math.max(0, text.length));
                    text.insert(pos, rng.string(rng.int(1, 3)));

                    const diff = Y.encodeStateAsUpdate(sharedDoc, svBefore);
                    if (diff.byteLength > 2) {
                        updates.push(diff);
                    }
                }

                if (updates.length === 0) continue;

                // Method 1: Merge all updates, then apply
                const merged = Y.mergeUpdates(updates);
                const docMerged = new Y.Doc();
                Y.applyUpdate(docMerged, merged);
                const stateMerged = docMerged.getText('content').toString();

                // Method 2: Apply updates sequentially
                const docSeq = new Y.Doc();
                for (const update of updates) {
                    Y.applyUpdate(docSeq, update);
                }
                const stateSeq = docSeq.getText('content').toString();

                // Both should match original
                expect(stateMerged).toBe(text.toString());
                expect(stateSeq).toBe(text.toString());

                sharedDoc.destroy();
                docMerged.destroy();
                docSeq.destroy();
            }
        });
    });

    describe('commutativity', () => {
        it('should produce same result regardless of merge order', () => {
            const rng = new SeededRandom(SEED + 1);

            for (let trial = 0; trial < 50; trial++) {
                // Create updates from multiple clients that sync with each other
                const numClients = rng.int(2, 4);
                const clientDocs: Y.Doc[] = [];

                for (let c = 0; c < numClients; c++) {
                    const doc = new Y.Doc();
                    doc.clientID = c + 1;
                    clientDocs.push(doc);
                }

                const allUpdates: Uint8Array[] = [];

                // Each client makes some edits, then syncs
                for (let round = 0; round < 3; round++) {
                    const roundUpdates: Uint8Array[] = [];

                    for (const doc of clientDocs) {
                        const sv = Y.encodeStateVector(doc);
                        const text = doc.getText('content');
                        const pos = rng.int(0, Math.max(0, text.length));
                        text.insert(pos, rng.string(rng.int(1, 2)));

                        const diff = Y.encodeStateAsUpdate(doc, sv);
                        if (diff.byteLength > 2) {
                            roundUpdates.push(diff);
                            allUpdates.push(diff);
                        }
                    }

                    // Sync all clients
                    for (const update of roundUpdates) {
                        for (const doc of clientDocs) {
                            Y.applyUpdate(doc, update);
                        }
                    }
                }

                if (allUpdates.length === 0) {
                    for (const doc of clientDocs) doc.destroy();
                    continue;
                }

                // After full sync, all clients should be identical
                const finalState = clientDocs[0].getText('content').toString();

                // Merge in different orders should produce same state
                const merged1 = Y.mergeUpdates(allUpdates);
                const merged2 = Y.mergeUpdates([...allUpdates].reverse());
                const merged3 = Y.mergeUpdates(rng.shuffle([...allUpdates]));

                const doc1 = new Y.Doc();
                Y.applyUpdate(doc1, merged1);

                const doc2 = new Y.Doc();
                Y.applyUpdate(doc2, merged2);

                const doc3 = new Y.Doc();
                Y.applyUpdate(doc3, merged3);

                expect(doc1.getText('content').toString()).toBe(finalState);
                expect(doc2.getText('content').toString()).toBe(finalState);
                expect(doc3.getText('content').toString()).toBe(finalState);

                doc1.destroy();
                doc2.destroy();
                doc3.destroy();
                for (const doc of clientDocs) doc.destroy();
            }
        });
    });

    describe('idempotency', () => {
        it('should be safe to apply same update multiple times', () => {
            const rng = new SeededRandom(SEED + 2);

            for (let trial = 0; trial < 50; trial++) {
                // Create a base document
                const sourceDoc = new Y.Doc();
                sourceDoc.clientID = rng.int(1, 1000);
                const text = sourceDoc.getText('content');

                const numOps = rng.int(5, 15);
                for (let i = 0; i < numOps; i++) {
                    const pos = rng.int(0, Math.max(0, text.length));
                    text.insert(pos, rng.string(rng.int(1, 3)));
                }

                const update = Y.encodeStateAsUpdate(sourceDoc);

                // Apply once
                const doc1 = new Y.Doc();
                Y.applyUpdate(doc1, update);
                const state1 = doc1.getText('content').toString();

                // Apply many times
                const doc2 = new Y.Doc();
                for (let i = 0; i < 10; i++) {
                    Y.applyUpdate(doc2, update);
                }
                const state2 = doc2.getText('content').toString();

                // Should be identical
                expect(state1).toBe(text.toString());
                expect(state2).toBe(text.toString());

                sourceDoc.destroy();
                doc1.destroy();
                doc2.destroy();
            }
        });
    });

    describe('large scale', () => {
        it('should handle large number of updates without data loss', () => {
            const rng = new SeededRandom(SEED + 4);

            // Use a shared doc
            const sharedDoc = new Y.Doc();
            sharedDoc.clientID = 1;
            const text = sharedDoc.getText('content');

            const updates: Uint8Array[] = [];

            // Generate 100 updates
            for (let i = 0; i < 100; i++) {
                const svBefore = Y.encodeStateVector(sharedDoc);
                text.insert(rng.int(0, Math.max(0, text.length)), rng.string(rng.int(1, 5)));
                const diff = Y.encodeStateAsUpdate(sharedDoc, svBefore);
                if (diff.byteLength > 2) {
                    updates.push(diff);
                }
            }

            // Merge all
            const merged = Y.mergeUpdates(updates);

            // Should be valid
            const doc = new Y.Doc();
            expect(() => Y.applyUpdate(doc, merged)).not.toThrow();

            // Should match original
            expect(doc.getText('content').toString()).toBe(text.toString());

            sharedDoc.destroy();
            doc.destroy();
        });

        it('should handle very large content', () => {
            const rng = new SeededRandom(SEED + 5);

            const doc = new Y.Doc();
            doc.clientID = 1;
            const text = doc.getText('content');

            // Insert 10KB of content
            const largeContent = rng.string(10000);
            text.insert(0, largeContent);

            const update = Y.encodeStateAsUpdate(doc);

            // Merge with self (simulate multiple syncs)
            const merged = Y.mergeUpdates([update, update, update]);

            // Apply merged
            const doc2 = new Y.Doc();
            Y.applyUpdate(doc2, merged);

            expect(doc2.getText('content').toString()).toBe(largeContent);

            doc.destroy();
            doc2.destroy();
        });
    });
});
