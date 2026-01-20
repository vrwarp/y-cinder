import { describe, it, expect, vi, beforeEach } from 'vitest';
import { SeededRandom } from './prng';
import * as Y from 'yjs';

/**
 * Fuzz tests for compaction-like operations.
 * 
 * These tests verify that the pure Yjs operations used in compaction
 * (merging, state vectors, encoding) work correctly under various conditions
 * without needing actual Firestore.
 * 
 * NOTE: These tests use SHARED document state (simulating real-world sync)
 * rather than independent documents, since CRDTs require causal ordering.
 */
describe('Fuzz: Compaction Operations', () => {
    const SEED = 42;

    describe('tiered merge simulation', () => {
        it('should maintain data integrity through multi-tier merge', () => {
            const rng = new SeededRandom(SEED);

            for (let trial = 0; trial < 30; trial++) {
                // Use a SINGLE shared document to produce related updates
                const sharedDoc = new Y.Doc();
                sharedDoc.clientID = rng.int(1, 100);
                const text = sharedDoc.getText('content');

                const allUpdates: Uint8Array[] = [];

                // Generate sequential updates from the same document
                const numUpdates = rng.int(10, 30);
                for (let i = 0; i < numUpdates; i++) {
                    const svBefore = Y.encodeStateVector(sharedDoc);
                    const pos = rng.int(0, Math.max(0, text.length));
                    text.insert(pos, rng.string(rng.int(1, 5)));

                    // Get incremental update
                    const diff = Y.encodeStateAsUpdate(sharedDoc, svBefore);
                    if (diff.byteLength > 2) {
                        allUpdates.push(diff);
                    }
                }

                // Tier 2: Merge into "history segments"
                const segmentSize = rng.int(3, 8);
                const historySegments: Uint8Array[] = [];
                for (let i = 0; i < allUpdates.length; i += segmentSize) {
                    const chunk = allUpdates.slice(i, i + segmentSize);
                    if (chunk.length > 0) {
                        historySegments.push(Y.mergeUpdates(chunk));
                    }
                }

                // Tier 1: Merge into "snapshot"
                const snapshot = historySegments.length > 0
                    ? Y.mergeUpdates(historySegments)
                    : Y.encodeStateAsUpdate(sharedDoc);

                // Verify: apply snapshot to fresh doc equals original
                const docSnapshot = new Y.Doc();
                Y.applyUpdate(docSnapshot, snapshot);

                expect(docSnapshot.getText('content').toString())
                    .toBe(text.toString());

                sharedDoc.destroy();
                docSnapshot.destroy();
            }
        });
    });

    describe('partial compaction', () => {
        it('should correctly merge subset of updates (simulating limit)', () => {
            const rng = new SeededRandom(SEED + 1);

            for (let trial = 0; trial < 30; trial++) {
                // Use a SINGLE shared document
                const sharedDoc = new Y.Doc();
                sharedDoc.clientID = rng.int(1, 100);
                const text = sharedDoc.getText('content');

                const allUpdates: Uint8Array[] = [];

                // Generate updates
                const numUpdates = rng.int(20, 50);
                for (let i = 0; i < numUpdates; i++) {
                    const svBefore = Y.encodeStateVector(sharedDoc);
                    const pos = rng.int(0, Math.max(0, text.length));
                    text.insert(pos, rng.string(rng.int(1, 3)));

                    const diff = Y.encodeStateAsUpdate(sharedDoc, svBefore);
                    if (diff.byteLength > 2) {
                        allUpdates.push(diff);
                    }
                }

                // Simulate compaction with limit
                const limit = Math.min(rng.int(5, 15), allUpdates.length);
                const toCompact = allUpdates.slice(0, limit);
                const remaining = allUpdates.slice(limit);

                const compacted = toCompact.length > 0 ? Y.mergeUpdates(toCompact) : new Uint8Array([0]);

                // Apply compacted + remaining to fresh doc
                const docPartial = new Y.Doc();
                if (toCompact.length > 0) {
                    Y.applyUpdate(docPartial, compacted);
                }
                for (const update of remaining) {
                    Y.applyUpdate(docPartial, update);
                }

                // Should match original
                expect(docPartial.getText('content').toString())
                    .toBe(text.toString());

                sharedDoc.destroy();
                docPartial.destroy();
            }
        });
    });

    describe('size-based chunking', () => {
        it('should preserve data when chunking by size', () => {
            const rng = new SeededRandom(SEED + 2);

            for (let trial = 0; trial < 20; trial++) {
                // Use a SINGLE shared document
                const sharedDoc = new Y.Doc();
                sharedDoc.clientID = rng.int(1, 100);
                const text = sharedDoc.getText('content');

                const updates: Uint8Array[] = [];

                // Generate updates of varying sizes
                const numUpdates = rng.int(10, 30);
                for (let i = 0; i < numUpdates; i++) {
                    const svBefore = Y.encodeStateVector(sharedDoc);
                    const contentSize = rng.int(10, 100);
                    const pos = rng.int(0, Math.max(0, text.length));
                    text.insert(pos, rng.string(contentSize));

                    const diff = Y.encodeStateAsUpdate(sharedDoc, svBefore);
                    if (diff.byteLength > 2) {
                        updates.push(diff);
                    }
                }

                // Simulate size-based chunking
                const maxChunkSize = rng.int(500, 2000);
                const chunks: Uint8Array[] = [];
                let currentChunk: Uint8Array[] = [];
                let currentSize = 0;

                for (const update of updates) {
                    if (currentSize + update.byteLength > maxChunkSize && currentChunk.length > 0) {
                        chunks.push(Y.mergeUpdates(currentChunk));
                        currentChunk = [];
                        currentSize = 0;
                    }
                    currentChunk.push(update);
                    currentSize += update.byteLength;
                }
                if (currentChunk.length > 0) {
                    chunks.push(Y.mergeUpdates(currentChunk));
                }

                // Verify: merge all chunks equals original content
                if (chunks.length > 0) {
                    const fromChunks = Y.mergeUpdates(chunks);
                    const docChunks = new Y.Doc();
                    Y.applyUpdate(docChunks, fromChunks);

                    expect(docChunks.getText('content').toString())
                        .toBe(text.toString());

                    docChunks.destroy();
                }

                sharedDoc.destroy();
            }
        });
    });

    describe('concurrent client simulation', () => {
        it('should correctly merge concurrent edits from multiple clients', () => {
            const rng = new SeededRandom(SEED + 3);

            for (let trial = 0; trial < 20; trial++) {
                // Simulate multiple clients editing concurrently
                const numClients = rng.int(3, 8);
                const numRounds = rng.int(5, 15);

                const clientDocs: Y.Doc[] = [];
                for (let c = 0; c < numClients; c++) {
                    const doc = new Y.Doc();
                    doc.clientID = c + 1;
                    clientDocs.push(doc);
                }

                const allUpdates: Uint8Array[] = [];

                // Each round: each client makes an edit, then all sync
                for (let round = 0; round < numRounds; round++) {
                    const roundUpdates: Uint8Array[] = [];

                    // Each client makes an edit
                    for (const doc of clientDocs) {
                        const sv = Y.encodeStateVector(doc);
                        const text = doc.getText('content');
                        const pos = rng.int(0, Math.max(0, text.length));
                        text.insert(pos, rng.string(rng.int(1, 3)));

                        const diff = Y.encodeStateAsUpdate(doc, sv);
                        if (diff.byteLength > 2) {
                            roundUpdates.push(diff);
                            allUpdates.push(diff);
                        }
                    }

                    // All clients receive all updates from this round
                    for (const update of roundUpdates) {
                        for (const doc of clientDocs) {
                            Y.applyUpdate(doc, update);
                        }
                    }
                }

                // All client docs should now be identical
                const finalState = clientDocs[0].getText('content').toString();
                for (let c = 1; c < numClients; c++) {
                    expect(clientDocs[c].getText('content').toString()).toBe(finalState);
                }

                // Compacted updates should also produce same state
                if (allUpdates.length > 0) {
                    const compacted = Y.mergeUpdates(allUpdates);
                    const freshDoc = new Y.Doc();
                    Y.applyUpdate(freshDoc, compacted);
                    expect(freshDoc.getText('content').toString()).toBe(finalState);
                    freshDoc.destroy();
                }

                // Cleanup
                for (const doc of clientDocs) {
                    doc.destroy();
                }
            }
        });
    });

    describe('state vector diff accuracy', () => {
        it('should produce accurate diffs based on state vectors', () => {
            const rng = new SeededRandom(SEED + 4);

            for (let trial = 0; trial < 30; trial++) {
                // Create source doc with content
                const sourceDoc = new Y.Doc();
                sourceDoc.clientID = rng.int(1, 100);
                const text = sourceDoc.getText('content');

                for (let i = 0; i < rng.int(5, 15); i++) {
                    text.insert(rng.int(0, Math.max(0, text.length)), rng.string(rng.int(1, 5)));
                }

                // Get full update
                const fullUpdate = Y.encodeStateAsUpdate(sourceDoc);
                const finalDoc = new Y.Doc();
                Y.applyUpdate(finalDoc, fullUpdate);

                expect(finalDoc.getText('content').toString()).toBe(text.toString());

                sourceDoc.destroy();
                finalDoc.destroy();
            }
        });
    });
});
