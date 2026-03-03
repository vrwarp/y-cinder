/**
 * Death Spiral Compaction Tests
 *
 * Tests the chunked compaction strategy that prevents the "death spiral"
 * when updates exceed Firestore's 1MB document limit. Verifies that large
 * updates are correctly split into multiple history segments rather than
 * failing or causing data loss.
 *
 * @file death_spiral.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { collection, getDocs, doc, Bytes, setDoc, getDoc } from 'firebase/firestore';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionEquals, waitFor } from '../utils/wait';
import { getStableDate, seedFromString } from '../unit/prng';

describe('FireProvider Death Spiral Repro', () => {
    let app: any;
    let db: any;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        const seed = `death-spiral-${getStableDate()}-${counter++}`;
        const rng = seedFromString(seed);
        path = `tests/${seed}-${rng.string(5)}`;
    });

    it('should successfully compact large updates by offloading to Cloud Storage', { timeout: 60000 }, async () => {
        const generatorDoc = new Y.Doc();
        const providerDoc = new Y.Doc();

        // 1. Create a large amount of data (e.g. 1.2MB total in updates)
        // We'll create 10 updates of 120KB each.
        const updateSize = 120000;
        let lastStateVector: Uint8Array | undefined;

        console.log(`Creating ${10} updates of ${updateSize} bytes each...`);

        for (let i = 0; i < 10; i++) {
            const text = 'a'.repeat(updateSize);
            generatorDoc.getText('large').insert(0, text);

            // Get delta since last update
            const update = lastStateVector
                ? Y.encodeStateAsUpdate(generatorDoc, lastStateVector)
                : Y.encodeStateAsUpdate(generatorDoc);

            lastStateVector = Y.encodeStateVector(generatorDoc);

            console.log(`Update ${i}: ${update.byteLength} bytes`);

            await setDoc(doc(collection(db, path, 'updates'), `upd-${i}`), {
                update: Bytes.fromUint8Array(update),
                createdAt: Date.now() + i,
                createdBy: 'some-other-client'
            });
        }

        console.log(`Generator doc final length: ${generatorDoc.getText('large').toString().length}`);

        const provider = new FireProvider({
            firebaseApp: app,
            ydoc: providerDoc,
            path,
            maxUpdatesThreshold: 1000
        });

        // Give it a moment to sync before manually triggering compaction
        await new Promise(r => setTimeout(r, 1000));

        // 2. Try to compact. This should offload the >1MB blob to Cloud Storage.
        console.log("Triggering compaction...");
        const result = await provider.compact();
        console.log("Compaction result:", result);

        // 3. Verification
        const updatesSnap = await getDocs(collection(db, path, 'updates'));
        console.log(`Updates remaining: ${updatesSnap.size}`);
        // Compaction should have succeeded and deleted all 10 updates
        expect(updatesSnap.size).toBe(0);

        const historySnap = await getDocs(collection(db, path, 'history'));
        console.log(`History segments created: ${historySnap.size}`);
        historySnap.forEach(d => {
            const data = d.data();
            console.log(`  Segment ${d.id}: ${data.segment?.toUint8Array()?.length || 0} bytes, stateVector: ${data.stateVector ? 'present' : 'MISSING'}`);
        });

        // Check main doc
        const mainSnap = await getDoc(doc(db, path));
        if (mainSnap.exists()) {
            const mainData = mainSnap.data();
            console.log(`Main doc content: ${mainData?.content?.toUint8Array()?.length || 0} bytes`);
        } else {
            console.log(`Main doc: does not exist`);
        }

        // 4. Verify data integrity
        await provider.destroy();
        console.log("Provider 1 destroyed, creating provider 2 for sync test...");

        const ydoc2 = new Y.Doc();
        const provider2 = new FireProvider({
            firebaseApp: app,
            ydoc: ydoc2,
            path
        });

        // Wait for sync with progress logging
        console.log("Waiting for sync...");
        const expectedLength = 120000 * 10;

        try {
            await waitFor(
                () => ydoc2.getText('large').toString().length,
                (len) => {
                    console.log(`  Sync progress: ${len}/${expectedLength} (${Math.round(100 * len / expectedLength)}%)`);
                    return len === expectedLength;
                },
                {
                    timeout: 30000,
                    interval: 1000,
                    message: 'Data integrity check failed'
                }
            );
        } catch (e) {
            // On failure, log detailed state
            console.error("Sync failed! Final state:");
            console.log(`  Final length: ${ydoc2.getText('large').toString().length}`);
            console.log(`  Expected: ${expectedLength}`);
            throw e;
        }

        const text = ydoc2.getText('large').toString();
        expect(text.length).toBe(120000 * 10);

        await provider2.destroy();
    });
});

