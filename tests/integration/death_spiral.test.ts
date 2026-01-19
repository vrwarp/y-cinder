
import { describe, it, expect, beforeEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { collection, getDocs, doc, Bytes, setDoc } from 'firebase/firestore';
import { setupEmulator } from '../utils/emulator';

describe('FireProvider Death Spiral Repro', () => {
    let app: any;
    let db: any;
    const path = `tests/death-spiral-${Date.now()}`;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
    });

    it('should fail to compact if updates exceed 1MB without chunking', { timeout: 30000 }, async () => {
        const generatorDoc = new Y.Doc();
        const providerDoc = new Y.Doc();

        const provider = new FireProvider({
            firebaseApp: app,
            ydoc: providerDoc,
            path,
            maxUpdatesThreshold: 1000 // Don't trigger automatically too early
        });

        // 1. Create a large amount of data (e.g. 1.2MB total in updates)
        // We'll create 10 updates of 120KB each.
        const updateSize = 120000;
        let lastStateVector: Uint8Array | undefined;

        for (let i = 0; i < 10; i++) {
            const text = 'a'.repeat(updateSize);
            generatorDoc.getText('large').insert(0, text);

            // Get delta since last update
            const update = lastStateVector
                ? Y.encodeStateAsUpdate(generatorDoc, lastStateVector)
                : Y.encodeStateAsUpdate(generatorDoc);

            lastStateVector = Y.encodeStateVector(generatorDoc);

            await setDoc(doc(collection(db, path, 'updates'), `upd-${i}`), {
                update: Bytes.fromUint8Array(update),
                createdAt: Date.now() + i,
                createdBy: 'some-other-client'
            });
        }

        // 2. Try to compact. This should now succeed because of chunking.

        console.log("Triggering compaction...");
        await provider.compact();

        // 3. Verification
        const updatesSnap = await getDocs(collection(db, path, 'updates'));
        // Compaction should have succeeded and deleted all 10 updates
        expect(updatesSnap.size).toBe(0);

        const historySnap = await getDocs(collection(db, path, 'history'));
        // Since we had ~1.2MB, and limit is 900KB, it should have created 2 history segments
        expect(historySnap.size).toBeGreaterThanOrEqual(1);
        console.log(`Created ${historySnap.size} history segments.`);

        // 4. Verify data integrity
        provider.destroy();
        const ydoc2 = new Y.Doc();
        const provider2 = new FireProvider({
            firebaseApp: app,
            ydoc: ydoc2,
            path
        });

        // Wait for sync
        await new Promise(r => setTimeout(r, 2000));

        const text = ydoc2.getText('large').toString();
        expect(text.length).toBe(120000 * 10);

        provider2.destroy();
    });
});
