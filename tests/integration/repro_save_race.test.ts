/**
 * Reproduction test for Issue 1: Race condition in saveToFirestore() error recovery
 * 
 * Bug: If another update arrives during await addDoc() and the write fails,
 * recovery merges correctly. But if write succeeds with new updates arriving,
 * those may not trigger another save.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

const { mockControls } = vi.hoisted(() => {
    return {
        mockControls: {
            shouldFailOnce: false,
            callCount: 0,
            successCount: 0
        }
    }
});

vi.mock('@firebase/firestore', async (importOriginal) => {
    const actual = await importOriginal<any>();
    return {
        ...actual,
        addDoc: async (collectionRef: any, data: any) => {
            mockControls.callCount++;
            if (mockControls.shouldFailOnce && collectionRef.path.includes('updates')) {
                console.log(`[Mock] Failing addDoc attempt #${mockControls.callCount}`);
                mockControls.shouldFailOnce = false;
                throw new Error("Simulated Network Failure");
            }
            mockControls.successCount++;
            return actual.addDoc(collectionRef, data);
        }
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionEquals } from '../utils/wait';

describe('Issue 1: saveToFirestore Race Condition', () => {
    let app: any;
    let db: any;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        await clearFirestore(db);
        mockControls.shouldFailOnce = false;
        mockControls.callCount = 0;
        mockControls.successCount = 0;
    });

    it('should NOT lose updates when write fails and new updates arrive during recovery', async () => {
        const path = `tests/save-race-${Date.now()}`;

        const doc1 = new Y.Doc();
        const provider1 = new FireProvider({
            firebaseApp: app,
            ydoc: doc1,
            path,
            maxWaitTime: 50 // Fast debounce
        });

        const doc2 = new Y.Doc();
        const provider2 = new FireProvider({
            firebaseApp: app,
            ydoc: doc2,
            path,
            maxWaitTime: 50
        });

        // Wait for initial sync
        await new Promise(r => setTimeout(r, 1000));

        // Set up failure for next write
        mockControls.shouldFailOnce = true;

        // Make first update - this will fail
        doc1.getText('content').insert(0, 'First');

        // Wait a bit for the failure to occur and recovery to start
        await new Promise(r => setTimeout(r, 100));

        // Make second update while recovery is happening
        doc1.getText('content').insert(5, 'Second');

        // Make third update rapidly
        doc1.getText('content').insert(11, 'Third');

        // Wait for all writes to complete
        await new Promise(r => setTimeout(r, 500));

        // Verify both updates eventually sync to doc2
        try {
            await waitForConditionEquals(
                () => doc2.getText('content').toString(),
                'FirstSecondThird',
                { timeout: 5000, interval: 100, message: 'All updates should sync' }
            );
        } catch (e) {
            // Expected to fail if bug exists
        }

        const doc1Text = doc1.getText('content').toString();
        const doc2Text = doc2.getText('content').toString();

        console.log(`Doc1 text: "${doc1Text}"`);
        console.log(`Doc2 text: "${doc2Text}"`);
        console.log(`addDoc calls: ${mockControls.callCount}, successes: ${mockControls.successCount}`);

        // This is the critical assertion
        // Bug: doc2 might not have all updates if some were lost
        expect(doc2Text).toBe('FirstSecondThird');

        provider1.destroy();
        provider2.destroy();
    });

    it('should handle rapid updates during error recovery window', async () => {
        const path = `tests/save-race-rapid-${Date.now()}`;

        const doc1 = new Y.Doc();
        const provider1 = new FireProvider({
            firebaseApp: app,
            ydoc: doc1,
            path,
            maxWaitTime: 20 // Very fast debounce
        });

        const doc2 = new Y.Doc();
        const provider2 = new FireProvider({
            firebaseApp: app,
            ydoc: doc2,
            path,
            maxWaitTime: 20
        });

        await new Promise(r => setTimeout(r, 1000));

        mockControls.shouldFailOnce = true;

        // Rapid fire updates
        for (let i = 0; i < 10; i++) {
            doc1.getText('content').insert(i, String(i));
            await new Promise(r => setTimeout(r, 10)); // Small delay between updates
        }

        await new Promise(r => setTimeout(r, 1000));

        try {
            await waitForConditionEquals(
                () => doc2.getText('content').toString(),
                doc1.getText('content').toString(),
                { timeout: 5000, interval: 100, message: 'Rapid updates should sync' }
            );
        } catch (e) {
            // Expected to fail if bug exists
        }

        const expected = doc1.getText('content').toString();
        const actual = doc2.getText('content').toString();

        console.log(`Expected: "${expected}"`);
        console.log(`Actual: "${actual}"`);

        expect(actual).toBe(expected);

        provider1.destroy();
        provider2.destroy();
    });
});
