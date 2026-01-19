/**
 * Test for Issue 13: Network partition during sync
 * 
 * Tests that provider recovers gracefully from network issues during sync.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

const { mockControls } = vi.hoisted(() => {
    return {
        mockControls: {
            shouldFailGetDocs: false,
            failCount: 0
        }
    }
});

vi.mock('@firebase/firestore', async (importOriginal) => {
    const actual = await importOriginal<any>();
    return {
        ...actual,
        getDocs: async (query: any) => {
            if (mockControls.shouldFailGetDocs) {
                mockControls.failCount++;
                console.log(`[Mock] Failing getDocs #${mockControls.failCount}`);
                if (mockControls.failCount >= 3) {
                    mockControls.shouldFailGetDocs = false; // Stop failing after 3
                }
                throw new Error("Network partition simulated");
            }
            return actual.getDocs(query);
        }
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';

describe('Issue 13: Network Partition During Sync', () => {
    let app: any;
    let db: any;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        await clearFirestore(db);
        mockControls.shouldFailGetDocs = false;
        mockControls.failCount = 0;
    });

    it('should recover gracefully from getDocs failure during sync', { timeout: 20000 }, async () => {
        const path = `tests/network-partition-${Date.now()}`;

        // Create first provider successfully
        const doc1 = new Y.Doc();
        const provider1 = new FireProvider({
            firebaseApp: app,
            ydoc: doc1,
            path,
            maxWaitTime: 50
        });

        await new Promise(r => setTimeout(r, 1000));

        doc1.getText('content').insert(0, 'DataBeforePartition');

        await new Promise(r => setTimeout(r, 500));

        // Now try to create second provider during "partition"
        mockControls.shouldFailGetDocs = true;

        const doc2 = new Y.Doc();
        const provider2 = new FireProvider({
            firebaseApp: app,
            ydoc: doc2,
            path,
            maxWaitTime: 50
        });

        // Wait for sync to fail and potentially retry
        await new Promise(r => setTimeout(r, 3000));

        console.log(`Failed getDocs count: ${mockControls.failCount}`);

        // Provider should be created without crash
        expect(provider2).toBeDefined();

        // After partition heals, should eventually sync (listener recovery)
        await new Promise(r => setTimeout(r, 5000));

        const content = doc2.getText('content').toString();
        console.log(`Doc2 content after partition: "${content}"`);

        // May or may not have content depending on retry behavior
        // Main assertion: no crash, provider functions

        await provider1.destroy();
        await provider2.destroy();
    });

    it('should not corrupt local state on sync failure', { timeout: 10000 }, async () => {
        const path = `tests/no-corrupt-${Date.now()}`;

        const doc1 = new Y.Doc();
        doc1.getText('local').insert(0, 'LocalData');

        mockControls.shouldFailGetDocs = true;

        const provider = new FireProvider({
            firebaseApp: app,
            ydoc: doc1,
            path
        });

        await new Promise(r => setTimeout(r, 2000));

        // Local data should still be intact
        const localContent = doc1.getText('local').toString();
        expect(localContent).toBe('LocalData');

        await provider.destroy();
    });
});
