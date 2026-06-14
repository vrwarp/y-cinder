/**
 * Error Recovery Integration Tests
 *
 * Tests the provider's resilience to transient errors including:
 * - Compaction transaction failures and retry logic
 * - Network interruptions during sync
 * - Exponential backoff behavior
 *
 * @file error_recovery.test.ts
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
const { mockControls } = vi.hoisted(() => {
    return { mockControls: { shouldFailAddDoc: false, failCount: 0 } }
});

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        addDoc: async (collectionRef: any, data: any) => {
            if (mockControls.shouldFailAddDoc && collectionRef.path.includes('updates')) {
                console.log(`[Mock] Intercepting addDoc to ${collectionRef.path} - Simulation Failure`);
                mockControls.shouldFailAddDoc = false;
                mockControls.failCount++;
                throw new Error("Simulated Network Error");
            }
            return actual.addDoc(collectionRef, data);
        }
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('FireProvider Error Recovery (Emulator)', () => {
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
        mockControls.shouldFailAddDoc = false;
        mockControls.failCount = 0;
    });

    let counter = 0;

    it('should retry saving updates if write fails initially', async () => {
        const path = `integration-tests/error-recovery-${getStableDate()}-${counter++}`;

        const doc1 = new Y.Doc();
        // Short debounce to trigger saves quickly
        const provider1 = createProvider(doc1, path, { maxWaitTime: 50 });

        const doc2 = new Y.Doc();
        const provider2 = createProvider(doc2, path, { maxWaitTime: 50 });

        // Wait for initial sync to complete to avoid race conditions where sync() picks up the update
        await waitForConditionTruthy(
            () => provider1.synced && provider2.synced,
            { timeout: 30000, interval: 100, message: 'Both providers should finish initial sync' }
        );

        // Trigger failure for the NEXT addDoc
        mockControls.shouldFailAddDoc = true;

        // Make an update
        doc1.getText('content').insert(0, 'Critical Data');

        // We expect provider1 to try to save, fail, capture the error, put data back in cache, and retry.
        // Current implementation: Fails, logs error, clears cache. Data lost.

        // Wait for potential recovery
        try {
            await waitForConditionEquals(
                () => doc2.getText('content').toString(),
                'Critical Data',
                { timeout: 30000, interval: 100, message: 'Doc2 should eventually receive data' }
            );
        } catch (e) {
            // Check if it failed
        }

        expect(mockControls.failCount).toBeGreaterThan(0);

        // Assert: doc2 should have received the data.
        // If current code is buggy, this expect will fail.
        expect(doc2.getText('content').toString()).toBe('Critical Data');

        provider1.destroy();
        provider2.destroy();
    });
});
