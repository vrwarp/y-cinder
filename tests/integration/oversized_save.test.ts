/**
 * Oversized Save Integration Tests
 *
 * Tests the provider's handling of oversized documents and save failures:
 * - Proactive Cloud Storage offload for updates exceeding the inline limit
 * - Server-side size rejection detection
 * - Generic save failure retry cap (MAX_SAVE_RETRIES) with backoff
 * - save-rejected event payload structure
 *
 * @file oversized_save.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { mockControls } = vi.hoisted(() => {
    return {
        mockControls: {
            shouldFailAddDoc: false,
            failCode: '' as string,
            failMessage: '' as string,
            addDocCallCount: 0,
        }
    };
});

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        addDoc: async (collectionRef: any, data: any) => {
            if (mockControls.shouldFailAddDoc && collectionRef.path.includes('updates')) {
                mockControls.addDocCallCount++;
                const err: any = new Error(mockControls.failMessage || 'Simulated Error');
                if (mockControls.failCode) {
                    err.code = mockControls.failCode;
                }
                throw err;
            }
            mockControls.addDocCallCount++;
            return actual.addDoc(collectionRef, data);
        }
    };
});

import { collection, getDocs } from '@firebase/firestore';
import { FireProvider } from '../../src/provider';
import { DEFAULTS } from '../../src/types';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('Oversized Save Circuit Breaker (Emulator)', () => {
    let app: any;
    let db: any;
    let counter = 0;

    const createProvider = (doc: Y.Doc, path: string, config: any = {}) => {
        return new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path,
            ...config,
        });
    };

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        await clearFirestore(db);
        mockControls.shouldFailAddDoc = false;
        mockControls.failCode = '';
        mockControls.failMessage = '';
        mockControls.addDocCallCount = 0;
    });

    it('should emit save-rejected with document-too-large when server returns invalid-argument', async () => {
        const path = `integration-tests/oversized-save-${getStableDate()}-${counter++}`;
        const doc1 = new Y.Doc();
        const provider1 = createProvider(doc1, path, { maxWaitTime: 50 });

        // Wait for initial sync
        await waitForConditionTruthy(() => provider1.synced, { timeout: 30000, message: 'Provider should complete initial sync' });

        // Setup: mock addDoc to reject with invalid-argument (Firestore size error)
        mockControls.shouldFailAddDoc = true;
        mockControls.failCode = 'invalid-argument';
        mockControls.failMessage = 'Document exceeds the maximum allowed size';

        const rejectedEvents: any[] = [];
        provider1.on('save-rejected', (event: any) => {
            rejectedEvents.push(event);
        });

        // Make an update
        doc1.getText('content').insert(0, 'Some data');

        // Wait for the debounced save attempt to fail and emit save-rejected
        await waitForConditionEquals(() => rejectedEvents.length, 1, { timeout: 15000, interval: 100, message: 'save-rejected should be emitted' });

        // Should have emitted save-rejected
        expect(rejectedEvents.length).toBe(1);
        expect(rejectedEvents[0].code).toBe('document-too-large');
        expect(rejectedEvents[0].sizeBytes).toBeGreaterThan(0);
        expect(rejectedEvents[0].limitBytes).toBe(DEFAULTS.FIRESTORE_DOC_LIMIT);
        expect(rejectedEvents[0].error).toBeInstanceOf(Error);
        expect(rejectedEvents[0].update).toBeInstanceOf(Uint8Array);

        // Should NOT have retried (only 1 addDoc call)
        expect(mockControls.addDocCallCount).toBe(1);

        await provider1.destroy();
    });

    it('should emit save-rejected with max-retries-exceeded after MAX_SAVE_RETRIES generic failures', async () => {
        const path = `integration-tests/oversized-save-${getStableDate()}-${counter++}`;
        const doc1 = new Y.Doc();
        // Very short debounce so retries are fast
        const provider1 = createProvider(doc1, path, { maxWaitTime: 30 });

        // Wait for initial sync
        await waitForConditionTruthy(() => provider1.synced, { timeout: 30000, message: 'Provider should complete initial sync' });

        // Setup: mock addDoc to always fail with a generic error
        mockControls.shouldFailAddDoc = true;
        mockControls.failCode = '';
        mockControls.failMessage = 'Simulated persistent network error';

        // IMPORTANT: Keep the mock failing for all retries
        const originalShouldFail = Object.getOwnPropertyDescriptor(mockControls, 'shouldFailAddDoc');
        Object.defineProperty(mockControls, 'shouldFailAddDoc', {
            get: () => true,
            set: () => { },  // ignore sets (the mock resets it otherwise)
            configurable: true,
        });

        const rejectedEvents: any[] = [];
        provider1.on('save-rejected', (event: any) => {
            rejectedEvents.push(event);
        });

        // Make an update
        doc1.getText('content').insert(0, 'Retry test data');

        // Wait for all MAX_SAVE_RETRIES attempts to exhaust and emit the
        // terminal save-rejected. Retries use exponential backoff (~300ms,
        // ~500ms, ~900ms, ~1700ms plus jitter), so allow generous headroom.
        await waitForConditionEquals(() => rejectedEvents.length, 1, { timeout: 25000, interval: 100, message: 'save-rejected (max-retries-exceeded) should be emitted' });

        // Restore original property
        Object.defineProperty(mockControls, 'shouldFailAddDoc', {
            value: true,
            writable: true,
            configurable: true,
        });

        // Should have emitted save-rejected with max-retries-exceeded
        expect(rejectedEvents.length).toBe(1);
        expect(rejectedEvents[0].code).toBe('max-retries-exceeded');
        expect(rejectedEvents[0].retries).toBe(DEFAULTS.MAX_SAVE_RETRIES);
        expect(rejectedEvents[0].error).toBeInstanceOf(Error);
        expect(rejectedEvents[0].update).toBeInstanceOf(Uint8Array);

        // Should have tried exactly MAX_SAVE_RETRIES times
        expect(mockControls.addDocCallCount).toBe(DEFAULTS.MAX_SAVE_RETRIES);

        await provider1.destroy();
    });

    it('should offload updates exceeding the inline limit to Cloud Storage', async () => {
        const path = `integration-tests/oversized-save-${getStableDate()}-${counter++}`;
        const doc1 = new Y.Doc();
        const provider1 = createProvider(doc1, path, { maxWaitTime: 50 });

        // Wait for initial sync
        await waitForConditionTruthy(() => provider1.synced, { timeout: 30000, message: 'Provider should complete initial sync' });

        const rejectedEvents: any[] = [];
        provider1.on('save-rejected', (event: any) => {
            rejectedEvents.push(event);
        });

        // Create a very large update that exceeds the inline limit (~1MB)
        // Yjs text encoding is ~1.5-2 bytes per character, so 1.2M characters should exceed 1MB
        const largeText = 'x'.repeat(1_200_000);
        doc1.getText('content').insert(0, largeText);

        // Wait for the debounced save to offload to Cloud Storage and write
        // the pointer doc (exactly one addDoc).
        await waitForConditionEquals(() => mockControls.addDocCallCount, 1, { timeout: 20000, interval: 100, message: 'Pointer doc should be written after storage offload' });

        // The update must NOT be rejected — it is offloaded to Cloud Storage
        expect(rejectedEvents.length).toBe(0);

        // Exactly one pointer doc written via addDoc
        expect(mockControls.addDocCallCount).toBe(1);

        const snap = await getDocs(collection(db, path, 'updates'));
        expect(snap.size).toBe(1);
        const pointer = snap.docs[0].data();
        expect(pointer.updateStoragePath).toContain('large_updates/');
        expect(pointer.update).toBeUndefined();

        // A fresh client must be able to sync the offloaded content
        const doc2 = new Y.Doc();
        const provider2 = createProvider(doc2, path);
        await waitForConditionEquals(
            () => doc2.getText('content').length,
            largeText.length,
            { timeout: 30000, interval: 200, message: 'Fresh client should download offloaded update' }
        );

        await provider1.destroy();
        await provider2.destroy();
    }, 50000);
});
