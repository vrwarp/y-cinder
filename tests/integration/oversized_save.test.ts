/**
 * Oversized Save Integration Tests
 *
 * Tests the provider's circuit breaker for oversized documents:
 * - Proactive size rejection before Firestore write
 * - Server-side size rejection detection
 * - Generic save failure retry cap (MAX_SAVE_RETRIES)
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

import { FireProvider } from '../../src/provider';
import { DEFAULTS } from '../../src/types';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
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
        await new Promise(r => setTimeout(r, 1000));

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

        // Wait for debounce + save attempt
        await new Promise(r => setTimeout(r, 500));

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
        await new Promise(r => setTimeout(r, 1000));

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

        // Wait long enough for MAX_SAVE_RETRIES attempts
        // Each attempt: ~30ms debounce + save attempt overhead
        await new Promise(r => setTimeout(r, DEFAULTS.MAX_SAVE_RETRIES * 200 + 1000));

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

    it('should emit save-rejected proactively when update exceeds FIRESTORE_DOC_LIMIT', async () => {
        const path = `integration-tests/oversized-save-${getStableDate()}-${counter++}`;
        const doc1 = new Y.Doc();
        const provider1 = createProvider(doc1, path, { maxWaitTime: 50 });

        // Wait for initial sync
        await new Promise(r => setTimeout(r, 1000));

        const rejectedEvents: any[] = [];
        provider1.on('save-rejected', (event: any) => {
            rejectedEvents.push(event);
        });

        // Create a very large update that exceeds FIRESTORE_DOC_LIMIT (1MB)
        // Yjs text encoding is ~1.5-2 bytes per character, so 1.2M characters should exceed 1MB
        const largeText = 'x'.repeat(1_200_000);
        doc1.getText('content').insert(0, largeText);

        // Wait for debounce + save
        await new Promise(r => setTimeout(r, 500));

        // Should have been rejected proactively (no Firestore call needed)
        expect(rejectedEvents.length).toBe(1);
        expect(rejectedEvents[0].code).toBe('document-too-large');
        expect(rejectedEvents[0].sizeBytes).toBeGreaterThan(DEFAULTS.FIRESTORE_DOC_LIMIT);
        expect(rejectedEvents[0].limitBytes).toBe(DEFAULTS.FIRESTORE_DOC_LIMIT);
        expect(rejectedEvents[0].update).toBeInstanceOf(Uint8Array);

        // addDoc should NOT have been called (proactive check)
        expect(mockControls.addDocCallCount).toBe(0);

        await provider1.destroy();
    });
});
