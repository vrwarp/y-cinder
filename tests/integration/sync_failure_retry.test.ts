/**
 * Regression test: failed initial sync must retry with backoff.
 *
 * Bug: performInitialSync() reports failures via { success: false } instead
 * of throwing, but provider.sync() never checked result.success — a failed
 * sync was treated as success: no retry, no sync-failure event, and local
 * changes were never pushed.
 *
 * Also covers the 'sync' event and synced property.
 *
 * @file sync_failure_retry.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { mockControls } = vi.hoisted(() => ({
    mockControls: {
        failGetDocs: false,
        getDocsCalls: 0,
    }
}));

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getDocs: async (q: any) => {
            mockControls.getDocsCalls++;
            if (mockControls.failGetDocs) {
                const err: any = new Error('Simulated network failure');
                err.code = 'unavailable';
                throw err;
            }
            return actual.getDocs(q);
        },
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { getDocs as realGetDocs, collection, query } from '@firebase/firestore';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('Initial sync failure handling', () => {
    let app: any;
    let db: any;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        mockControls.failGetDocs = false;
        mockControls.getDocsCalls = 0;
    });

    it('retries a failed initial sync and pushes local data once it recovers', async () => {
        const path = `integration-tests/sync-retry-${getStableDate()}-${counter++}`;

        // Local changes exist BEFORE the provider connects
        const doc = new Y.Doc();
        doc.getText('t').insert(0, 'LOCAL-ONLY DATA');

        // Fail the first sync attempt, then recover
        mockControls.failGetDocs = true;

        const provider = new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path,
            maxWaitTime: 50,
        });

        expect(provider.synced).toBe(false);

        // Let the first attempt fail, then restore the network
        await new Promise(r => setTimeout(r, 500));
        const callsWhileFailing = mockControls.getDocsCalls;
        expect(callsWhileFailing).toBeGreaterThan(0);
        mockControls.failGetDocs = false;

        // The retry (exponential backoff starts at ~300ms) must complete the
        // sync and push the pre-existing local data
        await waitForConditionTruthy(
            () => provider.synced,
            { timeout: 15000, interval: 100, message: 'Provider should sync after retry' }
        );

        await waitForConditionTruthy(
            async () => {
                const snap = await realGetDocs(query(collection(db, path, 'updates')));
                return snap.size > 0;
            },
            { timeout: 15000, interval: 200, message: 'Local data should be pushed after retry' }
        );

        await provider.destroy();
    }, 30000);

    it('emits sync-failure after MAX_RETRIES consecutive failures', async () => {
        const path = `integration-tests/sync-fail-${getStableDate()}-${counter++}`;

        mockControls.failGetDocs = true;

        const doc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path,
            maxWaitTime: 50,
        });

        let syncFailure: Error | null = null;
        provider.on('sync-failure', (err: Error) => {
            syncFailure = err;
        });

        // 5 attempts with backoff (~300+500+900+1700+3300ms) ≈ 7s
        await waitForConditionTruthy(
            () => syncFailure !== null,
            { timeout: 20000, interval: 200, message: 'sync-failure should be emitted after MAX_RETRIES' }
        );

        expect(syncFailure).toBeInstanceOf(Error);
        expect(provider.synced).toBe(false);

        mockControls.failGetDocs = false;
        await provider.destroy();
    }, 30000);

    it('emits sync event on successful initial sync', async () => {
        const path = `integration-tests/sync-ok-${getStableDate()}-${counter++}`;

        const doc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path,
            maxWaitTime: 50,
        });

        let syncedEvent = false;
        provider.on('sync', (state: boolean) => {
            syncedEvent = state;
        });

        await waitForConditionTruthy(
            () => syncedEvent && provider.synced,
            { timeout: 10000, interval: 100, message: 'sync event should fire' }
        );

        await provider.destroy();
    }, 20000);
});
