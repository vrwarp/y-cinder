/**
 * Regression test: compact() and squash() settle while offline, even when
 * this client has not measured its clock skew yet.
 *
 * Bug: the clock-skew probe moved from startup to the first lock need
 * (sharedClockOffset, once per Firestore instance). _executeCompaction and
 * squash() await that measurement before anything else, and the probe
 * starts with a setDoc that resolves only on the server ack, which never
 * arrives while the client is offline. Nothing rejects or times out, so a
 * client that goes offline before its first lock of the session gets a
 * compact() / squash() promise that stays pending (isCompacting stays true)
 * until connectivity returns, and then the queued compaction and squash
 * run, possibly long after the caller gave up. A squash the user started
 * offline commits on reconnect and every other device gets
 * 'epoch-changed'. Before the probe moved, the offset was measured at
 * startup and both calls settled offline at the lock transaction.
 *
 * Contract asserted here (each case on its own Firestore instance, so the
 * clock offset is unmeasured when the client goes offline):
 *  1. While offline, compact() settles (resolves or rejects) within
 *     SETTLE_DEADLINE_MS, and no compaction is left running afterwards.
 *     Giving up offline does not stick: once back online, the next
 *     compact() takes the lock and compacts.
 *  2. While offline, squash() settles within SETTLE_DEADLINE_MS. A squash
 *     that reported no success must not commit later, after reconnect.
 *
 * Offline is simulated with the SDK's own disableNetwork(db) (see
 * destroy_offline.test.ts / offline_launch.test.ts).
 *
 * @file offline_compact_squash_settle.test.ts
 */

import { describe, it, expect, beforeAll, afterEach } from 'vitest';
import * as Y from 'yjs';
import { initializeApp, deleteApp, type FirebaseApp } from 'firebase/app';
import {
    getFirestore,
    connectFirestoreEmulator,
    disableNetwork,
    enableNetwork,
    waitForPendingWrites,
    collection,
    getDocs,
    type Firestore,
} from 'firebase/firestore';
import { getStorage, connectStorageEmulator } from 'firebase/storage';
import { FireProvider } from '../../src/provider';
import type { SquashResult } from '../../src/squash';
import { FIRESTORE_PATHS } from '../../src/types';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/**
 * Generous upper bound for compact() / squash() while offline. A correct
 * implementation fails fast (lock transaction unavailable, skippedReason,
 * error) or caps the wait for the server. On the buggy code neither call
 * settles while offline, so the outcome does not depend on this value,
 * only how long the failure takes to report.
 */
const SETTLE_DEADLINE_MS = 20_000;

/** How long to watch for a late squash commit after reconnecting. */
const LATE_SQUASH_WINDOW_MS = 5_000;

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

type Outcome<T> = { state: 'settled'; value?: T; error?: unknown } | { state: 'still-pending' };

/** Races a promise against the deadline without leaking a timer. */
async function settlesWithin<T>(p: Promise<T>, ms: number): Promise<Outcome<T>> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<Outcome<T>>(resolve => {
        timer = setTimeout(() => resolve({ state: 'still-pending' }), ms);
    });
    try {
        return await Promise.race([
            p.then(
                (value): Outcome<T> => ({ state: 'settled', value }),
                (error): Outcome<T> => ({ state: 'settled', error }),
            ),
            deadline,
        ]);
    } finally {
        clearTimeout(timer);
    }
}

interface Client {
    app: FirebaseApp;
    db: Firestore;
    offline: boolean;
    providers: FireProvider[];
}

describe('compact() and squash() while offline, before the first lock of the session', () => {
    let counter = 0;
    const clients: Client[] = [];

    beforeAll(async () => {
        // Log level and the default app's emulator connection.
        await setupEmulator();
    });

    afterEach(async () => {
        while (clients.length) {
            const c = clients.pop()!;
            // Always restore connectivity before tearing down (toggle once
            // each way: a redundant enableNetwork() trips an SDK assertion).
            if (c.offline) {
                c.offline = false;
                try { await enableNetwork(c.db); } catch { /* best effort */ }
            }
            for (const p of c.providers) {
                try { await p.destroy(); } catch { /* best effort */ }
            }
            try { await deleteApp(c.app); } catch { /* best effort */ }
        }
    });

    /**
     * A fresh Firebase app (own Firestore instance): nothing on it has
     * measured clock skew, as for an app launch.
     */
    function newClient(): Client {
        const app = initializeApp({
            projectId: 'demo-test-project',
            apiKey: 'fake-api-key',
            storageBucket: 'demo-test-project.appspot.com',
        }, `offline-compact-squash-${getStableDate()}-${counter++}-${Date.now()}`);
        const db = getFirestore(app);
        connectFirestoreEmulator(db, '127.0.0.1', 8080);
        connectStorageEmulator(getStorage(app), '127.0.0.1', 9199);
        const client: Client = { app, db, offline: false, providers: [] };
        clients.push(client);
        return client;
    }

    const goOffline = async (c: Client) => { await disableNetwork(c.db); c.offline = true; };
    const goOnline = async (c: Client) => { if (c.offline) { c.offline = false; await enableNetwork(c.db); } };

    /** A synced provider whose one edit is committed, with no compaction or lock yet. */
    async function syncedProviderWithSavedEdit(c: Client, tag: string): Promise<{ provider: FireProvider; ydoc: Y.Doc; path: string }> {
        const ydoc = new Y.Doc();
        const path = `integration-tests/offline-compact-squash-${tag}-${getStableDate()}-${counter++}`;
        const provider = new FireProvider({
            firebaseApp: c.app,
            ydoc,
            path,
            maxWaitTime: 20,
            // no automatic compaction: the call under test is the first lock need
            maxUpdatesThreshold: 1000,
        });
        c.providers.push(provider);
        await waitForConditionTruthy(() => provider.synced, { timeout: 15_000, message: 'provider synced while online' });

        const saved = new Promise<void>(resolve => provider.on('saved', () => resolve()));
        ydoc.getText('t').insert(0, 'committed before going offline');
        await saved;
        expect(provider.isCompacting, 'no compaction before the test starts').toBe(false);
        return { provider, ydoc, path };
    }

    it('compact() settles while offline and leaves no compaction running', async () => {
        const c = newClient();
        const { provider, path } = await syncedProviderWithSavedEdit(c, 'compact');

        await goOffline(c);
        const t0 = Date.now();
        const outcome = await settlesWithin(provider.compact(), SETTLE_DEADLINE_MS);

        expect(outcome.state, `compact() must settle within ${SETTLE_DEADLINE_MS}ms while offline`)
            .toBe('settled');
        console.log(`[offline] compact() settled after ${Date.now() - t0}ms`);
        expect(provider.isCompacting, 'no compaction still in flight once compact() settled').toBe(false);

        // Giving up offline must not stick: once the queued writes reach
        // the server, the next compact() takes the lock and compacts the
        // committed edit.
        await goOnline(c);
        await waitForPendingWrites(c.db);
        await provider.compact();
        const updates = await getDocs(collection(c.db, path, FIRESTORE_PATHS.UPDATES));
        expect(updates.size, 'compact() after reconnect compacts the committed edit').toBe(0);
    }, 90_000);

    it('squash() settles while offline and a failed squash does not commit after reconnect', async () => {
        const c = newClient();
        const { provider } = await syncedProviderWithSavedEdit(c, 'squash');
        const squashedEvents: unknown[] = [];
        provider.on('squashed', (e: unknown) => squashedEvents.push(e));
        const epochBefore = provider.epoch;

        await goOffline(c);
        const t0 = Date.now();
        const outcome = await settlesWithin<SquashResult>(provider.squash(), SETTLE_DEADLINE_MS);

        expect(outcome.state, `squash() must settle within ${SETTLE_DEADLINE_MS}ms while offline`)
            .toBe('settled');
        const result = outcome.state === 'settled' ? outcome.value : undefined;
        console.log(`[offline] squash() settled after ${Date.now() - t0}ms:`,
            outcome.state === 'settled' && outcome.error !== undefined ? outcome.error : result);

        if (!result?.success) {
            // The caller was told the squash did not happen: it must not
            // happen later. Reconnect, let any write the SDK still queued
            // reach the server, and watch for a late commit.
            expect(squashedEvents, 'no squash committed while offline').toEqual([]);
            await goOnline(c);
            await waitForPendingWrites(c.db);
            await sleep(LATE_SQUASH_WINDOW_MS);
            expect(squashedEvents, 'a squash reported as not done must not commit after reconnect')
                .toEqual([]);
            expect(provider.epoch, 'epoch unchanged after a squash reported as not done').toBe(epochBefore);
        }
    }, 90_000);
});
