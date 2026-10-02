/**
 * Regression test: an app launched offline syncs once the network returns.
 *
 * Initial sync used to start by measuring clock skew, and the probe's
 * write resolves only when the server acknowledges it. That made an
 * offline launch wait for connectivity before reading anything. With the
 * measurement moved to the first lock need, nothing gated the reads: the
 * SDK answers getDocs from its cache once it considers the client offline,
 * getDoc of an uncached main doc then rejects 'unavailable', and the
 * provider spent its MAX_RETRIES budget within seconds, emitted
 * 'sync-failure' and never synced, even after reconnecting. A provider
 * whose offset was already known (a subdoc inheriting its parent's) never
 * had that gate in the first place.
 *
 * Contract asserted here, both before and after the clock offset is known:
 *  1. While offline, the provider neither emits 'sync-failure' nor reports
 *     synced (initial sync must not complete on cache-served reads).
 *  2. Once the network returns, it syncs with the server content.
 *
 * Offline is simulated with the SDK's own disableNetwork(db).
 *
 * @file offline_launch.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';
import { disableNetwork, enableNetwork } from '@firebase/firestore';

/**
 * How long the app stays offline: longer than the MAX_RETRIES backoff
 * sequence (about 3.2 s), after which the old code had given up for good.
 */
const OFFLINE_MS = 6000;

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

describe('launch while offline', () => {
    let app: any;
    let db: any;
    let counter = 0;
    // Toggle the network exactly once each way: a redundant enableNetwork()
    // on an already-online instance trips an internal SDK assertion (see
    // destroy_offline.test.ts).
    let offline = false;
    const goOffline = async () => { await disableNetwork(db); offline = true; };
    const goOnline = async () => { if (offline) { offline = false; await enableNetwork(db); } };
    const cleanup: Array<() => unknown> = [];

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        await clearFirestore(db);
    });

    afterEach(async () => {
        // Always restore connectivity so a failure here cannot leave the
        // shared Firestore instance offline for later tests.
        await goOnline();
        while (cleanup.length) {
            try { await cleanup.pop()!(); } catch { /* best effort */ }
        }
    });

    const createProvider = (doc: Y.Doc, path: string, config: Record<string, unknown> = {}) => {
        const provider = new FireProvider({ firebaseApp: app, ydoc: doc, path, maxWaitTime: 20, ...config });
        cleanup.push(() => provider.destroy());
        return provider;
    };

    // The first case runs before anything on this Firestore instance has
    // measured clock skew; the second starts with the offset known.
    for (const [label, config] of [
        ['clock offset not yet measured', {}],
        ['clock offset already known', { cachedClockOffset: 0 }],
    ] as const) {
        it(`waits for the network instead of failing (${label})`, async () => {
            const path = `integration-tests/offline-launch-${getStableDate()}-${counter++}`;

            // Server content written by another device.
            const seedDoc = new Y.Doc();
            const seed = createProvider(seedDoc, path, { cachedClockOffset: 0 });
            await waitForConditionTruthy(() => seed.synced, { timeout: 15000, message: 'seed provider synced' });
            const saved = new Promise<void>(resolve => seed.on('saved', () => resolve()));
            seedDoc.getText('t').insert(0, 'server content');
            await saved;
            await seed.destroy();

            await goOffline();
            const ydoc = new Y.Doc();
            const provider = createProvider(ydoc, path, config);
            const failures: unknown[] = [];
            provider.on('sync-failure', (err: unknown) => failures.push(err));

            await sleep(OFFLINE_MS);
            expect(failures, 'no sync-failure while offline').toEqual([]);
            expect(provider.synced, 'not synced against the local cache').toBe(false);

            await goOnline();
            await waitForConditionTruthy(() => provider.synced, {
                timeout: 20000,
                message: 'provider launched offline syncs after reconnect',
            });
            expect(failures).toEqual([]);
            expect(ydoc.getText('t').toString()).toBe('server content');
        }, 60000);
    }
});
