/**
 * Regression test: destroy() must settle in bounded time while offline.
 *
 * Bug: destroy() awaited the in-flight save and then the final flush, and
 * both await the Firestore write's server acknowledgement. The Firestore
 * web SDK resolves a write only when the backend acks it — never while the
 * client is offline (the write just sits in the SDK's local queue). So with
 * any buffered or in-flight edit, destroy() stayed pending until the network
 * came back (possibly forever), observer cleanup (super.destroy()) waited
 * with it, and a parent provider's destroy() hung too because it first
 * awaits its subdocument providers' destroy().
 *
 * Contract asserted here:
 *  1. With the network disabled, destroy() settles within DESTROY_DEADLINE_MS
 *     (for a buffered edit, for an in-flight save, and for a parent whose
 *     subdocument has a buffered edit).
 *  2. Settling offline must not drop data: by the time destroy() settles,
 *     the edits are in the SDK's offline queue (not just in a promise chain
 *     still waiting on an earlier write), and once connectivity returns they
 *     are readable by a fresh provider.
 *
 * Online, destroy() keeps waiting for the server to commit its final writes
 * (repro_destroy_flush, destroy_inflight_save, repro_mixed_sync rely on
 * that); only the offline wait is bounded.
 *
 * Offline is simulated with the SDK's own disableNetwork(db), which is
 * exactly the "no connectivity" state the SDK enters in the browser.
 *
 * @file destroy_offline.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';
import {
    collection,
    disableNetwork,
    enableNetwork,
    getDocsFromCache,
    onSnapshot,
} from '@firebase/firestore';

/**
 * Generous upper bound for destroy() while offline. A correct destroy()
 * hands the final write to the SDK's local queue (or caps the flush with a
 * timeout well below this) instead of waiting for a server ack that cannot
 * arrive. On the buggy code destroy() never settles while offline, so the
 * outcome does not depend on this value — only how long the failure takes.
 */
const DESTROY_DEADLINE_MS = 10_000;

type Outcome = 'settled' | 'still-pending';

/** Races a destroy() promise against the deadline without leaking a timer. */
async function settlesWithinDeadline(destroyed: Promise<unknown>): Promise<Outcome> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<Outcome>(resolve => {
        timer = setTimeout(() => resolve('still-pending'), DESTROY_DEADLINE_MS);
    });
    try {
        return await Promise.race([destroyed.then((): Outcome => 'settled'), deadline]);
    } finally {
        clearTimeout(timer);
    }
}

describe('destroy() while offline', () => {
    let app: any;
    let db: any;
    let counter = 0;
    // Toggle the network exactly once each way: a redundant enableNetwork()
    // on an already-online instance trips an internal SDK assertion
    // ("Unexpected state (ID: ca9) pendingResponses:-1") on the next watch
    // response, which poisons the shared Firestore instance for later tests.
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
        const provider = new FireProvider({ firebaseApp: app, ydoc: doc, path, ...config });
        cleanup.push(() => provider.destroy());
        return provider;
    };

    /** Rebuilds the text from the SDK's local view of the updates collection. */
    const readLocalQueue = async (path: string) => {
        const snap = await getDocsFromCache(collection(db, path, 'updates'));
        const local = new Y.Doc();
        snap.forEach(d => {
            const u = d.data().update;
            if (u) Y.applyUpdate(local, u.toUint8Array());
        });
        return local.getText('t').toString();
    };

    const waitForSynced = (provider: FireProvider) =>
        waitForConditionTruthy(() => provider.synced, {
            timeout: 15_000,
            message: 'provider should complete initial sync while online',
        });

    /**
     * Brings the network back, lets the (now unblocked) destroy() finish,
     * and asserts the offline edit is readable by a brand-new provider.
     */
    const expectDurableAfterReconnect = async (
        destroyed: Promise<unknown>,
        path: string,
        read: (doc: Y.Doc) => string | undefined,
        expected: string,
    ) => {
        await goOnline();
        await destroyed;

        const fresh = new Y.Doc();
        createProvider(fresh, path);
        await waitForConditionEquals(() => read(fresh), expected, {
            timeout: 20_000,
            interval: 100,
            message: 'edit made before an offline destroy() must reach the server after reconnect',
        });
    };

    it('settles when an edit is still buffered in the debounce window', async () => {
        const path = `integration-tests/destroy-offline-buffered-${getStableDate()}-${counter++}`;
        const doc = new Y.Doc();
        // Debounce far longer than the test so the edit is only ever written
        // by destroy()'s final flush.
        const provider = createProvider(doc, path, { maxWaitTime: 120_000 });
        await waitForSynced(provider);

        await goOffline();
        doc.getText('t').insert(0, 'OFFLINE-EDIT');

        const destroyed = provider.destroy();
        const outcome = await settlesWithinDeadline(destroyed);

        expect(outcome, `destroy() must settle within ${DESTROY_DEADLINE_MS}ms while offline`)
            .toBe('settled');

        await expectDurableAfterReconnect(destroyed, path, d => d.getText('t').toString(), 'OFFLINE-EDIT');
    }, 60_000);

    it('settles when a save is in flight and another edit is buffered behind it', async () => {
        const path = `integration-tests/destroy-offline-inflight-${getStableDate()}-${counter++}`;
        const doc = new Y.Doc();
        const provider = createProvider(doc, path, { maxWaitTime: 50 });
        await waitForSynced(provider);

        // Observe the SDK's local view of the updates collection so we know,
        // without timing guesses, when the first save has been handed to
        // Firestore and is waiting for a server ack that cannot arrive.
        let saveHandedToSdk = false;
        const unsubscribe = onSnapshot(
            collection(db, path, 'updates'),
            { includeMetadataChanges: true },
            snap => {
                if (snap.docs.some(d => d.metadata.hasPendingWrites)) saveHandedToSdk = true;
            },
        );
        cleanup.push(unsubscribe);

        await goOffline();
        doc.getText('t').insert(0, 'FIRST');
        await waitForConditionTruthy(() => saveHandedToSdk, {
            timeout: 10_000,
            message: 'debounced save should start (pending local write) while offline',
        });

        // This edit is buffered behind the in-flight save.
        doc.getText('t').insert(5, '-SECOND');

        const destroyed = provider.destroy();
        const outcome = await settlesWithinDeadline(destroyed);

        expect(outcome, `destroy() must settle within ${DESTROY_DEADLINE_MS}ms while offline`)
            .toBe('settled');
        // The buffered edit must not wait behind the unacknowledged write:
        // both are queued in the SDK before reconnecting.
        expect(await readLocalQueue(path)).toBe('FIRST-SECOND');

        await expectDurableAfterReconnect(destroyed, path, d => d.getText('t').toString(), 'FIRST-SECOND');
    }, 60_000);

    it('settles for a parent provider whose subdocument has an unsaved edit', async () => {
        const path = `integration-tests/destroy-offline-subdoc-${getStableDate()}-${counter++}`;
        const parent = new Y.Doc();
        const provider = createProvider(parent, path, { maxWaitTime: 50 });
        await waitForSynced(provider);

        // Attach the subdocument while online and let the parent persist the
        // reference, so the parent itself has nothing left to flush: any hang
        // below comes from waiting on the subdocument provider's teardown.
        const parentSaved = new Promise<void>(resolve => provider.once('saved', () => resolve()));
        const child = new Y.Doc();
        parent.getMap('subdocs').set('child', child);
        await parentSaved;

        await goOffline();
        child.getText('t').insert(0, 'CHILD-OFFLINE-EDIT');

        const destroyed = provider.destroy();
        const outcome = await settlesWithinDeadline(destroyed);

        expect(outcome, `parent destroy() must settle within ${DESTROY_DEADLINE_MS}ms while offline`)
            .toBe('settled');

        await expectDurableAfterReconnect(
            destroyed, `${path}/subdocs/${child.guid}`, d => d.getText('t').toString(), 'CHILD-OFFLINE-EDIT');
    }, 60_000);
});
