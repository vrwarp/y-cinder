/**
 * Regression test: a Firestore listener error must not leave the provider
 * deaf while it keeps claiming to be synced.
 *
 * Firestore terminates an onSnapshot listener permanently once its error
 * callback fires (permission-denied while auth is briefly invalid,
 * resource-exhausted, failed-precondition, ...). The provider only emitted
 * 'connection-error': it kept the dead listeners, never re-synced or
 * re-attached them, and left `synced === true`. Saves kept succeeding, so
 * the peer kept uploading its own edits while silently receiving nothing.
 *
 * Contract (README: `synced` is "true once initial sync has completed and
 * real-time listeners are active"): after a listener error the provider
 * must either recover (re-sync / re-listen, so remote edits still arrive)
 * or stop reporting `synced === true`.
 *
 * The error is injected at the SDK boundary: onSnapshot is wrapped so the
 * test can terminate a listener exactly the way the SDK does — the
 * underlying listener is removed and its error callback is invoked once.
 * Listeners registered later (e.g. by a recovering provider) are real,
 * untouched emulator listeners.
 *
 * compact() pauses the history listener and re-creates it afterwards with
 * its own error wiring, so that listener is covered separately.
 *
 * @file listener_error_resubscribe.test.ts
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

type ListenerEntry = {
    fail: (error: unknown) => void;
};

const { registry } = vi.hoisted(() => ({
    registry: { listeners: [] as ListenerEntry[] },
}));

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        onSnapshot: (ref: any, ...args: any[]) => {
            // Supports onSnapshot(ref, [options], onNext, onError) and the
            // observer-object form.
            const fns = args.filter(a => typeof a === 'function');
            const observer = args.find(a => a && typeof a === 'object' && (typeof a.next === 'function' || typeof a.error === 'function'));
            const onError: ((e: unknown) => void) | undefined = fns.length > 0 ? fns[1] : observer?.error?.bind(observer);

            const realUnsubscribe = actual.onSnapshot(ref, ...args);
            let unsubscribed = false;
            const unsubscribe = () => {
                if (unsubscribed) return;
                unsubscribed = true;
                realUnsubscribe();
            };

            registry.listeners.push({
                // SDK semantics: after the error callback the listener is
                // gone and receives no further events.
                fail: (error: unknown) => {
                    if (unsubscribed) return;
                    unsubscribe();
                    onError?.(error);
                },
            });

            return unsubscribe;
        },
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { FirestoreError, getDocs as realGetDocs, collection } from '@firebase/firestore';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitFor, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

// The SDK's typings hide FirestoreError's constructor; build the same
// error instance the SDK delivers.
const permissionDenied = () => {
    const FirestoreErrorCtor = FirestoreError as unknown as new (code: string, message: string) => FirestoreError;
    return new FirestoreErrorCtor('permission-denied', 'Missing or insufficient permissions.');
};

describe('Listener error recovery', () => {
    let app: any;
    let db: any;
    let counter = 0;
    const providers: FireProvider[] = [];

    const createProvider = (ydoc: Y.Doc, path: string) => {
        const provider = new FireProvider({ firebaseApp: app, ydoc, path, maxWaitTime: 50 });
        providers.push(provider);
        return provider;
    };

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        await clearFirestore(db);
        registry.listeners.length = 0;
    });

    afterEach(async () => {
        await Promise.all(providers.splice(0).map(p => p.destroy()));
    });

    it('does not keep reporting synced=true while its listeners are dead after a permission-denied error', { timeout: 90_000 }, async () => {
        const path = `integration-tests/listener-error-${getStableDate()}-${counter++}`;

        // Peer A syncs and attaches its real-time listeners.
        const docA = new Y.Doc();
        const providerA = createProvider(docA, path);
        await waitForConditionTruthy(() => providerA.synced, {
            timeout: 30_000, interval: 50, message: 'provider A should finish initial sync',
        });

        const connectionErrors: unknown[] = [];
        providerA.on('connection-error', (e: unknown) => connectionErrors.push(e));

        // Auth is briefly invalid: every one of A's listeners is terminated
        // with permission-denied (only A exists so far, so these are all A's).
        const aListeners = registry.listeners.slice();
        expect(aListeners.length).toBeGreaterThan(0);
        const error = permissionDenied();
        for (const listener of aListeners) listener.fail(error);

        // Sanity: the injected error reached the provider.
        expect(connectionErrors.length).toBeGreaterThan(0);

        // Another client edits the document.
        const docB = new Y.Doc();
        const providerB = createProvider(docB, path);
        await waitForConditionTruthy(() => providerB.synced, {
            timeout: 30_000, interval: 50, message: 'provider B should finish initial sync',
        });
        docB.getText('content').insert(0, 'edit from B');

        // B's edit is durably on the server.
        await waitFor(
            async () => (await realGetDocs(collection(db, path, 'updates'))).size,
            size => size > 0,
            { timeout: 30_000, interval: 100, message: "B's update should reach Firestore" },
        );

        // Give a recovering provider ample time to re-sync / re-listen with
        // backoff (the sync retry backoff tops out at a few seconds).
        let converged = false;
        try {
            await waitFor(() => docA.getText('content').toString(), v => v === 'edit from B', {
                timeout: 20_000, interval: 100,
            });
            converged = true;
        } catch {
            converged = false;
        }

        // Either A recovered and received the remote edit, or it must stop
        // claiming that its real-time listeners are active.
        if (!converged) {
            expect(
                providerA.synced,
                'provider A never received the remote edit (its listeners died) yet still reports synced=true',
            ).toBe(false);
        }
    });

    it('re-syncs when the history listener re-created after compaction dies', { timeout: 90_000 }, async () => {
        const path = `integration-tests/listener-error-${getStableDate()}-${counter++}`;

        const docA = new Y.Doc();
        const providerA = createProvider(docA, path);
        await waitForConditionTruthy(() => providerA.synced, {
            timeout: 30_000, interval: 50, message: 'provider A should finish initial sync',
        });

        // Give compaction something to fold.
        docA.getText('content').insert(0, 'from A');
        await waitFor(
            async () => (await realGetDocs(collection(db, path, 'updates'))).size,
            size => size > 0,
            { timeout: 30_000, interval: 100, message: "A's update should reach Firestore" },
        );

        // compact() pauses the history listener and re-creates it when done.
        const before = registry.listeners.length;
        await providerA.compact();
        const resumed = registry.listeners.slice(before);
        expect(resumed.length).toBe(1);

        const connectionErrors: unknown[] = [];
        providerA.on('connection-error', (e: unknown) => connectionErrors.push(e));
        let syncEvents = 0;
        providerA.on('sync', () => syncEvents++);

        resumed[0].fail(permissionDenied());
        expect(connectionErrors.length).toBeGreaterThan(0);

        // One of its listeners is dead: it must stop claiming they are active...
        expect(
            providerA.synced,
            'provider A still reports synced=true after its resumed history listener died',
        ).toBe(false);

        // ...and recover by re-syncing, which attaches fresh listeners.
        await waitForConditionTruthy(() => syncEvents > 0 && providerA.synced, {
            timeout: 20_000, interval: 50, message: 'provider A should re-sync after the listener error',
        });

        // The fresh listeners deliver remote edits.
        const docB = new Y.Doc();
        const providerB = createProvider(docB, path);
        await waitForConditionTruthy(() => providerB.synced, {
            timeout: 30_000, interval: 50, message: 'provider B should finish initial sync',
        });
        docB.getText('content').insert(0, 'B ');
        await waitFor(() => docA.getText('content').toString(), v => v === 'B from A', {
            timeout: 20_000, interval: 100, message: 'provider A should receive the remote edit after recovering',
        });
    });
});
