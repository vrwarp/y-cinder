/**
 * Subdocuments that already exist when a FireProvider attaches.
 *
 * Subdoc providers used to be started only from the Y.Doc 'subdocs' event,
 * which fires once, when a subdoc is integrated into its parent. A provider
 * constructed on a doc that already holds subdocs (local-first content,
 * y-idb hydration, or a provider destroyed and recreated on the same doc,
 * e.g. a React StrictMode remount) never saw that event, so those subdocs
 * got no provider: their content was neither pushed nor pulled, and later
 * edits to them were never saved.
 *
 * The contract checked here is the user-visible one: a second device
 * opening the same path sees the subdocument content. Lazy mode must still
 * defer pre-existing subdocs whose shouldLoad flag is unset.
 *
 * @file preexisting_subdocs.test.ts
 */

import { describe, it, expect, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy, waitForConditionEquals } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('Subdocuments present before the provider attaches', () => {
    let counter = 0;
    const providers: FireProvider[] = [];

    const track = (p: FireProvider) => { providers.push(p); return p; };

    afterEach(async () => {
        await Promise.allSettled(providers.map(p => p.destroy()));
        providers.length = 0;
    });

    /**
     * Opens `path` on a fresh "device B" and returns a getter for the text
     * of the subdocument stored under map key `key`.
     */
    async function openDeviceB(app: any, path: string, key: string): Promise<() => string> {
        const docB = new Y.Doc();
        const pB = track(new FireProvider({ firebaseApp: app, ydoc: docB, path, maxWaitTime: 50 }));
        await waitForConditionTruthy(() => pB.synced, { timeout: 30000, message: 'device B synced' });
        await waitForConditionTruthy(
            () => docB.getMap('subs').get(key),
            { timeout: 20000, interval: 100, message: 'device B received the subdoc reference' }
        );
        return () => (docB.getMap('subs').get(key) as Y.Doc).getText('t').toString();
    }

    it('syncs a subdoc that was added to the doc before the provider was constructed', async () => {
        const { app } = await setupEmulator();
        const path = `integration-tests/preexisting-subdocs-s1-${getStableDate()}-${counter++}-${Date.now()}`;

        // Local-first: the subdoc and its content exist before any provider.
        const docA = new Y.Doc();
        const sub = new Y.Doc();
        docA.getMap('subs').set('s', sub);
        sub.getText('t').insert(0, 'hi');

        const pA = track(new FireProvider({ firebaseApp: app, ydoc: docA, path, maxWaitTime: 50 }));
        await waitForConditionTruthy(() => pA.synced, { timeout: 30000, message: 'device A synced' });

        const subTextOnB = await openDeviceB(app, path, 's');

        // Content that existed before the provider attached must reach B.
        await waitForConditionEquals(subTextOnB, 'hi', {
            timeout: 20000, interval: 100,
            message: "device B sees the pre-existing subdoc content 'hi'",
        });

        // Edits made to that subdoc after the provider attached must be saved too.
        sub.getText('t').insert(2, '!');
        await waitForConditionEquals(subTextOnB, 'hi!', {
            timeout: 20000, interval: 100,
            message: "device B sees the later subdoc edit 'hi!'",
        });
    }, 120000);

    it('keeps syncing a subdoc after the provider is destroyed and recreated on the same doc', async () => {
        const { app } = await setupEmulator();
        const path = `integration-tests/preexisting-subdocs-s2-${getStableDate()}-${counter++}-${Date.now()}`;

        const docA = new Y.Doc();
        const pA1 = new FireProvider({ firebaseApp: app, ydoc: docA, path, maxWaitTime: 50 });
        await waitForConditionTruthy(() => pA1.synced, { timeout: 30000, message: 'first provider synced' });

        const sub = new Y.Doc();
        docA.getMap('subs').set('s', sub);
        sub.getText('t').insert(0, 'v1');

        // Remount: destroy (flushes pending writes) and recreate on the same doc.
        await pA1.destroy();
        const pA2 = track(new FireProvider({ firebaseApp: app, ydoc: docA, path, maxWaitTime: 50 }));
        await waitForConditionTruthy(() => pA2.synced, { timeout: 30000, message: 'recreated provider synced' });

        sub.getText('t').insert(2, '+v2');

        const subTextOnB = await openDeviceB(app, path, 's');
        await waitForConditionEquals(subTextOnB, 'v1+v2', {
            timeout: 20000, interval: 100,
            message: "device B sees the subdoc edit made after the provider was recreated ('v1+v2')",
        });
    }, 120000);

    it('lazy mode starts pre-existing subdocs that should load and defers the rest until load()', async () => {
        const { app } = await setupEmulator();
        const path = `integration-tests/preexisting-subdocs-lazy-${getStableDate()}-${counter++}-${Date.now()}`;

        const docA = new Y.Doc();
        const eager = new Y.Doc(); // locally created: shouldLoad === true
        const deferred = new Y.Doc({ shouldLoad: false });
        docA.getMap('subs').set('eager', eager);
        docA.getMap('subs').set('deferred', deferred);
        eager.getText('t').insert(0, 'now');
        deferred.getText('t').insert(0, 'later');

        const pA = track(new FireProvider({
            firebaseApp: app, ydoc: docA, path, maxWaitTime: 50, subdocLoadingMode: 'lazy',
        }));
        await waitForConditionTruthy(() => pA.synced, { timeout: 30000, message: 'device A synced' });

        // Only the subdoc that should load gets a provider; lazy stays lazy.
        expect([...(pA as any).subProviders.keys()]).toEqual([eager.guid]);

        const eagerTextOnB = await openDeviceB(app, path, 'eager');
        await waitForConditionEquals(eagerTextOnB, 'now', {
            timeout: 20000, interval: 100,
            message: "device B sees the pre-existing lazy-mode subdoc content 'now'",
        });

        // load() starts the deferred subdoc, which pushes its local content.
        deferred.load();
        const deferredTextOnB = await openDeviceB(app, path, 'deferred');
        await waitForConditionEquals(deferredTextOnB, 'later', {
            timeout: 20000, interval: 100,
            message: "device B sees the deferred subdoc content 'later' after load()",
        });
    }, 120000);
});
