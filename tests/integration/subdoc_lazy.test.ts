/**
 * Subdocument lazy loading (subdocLoadingMode: 'lazy') + fan-out profiling.
 *
 * Eager mode (the default) starts a FireProvider for every subdocument the
 * moment it appears: a document with N subdocs pays N initial syncs and 3N
 * Firestore listeners at startup, whether or not the app renders them.
 *
 * Lazy mode follows the Yjs convention: remote-arriving subdocs
 * (shouldLoad === false) are not synced until the app calls
 * `subdoc.load()`. Locally created subdocs (shouldLoad === true) still
 * sync immediately, so the writing client is unaffected.
 *
 * @file subdoc_lazy.test.ts
 */

import { describe, it, expect, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy, waitForConditionEquals } from '../utils/wait';
import { getStableDate } from '../unit/prng';

const SUBDOC_COUNT = 20;

describe('Subdocument lazy loading', () => {
    let counter = 0;
    const providers: FireProvider[] = [];

    const track = (p: FireProvider) => { providers.push(p); return p; };

    afterEach(async () => {
        await Promise.allSettled(providers.map(p => p.destroy()));
        providers.length = 0;
    });

    /**
     * Seeds a parent doc with N subdocs, each carrying content, via direct
     * Firestore writes. Seeding through live providers would spin up N+1
     * concurrent provider instances just for setup, which overloads the
     * emulator's gRPC stream (RESOURCE_EXHAUSTED) — the very fan-out cost
     * this feature addresses.
     */
    async function seed(db: any, path: string): Promise<void> {
        const { addDoc, collection, serverTimestamp, Bytes } = await import('@firebase/firestore');
        const put = (p: string, update: Uint8Array) =>
            addDoc(collection(db, p, 'updates'), {
                update: Bytes.fromUint8Array(update),
                createdAt: serverTimestamp(),
            });

        const parent = new Y.Doc();
        const map = parent.getMap('subdocs');
        for (let i = 0; i < SUBDOC_COUNT; i++) {
            map.set(`sub-${i}`, new Y.Doc({ guid: `subg-${i}` }));
        }
        await put(path, Y.encodeStateAsUpdate(parent));
        parent.destroy();

        for (let i = 0; i < SUBDOC_COUNT; i++) {
            const sd = new Y.Doc();
            sd.getText('t').insert(0, `content-${i}`);
            await put(`${path}/subdocs/subg-${i}`, Y.encodeStateAsUpdate(sd));
            sd.destroy();
        }
    }

    it('lazy mode defers subdoc sync until load(); eager syncs everything', async () => {
        const { app, db } = await setupEmulator();
        const path = `integration-tests/subdoc-lazy-${getStableDate()}-${counter++}`;
        await seed(db, path);

        // --- Lazy client: parent syncs, subdocs stay empty ---
        const lazyDoc = new Y.Doc();
        const tLazy0 = performance.now();
        const lazyProvider = track(new FireProvider({
            firebaseApp: app, ydoc: lazyDoc, path,
            maxWaitTime: 50,
            subdocLoadingMode: 'lazy',
        }));
        await waitForConditionTruthy(() => lazyProvider.synced, { timeout: 30000 });
        await waitForConditionEquals(() => lazyDoc.getMap('subdocs').size, SUBDOC_COUNT, { timeout: 30000 });
        const lazyParentMs = performance.now() - tLazy0;

        // Remote-arriving subdocs must not have synced content
        const sub0 = lazyDoc.getMap('subdocs').get('sub-0') as Y.Doc;
        expect(sub0).toBeDefined();
        expect(sub0.shouldLoad).toBe(false);
        // Give any (erroneous) eager sync a moment to show up
        await new Promise(r => setTimeout(r, 1500));
        expect(sub0.getText('t').toString()).toBe('');

        // load() triggers sync for exactly that subdoc
        sub0.load();
        await waitForConditionEquals(
            () => sub0.getText('t').toString(), 'content-0',
            { timeout: 30000, interval: 100, message: 'loaded subdoc content synced' }
        );

        // Unloaded siblings still untouched
        const sub1 = lazyDoc.getMap('subdocs').get('sub-1') as Y.Doc;
        expect(sub1.getText('t').toString()).toBe('');

        // --- Eager client: everything syncs automatically ---
        const eagerDoc = new Y.Doc();
        const tEager0 = performance.now();
        const eagerProvider = track(new FireProvider({
            firebaseApp: app, ydoc: eagerDoc, path,
            maxWaitTime: 50,
        }));
        await waitForConditionTruthy(() => eagerProvider.synced, { timeout: 30000 });
        await waitForConditionTruthy(() => {
            const m = eagerDoc.getMap('subdocs');
            if (m.size !== SUBDOC_COUNT) return false;
            for (let i = 0; i < SUBDOC_COUNT; i++) {
                const s = m.get(`sub-${i}`) as Y.Doc;
                if (!s || s.getText('t').toString() !== `content-${i}`) return false;
            }
            return true;
        }, { timeout: 60000, interval: 200, message: 'eager client synced all subdoc content' });
        const eagerAllMs = performance.now() - tEager0;

        console.log(
            `\nSubdoc fan-out (${SUBDOC_COUNT} subdocs): ` +
            `lazy parent-only sync ${lazyParentMs.toFixed(0)}ms, ` +
            `eager full sync ${eagerAllMs.toFixed(0)}ms\n`
        );
    }, 120000);

    it('locally created subdocs still sync immediately in lazy mode', async () => {
        const { app } = await setupEmulator();
        const path = `integration-tests/subdoc-lazy-local-${getStableDate()}-${counter++}`;

        const doc = new Y.Doc();
        const provider = track(new FireProvider({
            firebaseApp: app, ydoc: doc, path,
            maxWaitTime: 50,
            subdocLoadingMode: 'lazy',
        }));
        await waitForConditionTruthy(() => provider.synced, { timeout: 30000 });

        // Locally constructed Y.Doc has shouldLoad === true
        const sub = new Y.Doc();
        doc.getMap('subdocs').set('local-sub', sub);
        expect(sub.shouldLoad).toBe(true);
        sub.getText('t').insert(0, 'local-content');

        // Its provider must start and persist without any load() call
        await waitForConditionTruthy(async () => {
            const { getDocs, collection } = await import('@firebase/firestore');
            const snap = await getDocs(collection((provider as any).db, `${path}/subdocs/${sub.guid}`, 'updates'));
            return !snap.empty;
        }, { timeout: 30000, interval: 250, message: 'local subdoc persisted in lazy mode' });
    }, 60000);
});
