/**
 * Regression test: the epoch fence must see local state that local
 * persistence loads after the provider was constructed.
 *
 * Bug: performInitialSync runs the epoch fence (server epoch newer than the
 * local doc's, AND the local doc has content) right after its main-doc
 * read. An app that constructs the provider before y-idb hydrates
 * (versicle builds a fresh Y.Doc + provider on every page load) can reach
 * that check while the doc is still empty: an old-epoch local state that
 * lands just after the read bypasses the fence. Initial sync then applies
 * the new-epoch snapshot on top of it (unrelated id spaces: the content is
 * DUPLICATED) and pushes the old-epoch structs tagged with the new epoch,
 * which poisons the new epoch for every peer.
 *
 * With `localReady` (the persistence's whenSynced), initial sync waits for
 * the load before the fence: 'epoch-changed' fires, nothing is applied and
 * nothing is written.
 *
 * Deterministic ordering: the provider's main-document getDoc is wrapped
 * and hydration is scheduled on the macrotask after that read resolves (a
 * fallback timer covers a fix that waits before issuing the read at all).
 *
 * @file epoch_fence_late_hydration.test.ts
 */

import { describe, it, expect, beforeAll, vi } from 'vitest';

const { io } = vi.hoisted(() => ({
    io: {
        /** Provider path whose main-document read is watched */
        watchPath: null as string | null,
        /** Called (synchronously) when the watched main-doc read resolves */
        onMainDocRead: null as (() => void) | null,
    },
}));

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getDoc: async (ref: any) => {
            const snap = await actual.getDoc(ref);
            if (io.watchPath !== null && ref?.path === io.watchPath) io.onMainDocRead?.();
            return snap;
        },
    };
});

import * as Y from 'yjs';
import { collection, getDocs } from 'firebase/firestore';
import { FireProvider } from '../../src/provider';
import { readDocEpoch } from '../../src/squash';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** Stand-in for the IndexeddbPersistence instance y-idb uses as origin. */
const IDB_ORIGIN = { name: 'y-idb persistence (test stand-in)' };

describe('Epoch fence vs local persistence hydrating after the main-doc read', () => {
    let app: any;
    let db: any;

    beforeAll(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
    });

    it('with localReady, old-epoch local state is fenced: epoch-changed, nothing applied or written', { timeout: 120000 }, async () => {
        const path = `tests/epoch-fence-late-hydration-${getStableDate()}-${Date.now()}`;
        const opts = { firebaseApp: app, path, maxUpdatesThreshold: 1000, maxWaitTime: 50 };
        const timers: ReturnType<typeof setTimeout>[] = [];
        let providerC: FireProvider | undefined;

        try {
            // --- 1. Epoch-0 content, in sync on client C's device, then squashed ---
            const docS = new Y.Doc();
            const providerS = new FireProvider({ ...opts, ydoc: docS });
            await waitForConditionTruthy(() => providerS.synced, { timeout: 30000, message: 'S synced' });
            docS.getArray('list').push(['a', 'b']);
            await waitForConditionTruthy(async () =>
                (await getDocs(collection(db, path, 'updates'))).size >= 1,
                { timeout: 20000, message: 'S update persisted' });
            // What C's IndexedDB holds: the epoch-0 state, fully in sync
            const persisted = Y.encodeStateAsUpdate(docS);
            const squashed = await providerS.squash();
            expect(squashed.success).toBe(true);
            expect(squashed.epoch).toBe(1);
            await providerS.destroy();

            // --- 2. C cold-starts: provider first, hydration after the main-doc read ---
            const docC = new Y.Doc();
            let resolveReady!: () => void;
            const localReady = new Promise<void>(resolve => { resolveReady = resolve; });
            let trigger = '';
            const hydrate = (why: string) => {
                if (trigger) return;
                trigger = why;
                Y.transact(docC, () => Y.applyUpdate(docC, persisted), IDB_ORIGIN, false);
                resolveReady();
            };
            io.watchPath = path;
            io.onMainDocRead = () => { timers.push(setTimeout(() => hydrate('main-doc-read'), 0)); };
            timers.push(setTimeout(() => hydrate('fallback'), 1500));

            const events: { previousEpoch: number; epoch: number }[] = [];
            providerC = new FireProvider({ ...opts, ydoc: docC, localReady });
            providerC.on('epoch-changed', (e: any) => events.push(e));

            await waitForConditionTruthy(() => providerC!.synced || events.length > 0, { timeout: 30000, message: 'C settled' });
            // Long enough for a debounced save (maxWaitTime 50 ms) to run
            await new Promise(r => setTimeout(r, 1500));
            io.watchPath = null;
            io.onMainDocRead = null;
            const pC = providerC;
            providerC = undefined;
            await pC.destroy();

            const writesByC = (await getDocs(collection(db, path, 'updates'))).docs
                .map(d => d.data())
                .filter(d => d.createdBy === pC.uid);
            console.log(`[epoch-fence-late-hydration] trigger=${trigger} events=${JSON.stringify(events.map(e => [e.previousEpoch, e.epoch]))} ` +
                `synced=${pC.synced} list=${JSON.stringify(docC.getArray('list').toJSON())} writesByC=${writesByC.length}`);

            expect(trigger, 'hydration landed after initial sync read the main document').toBe('main-doc-read');
            expect(events.map(e => [e.previousEpoch, e.epoch])).toEqual([[0, 1]]);
            expect(pC.synced).toBe(false);
            // The new-epoch snapshot was not applied on top: no duplicated content
            expect(docC.getArray('list').toJSON()).toEqual(['a', 'b']);
            expect(readDocEpoch(docC)).toBe(0);
            // Nothing old-epoch reached the new epoch (push, saves, destroy flush)
            expect(writesByC).toEqual([]);
        } finally {
            timers.forEach(clearTimeout);
            io.watchPath = null;
            io.onMainDocRead = null;
            await providerC?.destroy();
        }
    });
});
