/**
 * The provider's bounded wait for local persistence
 * (FireProviderConfig.localReady), as handed to performInitialSync.
 *
 * Initial sync awaits SyncContext.localReady before it compares the local
 * doc with the server (see hydration_race_snapshot_download.test.ts and
 * epoch_fence_late_hydration.test.ts for the end-to-end effect). That wait
 * must never stall sync: y-idb's whenSynced never rejects, and never
 * settles if the persistence is destroyed first. Pinned here: it resolves
 * on the app's promise, on its rejection, after LOCAL_READY_TIMEOUT_MS,
 * and on destroy(); retried syncs reuse the one bounded wait; without the
 * option nothing is waited for.
 *
 * performInitialSync and the listeners are stubbed at the module boundary
 * and time is controlled with fake timers.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as Y from 'yjs';

vi.mock('@firebase/firestore', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/firestore')>();
    return {
        ...actual,
        getFirestore: vi.fn(() => ({ __fake: 'firestore' })),
        initializeFirestore: vi.fn(() => ({ __fake: 'firestore' })),
    };
});

vi.mock('@firebase/storage', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/storage')>();
    return {
        ...actual,
        getStorage: vi.fn(() => ({ __fake: 'storage' })),
    };
});

vi.mock('../../src/sync', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../src/sync')>();
    return {
        ...actual,
        performInitialSync: vi.fn(),
        createUpdateListener: vi.fn(() => () => {}),
        createSnapshotListener: vi.fn(() => () => {}),
        createHistoryListener: vi.fn(() => () => {}),
    };
});

import { FireProvider } from '../../src/provider';
import { performInitialSync, SyncContext, SyncResult } from '../../src/sync';
import { DEFAULTS } from '../../src/types';

const performInitialSyncMock = vi.mocked(performInitialSync);

const synced: SyncResult = {
    success: true,
    updatesApplied: 0,
    localUpdatesPushed: false,
    lastSyncedDoc: null,
    syncedUpdateCount: 0,
    lastHistoryDoc: null,
    snapshotVersion: null,
    epoch: 0,
};

/** Tracks whether a promise has settled, without awaiting it. */
function track(p: Promise<unknown> | undefined): { settled: boolean } {
    const state = { settled: false };
    p?.then(() => { state.settled = true; }, () => { state.settled = true; });
    return state;
}

describe('FireProvider localReady: bounded wait handed to initial sync', () => {
    const created: FireProvider[] = [];
    const contexts: SyncContext[] = [];

    beforeEach(() => {
        vi.useFakeTimers();
        contexts.length = 0;
        performInitialSyncMock.mockReset();
        performInitialSyncMock.mockImplementation(async (ctx) => {
            contexts.push(ctx);
            return synced;
        });
        vi.spyOn(console, 'log').mockImplementation(() => {});
        vi.spyOn(console, 'debug').mockImplementation(() => {});
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(console, 'warn').mockImplementation(() => {});
    });

    afterEach(async () => {
        for (const p of created.splice(0)) {
            const destroyed = p.destroy();
            await vi.runOnlyPendingTimersAsync();
            await destroyed;
        }
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    function createProvider(localReady?: Promise<unknown>): FireProvider {
        const provider = new FireProvider({
            firebaseApp: {} as any,
            ydoc: new Y.Doc(),
            path: 'tests/local-ready-wait',
            cachedClockOffset: 0,
            ...(localReady ? { localReady } : {}),
        });
        created.push(provider);
        return provider;
    }

    it('without the option, initial sync gets no wait', async () => {
        createProvider();
        await vi.advanceTimersByTimeAsync(0);
        expect(contexts).toHaveLength(1);
        expect(contexts[0].localReady).toBeUndefined();
    });

    it('resolves when local persistence reports it has loaded', async () => {
        let markLoaded!: () => void;
        createProvider(new Promise<void>(resolve => { markLoaded = resolve; }));
        await vi.advanceTimersByTimeAsync(0);
        const wait = track(contexts[0].localReady);

        await vi.advanceTimersByTimeAsync(DEFAULTS.LOCAL_READY_TIMEOUT_MS / 2);
        expect(wait.settled).toBe(false);
        markLoaded();
        await vi.advanceTimersByTimeAsync(0);
        expect(wait.settled).toBe(true);
    });

    it('resolves (never rejects) when local persistence fails to load', async () => {
        let fail!: (err: Error) => void;
        createProvider(new Promise<void>((_, reject) => { fail = reject; }));
        await vi.advanceTimersByTimeAsync(0);
        const wait = contexts[0].localReady!;
        fail(new Error('IndexedDB unavailable'));
        await expect(wait).resolves.toBeUndefined();
    });

    it('resolves after LOCAL_READY_TIMEOUT_MS when local persistence never settles', async () => {
        createProvider(new Promise(() => { /* never settles */ }));
        await vi.advanceTimersByTimeAsync(0);
        const wait = track(contexts[0].localReady);

        await vi.advanceTimersByTimeAsync(DEFAULTS.LOCAL_READY_TIMEOUT_MS - 1);
        expect(wait.settled).toBe(false);
        await vi.advanceTimersByTimeAsync(1);
        expect(wait.settled).toBe(true);
    });

    it('destroy() ends the wait at once', async () => {
        const provider = createProvider(new Promise(() => { /* never settles */ }));
        await vi.advanceTimersByTimeAsync(0);
        const wait = track(contexts[0].localReady);

        const destroyed = provider.destroy();
        await vi.advanceTimersByTimeAsync(0);
        expect(wait.settled).toBe(true);
        await destroyed;
        expect(vi.getTimerCount()).toBe(0);
    });

    it('a retried initial sync reuses the bounded wait instead of waiting again', async () => {
        performInitialSyncMock.mockImplementationOnce(async (ctx) => {
            contexts.push(ctx);
            return { ...synced, success: false, error: new Error('unavailable') };
        });
        const provider = createProvider(new Promise(() => { /* never settles */ }));
        await vi.advanceTimersByTimeAsync(0);
        expect(contexts).toHaveLength(1);

        // The retry backoff is well below the timeout; the retry gets the
        // same (still pending) wait, not a fresh LOCAL_READY_TIMEOUT_MS.
        await vi.advanceTimersByTimeAsync(1000);
        expect(contexts).toHaveLength(2);
        expect(contexts[1].localReady).toBe(contexts[0].localReady);
        expect(provider.synced).toBe(true);
    });
});
