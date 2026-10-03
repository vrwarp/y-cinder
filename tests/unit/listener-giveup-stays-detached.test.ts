/**
 * After listener recovery gives up ('sync-failure'), nothing listens.
 *
 * A listener error makes the provider drop `synced`, detach every
 * listener and re-sync with backoff. When the listeners keep failing right
 * after each re-attach, recovery gives up and emits 'sync-failure'. The
 * README contract for that event: "Remote changes are no longer received"
 * — the provider is stopped until the application rebuilds it.
 *
 * compact() pauses the history listener and resumes it in its `finally`
 * block. That resume must not re-attach a lone history listener on a
 * provider that gave up: no later sync() would ever replace it, so it
 * would keep billing reads and applying remote history segments until
 * destroy(), while the update and snapshot listeners stay dead and
 * `synced` stays false. This holds whether the compaction was in flight
 * at the give-up error or the application calls compact() afterwards,
 * and equally after the initial-sync circuit breaker gives up. The leak
 * needs the history query itself to keep succeeding: if the give-up's
 * cause also rejects it, Firestore ends the revived listener on its own.
 *
 * Firestore is faked at the SDK boundary: `onSnapshot` registrations are
 * tracked so the test can see which listeners are live, fail them, and
 * deliver server snapshots to them. Initial sync and tiered compaction are
 * stubbed — they are not under test; the provider's lifecycle around them
 * is. Timers are faked so the recovery backoffs run deterministically.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as Y from 'yjs';

type Subscription = {
    target: { kind: string; path: string };
    next: (snapshot: any) => unknown;
    error?: (error: Error) => unknown;
    active: boolean;
};

const fake = vi.hoisted(() => ({
    subscriptions: [] as Subscription[],
}));

vi.mock('@firebase/firestore', () => {
    class FakeBytes {
        constructor(private readonly bytes: Uint8Array) { }
        static fromUint8Array(bytes: Uint8Array) { return new FakeBytes(bytes); }
        toUint8Array() { return this.bytes; }
    }
    const refPath = (parts: any[]) =>
        parts.map(p => (typeof p === 'string' ? p : p?.path)).filter(Boolean).join('/');
    return {
        getFirestore: vi.fn(() => ({})),
        initializeFirestore: vi.fn(() => ({})),
        persistentLocalCache: vi.fn(() => ({})),
        collection: vi.fn((_db: unknown, ...parts: any[]) => ({ kind: 'collection', path: refPath(parts) })),
        doc: vi.fn((_db: unknown, ...parts: any[]) => ({ kind: 'doc', path: refPath(parts) })),
        query: vi.fn((ref: any, ...constraints: unknown[]) => ({ ...ref, constraints })),
        orderBy: vi.fn((field: string) => ({ orderBy: field })),
        startAfter: vi.fn((cursor: unknown) => ({ startAfter: cursor })),
        limit: vi.fn((n: number) => ({ limit: n })),
        limitToLast: vi.fn((n: number) => ({ limitToLast: n })),
        serverTimestamp: vi.fn(() => ({ serverTimestamp: true })),
        deleteField: vi.fn(() => ({ deleteField: true })),
        addDoc: vi.fn(async () => ({ id: 'added' })),
        getDocs: vi.fn(async () => ({ docs: [], empty: true, size: 0 })),
        getDoc: vi.fn(async () => ({ exists: () => false, data: () => undefined })),
        runTransaction: vi.fn(async () => undefined),
        Bytes: FakeBytes,
        onSnapshot: vi.fn((target: any, next: (s: any) => unknown, error?: (e: Error) => unknown) => {
            const sub: Subscription = { target, next, error, active: true };
            fake.subscriptions.push(sub);
            return () => { sub.active = false; };
        }),
    };
});

vi.mock('@firebase/storage', () => ({
    getStorage: vi.fn(() => ({})),
    ref: vi.fn((_s: unknown, path: string) => ({ path })),
    uploadBytes: vi.fn(async () => undefined),
    getBytes: vi.fn(async () => new ArrayBuffer(0)),
    deleteObject: vi.fn(async () => undefined),
}));

// Initial sync (and every re-sync) against an empty server at epoch 0.
vi.mock('../../src/sync', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../src/sync')>();
    return {
        ...actual,
        performInitialSync: vi.fn(async () => ({
            success: true,
            updatesApplied: 0,
            localUpdatesPushed: false,
            lastSyncedDoc: null,
            lastHistoryDoc: null,
            snapshotVersion: null,
            epoch: 0,
        })),
    };
});

// Tiered compaction: each test controls when it completes.
vi.mock('../../src/compaction', () => ({
    compact: vi.fn(),
}));

vi.mock('../../src/locking', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../src/locking')>();
    return { ...actual, measureClockSkew: vi.fn(async () => 0) };
});

import { FireProvider } from '../../src/provider';
import { compact as tieredCompaction, type CompactionResult } from '../../src/compaction';
import { performInitialSync } from '../../src/sync';
import { DEFAULTS } from '../../src/types';

const PATH = 'docs/gave-up';

const NOTHING_COMPACTED: CompactionResult = {
    success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0,
};

const activeListeners = () =>
    fake.subscriptions.filter(s => s.active).map(s => `${s.target.kind}:${s.target.path}`);

/** Fails the live updates listener (Firestore terminates it after this). */
const failUpdatesListener = () => {
    const live = fake.subscriptions.filter(s => s.active && s.target.path === `${PATH}/updates`);
    expect(live).toHaveLength(1);
    live[0].error!(Object.assign(new Error('Missing or insufficient permissions.'), { code: 'permission-denied' }));
};

/** Delivers a server snapshot to every live listener on `path`. */
const deliver = (path: string, snapshot: unknown) => {
    for (const sub of fake.subscriptions) {
        if (sub.active && sub.target.path === path) sub.next(snapshot);
    }
};

/** A history segment another client wrote after this provider stopped. */
const remoteHistorySegment = () => {
    const writer = new Y.Doc();
    writer.getMap('data').set('remoteKey', 'written after sync-failure');
    const segment = Y.encodeStateAsUpdate(writer);
    writer.destroy();
    return {
        docChanges: () => [{
            type: 'added',
            doc: {
                id: 'history-seg-after-giveup',
                data: () => ({
                    segment: { toUint8Array: () => segment },
                    epoch: 0,
                    startTime: 1,
                }),
            },
        }],
    };
};

describe('listener recovery gave up: compaction must not re-attach the history listener', () => {
    let provider: FireProvider | null = null;
    let ydoc: Y.Doc | null = null;
    let syncFailures: Error[] = [];

    beforeEach(() => {
        vi.useFakeTimers();
        fake.subscriptions.length = 0;
        syncFailures = [];
        vi.mocked(tieredCompaction).mockReset();
        vi.mocked(tieredCompaction).mockImplementation(async () => NOTHING_COMPACTED);
        vi.mocked(performInitialSync).mockClear();
        vi.spyOn(console, 'log').mockImplementation(() => undefined);
        vi.spyOn(console, 'error').mockImplementation(() => undefined);
        vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    });

    afterEach(async () => {
        if (provider) {
            const p = provider.destroy();
            await vi.runOnlyPendingTimersAsync();
            await p;
        }
        ydoc?.destroy();
        provider = null;
        ydoc = null;
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    const createProvider = () => {
        ydoc = new Y.Doc();
        ydoc.getMap('data').set('k', 'local content');
        provider = new FireProvider({
            firebaseApp: {} as any,
            ydoc,
            path: PATH,
            cachedClockOffset: 0,
            maxUpdatesThreshold: 1000,
        });
        provider.on('sync-failure', (err: Error) => syncFailures.push(err));
        return provider;
    };

    const startSyncedProvider = async () => {
        createProvider();
        await vi.advanceTimersByTimeAsync(0);
        expect(provider!.synced).toBe(true);
        // Sanity: updates, main-document snapshot and history listeners are live.
        expect(activeListeners().sort()).toEqual([
            `collection:${PATH}/history`,
            `collection:${PATH}/updates`,
            `doc:${PATH}`,
        ]);
        return provider!;
    };

    /**
     * Fails the listeners right after each re-attach until recovery gives
     * up. Each recoverable error schedules a re-sync whose backoff (well
     * under LISTENER_HEALTHY_MS in total) the fake clock runs through.
     */
    const failListenersUntilGiveUp = async (p: FireProvider) => {
        for (let i = 1; i < DEFAULTS.MAX_RETRIES; i++) {
            failUpdatesListener();
            expect(p.synced).toBe(false);
            expect(syncFailures).toHaveLength(0);
            // Run the recovery backoff (2^i * 100ms + up to 100ms jitter).
            await vi.advanceTimersByTimeAsync(2 ** i * 100 + 200);
            expect(p.synced).toBe(true);
        }
        failUpdatesListener();
        await vi.advanceTimersByTimeAsync(0);
        expect(syncFailures).toHaveLength(1);
        expect(p.synced).toBe(false);
    };

    it('compact() called after sync-failure leaves no listener attached', async () => {
        const p = await startSyncedProvider();

        await failListenersUntilGiveUp(p);
        // Recovery gave up: every listener is detached.
        expect(activeListeners()).toEqual([]);

        // The application (or a lingering caller) compacts afterwards; the
        // history collection itself is still readable.
        await p.compact().catch(() => undefined);
        // Long after: no re-sync ever runs again.
        await vi.advanceTimersByTimeAsync(60_000);

        // Stopped-state contract: nothing listens to Firestore...
        expect(activeListeners()).toEqual([]);
        expect(p.synced).toBe(false);

        // ...and remote history written meanwhile is not received.
        deliver(`${PATH}/history`, remoteHistorySegment());
        expect(ydoc!.getMap('data').has('remoteKey')).toBe(false);
    });

    it('a compaction in flight when recovery gives up leaves no listener attached once it finishes', async () => {
        const p = await startSyncedProvider();

        // Bring recovery one error short of giving up.
        for (let i = 1; i < DEFAULTS.MAX_RETRIES; i++) {
            failUpdatesListener();
            await vi.advanceTimersByTimeAsync(2 ** i * 100 + 200);
            expect(p.synced).toBe(true);
        }

        // Park a compaction mid-flight (it pauses the history listener).
        let finishCompaction!: () => void;
        vi.mocked(tieredCompaction).mockImplementationOnce(
            () => new Promise<CompactionResult>(resolve => { finishCompaction = () => resolve(NOTHING_COMPACTED); })
        );
        const inflight = p.compact();
        await vi.advanceTimersByTimeAsync(0);
        expect(p.isCompacting).toBe(true);

        // The last error: recovery gives up while the compaction runs.
        failUpdatesListener();
        await vi.advanceTimersByTimeAsync(0);
        expect(syncFailures).toHaveLength(1);
        expect(activeListeners()).toEqual([]);

        // The compaction now completes on the stopped provider.
        finishCompaction();
        await inflight;
        await vi.advanceTimersByTimeAsync(60_000);

        // Stopped-state contract: nothing listens to Firestore...
        expect(activeListeners()).toEqual([]);
        expect(p.synced).toBe(false);

        // ...and remote history written meanwhile is not received.
        deliver(`${PATH}/history`, remoteHistorySegment());
        expect(ydoc!.getMap('data').has('remoteKey')).toBe(false);
    });

    it('compact() called after the initial-sync circuit breaker gave up leaves no listener attached', async () => {
        // Every initial sync attempt fails (online, so each one counts):
        // sync() retries with backoff and gives up after MAX_RETRIES.
        for (let i = 0; i < DEFAULTS.MAX_RETRIES; i++) {
            vi.mocked(performInitialSync).mockResolvedValueOnce({
                success: false,
                error: Object.assign(new Error('Missing or insufficient permissions.'), { code: 'permission-denied' }),
                updatesApplied: 0,
                localUpdatesPushed: false,
                lastSyncedDoc: null,
                syncedUpdateCount: 0,
                lastHistoryDoc: null,
                snapshotVersion: null,
                epoch: 0,
            });
        }
        const p = createProvider();
        // Run every retry backoff (2^i * 100ms + up to 100ms jitter).
        await vi.advanceTimersByTimeAsync(10_000);
        expect(vi.mocked(performInitialSync)).toHaveBeenCalledTimes(DEFAULTS.MAX_RETRIES);
        expect(syncFailures).toHaveLength(1);
        expect(p.synced).toBe(false);
        expect(activeListeners()).toEqual([]);

        await p.compact().catch(() => undefined);
        await vi.advanceTimersByTimeAsync(60_000);

        // Stopped-state contract: nothing listens to Firestore...
        expect(activeListeners()).toEqual([]);
        expect(p.synced).toBe(false);

        // ...and remote history written meanwhile is not received.
        deliver(`${PATH}/history`, remoteHistorySegment());
        expect(ydoc!.getMap('data').has('remoteKey')).toBe(false);
    });
});
