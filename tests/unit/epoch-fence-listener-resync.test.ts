/**
 * Epoch fence vs. the re-sync a listener error schedules.
 *
 * Once this client's squash() succeeds, the provider is epoch-fenced: it
 * emits 'squashed' and, per the contract (provider.ts `_stopSyncing`), no
 * listener stays active, `synced` stays false, `epoch` reports the new
 * epoch and no data crosses the epoch boundary until the application
 * rebuilds the doc and recreates its providers.
 *
 * A real-time listener error while the squash is uploading or committing
 * makes the provider drop `synced`, detach its listeners and schedule a
 * re-sync (sync()). That re-sync can read the main document while it is
 * still at the old epoch, and finish only after squash() has fenced the
 * provider. It must not undo the fence: no listener re-attached, no
 * second 'sync', no epoch rolled back, no new-epoch data applied onto the
 * old-epoch live doc, and no further retry if it fails.
 *
 * Firestore is faked at the SDK boundary: `onSnapshot` registrations are
 * tracked so the test can see which listeners are live, fail them, and
 * deliver server snapshots to them. Initial sync, tiered compaction and
 * the squash transaction are stubbed so the test controls exactly when
 * each completes; the provider's lifecycle around them is under test.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as Y from 'yjs';

type Subscription = {
    target: { kind: string; path: string };
    next: (snapshot: any) => unknown;
    error?: (err: Error) => unknown;
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

vi.mock('../../src/sync', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../src/sync')>();
    return { ...actual, performInitialSync: vi.fn() };
});

vi.mock('../../src/compaction', () => ({
    compact: vi.fn(),
}));

vi.mock('../../src/squash', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../src/squash')>();
    return { ...actual, squashDocument: vi.fn() };
});

vi.mock('../../src/locking', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../src/locking')>();
    return { ...actual, measureClockSkew: vi.fn(async () => 0) };
});

import { FireProvider } from '../../src/provider';
import { performInitialSync, type SyncContext, type SyncResult } from '../../src/sync';
import { compact as tieredCompaction, type CompactionResult } from '../../src/compaction';
import { squashDocument, type SquashResult } from '../../src/squash';

const PATH = 'docs/fenced-resync';

const NOTHING_COMPACTED: CompactionResult = {
    success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0,
};

/** Initial sync against a server whose main document is at epoch 0. */
const syncedAtEpoch0 = (): SyncResult => ({
    success: true,
    updatesApplied: 0,
    localUpdatesPushed: false,
    lastSyncedDoc: null,
    syncedUpdateCount: 0,
    lastHistoryDoc: null,
    snapshotVersion: 3,
    epoch: 0,
});

const activeListeners = () =>
    fake.subscriptions.filter(s => s.active).map(s => `${s.target.kind}:${s.target.path}`);

/** Delivers a server snapshot to every live listener on `path`. */
const deliver = (path: string, snapshot: unknown) => {
    for (const sub of fake.subscriptions) {
        if (sub.active && sub.target.path === path) sub.next(snapshot);
    }
};

/** A history segment written by a client already rebuilt into epoch 1. */
const newEpochHistorySegment = () => {
    const writer = new Y.Doc();
    writer.getMap('data').set('newEpochKey', 'written in epoch 1');
    const segment = Y.encodeStateAsUpdate(writer);
    writer.destroy();
    return {
        docChanges: () => [{
            type: 'added',
            doc: {
                id: 'history-seg-epoch1',
                data: () => ({
                    segment: { toUint8Array: () => segment },
                    epoch: 1,
                    startTime: 1,
                }),
            },
        }],
    };
};

/** Lets every pending promise continuation run (fake timers: no real waiting). */
const flush = () => vi.advanceTimersByTimeAsync(0);

describe('epoch fence: a listener-error re-sync must not undo an own squash', () => {
    let provider: FireProvider | null = null;
    let ydoc: Y.Doc | null = null;

    beforeEach(() => {
        vi.useFakeTimers();
        fake.subscriptions.length = 0;
        vi.mocked(performInitialSync).mockReset();
        vi.mocked(performInitialSync).mockImplementation(async () => syncedAtEpoch0());
        vi.mocked(tieredCompaction).mockReset();
        vi.mocked(tieredCompaction).mockImplementation(async () => NOTHING_COMPACTED);
        vi.mocked(squashDocument).mockReset();
        for (const k of ['log', 'warn', 'error', 'debug'] as const) {
            vi.spyOn(console, k).mockImplementation(() => undefined);
        }
    });

    afterEach(async () => {
        await provider?.destroy();
        ydoc?.destroy();
        provider = null;
        ydoc = null;
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    /**
     * Brings a provider to synced, starts its squash with the transaction
     * parked, fails a listener meanwhile, and lets the scheduled re-sync
     * start with its performInitialSync parked too.
     *
     * @param adoptEpochBeforeSquashCommits - Whether the parked re-sync
     *   adopts the epoch-0 main document it read before the squash commits
     *   (true: its initial-sync push, awaiting addDoc or uploadBlob, spans
     *   the commit) or only as it completes, after squash() returned
     *   (false: a Storage download between the main-document read and
     *   onEpochAdopted spans it, e.g. an offloaded delete-set fingerprint
     *   or a snapshot or fold-tail blob).
     */
    const squashWithListenerErrorResyncInFlight = async (adoptEpochBeforeSquashCommits: boolean) => {
        ydoc = new Y.Doc();
        ydoc.getMap('data').set('k', 'old-epoch content');
        provider = new FireProvider({
            firebaseApp: {} as any,
            ydoc,
            path: PATH,
            cachedClockOffset: 0,
            maxUpdatesThreshold: 1000,
        });
        const events: string[] = [];
        provider.on('sync', () => events.push('sync'));
        provider.on('squashed', () => events.push('squashed'));
        await flush();
        expect(provider.synced).toBe(true);
        expect(activeListeners()).toEqual(expect.arrayContaining([
            `collection:${PATH}/updates`,
            `doc:${PATH}`,
            `collection:${PATH}/history`,
        ]));

        // The squash transaction is parked mid-commit.
        let commitSquash!: () => void;
        vi.mocked(squashDocument).mockImplementationOnce(() => new Promise<SquashResult>(resolve => {
            commitSquash = () => resolve({ success: true, epoch: 1 });
        }));
        const squashP = provider.squash();
        await flush();
        expect(commitSquash).toBeTypeOf('function');

        // The re-sync reads the main document while it is still at epoch 0,
        // then stays parked until the test lets it complete.
        let completeResync!: () => void;
        let failResync!: (err: Error) => void;
        vi.mocked(performInitialSync).mockImplementationOnce((ctx: SyncContext) => {
            if (adoptEpochBeforeSquashCommits) ctx.onEpochAdopted?.(0);
            return new Promise<SyncResult>((resolve, reject) => {
                completeResync = () => {
                    if (!adoptEpochBeforeSquashCommits) ctx.onEpochAdopted?.(0);
                    resolve(syncedAtEpoch0());
                };
                failResync = reject;
            });
        });

        // A listener fails (e.g. auth briefly invalid) while the squash commits.
        const mainDocListener = fake.subscriptions.find(s => s.active && s.target.path === PATH);
        mainDocListener!.error!(Object.assign(new Error('listen rejected'), { code: 'permission-denied' }));
        expect(provider.synced).toBe(false);

        // The backoff elapses: the re-sync starts and parks.
        await vi.advanceTimersByTimeAsync(5_000);
        expect(completeResync).toBeTypeOf('function');

        // The squash commits: the provider fences itself.
        commitSquash();
        const result = await squashP;
        expect(result.success).toBe(true);
        expect(events).toEqual(['sync', 'squashed']);
        expect(provider.epoch).toBe(1);
        expect(activeListeners()).toEqual([]);

        return { events, completeResync, failResync };
    };

    it('a re-sync that read the old-epoch main doc and completes after squash() returned leaves the provider fenced', async () => {
        const { events, completeResync } = await squashWithListenerErrorResyncInFlight(false);

        completeResync();
        await vi.advanceTimersByTimeAsync(5_000);

        // 'squashed' contract: fenced until the application rebuilds.
        expect({
            events,
            synced: provider!.synced,
            epoch: provider!.epoch,
            listeners: activeListeners(),
        }).toEqual({
            events: ['sync', 'squashed'],
            synced: false,
            epoch: 1,
            listeners: [],
        });
    });

    it('a re-sync that adopted the old epoch before the squash committed attaches no listener and lets no new-epoch data into the old doc', async () => {
        const { events, completeResync } = await squashWithListenerErrorResyncInFlight(true);
        const fencedContent = JSON.stringify(ydoc!.getMap('data').toJSON());

        completeResync();
        await vi.advanceTimersByTimeAsync(5_000);

        // A client already rebuilt into epoch 1 writes a history segment.
        deliver(`${PATH}/history`, newEpochHistorySegment());

        // No data crossed the epoch boundary into the old-epoch live doc...
        expect(ydoc!.getMap('data').has('newEpochKey')).toBe(false);
        expect(JSON.stringify(ydoc!.getMap('data').toJSON())).toBe(fencedContent);
        // ...and the provider is still fenced.
        expect({
            events,
            synced: provider!.synced,
            epoch: provider!.epoch,
            listeners: activeListeners(),
        }).toEqual({
            events: ['sync', 'squashed'],
            synced: false,
            epoch: 1,
            listeners: [],
        });
    });

    it.each([
        ['rejected', 'permission-denied'],
        ['offline', 'client-offline'],
    ])('a re-sync that fails (%s) after squash() returned schedules no retry', async (_label, code) => {
        const { events, failResync } = await squashWithListenerErrorResyncInFlight(true);
        const syncAttempts = vi.mocked(performInitialSync).mock.calls.length;

        // E.g. its initial-sync push is rejected, or the client went offline.
        failResync(Object.assign(new Error('initial sync failed'), { code }));
        await vi.advanceTimersByTimeAsync(60_000);

        // A retry scheduled now would outlive _stopSyncing and sync the
        // fenced provider again.
        expect(vi.mocked(performInitialSync).mock.calls.length).toBe(syncAttempts);
        expect({
            events,
            synced: provider!.synced,
            epoch: provider!.epoch,
            listeners: activeListeners(),
        }).toEqual({
            events: ['sync', 'squashed'],
            synced: false,
            epoch: 1,
            listeners: [],
        });
    });
});
