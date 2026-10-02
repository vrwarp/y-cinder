/**
 * Epoch fence vs. compaction's history-listener resume.
 *
 * Once a provider is epoch-fenced — its own squash() succeeded, or it saw
 * the server move to a newer epoch — the contract (provider.ts
 * `_stopSyncing`) is that no listener stays active and no data crosses the
 * epoch boundary until the application rebuilds.
 *
 * compact() pauses the history listener and resumes it in its `finally`
 * block. That resume must not resurrect a Firestore listener on a fenced
 * provider, whether the fence went up while the compaction was in flight
 * or before compact() was called. On the squasher the resumed listener
 * even accepts NEW-epoch history segments (its epoch is already the new
 * one) and applies them onto the old-epoch live document.
 *
 * Firestore is faked at the SDK boundary: `onSnapshot` registrations are
 * tracked so the test can see which listeners are live and deliver server
 * snapshots to them. Initial sync, tiered compaction and the squash
 * transaction are stubbed — they are not under test; the provider's
 * lifecycle around them is.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as Y from 'yjs';

type Subscription = {
    target: { kind: string; path: string };
    next: (snapshot: any) => unknown;
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
        onSnapshot: vi.fn((target: any, next: (s: any) => unknown) => {
            const sub: Subscription = { target, next, active: true };
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

// Initial sync against an empty server at epoch 0.
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

// The squash transaction itself succeeds into epoch 1.
vi.mock('../../src/squash', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../src/squash')>();
    return {
        ...actual,
        squashDocument: vi.fn(async () => ({ success: true, epoch: 1 })),
    };
});

vi.mock('../../src/locking', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../src/locking')>();
    return { ...actual, measureClockSkew: vi.fn(async () => 0) };
});

import { FireProvider } from '../../src/provider';
import { compact as tieredCompaction, type CompactionResult } from '../../src/compaction';

const PATH = 'docs/fenced';

const NOTHING_COMPACTED: CompactionResult = {
    success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0,
};

const activeListeners = () =>
    fake.subscriptions.filter(s => s.active).map(s => `${s.target.kind}:${s.target.path}`);

/** Delivers a server snapshot to every live listener on `path`. */
const deliver = (path: string, snapshot: unknown) => {
    for (const sub of fake.subscriptions) {
        if (sub.active && sub.target.path === path) sub.next(snapshot);
    }
};

/** A history segment written by a new-epoch client (independent root-map insert). */
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

const startSyncedProvider = async () => {
    const ydoc = new Y.Doc();
    ydoc.getMap('data').set('k', 'old-epoch content');
    const provider = new FireProvider({
        firebaseApp: {} as any,
        ydoc,
        path: PATH,
        cachedClockOffset: 0,
        maxUpdatesThreshold: 1000,
    });
    await vi.waitFor(() => expect(provider.synced).toBe(true));
    // Sanity: updates, main-document snapshot and history listeners are live.
    expect(activeListeners()).toEqual(expect.arrayContaining([
        `collection:${PATH}/updates`,
        `doc:${PATH}`,
        `collection:${PATH}/history`,
    ]));
    return { ydoc, provider };
};

describe('epoch fence: compaction must not resume the history listener', () => {
    let provider: FireProvider | null = null;
    let ydoc: Y.Doc | null = null;

    beforeEach(() => {
        fake.subscriptions.length = 0;
        vi.mocked(tieredCompaction).mockReset();
        vi.mocked(tieredCompaction).mockImplementation(async () => NOTHING_COMPACTED);
        vi.spyOn(console, 'log').mockImplementation(() => undefined);
    });

    afterEach(async () => {
        await provider?.destroy();
        ydoc?.destroy();
        provider = null;
        ydoc = null;
        vi.restoreAllMocks();
    });

    it('compaction in flight when the server moves to a newer epoch leaves no listener active once it finishes', async () => {
        ({ ydoc, provider } = await startSyncedProvider());
        const epochChanged = vi.fn();
        provider.on('epoch-changed', epochChanged);

        // Park a compaction mid-flight.
        let finishCompaction!: () => void;
        vi.mocked(tieredCompaction).mockImplementationOnce(
            () => new Promise<CompactionResult>(resolve => { finishCompaction = () => resolve(NOTHING_COMPACTED); })
        );
        const inflight = provider.compact();
        expect(provider.isCompacting).toBe(true);

        // Someone squashed: the main document now reports epoch 1.
        deliver(PATH, {
            exists: () => true,
            data: () => ({ epoch: 1, version: 7, origin: 'another-client' }),
        });
        expect(epochChanged).toHaveBeenCalledTimes(1);
        expect(activeListeners()).toEqual([]);

        // The compaction now completes on the fenced provider.
        finishCompaction();
        await inflight;

        // Fence contract: nothing listens to Firestore until the app rebuilds.
        expect(activeListeners()).toEqual([]);
    });

    it('compact() after a successful squash neither re-attaches a listener nor lets new-epoch history into the fenced doc', async () => {
        ({ ydoc, provider } = await startSyncedProvider());
        const squashed = vi.fn();
        provider.on('squashed', squashed);

        const result = await provider.squash();
        expect(result.success).toBe(true);
        expect(squashed).toHaveBeenCalledWith({ epoch: 1 });
        expect(activeListeners()).toEqual([]);
        const fencedContent = JSON.stringify(ydoc.getMap('data').toJSON());

        // A later compaction (manual, or a lingering trigger) on the fenced provider.
        await provider.compact().catch(() => undefined);

        // A new-epoch client's history segment lands on the server.
        deliver(`${PATH}/history`, newEpochHistorySegment());

        // No data crossed the epoch boundary into the old-epoch live doc...
        expect(ydoc.getMap('data').has('newEpochKey')).toBe(false);
        expect(JSON.stringify(ydoc.getMap('data').toJSON())).toBe(fencedContent);
        // ...and nothing is listening to Firestore any more.
        expect(activeListeners()).toEqual([]);
    });
});
