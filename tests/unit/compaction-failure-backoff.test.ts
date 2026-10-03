/**
 * Automatic compaction backs off after a failure that retrying cannot fix.
 *
 * A document compaction cannot get past (an undecodable update, a
 * storage-backed update whose blob is gone, Storage rejecting the fold
 * upload) never drains. Its updates collection stays at the realtime hard
 * cap, where the update listener used to trigger compaction on EVERY
 * delivery, and each attempt took the lock and re-read the whole backlog
 * before failing the same way again (see
 * tests/integration/poison_doc_compaction_storm.test.ts for the emulator
 * counts).
 *
 * This pins the provider wiring: which failures pause the automatic
 * trigger, that the pause holds even at the hard cap, that manual
 * compact() is never gated, and that progress (our own, or seen from
 * another client) ends the pause. The real update and snapshot listeners
 * run against a faked onSnapshot; compaction itself is faked at the module
 * boundary, so only the trigger decisions are exercised. Time is fake.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as Y from 'yjs';
import { toBase64 } from 'lib0/buffer';

const { listeners } = vi.hoisted(() => ({
    listeners: [] as { target: any; next: (snapshot: any) => unknown }[],
}));

vi.mock('@firebase/firestore', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/firestore')>();
    return {
        ...actual,
        getFirestore: vi.fn(() => ({ __fake: 'firestore' })),
        initializeFirestore: vi.fn(() => ({ __fake: 'firestore' })),
        collection: vi.fn((_db: unknown, ...segments: string[]) => ({ type: 'collection', path: segments.join('/') })),
        doc: vi.fn((_db: unknown, ...segments: string[]) => ({ type: 'document', path: segments.join('/') })),
        query: vi.fn((ref: { path: string }) => ({ type: 'query', path: ref.path })),
        orderBy: vi.fn(() => ({})),
        startAfter: vi.fn(() => ({})),
        onSnapshot: vi.fn((target: any, next: (snapshot: any) => unknown) => {
            listeners.push({ target, next });
            return () => {};
        }),
    };
});

vi.mock('@firebase/storage', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/storage')>();
    return {
        ...actual,
        getStorage: vi.fn(() => ({ __fake: 'storage' })),
        ref: vi.fn(),
    };
});

vi.mock('../../src/sync', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../src/sync')>();
    return {
        ...actual,
        performInitialSync: vi.fn(async () => ({
            success: true,
            updatesApplied: 0,
            localUpdatesPushed: false,
            lastSyncedDoc: null,
            syncedUpdateCount: 0,
            lastHistoryDoc: null,
            snapshotVersion: null,
            epoch: 0,
        })),
    };
});

vi.mock('../../src/compaction', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../src/compaction')>();
    return { ...actual, compact: vi.fn() };
});

import { compact, CompactionResult } from '../../src/compaction';
import { FireProvider } from '../../src/provider';
import { DEFAULTS } from '../../src/types';

const compactMock = vi.mocked(compact);
const PATH = 'tests/compaction-failure-backoff';
const HARD_CAP = DEFAULTS.REALTIME_LIMIT;

const failed = (error: unknown): CompactionResult => ({
    success: false,
    type: 'none',
    updatesCompacted: 0,
    historySegmentsMerged: 0,
    error: error as Error,
});
const compacted: CompactionResult = { success: true, type: 'history', updatesCompacted: 200, historySegmentsMerged: 0 };
const lockBusy: CompactionResult = { success: true, type: 'none', updatesCompacted: 0, historySegmentsMerged: 0 };

/** What the merge throws on an undecodable inline update. */
const poisonPill = () => new Error('Compaction candidate failed validation: Error: Unexpected end of array');

type FailedEvent = { error: Error; consecutiveFailures: number; retryInMs: number };

describe('automatic compaction backs off after failures retrying cannot fix', () => {
    let provider: FireProvider | null = null;
    let events: FailedEvent[] = [];

    beforeEach(() => {
        vi.useFakeTimers();
        listeners.length = 0;
        compactMock.mockReset();
        events = [];
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(console, 'warn').mockImplementation(() => {});
        vi.spyOn(console, 'log').mockImplementation(() => {});
    });

    afterEach(async () => {
        if (provider) {
            const p = provider.destroy();
            await vi.runOnlyPendingTimersAsync();
            await p;
            provider = null;
        }
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    async function createSyncedProvider(): Promise<FireProvider> {
        const p = new FireProvider({
            firebaseApp: {} as any,
            ydoc: new Y.Doc(),
            path: PATH,
            maxUpdatesThreshold: 10,
            cachedClockOffset: 0,
        });
        p.on('compaction-failed', (event: FailedEvent) => events.push(event));
        await vi.advanceTimersByTimeAsync(0);
        expect(p.synced).toBe(true);
        return p;
    }

    function listenerFor(predicate: (target: any) => boolean) {
        const found = listeners.filter(l => predicate(l.target));
        expect(found.length).toBeGreaterThan(0);
        return found[found.length - 1];
    }

    /**
     * One update-listener delivery holding `size` documents, `removed` of
     * them having left the collection (still pending, i.e. our own
     * rejected write, when `pending`).
     */
    async function deliverUpdates(size: number, removed = 0, pending = false): Promise<void> {
        const changes = Array.from({ length: removed }, (_, i) => ({
            type: 'removed',
            doc: { id: `gone-${i}`, metadata: { hasPendingWrites: pending } },
        }));
        listenerFor(t => t.type === 'query' && t.path === `${PATH}/updates`).next({ size, docChanges: () => changes });
        // Let any compaction the delivery started settle.
        await vi.advanceTimersByTimeAsync(0);
    }

    /**
     * One main-document delivery carrying snapshot `version`, folded from
     * nothing the local document lacks (so it is processed without a
     * download).
     */
    async function deliverSnapshotVersion(version: number): Promise<void> {
        await listenerFor(t => t.type === 'document' && t.path === PATH).next({
            exists: () => true,
            data: () => ({ version, stateVector: toBase64(Y.encodeStateVector(new Y.Doc())) }),
        });
        await vi.advanceTimersByTimeAsync(0);
    }

    it('pauses the trigger after a persistent failure, even at the hard cap, then retries with a doubled pause', async () => {
        compactMock.mockResolvedValue(failed(poisonPill()));
        provider = await createSyncedProvider();

        await deliverUpdates(HARD_CAP);
        expect(compactMock).toHaveBeenCalledTimes(1);
        expect(events).toHaveLength(1);
        expect(events[0].consecutiveFailures).toBe(1);
        expect(events[0].error.message).toMatch(/failed validation/);
        expect(events[0].retryInMs).toBeGreaterThanOrEqual(DEFAULTS.COMPACTION_TRIGGER_COOLDOWN_MS);
        expect(events[0].retryInMs).toBeLessThan(DEFAULTS.COMPACTION_TRIGGER_COOLDOWN_MS * 1.25);

        // Unfixed, each of these re-ran the failing compaction.
        for (let i = 1; i <= 8; i++) {
            await deliverUpdates(HARD_CAP + i);
        }
        expect(compactMock).toHaveBeenCalledTimes(1);

        await vi.advanceTimersByTimeAsync(Math.ceil(events[0].retryInMs));
        await deliverUpdates(HARD_CAP + 9);
        expect(compactMock).toHaveBeenCalledTimes(2);
        expect(events.map(e => e.consecutiveFailures)).toEqual([1, 2]);
        expect(events[1].retryInMs).toBeGreaterThanOrEqual(2 * DEFAULTS.COMPACTION_TRIGGER_COOLDOWN_MS);
        expect(events[1].retryInMs).toBeLessThan(2.5 * DEFAULTS.COMPACTION_TRIGGER_COOLDOWN_MS);
    });

    it.each([
        ['a storage-backed update whose blob is gone', { code: 'storage/object-not-found' }],
        ['Storage rules rejecting the fold upload', { code: 'storage/unauthorized' }],
    ])('counts %s', async (_label, error) => {
        compactMock.mockResolvedValue(failed(error));
        provider = await createSyncedProvider();

        await deliverUpdates(HARD_CAP);
        await deliverUpdates(HARD_CAP + 1);

        expect(compactMock).toHaveBeenCalledTimes(1);
        expect(events.map(e => e.consecutiveFailures)).toEqual([1]);
    });

    it('never gates a manual compact(), and counts its failure too', async () => {
        compactMock.mockResolvedValue(failed(poisonPill()));
        provider = await createSyncedProvider();

        await deliverUpdates(HARD_CAP);
        await provider.compact();

        expect(compactMock).toHaveBeenCalledTimes(2);
        expect(events.map(e => e.consecutiveFailures)).toEqual([1, 2]);
    });

    it.each([
        ['a lost lock', new Error('Lock lost or expired during compaction phase - Aborting write.')],
        ['a version race with a concurrent fold', new Error('Document version changed during compaction upload. Aborting to retry.')],
        ['the SDK being offline', Object.assign(new Error('Failed to get document because the client is offline.'), { code: 'unavailable' })],
        ['Storage retries running out', { code: 'storage/retry-limit-exceeded' }],
    ])('does not pause for %s', async (_label, error) => {
        compactMock.mockResolvedValue(failed(error));
        provider = await createSyncedProvider();

        await deliverUpdates(HARD_CAP);
        await deliverUpdates(HARD_CAP + 1);

        expect(compactMock).toHaveBeenCalledTimes(2);
        expect(events).toEqual([]);
    });

    it('ends the pause when a compaction makes progress', async () => {
        compactMock.mockResolvedValueOnce(failed(poisonPill())).mockResolvedValueOnce(compacted);
        provider = await createSyncedProvider();

        await deliverUpdates(HARD_CAP);
        await provider.compact();

        compactMock.mockResolvedValue(failed(poisonPill()));
        await deliverUpdates(HARD_CAP + 1);
        expect(compactMock).toHaveBeenCalledTimes(3);
        // The count started over.
        expect(events.map(e => e.consecutiveFailures)).toEqual([1, 1]);
    });

    it('keeps the pause when a compaction succeeds without compacting anything (lock busy)', async () => {
        compactMock.mockResolvedValueOnce(failed(poisonPill())).mockResolvedValueOnce(lockBusy);
        provider = await createSyncedProvider();

        await deliverUpdates(HARD_CAP);
        await provider.compact();
        await deliverUpdates(HARD_CAP + 1);

        expect(compactMock).toHaveBeenCalledTimes(2);
    });

    it('ends the pause when another client deletes update documents', async () => {
        compactMock.mockResolvedValue(failed(poisonPill()));
        provider = await createSyncedProvider();

        await deliverUpdates(HARD_CAP);
        await deliverUpdates(HARD_CAP + 1);
        expect(compactMock).toHaveBeenCalledTimes(1);

        // The removal is seen before the delivery's own trigger decision.
        await deliverUpdates(HARD_CAP, 2);
        expect(compactMock).toHaveBeenCalledTimes(2);
        expect(events.map(e => e.consecutiveFailures)).toEqual([1, 1]);
    });

    it('does not take our own rejected write leaving the view for progress', async () => {
        compactMock.mockResolvedValue(failed(poisonPill()));
        provider = await createSyncedProvider();

        await deliverUpdates(HARD_CAP);
        await deliverUpdates(HARD_CAP, 1, true);

        expect(compactMock).toHaveBeenCalledTimes(1);
    });

    it('ends the pause when a new snapshot version arrives', async () => {
        compactMock.mockResolvedValue(failed(poisonPill()));
        provider = await createSyncedProvider();

        await deliverUpdates(HARD_CAP);
        await deliverSnapshotVersion(7);
        await deliverUpdates(HARD_CAP + 1);

        expect(compactMock).toHaveBeenCalledTimes(2);
        expect(events.map(e => e.consecutiveFailures)).toEqual([1, 1]);
    });

    it('does not take a repeated snapshot version for progress', async () => {
        compactMock.mockResolvedValue(failed(poisonPill()));
        provider = await createSyncedProvider();

        await deliverSnapshotVersion(7);
        await deliverUpdates(HARD_CAP);
        await deliverSnapshotVersion(7);
        await deliverUpdates(HARD_CAP + 1);

        expect(compactMock).toHaveBeenCalledTimes(1);
    });
});
