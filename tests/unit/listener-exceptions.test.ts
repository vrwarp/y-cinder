/**
 * Regression: exceptions thrown by consumer event listeners must not change
 * the provider's persistence state.
 *
 * lib0's ObservableV2.emit() calls listeners without a try/catch, and
 * _executeSave() emitted 'saved' INSIDE the try block that guards the
 * Firestore write. A throwing 'saved' listener (e.g. an app handler that
 * records the last-sync time into a store that is not initialised yet) was
 * therefore classified as a failed write: the already-committed update was
 * re-queued and re-written after a backoff, and because every successful
 * attempt reset the retry counter before the throwing emit ran again, the
 * circuit breaker never tripped — one edit produced an unbounded stream of
 * identical update documents until destroy().
 *
 * The related 'save-rejected' path has the same shape: a throwing listener
 * escaped _executeSave (an unhandled rejection from the debounce timer) and
 * skipped the reschedule of updates buffered during the failed attempt (and,
 * on the retry-cap path, the reset of the retry counter). sync() emitted
 * 'sync' the same way: a throwing listener re-ran the initial sync forever.
 *
 * Firestore is faked at the module boundary (addDoc is the only write the
 * save path performs) and the initial sync is stubbed to succeed, so the
 * provider is in the normal synced state. Time is fully controlled with fake
 * timers; the windows advanced below cover dozens of backoff retries.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as Y from 'yjs';

vi.mock('@firebase/firestore', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/firestore')>();
    return {
        ...actual,
        getFirestore: vi.fn(() => ({ __fake: 'firestore' })),
        initializeFirestore: vi.fn(() => ({ __fake: 'firestore' })),
        collection: vi.fn((_db: unknown, ...segments: string[]) => ({ path: segments.join('/') })),
        addDoc: vi.fn(),
    };
});

vi.mock('@firebase/storage', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/storage')>();
    return {
        ...actual,
        getStorage: vi.fn(() => ({ __fake: 'storage' })),
        ref: vi.fn(),
        uploadBytes: vi.fn(),
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
            lastHistoryDoc: null,
            snapshotVersion: null,
            epoch: 0,
        })),
        createUpdateListener: vi.fn(() => () => {}),
        createSnapshotListener: vi.fn(() => () => {}),
        createHistoryListener: vi.fn(() => () => {}),
    };
});

import { addDoc } from '@firebase/firestore';
import { FireProvider } from '../../src/provider';
import { createUpdateListener, performInitialSync } from '../../src/sync';
import { DEFAULTS } from '../../src/types';

const addDocMock = vi.mocked(addDoc);

/** A transient (retryable) Firestore write failure. */
const unavailable = () => Object.assign(new Error('Service unavailable'), { code: 'unavailable' });

/** Update payloads of every document written to the updates collection. */
function writtenUpdates(): Uint8Array[] {
    return addDocMock.mock.calls.map(([, data]) => (data as any).update.toUint8Array());
}

describe('consumer listener exceptions do not affect persistence', () => {
    let provider: FireProvider | null = null;

    beforeEach(() => {
        vi.useFakeTimers();
        addDocMock.mockReset();
        // Keep the provider's own error logging out of the test output.
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

    function createProvider(doc: Y.Doc): FireProvider {
        return new FireProvider({
            firebaseApp: {} as any,
            ydoc: doc,
            path: 'tests/listener-exceptions',
            maxWaitTime: 30,
            cachedClockOffset: 0,
        });
    }

    async function createSyncedProvider(doc: Y.Doc): Promise<FireProvider> {
        const p = createProvider(doc);
        await vi.advanceTimersByTimeAsync(0);
        expect(p.synced).toBe(true);
        return p;
    }

    it("a throwing 'saved' listener does not make a committed update get re-written", async () => {
        addDocMock.mockResolvedValue({ id: 'written' } as any);

        const doc = new Y.Doc();
        provider = await createSyncedProvider(doc);

        let savedEvents = 0;
        provider.on('saved', () => {
            savedEvents++;
            throw new Error('consumer bug: store not initialised yet');
        });

        doc.getText('t').insert(0, 'once');

        // Debounce (30ms) plus far more than enough time for any backoff retry.
        await vi.advanceTimersByTimeAsync(10_000);

        // The edit is committed exactly once and reported exactly once.
        expect(addDocMock).toHaveBeenCalledTimes(1);
        expect(savedEvents).toBe(1);

        // The re-write loop is gone, not merely slowed down.
        await vi.advanceTimersByTimeAsync(30_000);
        expect(addDocMock).toHaveBeenCalledTimes(1);

        const replica = new Y.Doc();
        writtenUpdates().forEach((u) => Y.applyUpdate(replica, u));
        expect(replica.getText('t').toString()).toBe('once');

        // Persistence keeps working normally after the listener threw.
        doc.getText('t').insert(4, ' more');
        await vi.advanceTimersByTimeAsync(10_000);

        expect(addDocMock).toHaveBeenCalledTimes(2);
        expect(savedEvents).toBe(2);

        const replica2 = new Y.Doc();
        writtenUpdates().forEach((u) => Y.applyUpdate(replica2, u));
        expect(replica2.getText('t').toString()).toBe('once more');
    });

    it("a throwing 'save-rejected' listener does not strand updates buffered during the rejected save", async () => {
        // First write: held open, then rejected as too large (terminal).
        let rejectFirstWrite!: (err: unknown) => void;
        addDocMock.mockImplementationOnce(
            () => new Promise((_resolve, reject) => { rejectFirstWrite = reject; }) as any,
        );
        addDocMock.mockResolvedValue({ id: 'written' } as any);

        const doc = new Y.Doc();
        provider = await createSyncedProvider(doc);

        const rejected: Uint8Array[] = [];
        provider.on('save-rejected', (info: { update: Uint8Array }) => {
            rejected.push(info.update);
            throw new Error('consumer bug in save-rejected handler');
        });

        doc.getText('t').insert(0, 'first');
        await vi.advanceTimersByTimeAsync(100); // debounce fires; write #1 in flight
        expect(addDocMock).toHaveBeenCalledTimes(1);

        // An edit made while write #1 is in flight stays buffered.
        doc.getText('t').insert(5, '-second');
        await vi.advanceTimersByTimeAsync(100);

        rejectFirstWrite(Object.assign(new Error('Document exceeds the maximum size'), {
            code: 'invalid-argument',
        }));
        await vi.advanceTimersByTimeAsync(10_000);

        // The rejection is reported once, and the buffered edit is still saved.
        expect(rejected).toHaveLength(1);
        expect(addDocMock).toHaveBeenCalledTimes(2);

        // Every edit is accounted for: either reported as rejected or written.
        const replica = new Y.Doc();
        rejected.forEach((u) => Y.applyUpdate(replica, u));
        writtenUpdates().slice(1).forEach((u) => Y.applyUpdate(replica, u));
        expect(replica.getText('t').toString()).toBe('first-second');
    });

    it("a throwing 'save-rejected' listener on the retry cap still resets the breaker and saves buffered updates", async () => {
        // Every attempt of the first batch fails; the last one is held open
        // so an edit can be buffered while it is in flight.
        let rejectLastAttempt!: (err: unknown) => void;
        for (let i = 0; i < DEFAULTS.MAX_SAVE_RETRIES - 1; i++) {
            addDocMock.mockRejectedValueOnce(unavailable());
        }
        addDocMock.mockImplementationOnce(
            () => new Promise((_resolve, reject) => { rejectLastAttempt = reject; }) as any,
        );
        // One transient failure after the cap: it must be retried, not rejected.
        addDocMock.mockRejectedValueOnce(unavailable());
        addDocMock.mockResolvedValue({ id: 'written' } as any);

        const doc = new Y.Doc();
        provider = await createSyncedProvider(doc);

        const rejected: Array<{ code: string; update: Uint8Array }> = [];
        provider.on('save-rejected', (info: { code: string; update: Uint8Array }) => {
            rejected.push(info);
            throw new Error('consumer bug in save-rejected handler');
        });

        doc.getText('t').insert(0, 'first');
        await vi.advanceTimersByTimeAsync(10_000); // failed attempts + backoffs
        expect(addDocMock).toHaveBeenCalledTimes(DEFAULTS.MAX_SAVE_RETRIES);

        doc.getText('t').insert(5, '-second');
        await vi.advanceTimersByTimeAsync(100);

        rejectLastAttempt(unavailable());
        await vi.advanceTimersByTimeAsync(10_000);

        // The cap trips once; the buffered edit gets a fresh retry budget
        // (one failure, one success) instead of being stranded or rejected.
        expect(rejected.map((r) => r.code)).toEqual(['max-retries-exceeded']);
        expect(addDocMock).toHaveBeenCalledTimes(DEFAULTS.MAX_SAVE_RETRIES + 2);

        const replica = new Y.Doc();
        rejected.forEach((r) => Y.applyUpdate(replica, r.update));
        writtenUpdates().forEach((u) => Y.applyUpdate(replica, u));
        expect(replica.getText('t').toString()).toBe('first-second');
    });

    it("a throwing 'sync' listener does not make a completed initial sync run again", async () => {
        vi.mocked(performInitialSync).mockClear();
        vi.mocked(createUpdateListener).mockClear();

        provider = createProvider(new Y.Doc());
        let syncEvents = 0;
        provider.on('sync', () => {
            syncEvents++;
            throw new Error('consumer bug in sync handler');
        });

        await vi.advanceTimersByTimeAsync(10_000);

        expect(provider.synced).toBe(true);
        expect(syncEvents).toBe(1);
        expect(performInitialSync).toHaveBeenCalledTimes(1);
        expect(createUpdateListener).toHaveBeenCalledTimes(1);
    });
});
