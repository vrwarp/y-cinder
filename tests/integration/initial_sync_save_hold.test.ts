/**
 * Regression tests: the save contracts that must survive initial sync's
 * single-writer handling of buffered updates.
 *
 * Initial sync's push covers every local update buffered before it reads
 * the local doc, so the provider retires those entries instead of saving
 * them a second time, and a due save waits for a running initial sync (see
 * hydration_echo_upload.test.ts for the cost this removes). What must not
 * change:
 *  - retired updates are reported with 'saved', like any committed save
 *    (consumers map it to a last-sync time);
 *  - only initial sync holds saves: a clock-skew probe write that is never
 *    acknowledged (offline with persistence) does not;
 *  - the hold ends at maxAggregationTime: neither a stalled read nor a push
 *    that is never acknowledged keeps edits from the SDK's write queue;
 *  - a re-sync after a listener error retires buffered edits without
 *    losing any of them.
 *
 * Reads and writes are held at the SDK boundary, so each scenario is set up
 * explicitly. Outcomes are read from events and from what reached the SDK;
 * the timeouts are generous upper bounds, never timing assertions.
 *
 * @file initial_sync_save_hold.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach, afterEach, vi } from 'vitest';

const { ctl } = vi.hoisted(() => ({
    ctl: {
        /** While set, every getDocs (initial sync's reads) waits on it. */
        getDocsGate: null as Promise<void> | null,
        /** While set, the clock-skew probe's write waits on it. */
        probeGate: null as Promise<void> | null,
        /** Clock-skew probe writes handed to the SDK. */
        probeWrites: 0,
        /** While set, the next update write by `holdUid` waits on it. */
        holdUid: null as string | null,
        holdGate: null as Promise<void> | null,
        /** Update documents handed to the SDK, in order. */
        writes: [] as { createdBy?: string }[],
        /** Live listeners; fail() ends one the way the SDK does on error. */
        listeners: [] as { fail: (error: unknown) => void }[],
    },
}));

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        addDoc: async (collectionRef: any, data: any) => {
            if (String(collectionRef.path).endsWith('/updates')) {
                ctl.writes.push({ createdBy: data?.createdBy });
                if (ctl.holdGate && data?.createdBy === ctl.holdUid) {
                    const gate = ctl.holdGate;
                    ctl.holdGate = null;
                    await gate;
                }
            }
            return actual.addDoc(collectionRef, data);
        },
        setDoc: async (ref: any, ...rest: any[]) => {
            if (String(ref.path).includes('/maintenance/')) ctl.probeWrites++;
            if (ctl.probeGate && String(ref.path).includes('/maintenance/')) await ctl.probeGate;
            return actual.setDoc(ref, ...rest);
        },
        getDocs: async (q: any) => {
            if (ctl.getDocsGate) await ctl.getDocsGate;
            return actual.getDocs(q);
        },
        onSnapshot: (ref: any, ...args: any[]) => {
            const fns = args.filter(a => typeof a === 'function');
            const onError: ((e: unknown) => void) | undefined = fns[1];
            const realUnsubscribe = actual.onSnapshot(ref, ...args);
            let unsubscribed = false;
            const unsubscribe = () => {
                if (unsubscribed) return;
                unsubscribed = true;
                realUnsubscribe();
            };
            ctl.listeners.push({
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

import * as Y from 'yjs';
import { FirestoreError } from '@firebase/firestore';
import { FireProvider } from '../../src/provider';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// The SDK's typings hide FirestoreError's constructor; build the same
// error instance the SDK delivers.
const permissionDenied = () => {
    const FirestoreErrorCtor = FirestoreError as unknown as new (code: string, message: string) => FirestoreError;
    return new FirestoreErrorCtor('permission-denied', 'Missing or insufficient permissions.');
};

describe('Saves around initial sync', () => {
    let app: any;
    let counter = 0;
    const live: FireProvider[] = [];

    beforeAll(async () => {
        app = (await setupEmulator()).app;
    });

    beforeEach(() => {
        ctl.writes = [];
        ctl.listeners = [];
        ctl.probeWrites = 0;
    });

    afterEach(async () => {
        ctl.getDocsGate = null;
        ctl.probeGate = null;
        ctl.holdUid = null;
        ctl.holdGate = null;
        while (live.length > 0) {
            await live.pop()!.destroy();
        }
    });

    const newPath = (tag: string) => `tests/initial-sync-save-hold-${tag}-${getStableDate()}-${Date.now()}-${counter++}`;

    const createProvider = (ydoc: Y.Doc, path: string, extra: Record<string, unknown> = {}) => {
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            // Skip the clock-skew probe unless a test turns it back on.
            cachedClockOffset: 0,
            // Keep compaction out of the picture.
            maxUpdatesThreshold: 1000,
            ...extra,
        });
        live.push(provider);
        return provider;
    };

    const release = async (provider: FireProvider) => {
        live.splice(live.indexOf(provider), 1);
        await provider.destroy();
    };

    const waitSynced = (provider: FireProvider) =>
        waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: 'provider synced' });

    const writesBy = (provider: FireProvider) => ctl.writes.filter(w => w.createdBy === provider.uid).length;

    /** What a brand-new device reads; nothing may be left waiting on a gap. */
    const readFromFreshClient = async (path: string): Promise<string> => {
        const fresh = new Y.Doc();
        const reader = createProvider(fresh, path);
        await waitSynced(reader);
        expect((fresh.store as any).pendingStructs).toBeNull();
        const text = fresh.getText('t').toString();
        await release(reader);
        fresh.destroy();
        return text;
    };

    for (const ordering of ['save due before the reads return', 'reads return first'] as const) {
        it(`reports an edit made during initial sync as 'saved' once the sync's push commits it (${ordering})`, { timeout: 60000 }, async () => {
            const path = newPath('saved');
            const doc = new Y.Doc();
            let open!: () => void;
            ctl.getDocsGate = new Promise<void>(resolve => { open = resolve; });
            // The ceiling is out of reach: the push commits the edit, not a save.
            const provider = createProvider(doc, path, { maxWaitTime: 50, maxAggregationTime: 60000 });
            const savedAt: number[] = [];
            provider.on('saved', (at: number) => savedAt.push(at));

            const before = Date.now();
            doc.getText('t').insert(0, 'during-sync');
            if (ordering === 'save due before the reads return') await sleep(200);
            ctl.getDocsGate = null;
            open();
            await waitSynced(provider);

            expect({ updateDocs: writesBy(provider), savedEvents: savedAt.length }).toEqual({ updateDocs: 1, savedEvents: 1 });
            expect(savedAt[0]).toBeGreaterThanOrEqual(before);

            await release(provider);
            expect(writesBy(provider)).toBe(1);
            expect(await readFromFreshClient(path)).toBe('during-sync');
        });
    }

    it('does not hold saves while the clock-skew probe write is unacknowledged', { timeout: 60000 }, async () => {
        const path = newPath('probe');
        const doc = new Y.Doc();
        let acknowledge!: () => void;
        ctl.probeGate = new Promise<void>(resolve => { acknowledge = resolve; });
        // The offset is unmeasured. The probe is off the startup path: initial
        // sync completes without it, and the first lock need takes it.
        const provider = createProvider(doc, path, { maxWaitTime: 50, maxAggregationTime: 60000, cachedClockOffset: undefined });
        await waitSynced(provider);
        expect(ctl.probeWrites).toBe(0);

        // The first compaction measures the offset; its probe write stays
        // unacknowledged.
        const compaction = provider.compact();
        await waitForConditionTruthy(() => ctl.probeWrites > 0, {
            timeout: 10000, message: 'the compaction starts the clock-skew probe',
        });

        doc.getText('t').insert(0, 'typed-while-probing');
        await waitForConditionTruthy(() => writesBy(provider) > 0, {
            timeout: 10000, message: 'the edit is handed to the SDK while the probe is unacknowledged',
        });
        expect(ctl.probeWrites).toBe(1);

        ctl.probeGate = null;
        acknowledge();
        await compaction;
        await release(provider);
        expect(await readFromFreshClient(path)).toBe('typed-while-probing');
    });

    it('saves an edit at maxAggregationTime while initial sync is stalled reading', { timeout: 60000 }, async () => {
        const path = newPath('stalled-read');
        const doc = new Y.Doc();
        let open!: () => void;
        ctl.getDocsGate = new Promise<void>(resolve => { open = resolve; });
        const provider = createProvider(doc, path, { maxWaitTime: 50, maxAggregationTime: 500 });

        doc.getText('t').insert(0, 'typed-while-stalled');
        await waitForConditionTruthy(() => writesBy(provider) > 0, {
            timeout: 10000, message: 'the edit is handed to the SDK while the reads are held',
        });
        expect(provider.synced).toBe(false);

        ctl.getDocsGate = null;
        open();
        await waitSynced(provider);
        await release(provider);
        expect(await readFromFreshClient(path)).toBe('typed-while-stalled');
    });

    it('saves an edit made after the push decision at maxAggregationTime while the push is unacknowledged', { timeout: 60000 }, async () => {
        const path = newPath('stalled-push');
        const doc = new Y.Doc();
        doc.getText('t').insert(0, 'local-first');
        let acknowledge!: () => void;
        const provider = createProvider(doc, path, { maxWaitTime: 50, maxAggregationTime: 500 });
        ctl.holdUid = provider.uid;
        ctl.holdGate = new Promise<void>(resolve => { acknowledge = resolve; });

        await waitForConditionTruthy(() => writesBy(provider) === 1, { timeout: 30000, message: 'the push is handed to the SDK' });
        doc.getText('t').insert(11, ' +after-push');
        await waitForConditionTruthy(() => writesBy(provider) === 2, {
            timeout: 10000, message: 'the later edit is handed to the SDK while the push is unacknowledged',
        });
        expect(provider.synced).toBe(false);

        acknowledge();
        await waitSynced(provider);
        await release(provider);
        expect(await readFromFreshClient(path)).toBe('local-first +after-push');
    });

    it('loses no buffered edit when a re-sync after a listener error retires them', { timeout: 60000 }, async () => {
        const path = newPath('resync');
        const doc = new Y.Doc();
        // No debounced save within the test: the re-sync's push and the
        // destroy() flush are the only writers.
        const provider = createProvider(doc, path, { maxWaitTime: 60000 });
        await waitSynced(provider);
        const savedAt: number[] = [];
        provider.on('saved', (at: number) => savedAt.push(at));

        doc.getText('t').insert(0, 'before-error');
        // Auth is briefly invalid: every listener of this provider dies.
        for (const listener of ctl.listeners.slice()) listener.fail(permissionDenied());
        expect(provider.synced).toBe(false);
        doc.getText('t').insert(0, '[during-resync]');

        await waitSynced(provider);
        expect(savedAt).toHaveLength(1);
        doc.getText('t').insert(0, '[after]');
        await release(provider);

        expect(await readFromFreshClient(path)).toBe('[after][during-resync]before-error');
    });
});
