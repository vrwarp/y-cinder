/**
 * Regression test: the initial-sync push guard must push local data that
 * lies in a server-side clock GAP.
 *
 * Bug: update documents (and history/snapshot state vectors) only record
 * per-client END clocks. performInitialSync folds them into a server state
 * vector with max() and treats `serverSV[X] = E` as "the server holds
 * X:[0, E)". When the server actually holds X:[5, 11) but not X:[0, 5),
 * the guard believes the server covers every local struct and pushes
 * nothing — X:[0, 5) never reaches Firestore, and every other client is
 * left with X:[5, 11) stuck as unresolvable pending structs.
 *
 * The usual way such a gap appears: the Y.Doc already holds content
 * authored by its clientID that is not yet on the server (local-first
 * edits, a provider recreated on the same doc), and the provider writes an
 * update document carrying only the NEW range before initial sync pushes
 * the pre-existing state — the debounced save during initial sync, or the
 * destroy() flush of a provider that never synced.
 *
 * The same contiguity assumption exists on the reading side: initial sync
 * folds each applied item's END clocks into its local state vector, so an
 * update that fills the gap but is ordered after the gapped one is skipped
 * as "redundant" by a fresh client.
 *
 * The realtime listener had the same flaw: a live peer that received the
 * gapped update first skipped the push that fills the gap.
 *
 * Contract asserted: once a client holding the data has completed initial
 * sync, a fresh client reading the server sees ALL of it (with nothing left
 * in pendingStructs), and so does a peer that was already connected.
 *
 * @file push_guard_clock_gap.test.ts
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

/**
 * Lets a test hold back performInitialSync's first server read until a
 * chosen moment (here: until the provider's debounced save has committed),
 * so the "save lands before initial sync reads updates" ordering is
 * explicit instead of a timing race. `failGetDocs` instead simulates a
 * network outage, so initial sync fails and retries with backoff.
 */
const mockControls: { getDocsGate: Promise<void> | null; failGetDocs: boolean } = {
    getDocsGate: null,
    failGetDocs: false,
};

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getDocs: async (q: any) => {
            if (mockControls.getDocsGate) {
                await mockControls.getDocsGate;
            }
            if (mockControls.failGetDocs) {
                const err: any = new Error('Simulated network failure');
                err.code = 'unavailable';
                throw err;
            }
            return actual.getDocs(q);
        },
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { addDoc, collection, Bytes, serverTimestamp } from '@firebase/firestore';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';
import { extractClockEnds, aggregateClockEnds } from '../../src/update-metadata';
import { FIRESTORE_PATHS } from '../../src/types';

describe('Initial-sync push guard with server-side clock gaps', () => {
    let app: any;
    let db: any;
    let counter = 0;
    const providers: FireProvider[] = [];

    const createProvider = (ydoc: Y.Doc, path: string, config: Record<string, any> = {}) => {
        const p = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            // Skip the clock-skew probe: initial sync starts immediately.
            cachedClockOffset: 0,
            // Keep compaction out of the picture.
            maxUpdatesThreshold: 1000,
            ...config,
        });
        providers.push(p);
        return p;
    };

    const waitSynced = (p: FireProvider) =>
        waitForConditionTruthy(() => p.synced, { timeout: 30000, message: 'provider should sync' });

    /** Structs Yjs could not integrate for lack of their predecessors. */
    const pendingStructsOf = (doc: Y.Doc) => (doc.store as any).pendingStructs;

    /** What a brand-new device sees after initial sync of `path`. */
    const readFromFreshClient = async (path: string): Promise<string> => {
        const fresh = new Y.Doc();
        const reader = createProvider(fresh, path);
        await waitSynced(reader);
        const text = fresh.getText('t').toString();
        // Healed, not merely padded: nothing may be left waiting on a gap.
        expect(pendingStructsOf(fresh)).toBeNull();
        await reader.destroy();
        return text;
    };

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        mockControls.getDocsGate = null;
        mockControls.failGetDocs = false;
    });

    afterEach(async () => {
        mockControls.getDocsGate = null;
        mockControls.failGetDocs = false;
        for (const p of providers.splice(0)) {
            await p.destroy().catch(() => { /* already destroyed */ });
        }
    });

    // Direct push-guard test with a synthetic gap: constrains the guard and
    // the readers' redundancy checks, not how the gap came about.
    it('pushes local data the server is missing even when later clocks of the same client are on the server', async () => {
        const path = `integration-tests/push-guard-gap-${getStableDate()}-${counter++}`;

        // One client authors three sequential updates: U1 = X:[0,4),
        // U2 = X:[4,8), U3 = X:[8,12).
        const local = new Y.Doc();
        const updates: Uint8Array[] = [];
        local.on('update', (u: Uint8Array) => updates.push(u));
        local.getText('t').insert(0, 'aaaa');
        local.getText('t').insert(4, 'bbbb');
        local.getText('t').insert(8, 'cccc');
        expect(updates).toHaveLength(3);

        // The server holds U1 and U3 but not U2 (e.g. a dropped save batch),
        // with the same metadata the provider writes for its own saves.
        for (const u of [updates[0], updates[2]]) {
            await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
                update: Bytes.fromUint8Array(u),
                createdAt: serverTimestamp(),
                createdBy: 'seed',
                ...aggregateClockEnds(extractClockEnds(u)),
            });
        }

        // The client holding everything connects and completes initial sync.
        const provider = createProvider(local, path);
        await waitSynced(provider);
        await provider.destroy();

        // A new device must see the full document.
        expect(await readFromFreshClient(path)).toBe('aaaabbbbcccc');
    }, 90000);

    it('pushes pre-existing local content when an edit made during initial sync is saved before the server is read', async () => {
        const path = `integration-tests/push-guard-gap-${getStableDate()}-${counter++}`;

        // A peer already connected to the (empty) document.
        const peer = new Y.Doc();
        await waitSynced(createProvider(peer, path));

        // Local-first content authored before the provider attaches.
        const local = new Y.Doc();
        local.getText('t').insert(0, 'hello');

        // Hold initial sync's first read until the debounced save of the
        // edit below has committed. (Bounded, so an implementation that
        // defers saves until initial sync completes cannot deadlock here.)
        let releaseRead!: () => void;
        mockControls.getDocsGate = new Promise<void>(resolve => {
            releaseRead = resolve;
            setTimeout(resolve, 5000);
        });

        const provider = createProvider(local, path, { maxWaitTime: 1 });
        provider.on('saved', () => releaseRead());

        // The user keeps typing while initial sync is in progress.
        local.getText('t').insert(5, ' world');

        await waitSynced(provider);
        await provider.destroy();

        expect(local.getText('t').toString()).toBe('hello world');
        expect(await readFromFreshClient(path)).toBe('hello world');

        // The peer received the gapped save first; the push that fills the
        // gap must not be skipped as redundant.
        await waitForConditionTruthy(() => peer.getText('t').toString() === 'hello world', {
            timeout: 10000,
            message: 'connected peer should converge',
        });
        expect(pendingStructsOf(peer)).toBeNull();
    }, 90000);

    it('pushes pre-existing local content when an edit made while a failed initial sync retries is saved first', async () => {
        const path = `integration-tests/push-guard-gap-${getStableDate()}-${counter++}`;

        // Local-first content authored before the provider attaches.
        const local = new Y.Doc();
        local.getText('t').insert(0, 'hello');

        // Default timings (clock-skew probe, debounce); the network is down,
        // so the first initial sync fails and retries with backoff.
        mockControls.failGetDocs = true;
        const provider = createProvider(local, path, { cachedClockOffset: undefined });
        const saved = new Promise<void>(resolve => provider.on('saved', () => resolve()));

        // The user keeps typing; the debounced save commits during backoff.
        local.getText('t').insert(5, ' world');
        await saved;
        expect(provider.synced).toBe(false);

        mockControls.failGetDocs = false;
        await waitSynced(provider);
        await provider.destroy();

        expect(await readFromFreshClient(path)).toBe('hello world');
    }, 90000);

    it('pushes pre-existing local content after a provider destroyed before sync is recreated on the same doc', async () => {
        const path = `integration-tests/push-guard-gap-${getStableDate()}-${counter++}`;

        // Local-first content authored before any provider attaches.
        const local = new Y.Doc();
        local.getText('t').insert(0, 'hello');

        // A provider attaches, the user edits, and the provider is torn down
        // before initial sync completes (e.g. a React StrictMode remount or
        // sign-out). destroy() flushes the pending edit.
        const first = createProvider(local, path, { maxWaitTime: 60000 });
        local.getText('t').insert(5, ' world');
        await first.destroy();

        // A new provider on the same doc completes initial sync.
        const second = createProvider(local, path);
        await waitSynced(second);
        await second.destroy();

        expect(await readFromFreshClient(path)).toBe('hello world');
    }, 90000);
});
