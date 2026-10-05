/**
 * Regression test: a compaction that merges update documents across a
 * server-side clock gap must not hide that gap from the initial-sync push
 * guard.
 *
 * Background (771070f): an update document can be written without the
 * same client's earlier structs, e.g. the destroy() flush of a provider
 * that never completed initial sync uploads only the edits made after it
 * was constructed, while older local-first content of the same clientID is
 * still unsaved. The server then holds X:[b, c) but not X:[a, b). The fix
 * relies on the holder's NEXT initial sync to heal this: the push guard
 * computes, per client, the clock up to which the server holds every
 * struct, and pushes whatever lies beyond it.
 *
 * Bug: compaction can turn such a gap into stored metadata that claims it.
 * - FOLD: the GC rebuild of the merged snapshot leaves pendingStructs, so
 *   the stored state vector falls back to the merged update's clock ENDS
 *   (X:c), and the push guard treats a snapshot state vector as the
 *   contiguous range [0, sv).
 * - DELTA: merging X:[0, a) and X:[b, c) into one history segment yields a
 *   blob whose per-client range reads as X:[0, c).
 * Either way the holder's next initial sync believes the server covers
 * every local struct and pushes nothing, on that and every later session.
 * Every other device keeps the client's later structs in pendingStructs
 * forever.
 *
 * A second gap producer (stale epoch, 424d227 incomplete): a provider
 * constructed before local persistence hydrates a squashed document reads
 * epoch 0 and tags a save made before initial sync with it; epoch-N peers
 * and compaction drop that save, while a re-created provider tags its
 * later save with the right epoch, leaving the same kind of gap.
 *
 * Contract asserted: once a peer has compacted, the device holding the
 * missing range pushes it at its next successful initial sync, and a peer
 * that stayed connected as well as a brand-new device converge on the full
 * document with nothing left in pendingStructs.
 *
 * @file compaction_clock_gap_coverage.test.ts
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

/**
 * `failGetDocs` simulates a network outage for collection reads, so a
 * provider's initial sync fails and retries with backoff while its saves
 * (single-document writes) still commit.
 */
const mockControls: { failGetDocs: boolean } = { failGetDocs: false };

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getDocs: async (q: any) => {
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
import { readDocEpoch } from '../../src/squash';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { addDoc, collection, Bytes, serverTimestamp, getDocs, getDoc, doc, setDoc, deleteField } from '@firebase/firestore';
import { toBase64 } from 'lib0/buffer';
import { waitFor, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';
import { extractClockEnds, aggregateClockEnds } from '../../src/update-metadata';
import { FIRESTORE_PATHS } from '../../src/types';

describe('Compaction across a server-side clock gap', () => {
    let app: any;
    let db: any;
    let counter = 0;
    const providers: FireProvider[] = [];

    const createProvider = (ydoc: Y.Doc, path: string, config: Record<string, any> = {}) => {
        const p = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            // Skip the clock-skew probe.
            cachedClockOffset: 0,
            // Only the explicit compact() calls below compact.
            maxUpdatesThreshold: 1000,
            ...config,
        });
        providers.push(p);
        return p;
    };

    const newPath = (name: string) =>
        `integration-tests/compaction-clock-gap-${name}-${getStableDate()}-${Date.now()}-${counter++}`;

    const waitSynced = (p: FireProvider, who = 'provider') =>
        waitForConditionTruthy(() => p.synced, { timeout: 30000, message: `${who} should sync` });

    /** Structs Yjs could not integrate for lack of their predecessors. */
    const hasPendingStructs = (d: Y.Doc) => (d.store as any).pendingStructs !== null;

    /** Resolves on the provider's next 'saved' event, or after `ms`. */
    const nextSaveOrTimeout = (p: FireProvider, ms: number) =>
        new Promise<void>(resolve => {
            const timer = setTimeout(resolve, ms);
            p.on('saved', () => { clearTimeout(timer); resolve(); });
        });

    /** Seeds an update document exactly as a provider's save would write it. */
    const seedUpdate = (path: string, update: Uint8Array) =>
        addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
            update: Bytes.fromUint8Array(update),
            createdAt: serverTimestamp(),
            createdBy: 'seed',
            ...aggregateClockEnds(extractClockEnds(update)),
        });

    /** Server layout after a compaction, for diagnosis only. */
    const describeServer = async (path: string) => {
        const main = (await getDoc(doc(db, path))).data() ?? {};
        const history = await getDocs(collection(db, path, FIRESTORE_PATHS.HISTORY));
        const updates = await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES));
        return `snapshot=${Boolean(main.snapshotStoragePath)} version=${main.version ?? 0} ` +
            `epoch=${main.epoch ?? 0} history=${history.size} updates=${updates.size}`;
    };

    /** Per update document: its epoch tag and the clock ranges it holds of `clientID`. */
    const describeUpdatesOf = async (path: string, clientID: number) => {
        const updates = await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES));
        return updates.docs.map(d => {
            const data = d.data();
            if (!data.update) return `epoch=${data.epoch ?? 0} (storage-backed)`;
            const meta = Y.parseUpdateMeta(data.update.toUint8Array());
            return `epoch=${data.epoch ?? 0} range=[${meta.from.get(clientID) ?? '-'},${meta.to.get(clientID) ?? '-'})`;
        }).join('; ');
    };

    /** A peer that stayed connected must converge on `expected` with nothing pending. */
    const expectPeerConverges = async (peer: Y.Doc, read: (d: Y.Doc) => string, expected: string) => {
        const view = await waitFor(
            () => ({ text: read(peer), pending: hasPendingStructs(peer) }),
            v => v.text === expected && !v.pending,
            { timeout: 10000, interval: 100 },
        ).catch(() => ({ text: read(peer), pending: hasPendingStructs(peer) }));
        expect(view).toEqual({ text: expected, pending: false });
    };

    /** What a brand-new device sees after initial sync of `path`. */
    const expectFreshClientSees = async (path: string, read: (d: Y.Doc) => string, expected: string) => {
        const fresh = new Y.Doc();
        const reader = createProvider(fresh, path);
        await waitSynced(reader, 'fresh reader');
        const view = { text: read(fresh), pending: hasPendingStructs(fresh) };
        await reader.destroy();
        expect(view).toEqual({ text: expected, pending: false });
    };

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        mockControls.failGetDocs = false;
    });

    afterEach(async () => {
        mockControls.failGetDocs = false;
        for (const p of providers.splice(0)) {
            await p.destroy().catch(() => { /* already destroyed */ });
        }
    });

    const text = (d: Y.Doc) => d.getText('t').toString();

    it('pushes local-first content at the next session after a peer folded the edit flushed by a provider destroyed before sync', async () => {
        const path = newPath('destroy-flush-fold');

        // Session 1: local-first content (X:[0,5)), a provider attaches, the
        // user types (X:[5,11)), and the provider is torn down before
        // initial sync completes (StrictMode remount, sign-out, app close).
        // destroy() flushes the pending edit.
        const local = new Y.Doc();
        local.getText('t').insert(0, 'hello');
        const first = createProvider(local, path, { maxWaitTime: 60000 });
        local.getText('t').insert(5, ' world');
        await first.destroy();

        // Another device is online and compacts what the server holds.
        const peer = new Y.Doc();
        const peerProvider = createProvider(peer, path);
        await waitSynced(peerProvider, 'peer');
        await peerProvider.compact().catch(e => console.log('[destroy-flush-fold] compact() rejected:', e));
        console.log(`[destroy-flush-fold] after compaction: ${await describeServer(path)}`);

        // Session 2 on the same device: local persistence restores the doc
        // before the provider is constructed, and initial sync completes.
        const restored = new Y.Doc();
        Y.applyUpdate(restored, Y.encodeStateAsUpdate(local), 'idb');
        const second = createProvider(restored, path);
        await waitSynced(second, 'second session');
        await second.destroy();
        console.log(`[destroy-flush-fold] after the next session: ${await describeServer(path)}`);

        await expectPeerConverges(peer, text, 'hello world');
        await expectFreshClientSees(path, text, 'hello world');
    }, 120000);

    // Synthetic gap: constrains compaction and the push guard, not how the
    // gap came about. One client authors U1 = X:[0,4), U2 = X:[4,8),
    // U3 = X:[8,12); the server holds U1 and U3 but not U2.
    const variants = [
        { name: 'a fold into the first snapshot', base: false, compactorConfig: {} },
        { name: 'a fold onto an existing snapshot', base: true, compactorConfig: { historyFoldThreshold: 1 } },
        { name: 'a delta history segment', base: true, compactorConfig: {} },
    ] as const;

    for (const variant of variants) {
        it(`pushes the missing range at the holder's next initial sync after ${variant.name} merged across the gap`, async () => {
            const path = newPath(variant.name.replace(/\W+/g, '-'));

            if (variant.base) {
                // An unrelated client establishes a base snapshot.
                const z = new Y.Doc();
                const zProvider = createProvider(z, path, { maxWaitTime: 20 });
                await waitSynced(zProvider, 'base writer');
                z.getText('other').insert(0, 'zzz');
                await waitForConditionTruthy(
                    async () => (await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES))).size > 0,
                    { timeout: 20000, message: 'base content saved' });
                await zProvider.compact();
                expect((await getDoc(doc(db, path))).data()?.snapshotStoragePath).toBeTruthy();
                await zProvider.destroy();
            }

            const local = new Y.Doc();
            const updates: Uint8Array[] = [];
            local.on('update', (u: Uint8Array) => updates.push(u));
            local.getText('t').insert(0, 'aaaa');
            local.getText('t').insert(4, 'bbbb');
            local.getText('t').insert(8, 'cccc');
            expect(updates).toHaveLength(3);
            await seedUpdate(path, updates[0]);
            await seedUpdate(path, updates[2]);

            // A peer compacts what the server holds, and stays connected.
            const peer = new Y.Doc();
            const peerProvider = createProvider(peer, path, variant.compactorConfig);
            await waitSynced(peerProvider, 'peer');
            await peerProvider.compact().catch(e => console.log(`[${variant.name}] compact() rejected:`, e));
            console.log(`[${variant.name}] after compaction: ${await describeServer(path)}`);

            // The client holding everything connects and completes initial sync.
            const holder = createProvider(local, path);
            await waitSynced(holder, 'holder');
            await holder.destroy();
            console.log(`[${variant.name}] after the holder's sync: ${await describeServer(path)}`);

            await expectPeerConverges(peer, text, 'aaaabbbbcccc');
            await expectFreshClientSees(path, text, 'aaaabbbbcccc');
        }, 120000);
    }

    // Legacy data (field report): snapshots folded by clients from before
    // fee33ab stored the merge's clock ENDS as the snapshot state vector,
    // claiming the gap. That stored data outlives the writer fix: a holder
    // trusting it never pushes the missing range, on any session, and every
    // other device keeps the holder's later structs parked forever.
    it("pushes the missing range when a legacy snapshot's stored state vector claims the gap", async () => {
        const path = newPath('legacy-snapshot-sv');

        const local = new Y.Doc();
        const updates: Uint8Array[] = [];
        local.on('update', (u: Uint8Array) => updates.push(u));
        local.getText('t').insert(0, 'aaaa');
        local.getText('t').insert(4, 'bbbb');
        local.getText('t').insert(8, 'cccc');
        await seedUpdate(path, updates[0]);
        await seedUpdate(path, updates[2]);

        // A peer folds what the server holds into the first snapshot.
        const peer = new Y.Doc();
        const peerProvider = createProvider(peer, path);
        await waitSynced(peerProvider, 'peer');
        await peerProvider.compact();
        const folded = (await getDoc(doc(db, path))).data();
        expect(folded?.snapshotStoragePath).toBeTruthy();

        // Rewrite the main doc as a pre-fee33ab fold left it: the state
        // vector is the clock ends (X:12, across the gap) and nothing marks
        // it as contiguous.
        const clockEnds = Y.encodeStateVector(Y.parseUpdateMeta(Y.mergeUpdates([updates[0], updates[2]])).to);
        await setDoc(doc(db, path), {
            stateVector: toBase64(clockEnds),
            stateVectorContiguous: deleteField(),
        }, { merge: true });

        // The holder's next session must push X:[4,8).
        const holder = createProvider(local, path);
        await waitSynced(holder, 'holder');
        await holder.destroy();

        await expectPeerConverges(peer, text, 'aaaabbbbcccc');
        await expectFreshClientSees(path, text, 'aaaabbbbcccc');
    }, 120000);

    it('marks a folded snapshot state vector as contiguous', async () => {
        const path = newPath('snapshot-sv-marker');
        const writer = new Y.Doc();
        const updates: Uint8Array[] = [];
        writer.on('update', (u: Uint8Array) => updates.push(u));
        writer.getText('t').insert(0, 'abc');
        await seedUpdate(path, updates[0]);
        const p = createProvider(new Y.Doc(), path);
        await waitSynced(p, 'compactor');
        await p.compact();
        const main = (await getDoc(doc(db, path))).data();
        expect(main?.stateVector).toBeTruthy();
        expect(main?.stateVectorContiguous).toBe(true);
    }, 60000);

    it('pushes edits saved under a stale epoch before initial sync once a peer folded the later, correctly tagged save', async () => {
        const path = newPath('stale-epoch-fold');
        const read = (d: Y.Doc) => d.getText('t').toString();

        // --- A document squashed into epoch 1 ---
        const docS = new Y.Doc();
        const providerS = createProvider(docS, path, { maxWaitTime: 20 });
        await waitSynced(providerS, 'S');
        docS.getText('t').insert(0, 'hello');
        await waitForConditionTruthy(
            async () => (await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES))).size > 0,
            { timeout: 20000, message: 'S content saved' });
        const squashed = await providerS.squash();
        expect(squashed.success).toBe(true);
        await providerS.destroy();

        // --- Device W's previous session bootstrapped epoch 1; its state is
        // what local persistence (y-idb) holds. ---
        const docPrev = new Y.Doc();
        const providerPrev = createProvider(docPrev, path);
        await waitSynced(providerPrev, 'previous session');
        expect(read(docPrev)).toBe('hello');
        expect(readDocEpoch(docPrev)).toBe(1);
        await providerPrev.destroy();
        const persisted = Y.encodeStateAsUpdate(docPrev);

        // --- A live epoch-1 peer, which will fold ---
        const peer = new Y.Doc();
        const peerProvider = createProvider(peer, path, { historyFoldThreshold: 1 });
        await waitSynced(peerProvider, 'peer');
        expect(read(peer)).toBe('hello');

        // --- Device W launches with collection reads failing, so initial
        // sync keeps retrying. The provider is constructed before local
        // persistence hydrates the doc (with localReady, as the README
        // allows); the user edits and the edit is saved. ---
        mockControls.failGetDocs = true;
        const docW = new Y.Doc();
        let hydrated!: () => void;
        const localReady = new Promise<void>(resolve => { hydrated = resolve; });
        const w1 = createProvider(docW, path, { maxWaitTime: 20, localReady });
        const w1Saved = nextSaveOrTimeout(w1, 5000);
        Y.applyUpdate(docW, persisted, 'idb');
        hydrated();
        docW.getText('t').insert(5, ' w');
        await w1Saved;
        expect(w1.synced).toBe(false);

        // The app re-creates the provider on the hydrated doc; the user
        // keeps typing and that edit is saved too.
        await w1.destroy();
        const w2 = createProvider(docW, path, { maxWaitTime: 20 });
        const w2Saved = nextSaveOrTimeout(w2, 5000);
        docW.getText('t').insert(7, 'orld');
        await w2Saved;
        expect(w2.synced).toBe(false);

        // The app closes before initial sync completes.
        await w2.destroy();
        expect(read(docW)).toBe('hello world');
        mockControls.failGetDocs = false;
        console.log(`[stale-epoch-fold] W's update docs before compaction: ${await describeUpdatesOf(path, docW.clientID)}`);

        // The peer folds what the server holds.
        await peerProvider.compact().catch(e => console.log('[stale-epoch-fold] compact() rejected:', e));
        console.log(`[stale-epoch-fold] after compaction: ${await describeServer(path)}`);

        // Next launch of W: initial sync completes.
        const w3 = createProvider(docW, path);
        await waitSynced(w3, 'W next session');
        await w3.destroy();
        console.log(`[stale-epoch-fold] after W's next session: ${await describeServer(path)}`);

        await expectPeerConverges(peer, read, 'hello world');
        await expectFreshClientSees(path, read, 'hello world');
    }, 120000);
});
