/**
 * Regression: a subdocument moved under the SAME guid in TWO transactions
 * (insert the new instance first, delete the original afterwards) must end
 * up with a live provider bound to the new instance.
 *
 * Yjs requires a fresh `new Y.Doc({ guid })` to move a subdoc. When the move
 * is not wrapped in one doc.transact(), or when a peer's two-transaction move
 * arrives in separate deliveries, the parent emits two 'subdocs' events:
 *   1. added={newInstance}   while the original is still in the document
 *   2. removed={original}    (plus the follow-up event from its destroy())
 * The single-event case is covered by subdocs-same-guid-replace.test.ts.
 *
 * Contract: once the events settle, the live subdoc instance in the parent
 * has exactly one running (not destroyed) provider bound to it, and no
 * running provider is left bound to the removed original. Otherwise the
 * moved subdoc never loads its Firestore content and its edits are never
 * saved.
 *
 * The same applies when one guid is referenced twice on purpose and the
 * first reference is deleted or unloaded; destroying the parent still
 * starts no provider for the remaining reference.
 *
 * Uses real Yjs events wired to handleSubdocs exactly like FireProvider does;
 * providers are fakes created through ctx.createProvider, so no Firestore.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as Y from 'yjs';
import { handleSubdocs, type SubdocContext, type SubProviderMap } from '../../src/subdocs';

interface FakeProvider {
    doc: Y.Doc;
    destroyed: boolean;
    destroy: () => Promise<void>;
}

const setup = (mode: 'eager' | 'lazy') => {
    const created: FakeProvider[] = [];
    const ctx: SubdocContext = {
        firebaseApp: {} as any,
        parentPath: 'docs/parent',
        depth: 0,
        maxUpdatesThreshold: 50,
        maxWaitTime: 1000,
        maxAggregationTime: 2000,
        gcCompaction: true,
        historyFoldThreshold: 8,
        lockTTL: 30000,
        compactionLimit: 100,
        subdocLoadingMode: mode,
        createProvider: (config: any) => {
            const p: FakeProvider = {
                doc: config.ydoc,
                destroyed: false,
                // Flips the flag synchronously (async body runs up to first await).
                destroy: async () => {
                    p.destroyed = true;
                },
            };
            created.push(p);
            return p;
        },
    };
    const subProviders: SubProviderMap = new Map();
    const doc = new Y.Doc();
    doc.on('subdocs', (ev: any) => handleSubdocs(ev, ctx, subProviders));

    const liveProvidersFor = (subdoc: Y.Doc) =>
        created.filter(p => p.doc === subdoc && !p.destroyed);
    const providersFor = (subdoc: Y.Doc) => created.filter(p => p.doc === subdoc);
    const liveProviders = () => created.filter(p => !p.destroyed);

    return { doc, liveProvidersFor, providersFor, liveProviders };
};

/** A peer that records each of its transactions as a separate update. */
const makePeer = () => {
    const peer = new Y.Doc();
    const updates: Uint8Array[] = [];
    peer.on('update', (u: Uint8Array) => updates.push(u));
    return { peer, map: peer.getMap<Y.Doc>('m'), takeUpdates: () => updates.splice(0) };
};

beforeEach(() => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
});

afterEach(() => {
    vi.restoreAllMocks();
});

describe('subdoc moved under the same guid in two transactions (insert copy, then delete original)', () => {
    for (const mode of ['eager', 'lazy'] as const) {
        it(`binds a running provider to the moved instance after a local two-step move (${mode} mode)`, () => {
            const { doc, liveProvidersFor, liveProviders } = setup(mode);
            const m = doc.getMap<Y.Doc>('m');

            const original = new Y.Doc({ guid: 'g1' });
            m.set('a', original);
            expect(liveProvidersFor(original)).toHaveLength(1);

            // Move 'a' -> 'b' without wrapping it in one transaction.
            m.set('b', new Y.Doc({ guid: 'g1' })); // transaction 1: insert copy
            m.delete('a');                          // transaction 2: delete original

            expect(m.has('a')).toBe(false);
            const moved = m.get('b')!;
            expect(moved).toBeInstanceOf(Y.Doc);
            expect(moved).not.toBe(original);
            expect(moved.guid).toBe('g1');
            expect(moved.isDestroyed).toBe(false);

            // Nothing keeps syncing the removed original...
            expect(liveProvidersFor(original)).toHaveLength(0);
            // ...and the live instance has exactly one running provider.
            expect(liveProvidersFor(moved)).toHaveLength(1);
            expect(liveProviders()).toHaveLength(1);
        });
    }

    it('binds a running provider to the moved instance when a peer two-step move arrives in separate deliveries (eager mode)', () => {
        const { doc, liveProvidersFor, liveProviders } = setup('eager');
        const { peer, map: peerMap, takeUpdates } = makePeer();

        peerMap.set('a', new Y.Doc({ guid: 'g2' }));
        Y.applyUpdate(doc, Y.encodeStateAsUpdate(peer));
        takeUpdates();

        const m = doc.getMap<Y.Doc>('m');
        const original = m.get('a')!;
        expect(original.guid).toBe('g2');
        expect(liveProvidersFor(original)).toHaveLength(1);

        // Peer moves 'a' -> 'b' in two transactions: insert copy, then delete.
        peerMap.set('b', new Y.Doc({ guid: 'g2' }));
        peerMap.delete('a');
        const [insertCopy, deleteOriginal] = takeUpdates();
        expect(insertCopy).toBeDefined();
        expect(deleteOriginal).toBeDefined();

        // Each transaction reaches this client in its own listener delivery.
        Y.applyUpdate(doc, insertCopy);
        Y.applyUpdate(doc, deleteOriginal);

        expect(m.has('a')).toBe(false);
        const moved = m.get('b')!;
        expect(moved).toBeInstanceOf(Y.Doc);
        expect(moved).not.toBe(original);
        expect(moved.guid).toBe('g2');

        expect(liveProvidersFor(original)).toHaveLength(0);
        expect(liveProvidersFor(moved)).toHaveLength(1);
        expect(liveProviders()).toHaveLength(1);
    });

    it('binds a running provider to a peer-moved instance loaded before the original is deleted (lazy mode)', () => {
        const { doc, liveProvidersFor, liveProviders } = setup('lazy');
        const { peer, map: peerMap, takeUpdates } = makePeer();

        peerMap.set('a', new Y.Doc({ guid: 'g3' }));
        Y.applyUpdate(doc, Y.encodeStateAsUpdate(peer));
        takeUpdates();

        const m = doc.getMap<Y.Doc>('m');
        const original = m.get('a')!;
        original.load();
        expect(liveProvidersFor(original)).toHaveLength(1);

        peerMap.set('b', new Y.Doc({ guid: 'g3' }));
        peerMap.delete('a');
        const [insertCopy, deleteOriginal] = takeUpdates();

        // The copy arrives first and the app loads it right away...
        Y.applyUpdate(doc, insertCopy);
        const moved = m.get('b')!;
        expect(moved).not.toBe(original);
        moved.load();

        // ...then the deletion of the original arrives.
        Y.applyUpdate(doc, deleteOriginal);

        expect(m.has('a')).toBe(false);
        expect(m.get('b')).toBe(moved);
        expect(liveProvidersFor(original)).toHaveLength(0);
        expect(liveProvidersFor(moved)).toHaveLength(1);
        expect(liveProviders()).toHaveLength(1);
    });
});

describe('two instances of one guid referenced on purpose', () => {
    it('keeps the remaining reference synced when the first one is deleted', () => {
        const { doc, liveProvidersFor, liveProviders } = setup('eager');
        const m = doc.getMap<Y.Doc>('m');

        const first = new Y.Doc({ guid: 'r1' });
        m.set('a', first);
        const link = new Y.Doc({ guid: 'r1' });
        m.set('link', link);

        m.delete('a');

        expect(liveProvidersFor(first)).toHaveLength(0);
        expect(liveProvidersFor(link)).toHaveLength(1);
        expect(liveProviders()).toHaveLength(1);
    });

    it('keeps the remaining reference synced when the first one is unloaded', () => {
        const { doc, liveProvidersFor, liveProviders } = setup('eager');
        const m = doc.getMap<Y.Doc>('m');

        const first = new Y.Doc({ guid: 'r2' });
        m.set('a', first);
        const link = new Y.Doc({ guid: 'r2' });
        m.set('link', link);

        first.destroy();

        // The unloaded placeholder for 'a' still waits for load().
        expect(m.get('a')).not.toBe(first);
        expect(liveProvidersFor(m.get('a')!)).toHaveLength(0);
        expect(liveProvidersFor(link)).toHaveLength(1);
        expect(liveProviders()).toHaveLength(1);
    });

    it('starts no provider for the remaining reference when the parent is destroyed', () => {
        const { doc, providersFor, liveProviders } = setup('eager');
        const m = doc.getMap<Y.Doc>('m');

        m.set('a', new Y.Doc({ guid: 'r3' }));
        const link = new Y.Doc({ guid: 'r3' });
        m.set('link', link);

        doc.destroy();

        expect(providersFor(link)).toHaveLength(0);
        expect(liveProviders()).toHaveLength(0);
    });
});
