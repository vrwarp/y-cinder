/**
 * Regression: a subdocument replaced or moved under the SAME guid in one
 * transaction must keep a live provider.
 *
 * Yjs reports "delete old instance + insert new instance with the same guid"
 * as a single 'subdocs' event with added={newInstance}, removed={oldInstance}.
 * This is how a subdoc is moved (Yjs requires a fresh `new Y.Doc({ guid })`),
 * and a peer's move arrives here as a new instance built by readContentDoc.
 *
 * Contract: after the event(s) settle, the live subdoc instance in the parent
 * has exactly one running (not destroyed) provider bound to it, and the
 * provider bound to the removed instance has been destroyed. Otherwise the
 * moved/replaced subdoc never syncs and its edits are never written.
 *
 * Guards for the neighbouring case: subdoc.destroy() (also run for every
 * subdoc when the parent is destroyed) swaps in an unloaded placeholder with
 * the same guid. That unloads the subdoc, so it must not restart a provider.
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

beforeEach(() => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
});

afterEach(() => {
    vi.restoreAllMocks();
});

describe('subdoc replaced with the same guid in one transaction', () => {
    for (const mode of ['eager', 'lazy'] as const) {
        it(`keeps a running provider for the new instance after a local replace (${mode} mode)`, () => {
            const { doc, liveProvidersFor, providersFor } = setup(mode);
            const m = doc.getMap<Y.Doc>('m');

            const original = new Y.Doc({ guid: 'g1' });
            m.set('a', original);
            expect(liveProvidersFor(original)).toHaveLength(1);

            // Move 'a' -> 'b'. Yjs requires a fresh instance with the same guid.
            doc.transact(() => {
                m.delete('a');
                m.set('b', new Y.Doc({ guid: 'g1' }));
            });

            const moved = m.get('b')!;
            expect(moved).toBeInstanceOf(Y.Doc);
            expect(moved).not.toBe(original);
            expect(moved.guid).toBe('g1');

            // The provider bound to the removed instance is torn down...
            expect(providersFor(original).every(p => p.destroyed)).toBe(true);
            // ...and the live instance has exactly one running provider.
            expect(liveProvidersFor(moved)).toHaveLength(1);
        });
    }

    it('keeps a running provider for the new instance when a peer moves the subdoc (eager mode)', () => {
        const { doc, liveProvidersFor, providersFor } = setup('eager');

        // Peer creates the subdoc; this client receives it.
        const peer = new Y.Doc();
        const peerMap = peer.getMap<Y.Doc>('m');
        peerMap.set('a', new Y.Doc({ guid: 'g2' }));
        Y.applyUpdate(doc, Y.encodeStateAsUpdate(peer));

        const m = doc.getMap<Y.Doc>('m');
        const original = m.get('a')!;
        expect(original.guid).toBe('g2');
        expect(liveProvidersFor(original)).toHaveLength(1);

        // Peer moves 'a' -> 'b' in one transaction; this client receives it.
        const before = Y.encodeStateVector(doc);
        peer.transact(() => {
            peerMap.delete('a');
            peerMap.set('b', new Y.Doc({ guid: 'g2' }));
        });
        Y.applyUpdate(doc, Y.encodeStateAsUpdate(peer, before));

        expect(m.has('a')).toBe(false);
        const moved = m.get('b')!;
        expect(moved).toBeInstanceOf(Y.Doc);
        expect(moved).not.toBe(original);
        expect(moved.guid).toBe('g2');

        expect(providersFor(original).every(p => p.destroyed)).toBe(true);
        expect(liveProvidersFor(moved)).toHaveLength(1);
    });

    it('keeps a running provider when a peer move made in two transactions arrives merged (eager mode)', () => {
        const { doc, liveProvidersFor, providersFor } = setup('eager');

        const peer = new Y.Doc();
        const peerMap = peer.getMap<Y.Doc>('m');
        peerMap.set('a', new Y.Doc({ guid: 'g3' }));
        Y.applyUpdate(doc, Y.encodeStateAsUpdate(peer));

        const m = doc.getMap<Y.Doc>('m');
        const original = m.get('a')!;
        expect(liveProvidersFor(original)).toHaveLength(1);

        // Separate transactions, merged the way debounced saves merge them.
        const updates: Uint8Array[] = [];
        peer.on('update', (u: Uint8Array) => updates.push(u));
        peerMap.delete('a');
        peerMap.set('b', new Y.Doc({ guid: 'g3' }));
        Y.applyUpdate(doc, Y.mergeUpdates(updates));

        const moved = m.get('b')!;
        expect(moved).not.toBe(original);
        expect(providersFor(original).every(p => p.destroyed)).toBe(true);
        expect(liveProvidersFor(moved)).toHaveLength(1);
    });

    it('leaves a peer-moved subdoc unsynced until load() (lazy mode)', () => {
        const { doc, liveProvidersFor, providersFor } = setup('lazy');

        const peer = new Y.Doc();
        const peerMap = peer.getMap<Y.Doc>('m');
        peerMap.set('a', new Y.Doc({ guid: 'g4' }));
        Y.applyUpdate(doc, Y.encodeStateAsUpdate(peer));

        const m = doc.getMap<Y.Doc>('m');
        const original = m.get('a')!;
        original.load();
        expect(liveProvidersFor(original)).toHaveLength(1);

        const before = Y.encodeStateVector(doc);
        peer.transact(() => {
            peerMap.delete('a');
            peerMap.set('b', new Y.Doc({ guid: 'g4' }));
        });
        Y.applyUpdate(doc, Y.encodeStateAsUpdate(peer, before));

        const moved = m.get('b')!;
        expect(providersFor(original).every(p => p.destroyed)).toBe(true);
        expect(providersFor(moved)).toHaveLength(0);

        moved.load();
        expect(liveProvidersFor(moved)).toHaveLength(1);
    });
});

describe('subdoc destroyed (same guid swapped for an unloaded placeholder)', () => {
    it('does not sync the placeholder until load() (eager mode)', () => {
        const { doc, liveProvidersFor, providersFor } = setup('eager');
        const m = doc.getMap<Y.Doc>('m');

        const original = new Y.Doc({ guid: 'd1' });
        m.set('a', original);
        expect(liveProvidersFor(original)).toHaveLength(1);

        original.destroy();

        const placeholder = m.get('a')!;
        expect(placeholder).not.toBe(original);
        expect(placeholder.guid).toBe('d1');
        expect(providersFor(original).every(p => p.destroyed)).toBe(true);
        expect(providersFor(placeholder)).toHaveLength(0);

        placeholder.load();
        expect(liveProvidersFor(placeholder)).toHaveLength(1);
    });

    it('starts no providers when the parent is destroyed (eager mode)', () => {
        const { doc, liveProviders } = setup('eager');
        const m = doc.getMap<Y.Doc>('m');
        m.set('a', new Y.Doc({ guid: 'd2' }));
        m.set('b', new Y.Doc({ guid: 'd3' }));
        expect(liveProviders()).toHaveLength(2);

        doc.destroy();

        expect(liveProviders()).toHaveLength(0);
    });

    it('starts no provider for a subdoc inserted and destroyed in one transaction', () => {
        const { doc, liveProviders } = setup('eager');
        const m = doc.getMap<Y.Doc>('m');

        doc.transact(() => {
            const subdoc = new Y.Doc({ guid: 'd4' });
            m.set('a', subdoc);
            subdoc.destroy();
        });

        expect(liveProviders()).toHaveLength(0);
    });
});
