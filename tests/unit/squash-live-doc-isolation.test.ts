/**
 * Squash must never mutate the LIVE document it clones.
 *
 * buildSquashedDoc builds a throwaway clone that squashDocument encodes
 * and then destroys — whether or not the squash goes on to succeed
 * (upload error, lost lock and preemption all abort AFTER the clone was
 * built and destroyed). The application keeps using the live doc after a
 * failed squash, so building/destroying the clone (or refusing to build
 * it) must leave every live subdocument and every embedded Y type exactly
 * as it was.
 *
 * Regression: cloneValue only cloned AbstractType values, so a subdocument
 * (a Y.Doc value) was inserted into the clone as the SAME instance and got
 * re-parented there; destroying the clone then destroyed the live subdoc,
 * which can never be loaded again in this session. Y.Text embeds were
 * passed through toDelta()/applyDelta() as the integrated live instances,
 * which detached them from the live doc: later edits to the embed emitted
 * no live 'update' and never reached peers.
 */
import { describe, it, expect, vi, afterEach } from 'vitest';
import * as Y from 'yjs';
import { buildSquashedDoc } from '../../src/squash';

/**
 * Runs a squash attempt the way squashDocument does: build the clone,
 * then always destroy it. Refusing to build (throwing) is an acceptable
 * outcome; touching the live doc is not.
 */
function attemptSquash(live: Y.Doc, epoch: number): Y.Doc | null {
    try {
        return buildSquashedDoc(live, epoch);
    } catch {
        return null;
    }
}

describe('squash leaves the live document untouched', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('does not destroy or detach an unloaded (lazy) subdocument of the live doc', () => {
        // Yjs warns on stderr when a subdoc instance is integrated twice;
        // keep the run quiet while still exercising the real code path.
        vi.spyOn(console, 'error').mockImplementation(() => {});

        // A peer created the subdocument; the live client received it via
        // sync, so it is present but not loaded (lazy-mode subdoc, no
        // provider attached). buildSquashedDoc is public API, so it must
        // be safe on its own, not only behind the provider's guard.
        const peer = new Y.Doc();
        peer.getMap('m').set('child', new Y.Doc({ guid: 'child-guid' }));
        const live = new Y.Doc();
        Y.applyUpdate(live, Y.encodeStateAsUpdate(peer));
        const m = live.getMap('m');
        const child = m.get('child') as Y.Doc;
        expect(child).toBeInstanceOf(Y.Doc);
        expect(child.shouldLoad).toBe(false);
        expect(live.subdocs.has(child)).toBe(true);

        const squashed = attemptSquash(live, 1);
        if (squashed) {
            // A successful clone must still reference the subdocument
            const cloned = squashed.getMap('m').get('child') as Y.Doc;
            expect(cloned).toBeInstanceOf(Y.Doc);
            expect(cloned.guid).toBe('child-guid');
        }
        // squashDocument ALWAYS destroys the clone (squash.ts finally)
        squashed?.destroy();

        // The live subdoc is still alive and still belongs to the live doc
        expect(child.isDestroyed).toBe(false);
        expect(m.get('child')).toBe(child);
        expect(live.subdocs.has(child)).toBe(true);

        // ...and can still be loaded: the live doc announces it, which is
        // what makes the provider attach a sub-provider for it.
        const loadedEvents: Y.Doc[] = [];
        live.on('subdocs', ({ loaded }: { loaded: Set<Y.Doc> }) => {
            loaded.forEach((d) => loadedEvents.push(d));
        });
        child.load();
        expect(loadedEvents).toContain(child);

        peer.destroy();
        live.destroy();
    });

    it('does not detach a Y type embedded in a live Y.Text', () => {
        const live = new Y.Doc();
        const text = live.getText('t');
        text.insert(0, 'hello world');
        const embed = new Y.Map<number>();
        text.insertEmbed(5, embed);
        embed.set('k', 1);

        const squashed = attemptSquash(live, 1);
        if (squashed) {
            // A successful clone must preserve the embed's content
            const delta = squashed.getText('t').toDelta() as Array<{ insert: unknown }>;
            const clonedEmbed = delta.map((op) => op.insert).find((ins) => ins instanceof Y.Map) as Y.Map<number> | undefined;
            expect(clonedEmbed?.toJSON()).toEqual({ k: 1 });
            expect(clonedEmbed).not.toBe(embed);
        }
        squashed?.destroy();

        // The live doc still re-encodes correctly (a detached embed's items
        // would reference a parent that exists only in the dead clone)
        const before = new Y.Doc();
        Y.applyUpdate(before, Y.encodeStateAsUpdate(live));
        expect((before.store as any).pendingStructs).toBeNull();
        before.destroy();

        // Edits to the embed still produce live updates...
        const liveUpdates: Uint8Array[] = [];
        live.on('update', (u: Uint8Array) => liveUpdates.push(u));
        expect(() => embed.set('k', 2)).not.toThrow();
        expect(liveUpdates.length).toBe(1);

        // ...and a peer syncing the live doc sees the edited embed.
        const peer = new Y.Doc();
        Y.applyUpdate(peer, Y.encodeStateAsUpdate(live));
        const peerDelta = peer.getText('t').toDelta() as Array<{ insert: unknown }>;
        const peerEmbed = peerDelta.map((op) => op.insert).find((ins) => ins instanceof Y.Map) as Y.Map<number> | undefined;
        expect(peerEmbed?.toJSON()).toEqual({ k: 2 });

        // The embed still belongs to the live doc
        expect(embed.doc === live).toBe(true);

        peer.destroy();
        live.destroy();
    });

    /*
     * Nested types go through cloneValue, where Yjs's own clone() used to
     * hand the same live embed / subdoc instances to the clone without
     * throwing — the root-level fix alone would miss these.
     */
    it.each([
        {
            layout: 'Y.Text embed inside a Y.Map',
            build: (live: Y.Doc, embed: Y.Map<number>) => {
                const text = new Y.Text('hello');
                live.getMap('m').set('text', text);
                text.insertEmbed(2, embed);
            },
        },
        {
            layout: 'Y.XmlText embed inside a root Y.XmlFragment',
            build: (live: Y.Doc, embed: Y.Map<number>) => {
                const p = new Y.XmlElement('p');
                live.getXmlFragment('x').insert(0, [p]);
                const text = new Y.XmlText('hello');
                p.insert(0, [text]);
                text.insertEmbed(2, embed);
            },
        },
        {
            layout: 'Y.XmlHook value inside a root Y.XmlFragment',
            build: (live: Y.Doc, embed: Y.Map<number>) => {
                const hook = new Y.XmlHook('h');
                live.getXmlFragment('x').insert(0, [hook]);
                hook.set('embed', embed);
            },
        },
    ])('does not detach a nested Y type ($layout)', ({ build }) => {
        const live = new Y.Doc();
        const embed = new Y.Map<number>();
        build(live, embed);
        embed.set('k', 1);

        attemptSquash(live, 1)?.destroy();

        expect(embed.doc === live).toBe(true);
        const liveUpdates: Uint8Array[] = [];
        live.on('update', (u: Uint8Array) => liveUpdates.push(u));
        expect(() => embed.set('k', 2)).not.toThrow();
        expect(liveUpdates.length).toBe(1);

        const peer = new Y.Doc();
        Y.applyUpdate(peer, Y.encodeStateAsUpdate(live));
        expect((peer.store as any).pendingStructs).toBeNull();

        peer.destroy();
        live.destroy();
    });

    it('does not destroy a subdocument nested inside a Y.Map value', () => {
        vi.spyOn(console, 'error').mockImplementation(() => {});

        const peer = new Y.Doc();
        const inner = new Y.Map();
        peer.getMap('m').set('inner', inner);
        inner.set('child', new Y.Doc({ guid: 'nested-guid' }));
        const live = new Y.Doc();
        Y.applyUpdate(live, Y.encodeStateAsUpdate(peer));
        const child = (live.getMap('m').get('inner') as Y.Map<unknown>).get('child') as Y.Doc;
        expect(live.subdocs.has(child)).toBe(true);

        const squashed = attemptSquash(live, 1);
        if (squashed) {
            const cloned = (squashed.getMap('m').get('inner') as Y.Map<unknown>).get('child') as Y.Doc;
            expect(cloned.guid).toBe('nested-guid');
            expect(cloned).not.toBe(child);
        }
        squashed?.destroy();

        expect(child.isDestroyed).toBe(false);
        expect(live.subdocs.has(child)).toBe(true);

        peer.destroy();
        live.destroy();
    });
});
