/**
 * Squash Core Unit Tests
 *
 * Pins the correctness of buildSquashedDoc (the epoch-reset content clone)
 * and the epoch marker helpers:
 *
 *  - materialized content is IDENTICAL across the squash boundary
 *  - tombstone structure, delete-set, and state-vector entries reset to
 *    the live-content floor (the whole point of squashing)
 *  - the epoch marker is stamped and readable
 *  - untyped root shares are rejected instead of silently dropped
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import {
    buildSquashedDoc,
    readDocEpoch,
    docHasContent,
    PROVIDER_META_KEY,
    EPOCH_KEY,
} from '../../src/squash';
import { SeededRandom } from './prng';

/** Ages a doc with versicle-style churn: interleaved map overwrites. */
function buildAgedDoc(sessions: number, opsPerSession: number): Y.Doc {
    const rng = new SeededRandom(42);
    let persisted: Uint8Array | null = null;
    for (let s = 0; s < sessions; s++) {
        const doc = new Y.Doc();
        doc.clientID = 7_000_000 + s;
        if (persisted) Y.applyUpdate(doc, persisted);
        const positions = doc.getMap('positions');
        const annotations = doc.getMap('annotations');
        const list = doc.getArray('list');
        for (let i = 0; i < opsPerSession; i++) {
            positions.set('book' + (i % 10), { cfi: rng.string(30), pct: rng.next() });
            if (i % 7 === 0) {
                annotations.set('a' + s + '-' + i, rng.string(50));
            }
            if (i % 11 === 0) {
                list.push([rng.string(12)]);
                if (list.length > 20) list.delete(0, list.length - 20);
            }
        }
        persisted = Y.encodeStateAsUpdate(doc);
        doc.destroy();
    }
    const out = new Y.Doc();
    Y.applyUpdate(out, persisted!);
    // Type the roots the way an application does (hydration alone leaves
    // them as generic AbstractType placeholders, which squash refuses)
    out.getMap('positions');
    out.getMap('annotations');
    out.getArray('list');
    return out;
}

function structCount(doc: Y.Doc): number {
    let n = 0;
    (doc.store as any).clients.forEach((arr: any[]) => { n += arr.length; });
    return n;
}

function dsRangeCount(doc: Y.Doc): number {
    const ds = Y.createDeleteSetFromStructStore((doc as any).store);
    let n = 0;
    (ds as any).clients.forEach((arr: any[]) => { n += arr.length; });
    return n;
}

describe('buildSquashedDoc', () => {
    it('preserves materialized content exactly while resetting history overhead', () => {
        const aged = buildAgedDoc(30, 60);
        const before = {
            positions: aged.getMap('positions').toJSON(),
            annotations: aged.getMap('annotations').toJSON(),
            list: aged.getArray('list').toJSON(),
        };
        const svBefore = Y.decodeStateVector(Y.encodeStateVector(aged)).size;
        const structsBefore = structCount(aged);
        const dsBefore = dsRangeCount(aged);

        const squashed = buildSquashedDoc(aged, 3);

        expect(squashed.getMap('positions').toJSON()).toEqual(before.positions);
        expect(squashed.getMap('annotations').toJSON()).toEqual(before.annotations);
        expect(squashed.getArray('list').toJSON()).toEqual(before.list);

        // The floor reset: one client, no deletions, far fewer structs
        expect(Y.decodeStateVector(Y.encodeStateVector(squashed)).size).toBe(1);
        expect(dsRangeCount(squashed)).toBe(0);
        expect(structCount(squashed)).toBeLessThan(structsBefore / 2);
        expect(svBefore).toBe(30); // sanity: the aged doc really had 30 clients
        expect(dsBefore).toBeGreaterThan(0);

        // Snapshot bytes shrink toward the live-content floor (this
        // workload keeps a lot of live annotations, so the byte win is
        // smaller than the struct win — struct counts above are the
        // load-time-relevant metric)
        const agedBytes = Y.encodeStateAsUpdate(aged).byteLength;
        const squashedBytes = Y.encodeStateAsUpdate(squashed).byteLength;
        expect(squashedBytes).toBeLessThan(agedBytes * 0.75);

        expect(readDocEpoch(squashed)).toBe(3);
        aged.destroy();
        squashed.destroy();
    });

    it('round-trips nested Y types (maps in maps, arrays of maps, text)', () => {
        const src = new Y.Doc();
        const root = src.getMap('root');
        const nested = new Y.Map();
        root.set('nested', nested);
        nested.set('x', 1);
        const deep = new Y.Map();
        nested.set('deep', deep);
        deep.set('flag', true);
        const arr = new Y.Array();
        root.set('arr', arr);
        const row = new Y.Map();
        arr.push([row]);
        row.set('id', 'r1');
        arr.push(['plain', 42]);
        const text = src.getText('t');
        text.insert(0, 'hello world');
        text.format(0, 5, { bold: true });
        const rootArr = src.getArray('rootArr');
        rootArr.insert(0, ['a', 'b']);

        const squashed = buildSquashedDoc(src, 1);
        expect(squashed.getMap('root').toJSON()).toEqual(root.toJSON());
        expect(squashed.getArray('rootArr').toJSON()).toEqual(rootArr.toJSON());
        expect(squashed.getText('t').toDelta()).toEqual(text.toDelta());

        // The clone must be a live, editable document
        (squashed.getMap('root').get('nested') as Y.Map<unknown>).set('x', 2);
        expect((squashed.getMap('root').toJSON() as any).nested.x).toBe(2);

        src.destroy();
        squashed.destroy();
    });

    it('converges when the squashed state is exchanged between fresh clients', () => {
        const aged = buildAgedDoc(10, 40);
        const squashed = buildSquashedDoc(aged, 1);
        const update = Y.encodeStateAsUpdate(squashed);

        const a = new Y.Doc();
        const b = new Y.Doc();
        Y.applyUpdate(a, update);
        Y.applyUpdate(b, update);
        a.getMap('positions').set('bookNew', { cfi: 'x', pct: 0.5 });
        Y.applyUpdate(b, Y.encodeStateAsUpdate(a, Y.encodeStateVector(b)));
        expect(b.getMap('positions').toJSON()).toEqual(a.getMap('positions').toJSON());

        aged.destroy();
        squashed.destroy();
        a.destroy();
        b.destroy();
    });

    it('rejects untyped root shares that still hold live content', () => {
        // A doc where 'mystery' was written by another client but never
        // accessed through a typed getter here
        const writer = new Y.Doc();
        writer.getMap('mystery').set('live', 1);
        writer.getMap('typed').set('k', 1);
        const src = new Y.Doc();
        Y.applyUpdate(src, Y.encodeStateAsUpdate(writer));
        src.getMap('typed'); // typed by the app; 'mystery' stays abstract
        expect(() => buildSquashedDoc(src, 1)).toThrow(/no concrete type/);
        writer.destroy();
        src.destroy();
    });

    it('drops untyped root shares whose content is entirely deleted (legacy husks)', () => {
        const writer = new Y.Doc();
        const husk = writer.getMap('preferences/old-device');
        husk.set('theme', 'dark');
        husk.set('fontSize', 12);
        // v9-style cleanup: contents emptied, share remains forever
        husk.delete('theme');
        husk.delete('fontSize');
        writer.getMap('data').set('k', 'v');

        const src = new Y.Doc();
        Y.applyUpdate(src, Y.encodeStateAsUpdate(writer));
        src.getMap('data'); // the app types only its known stores

        const squashed = buildSquashedDoc(src, 1);
        expect(squashed.getMap('data').toJSON()).toEqual({ k: 'v' });
        // The husk did not survive into the new epoch
        expect(squashed.share.has('preferences/old-device')).toBe(false);
        writer.destroy();
        src.destroy();
        squashed.destroy();
    });

    it('does not copy the old provider meta but stamps the new epoch', () => {
        const src = new Y.Doc();
        src.getMap(PROVIDER_META_KEY).set(EPOCH_KEY, 4);
        src.getMap(PROVIDER_META_KEY).set('junk', 'old');
        src.getMap('data').set('k', 'v');
        const squashed = buildSquashedDoc(src, 5);
        expect(readDocEpoch(squashed)).toBe(5);
        expect(squashed.getMap(PROVIDER_META_KEY).get('junk')).toBeUndefined();
        src.destroy();
        squashed.destroy();
    });
});

describe('epoch helpers', () => {
    it('readDocEpoch defaults to 0 and reads the marker', () => {
        const doc = new Y.Doc();
        expect(readDocEpoch(doc)).toBe(0);
        doc.getMap(PROVIDER_META_KEY).set(EPOCH_KEY, 7);
        expect(readDocEpoch(doc)).toBe(7);
        doc.destroy();
    });

    it('docHasContent distinguishes fresh from hydrated docs', () => {
        const fresh = new Y.Doc();
        expect(docHasContent(fresh)).toBe(false);
        // getMap alone creates no structs
        fresh.getMap('m');
        expect(docHasContent(fresh)).toBe(false);
        fresh.getMap('m').set('k', 1);
        expect(docHasContent(fresh)).toBe(true);
        fresh.destroy();
    });

    it('re-squashes a hydrated doc whose meta share was never typed locally', () => {
        // A doc bootstrapped from a squashed snapshot has __ycinder as an
        // untyped share unless something called readDocEpoch/getMap on it.
        // Squash must not trip over its own marker (it rewrites it anyway).
        const src = new Y.Doc();
        src.getMap('data').set('k', 'v');
        const gen1 = buildSquashedDoc(src, 1);
        const hydrated = new Y.Doc();
        Y.applyUpdate(hydrated, Y.encodeStateAsUpdate(gen1));
        hydrated.getMap('data'); // the app types only its own stores
        const gen2 = buildSquashedDoc(hydrated, 2);
        expect(readDocEpoch(gen2)).toBe(2);
        expect(gen2.getMap('data').toJSON()).toEqual({ k: 'v' });
        src.destroy(); gen1.destroy(); hydrated.destroy(); gen2.destroy();
    });

    it('the epoch marker survives an encode/apply round trip', () => {
        const src = new Y.Doc();
        src.getMap('data').set('k', 'v');
        const squashed = buildSquashedDoc(src, 9);
        const blob = Y.encodeStateAsUpdate(squashed);
        const hydrated = new Y.Doc();
        Y.applyUpdate(hydrated, blob);
        expect(readDocEpoch(hydrated)).toBe(9);
        src.destroy();
        squashed.destroy();
        hydrated.destroy();
    });
});
