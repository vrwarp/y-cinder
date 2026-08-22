/**
 * Subdocument lifecycle unit tests.
 *
 * subdocs.ts routes every subdocument through `ctx.createProvider`, so it
 * needs no Firestore at all — yet only getSubdocStats was covered, leaving
 * the lazy-loading gate, the recursion-depth guard and the destroy paths
 * unverified. Those matter: a broken lazy gate makes a document with many
 * subdocs open three Firestore listeners each at startup, and a broken
 * depth guard recurses without bound.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as Y from 'yjs';
import {
    destroyAllSubdocs,
    handleSubdocs,
    startSubdocProvider,
    type SubdocContext,
    type SubProviderMap,
} from '../../src/subdocs';
import { DEFAULTS } from '../../src/types';

const makeSubdoc = (guid: string, shouldLoad = false): Y.Doc => {
    const doc = new Y.Doc({ guid });
    doc.shouldLoad = shouldLoad;
    return doc;
};

const makeCtx = (overrides: Partial<SubdocContext> = {}): SubdocContext => ({
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
    subdocLoadingMode: 'eager',
    createProvider: vi.fn((config: any) => ({ config, destroy: vi.fn(async () => undefined) })),
    ...overrides,
} as SubdocContext);

const event = (parts: Partial<{ added: Set<Y.Doc>; removed: Set<Y.Doc>; loaded: Set<Y.Doc> }>) => ({
    added: parts.added ?? new Set<Y.Doc>(),
    removed: parts.removed ?? new Set<Y.Doc>(),
    loaded: parts.loaded ?? new Set<Y.Doc>(),
}) as any;

beforeEach(() => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
});

afterEach(() => {
    vi.restoreAllMocks();
});

describe('startSubdocProvider', () => {
    it('creates a provider at the subdoc path with depth incremented', () => {
        const ctx = makeCtx();
        const map: SubProviderMap = new Map();
        const subdoc = makeSubdoc('guid-a');

        const provider = startSubdocProvider(subdoc, ctx, map);

        expect(provider).not.toBeNull();
        expect(map.get('guid-a')).toBe(provider);

        const config = (ctx.createProvider as any).mock.calls[0][0];

        expect(config.path).toBe('docs/parent/subdocs/guid-a');
        expect(config.depth).toBe(1);
        expect(config.ydoc).toBe(subdoc);
    });

    it('inherits the parent tuning settings verbatim', () => {
        const ctx = makeCtx({ maxUpdatesThreshold: 7, historyFoldThreshold: 3, gcCompaction: false });
        const map: SubProviderMap = new Map();

        startSubdocProvider(makeSubdoc('guid-a'), ctx, map);

        const config = (ctx.createProvider as any).mock.calls[0][0];

        expect(config.maxUpdatesThreshold).toBe(7);
        expect(config.historyFoldThreshold).toBe(3);
        expect(config.gcCompaction).toBe(false);
        expect(config.subdocLoadingMode).toBe('eager');
    });

    it('returns the existing provider instead of creating a second one', () => {
        const ctx = makeCtx();
        const map: SubProviderMap = new Map();
        const subdoc = makeSubdoc('guid-a');

        const first = startSubdocProvider(subdoc, ctx, map);
        const second = startSubdocProvider(subdoc, ctx, map);

        expect(second).toBe(first);
        expect(ctx.createProvider).toHaveBeenCalledTimes(1);
    });

    it('refuses to recurse past the depth limit and reports it', () => {
        const onConnectionError = vi.fn();
        const ctx = makeCtx({ depth: DEFAULTS.MAX_SUBDOC_DEPTH, onConnectionError });
        const map: SubProviderMap = new Map();

        const provider = startSubdocProvider(makeSubdoc('deep'), ctx, map);

        expect(provider).toBeNull();
        expect(ctx.createProvider).not.toHaveBeenCalled();
        expect(map.size).toBe(0);
        expect(onConnectionError).toHaveBeenCalledWith(expect.objectContaining({
            code: 'recursion-limit',
            path: 'docs/parent/subdocs/deep',
        }));
    });

    it('still refuses beyond the limit, not only exactly at it', () => {
        const ctx = makeCtx({ depth: DEFAULTS.MAX_SUBDOC_DEPTH + 3 });

        expect(startSubdocProvider(makeSubdoc('deeper'), ctx, new Map())).toBeNull();
    });

    it('allows the last depth below the limit', () => {
        const ctx = makeCtx({ depth: DEFAULTS.MAX_SUBDOC_DEPTH - 1 });

        expect(startSubdocProvider(makeSubdoc('ok'), ctx, new Map())).not.toBeNull();
    });

    it('does not require an error callback to be present', () => {
        const ctx = makeCtx({ depth: DEFAULTS.MAX_SUBDOC_DEPTH, onConnectionError: undefined });

        expect(() => startSubdocProvider(makeSubdoc('deep'), ctx, new Map())).not.toThrow();
    });
});

describe('handleSubdocs', () => {
    it('starts providers for added subdocs in eager mode regardless of shouldLoad', () => {
        const ctx = makeCtx({ subdocLoadingMode: 'eager' });
        const map: SubProviderMap = new Map();

        handleSubdocs(event({ added: new Set([makeSubdoc('a', false), makeSubdoc('b', true)]) }), ctx, map);

        expect(map.size).toBe(2);
    });

    it('skips remote-arriving subdocs in lazy mode', () => {
        const ctx = makeCtx({ subdocLoadingMode: 'lazy' });
        const map: SubProviderMap = new Map();

        handleSubdocs(event({ added: new Set([makeSubdoc('remote', false)]) }), ctx, map);

        expect(map.size).toBe(0);
        expect(ctx.createProvider).not.toHaveBeenCalled();
    });

    it('still starts locally created subdocs in lazy mode', () => {
        const ctx = makeCtx({ subdocLoadingMode: 'lazy' });
        const map: SubProviderMap = new Map();

        handleSubdocs(event({ added: new Set([makeSubdoc('local', true)]) }), ctx, map);

        expect(map.size).toBe(1);
    });

    it('starts a provider when a lazily skipped subdoc is later loaded', () => {
        const ctx = makeCtx({ subdocLoadingMode: 'lazy' });
        const map: SubProviderMap = new Map();
        const subdoc = makeSubdoc('deferred', false);

        handleSubdocs(event({ added: new Set([subdoc]) }), ctx, map);
        expect(map.size).toBe(0);

        handleSubdocs(event({ loaded: new Set([subdoc]) }), ctx, map);
        expect(map.size).toBe(1);
    });

    it('loads subdocs in the loaded set even in eager mode', () => {
        const ctx = makeCtx();
        const map: SubProviderMap = new Map();

        handleSubdocs(event({ loaded: new Set([makeSubdoc('l')]) }), ctx, map);

        expect(map.size).toBe(1);
    });

    it('destroys and forgets removed subdocs', async () => {
        const ctx = makeCtx();
        const map: SubProviderMap = new Map();
        const subdoc = makeSubdoc('gone', true);

        handleSubdocs(event({ added: new Set([subdoc]) }), ctx, map);

        const provider = map.get('gone') as { destroy: ReturnType<typeof vi.fn> };

        handleSubdocs(event({ removed: new Set([subdoc]) }), ctx, map);

        expect(provider.destroy).toHaveBeenCalled();
        expect(map.has('gone')).toBe(false);
    });

    it('forgets a removed subdoc even when destroy rejects', async () => {
        const ctx = makeCtx({
            createProvider: vi.fn(() => ({ destroy: vi.fn(async () => { throw new Error('nope'); }) })),
        } as Partial<SubdocContext>);
        const map: SubProviderMap = new Map();
        const subdoc = makeSubdoc('bad', true);

        handleSubdocs(event({ added: new Set([subdoc]) }), ctx, map);
        handleSubdocs(event({ removed: new Set([subdoc]) }), ctx, map);
        await Promise.resolve();
        await Promise.resolve();

        expect(map.has('bad')).toBe(false);
        expect(console.error).toHaveBeenCalled();
    });

    it('ignores removal of a subdoc it never tracked', () => {
        const ctx = makeCtx();
        const map: SubProviderMap = new Map();

        expect(() => handleSubdocs(event({ removed: new Set([makeSubdoc('unknown')]) }), ctx, map))
            .not.toThrow();
    });

    it('handles an event with nothing in it', () => {
        const ctx = makeCtx();
        const map: SubProviderMap = new Map();

        handleSubdocs(event({}), ctx, map);

        expect(map.size).toBe(0);
        expect(ctx.createProvider).not.toHaveBeenCalled();
    });
});

describe('destroyAllSubdocs', () => {
    it('destroys every provider and empties the map', async () => {
        const map: SubProviderMap = new Map();
        const first = { destroy: vi.fn(async () => undefined) };
        const second = { destroy: vi.fn(async () => undefined) };

        map.set('a', first);
        map.set('b', second);

        await destroyAllSubdocs(map);

        expect(first.destroy).toHaveBeenCalled();
        expect(second.destroy).toHaveBeenCalled();
        expect(map.size).toBe(0);
    });

    it('destroys the rest even when one fails, and clears regardless', async () => {
        const map: SubProviderMap = new Map();
        const failing = { destroy: vi.fn(async () => { throw new Error('boom'); }) };
        const healthy = { destroy: vi.fn(async () => undefined) };

        map.set('bad', failing);
        map.set('good', healthy);

        await expect(destroyAllSubdocs(map)).resolves.toBeUndefined();

        expect(healthy.destroy).toHaveBeenCalled();
        expect(map.size).toBe(0);
        expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('1 subdoc(s) failed'));
    });

    it('says nothing when every provider destroys cleanly', async () => {
        const map: SubProviderMap = new Map([['a', { destroy: vi.fn(async () => undefined) }]]);

        await destroyAllSubdocs(map);

        expect(console.warn).not.toHaveBeenCalled();
    });

    it('is a no-op on an empty map', async () => {
        const map: SubProviderMap = new Map();

        await expect(destroyAllSubdocs(map)).resolves.toBeUndefined();
        expect(console.warn).not.toHaveBeenCalled();
    });
});
