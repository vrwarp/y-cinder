/**
 * Unit tests for the worker-backed merge orchestration.
 *
 * In Node there is no `Worker`, so every previous test of this module took
 * the main-thread fallback and the worker half — request routing, error
 * recovery, timeout fallback, termination — never ran. That half is where
 * the interesting failure modes live: a dropped response hangs a
 * compaction, and a crashed worker that is not disabled hangs every one
 * after it.
 *
 * The Worker contract used here is tiny and standard (postMessage,
 * onmessage, onerror, terminate), so a stub is faithful in a way a
 * Firestore stub would not be.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as Y from 'yjs';

/** Captures the workers the module creates so tests can drive them. */
class FakeWorker {
    static instances: FakeWorker[] = [];
    static failOnConstruct = false;
    static failOnPost = false;

    onmessage: ((event: { data: any }) => void) | null = null;
    onerror: ((event: unknown) => void) | null = null;
    posted: any[] = [];
    terminated = false;

    constructor(public url: string) {
        if (FakeWorker.failOnConstruct) {
            throw new Error('Worker construction blocked (CSP)');
        }
        FakeWorker.instances.push(this);
    }

    postMessage(message: any): void {
        if (FakeWorker.failOnPost) {
            throw new Error('postMessage failed');
        }
        this.posted.push(message);
    }

    terminate(): void {
        this.terminated = true;
    }

    /** Simulate the worker replying to the most recent request. */
    respond(payload: Record<string, unknown>): void {
        const id = this.posted[this.posted.length - 1]?.id;
        this.onmessage?.({ data: { id, ...payload } });
    }

    static latest(): FakeWorker {
        return FakeWorker.instances[FakeWorker.instances.length - 1];
    }

    static reset(): void {
        FakeWorker.instances = [];
        FakeWorker.failOnConstruct = false;
        FakeWorker.failOnPost = false;
    }
}

const g = globalThis as any;

/** Fresh module state per test — the worker singleton is module-level. */
const loadModule = async () => {
    vi.resetModules();
    return import('../../src/merge-utils');
};

const makeUpdate = (clientID: number, count: number): Uint8Array => {
    const doc = new Y.Doc();
    doc.clientID = clientID;
    const map = doc.getMap('m');
    for (let i = 0; i < count; i += 1) {
        doc.transact(() => map.set(`k${i}`, i));
    }
    return Y.encodeStateAsUpdate(doc);
};

const applied = (update: Uint8Array): Record<string, unknown> => {
    const doc = new Y.Doc();
    Y.applyUpdate(doc, update);
    return doc.getMap('m').toJSON();
};

beforeEach(() => {
    FakeWorker.reset();
    vi.spyOn(console, 'debug').mockImplementation(() => undefined);
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    g.Blob = g.Blob ?? class { constructor(public parts: unknown[], public opts: unknown) {} };
    g.URL = g.URL ?? {};
    g.URL.createObjectURL = vi.fn(() => 'blob:fake');
});

afterEach(() => {
    delete g.Worker;
    vi.restoreAllMocks();
    vi.useRealTimers();
});

describe('main-thread fallback (no Worker in the environment)', () => {
    it('reports worker merge as unavailable', async () => {
        const mod = await loadModule();

        expect(mod.isWorkerMergeAvailable()).toBe(false);
    });

    it('merges correctly without a worker', async () => {
        const mod = await loadModule();
        const merged = await mod.mergeUpdatesAsync([makeUpdate(1, 2), makeUpdate(2, 1)]);

        expect(applied(merged)).toEqual({ k0: 0, k1: 1 });
    });

    it('returns an empty result for no updates, without merging', async () => {
        const mod = await loadModule();

        expect(await mod.mergeUpdatesAsync([])).toEqual(new Uint8Array(0));
    });

    it('round-trips a single update through the merge rather than short-circuiting', async () => {
        const mod = await loadModule();
        const merged = await mod.mergeUpdatesAsync([makeUpdate(1, 2)]);

        expect(applied(merged)).toEqual({ k0: 0, k1: 1 });
    });

    /*
     * Pins the real validation boundary, which the source comment used to
     * describe incorrectly: Y.mergeUpdates short-circuits a one-element
     * array and returns the blob unparsed, so a corrupt single update
     * survives the plain merge. Two or more are parsed and rejected. The
     * path compaction uses (with-meta) parses the result either way.
     */
    it('does not reject a corrupt single update (Yjs short-circuits one element)', async () => {
        const mod = await loadModule();
        const garbage = new Uint8Array([255, 255, 255, 255]);

        expect(Array.from(await mod.mergeUpdatesAsync([garbage]))).toEqual([255, 255, 255, 255]);
    });

    it('rejects a corrupt update when merged alongside another', async () => {
        const mod = await loadModule();

        await expect(mod.mergeUpdatesAsync([makeUpdate(1, 1), new Uint8Array([255, 255, 255, 255])]))
            .rejects.toBeDefined();
    });

    it('rejects a corrupt single update on the metadata path compaction uses', async () => {
        const mod = await loadModule();

        await expect(mod.mergeUpdatesWithMetaAsync([new Uint8Array([255, 255, 255, 255])]))
            .rejects.toBeDefined();
    });

    it('derives merge metadata on the main thread', async () => {
        const mod = await loadModule();
        const meta = await mod.mergeUpdatesWithMetaAsync([makeUpdate(1, 2)]);

        expect(meta.result.byteLength).toBeGreaterThan(0);
        expect(meta.stateVector.byteLength).toBeGreaterThan(0);
        expect(meta.dsUpdate).toBeDefined();
    });
});

describe('worker path', () => {
    beforeEach(() => {
        g.Worker = FakeWorker;
    });

    it('reports worker merge as available and creates exactly one worker', async () => {
        const mod = await loadModule();

        expect(mod.isWorkerMergeAvailable()).toBe(true);
        expect(mod.isWorkerMergeAvailable()).toBe(true);
        expect(FakeWorker.instances).toHaveLength(1);
    });

    it('routes a merge through the worker and resolves with its result', async () => {
        const mod = await loadModule();
        const expected = makeUpdate(1, 1);
        const promise = mod.mergeUpdatesAsync([makeUpdate(1, 1)]);

        await vi.waitFor(() => { expect(FakeWorker.latest().posted).toHaveLength(1); });
        expect(FakeWorker.latest().posted[0]).toMatchObject({ gc: false, meta: false });
        FakeWorker.latest().respond({ result: expected });

        expect(await promise).toBe(expected);
    });

    it('passes the gc flag through to the worker', async () => {
        const mod = await loadModule();
        const promise = mod.mergeUpdatesAsync([makeUpdate(1, 1)], { gc: true });

        await vi.waitFor(() => { expect(FakeWorker.latest().posted).toHaveLength(1); });
        expect(FakeWorker.latest().posted[0]).toMatchObject({ gc: true });
        FakeWorker.latest().respond({ result: makeUpdate(1, 1) });
        await promise;
    });

    it('requests metadata from the worker and returns what it sends back', async () => {
        const mod = await loadModule();
        const promise = mod.mergeUpdatesWithMetaAsync([makeUpdate(1, 1)]);

        await vi.waitFor(() => { expect(FakeWorker.latest().posted).toHaveLength(1); });
        expect(FakeWorker.latest().posted[0]).toMatchObject({ meta: true });

        const payload = {
            result: makeUpdate(1, 1),
            stateVector: new Uint8Array([1]),
            dsUpdate: new Uint8Array([2]),
        };

        FakeWorker.latest().respond(payload);

        expect(await promise).toEqual(payload);
    });

    it('derives metadata itself when the worker omits it', async () => {
        const mod = await loadModule();
        const promise = mod.mergeUpdatesWithMetaAsync([makeUpdate(1, 2)]);

        await vi.waitFor(() => { expect(FakeWorker.latest().posted).toHaveLength(1); });
        // Result only — no stateVector/dsUpdate.
        FakeWorker.latest().respond({ result: makeUpdate(1, 2) });

        const meta = await promise;

        expect(meta.stateVector.byteLength).toBeGreaterThan(0);
        expect(meta.dsUpdate).toBeDefined();
    });

    it('rejects the caller when the worker reports an error', async () => {
        const mod = await loadModule();
        const promise = mod.mergeUpdatesAsync([makeUpdate(1, 1)]);

        await vi.waitFor(() => { expect(FakeWorker.latest().posted).toHaveLength(1); });
        FakeWorker.latest().respond({ error: 'merge blew up' });

        await expect(promise).rejects.toThrow('merge blew up');
    });

    it('ignores the ready handshake and unknown response ids', async () => {
        const mod = await loadModule();
        const promise = mod.mergeUpdatesAsync([makeUpdate(1, 1)]);

        await vi.waitFor(() => { expect(FakeWorker.latest().posted).toHaveLength(1); });

        const worker = FakeWorker.latest();

        worker.onmessage?.({ data: { type: 'ready' } });
        worker.onmessage?.({ data: { id: 'nobody-is-waiting', result: new Uint8Array([9]) } });

        // The real request is still outstanding and still resolvable.
        const expected = makeUpdate(1, 1);

        worker.respond({ result: expected });
        expect(await promise).toBe(expected);
    });

    it('falls back to the main thread when the worker never answers', async () => {
        vi.useFakeTimers();

        const mod = await loadModule();
        const promise = mod.mergeUpdatesAsync([makeUpdate(1, 2)]);

        await vi.waitFor(() => { expect(FakeWorker.latest().posted).toHaveLength(1); });
        await vi.advanceTimersByTimeAsync(30_000);

        expect(applied(await promise)).toEqual({ k0: 0, k1: 1 });
    });

    it('rejects when the worker refuses the message', async () => {
        const mod = await loadModule();

        // Force the worker to exist, then make posting fail.
        expect(mod.isWorkerMergeAvailable()).toBe(true);
        FakeWorker.failOnPost = true;

        await expect(mod.mergeUpdatesAsync([makeUpdate(1, 1)])).rejects.toThrow('postMessage failed');
    });

    it('rejects every in-flight request and disables the worker when it crashes', async () => {
        const mod = await loadModule();
        const first = mod.mergeUpdatesAsync([makeUpdate(1, 1)]);
        const second = mod.mergeUpdatesAsync([makeUpdate(2, 1)]);

        await vi.waitFor(() => { expect(FakeWorker.latest().posted).toHaveLength(2); });
        FakeWorker.latest().onerror?.({ message: 'boom' });

        await expect(first).rejects.toThrow('Worker crashed');
        await expect(second).rejects.toThrow('Worker crashed');
        // Disabled, so later merges take the main thread rather than hanging.
        expect(mod.isWorkerMergeAvailable()).toBe(false);
        expect(applied(await mod.mergeUpdatesAsync([makeUpdate(1, 2)]))).toEqual({ k0: 0, k1: 1 });
    });

    it('falls back permanently when worker construction is blocked', async () => {
        FakeWorker.failOnConstruct = true;

        const mod = await loadModule();

        expect(mod.isWorkerMergeAvailable()).toBe(false);
        expect(applied(await mod.mergeUpdatesAsync([makeUpdate(1, 1)]))).toEqual({ k0: 0 });
    });

    it('terminates the worker and rejects anything still in flight', async () => {
        const mod = await loadModule();
        const promise = mod.mergeUpdatesAsync([makeUpdate(1, 1)]);

        await vi.waitFor(() => { expect(FakeWorker.latest().posted).toHaveLength(1); });

        const worker = FakeWorker.latest();

        mod.terminateMergeWorker();

        await expect(promise).rejects.toThrow('Merge worker terminated');
        expect(worker.terminated).toBe(true);
    });

    it('can restart after termination', async () => {
        const mod = await loadModule();

        expect(mod.isWorkerMergeAvailable()).toBe(true);
        mod.terminateMergeWorker();
        expect(mod.isWorkerMergeAvailable()).toBe(true);
        expect(FakeWorker.instances).toHaveLength(2);
    });

    it('terminating without a worker is a no-op', async () => {
        delete g.Worker;

        const mod = await loadModule();

        expect(() => { mod.terminateMergeWorker(); }).not.toThrow();
    });
});
