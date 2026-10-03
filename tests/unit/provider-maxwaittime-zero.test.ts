/**
 * Regression: `maxWaitTime: 0` ("save immediately, no debounce") must not
 * crash construction with an error about `maxAggregationTime`, an option the
 * caller never set.
 *
 * The default `maxAggregationTime` is derived as
 * `maxWaitTime * MAX_AGGREGATION_MULTIPLIER`, so a zero (or negative)
 * `maxWaitTime` derives a non-positive aggregation cap, and config
 * validation then rejects the derived value with
 * "Invalid maxAggregationTime: 0. Must be positive.".
 *
 * Contract asserted here (valid for any reasonable fix): a zero or
 * negative `maxWaitTime`, with `maxAggregationTime` left at its default, is
 * either accepted, or rejected with an error that names `maxWaitTime` (the
 * option the caller actually passed) and does not blame `maxAggregationTime`.
 *
 * The Firebase SDK instance getters are mocked at the module boundary; only
 * constructor-time behavior is checked.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as Y from 'yjs';
import { FireProvider } from '../../src/provider';

// provider.ts imports the scoped @firebase/* packages, so those are the
// module ids to mock. Only the instance getters are stubbed; everything else
// stays real, so the provider's async startup (sync()) fails inside its own
// error handling against the fake instances and never touches the network.
vi.mock('@firebase/firestore', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@firebase/firestore')>()),
    getFirestore: vi.fn(() => ({})),
    initializeFirestore: vi.fn(() => ({})),
}));
vi.mock('@firebase/storage', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@firebase/storage')>()),
    getStorage: vi.fn(() => ({})),
}));

/** Constructs a provider, returning it or the error the constructor threw. */
function construct(extra: { maxWaitTime: number; maxAggregationTime?: number }): { provider?: FireProvider; error?: Error } {
    try {
        const provider = new FireProvider({
            firebaseApp: {} as any,
            ydoc: new Y.Doc(),
            path: 'docs/maxwaittime-zero',
            ...extra,
        });
        return { provider };
    } catch (err) {
        return { error: err as Error };
    }
}

describe('FireProvider maxWaitTime validation', () => {
    const created: FireProvider[] = [];

    beforeEach(() => {
        // The constructor kicks off an async sync() that fails against the
        // mocked SDK and may schedule a retry; keep any timer from firing.
        vi.useFakeTimers();
        vi.spyOn(console, 'log').mockImplementation(() => {});
        vi.spyOn(console, 'debug').mockImplementation(() => {});
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(console, 'warn').mockImplementation(() => {});
    });

    afterEach(async () => {
        for (const p of created.splice(0)) {
            await p.destroy().catch(() => {});
        }
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    it.each([0, -1, -500])(
        'maxWaitTime: %d is accepted or rejected naming maxWaitTime, never the unset maxAggregationTime',
        (maxWaitTime) => {
            const { provider, error } = construct({ maxWaitTime });
            if (provider) created.push(provider);

            if (error) {
                // A rejection is acceptable only if it names the option the
                // caller actually passed.
                expect(error.message).not.toMatch(/maxAggregationTime/);
                expect(error.message).toMatch(/maxWaitTime/);
            }
        },
    );

    it('maxWaitTime: 0 is accepted and hands subdocs a config that constructs too', () => {
        const parent = construct({ maxWaitTime: 0 });
        if (parent.provider) created.push(parent.provider);
        expect(parent.error).toBeUndefined();

        // handleSubdocs passes the parent's resolved maxWaitTime and
        // maxAggregationTime to each child explicitly, so the derived cap
        // must also pass validation as explicit input.
        const child = construct({
            maxWaitTime: (parent.provider as any).maxWaitTime,
            maxAggregationTime: (parent.provider as any).maxAggregationTime,
        });
        if (child.provider) created.push(child.provider);
        expect(child.error).toBeUndefined();
        expect((child.provider as any).maxAggregationTime).toBeGreaterThan(0);
    });
});
