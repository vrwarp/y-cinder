/**
 * Regression test: continuous editing must not defer saves indefinitely.
 *
 * Bug: the save debounce timer resets on every local update. A user typing
 * continuously (keystroke interval < maxWaitTime) therefore never triggered
 * a save — the pending-update buffer grew without bound and nothing was
 * persisted until they paused, risking large data loss on tab crash and
 * eventually producing oversized merged updates.
 *
 * The fix caps total deferral at maxAggregationTime: once the oldest
 * buffered update has waited that long, the save fires even mid-burst.
 *
 * @file debounce_cap.test.ts
 */

import { describe, it, expect, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('Debounce aggregation cap', () => {
    let counter = 0;
    let provider: FireProvider | null = null;

    afterEach(async () => {
        if (provider) {
            await provider.destroy();
            provider = null;
        }
    });

    it('forces a save during continuous typing once maxAggregationTime elapses', async () => {
        const { app } = await setupEmulator();
        const path = `integration-tests/debounce-cap-${getStableDate()}-${counter++}`;

        const doc = new Y.Doc();
        provider = new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path,
            maxWaitTime: 300,          // debounce window
            maxAggregationTime: 900,   // hard cap under test
        });

        await waitForConditionTruthy(() => provider!.synced, { timeout: 30000 });

        const savedTimes: number[] = [];
        (provider as any).on('saved', (t: number) => savedTimes.push(t));

        // Type every 100ms — always inside the 300ms debounce window, so
        // without the cap the timer resets forever and no save happens
        // until typing stops.
        const text = doc.getText('t');
        const typingStart = Date.now();
        while (Date.now() - typingStart < 3000) {
            text.insert(text.length, 'x');
            await new Promise(r => setTimeout(r, 100));
        }
        const typingEnd = Date.now();

        // At least one save must have committed while typing was still in
        // progress (well before the final debounce could have fired).
        expect(savedTimes.length).toBeGreaterThan(0);
        expect(savedTimes[0]).toBeLessThan(typingEnd);
        // And it fired in the neighborhood of the cap, not the full burst
        expect(savedTimes[0] - typingStart).toBeLessThan(2500);
    }, 60000);

    it('still debounces normally when edits pause before the cap', async () => {
        const { app } = await setupEmulator();
        const path = `integration-tests/debounce-cap-${getStableDate()}-${counter++}`;

        const doc = new Y.Doc();
        provider = new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path,
            maxWaitTime: 200,
            maxAggregationTime: 10_000,
        });

        await waitForConditionTruthy(() => provider!.synced, { timeout: 30000 });

        const savedTimes: number[] = [];
        (provider as any).on('saved', (t: number) => savedTimes.push(t));

        doc.getText('t').insert(0, 'hello');

        // Debounce should deliver exactly one save shortly after the edit
        await waitForConditionTruthy(() => savedTimes.length > 0, { timeout: 10000 });
        expect(savedTimes.length).toBe(1);
    }, 60000);

    it('rejects a non-positive maxAggregationTime', async () => {
        const { app } = await setupEmulator();
        const doc = new Y.Doc();
        expect(() => new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path: 'integration-tests/invalid-config',
            maxAggregationTime: 0,
        })).toThrow(/maxAggregationTime/);
    });
});
