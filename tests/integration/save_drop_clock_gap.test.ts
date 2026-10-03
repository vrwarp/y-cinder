/**
 * Terminal Save Failure Clock-Gap Regression Tests
 *
 * When the save circuit breaker gives up on a batch ('max-retries-exceeded')
 * or a batch is rejected as 'document-too-large', the provider emits
 * 'save-rejected' and keeps saving later batches. If the rejected batch is
 * simply discarded, the next batch carries this Yjs client's structs from a
 * clock beyond the dropped range. Yjs requires each client's clocks to be
 * contiguous, so every peer parks that batch (and everything after it) in
 * pendingStructs: edits that the provider reported as 'saved' can never be
 * integrated by anyone else.
 *
 * Contract checked here (independent of how a fix is shaped — re-queueing
 * the rejected batch, or fencing further saves): whatever the provider
 * reports as committed after a terminal failure must be integrable by a
 * peer, and the persisted history must never leave a peer with structs
 * stuck behind a clock gap.
 *
 * @file save_drop_clock_gap.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { mockControls } = vi.hoisted(() => {
    return {
        mockControls: {
            /** Number of upcoming addDoc calls to the updates collection that fail */
            failuresRemaining: 0,
            /** Error code attached to the simulated failure ('' = generic) */
            failCode: '' as string,
            failMessage: '' as string,
        },
    };
});

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        addDoc: async (collectionRef: any, data: any) => {
            if (mockControls.failuresRemaining > 0 && collectionRef.path.includes('updates')) {
                mockControls.failuresRemaining--;
                const err: any = new Error(mockControls.failMessage || 'Simulated network error');
                if (mockControls.failCode) {
                    err.code = mockControls.failCode;
                }
                throw err;
            }
            return actual.addDoc(collectionRef, data);
        },
    };
});

import { FireProvider } from '../../src/provider';
import { DEFAULTS } from '../../src/types';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** Resolves with the payload of the next emission of `event`. */
function nextEvent(provider: FireProvider, event: string): Promise<any[]> {
    return new Promise((resolve) => {
        const handler = (...args: any[]) => {
            provider.off(event, handler);
            resolve(args);
        };
        provider.on(event, handler);
    });
}

describe('Terminal save failure must not leave a permanent clock gap (Emulator)', () => {
    let app: any;
    let db: any;
    let counter = 0;

    const createProvider = (doc: Y.Doc, path: string, config: any = {}) => {
        return new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path,
            ...config,
        });
    };

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        await clearFirestore(db);
        mockControls.failuresRemaining = 0;
        mockControls.failCode = '';
        mockControls.failMessage = '';
    });

    const scenarios = [
        {
            name: 'max-retries-exceeded',
            arm: () => {
                // Write failures spanning exactly the retry budget (in
                // practice a denied Storage upload or an auth/permission
                // hiccup; addDoc itself stays pending through outages)
                mockControls.failuresRemaining = DEFAULTS.MAX_SAVE_RETRIES;
                mockControls.failCode = '';
                mockControls.failMessage = 'Simulated network error';
            },
        },
        {
            name: 'document-too-large',
            arm: () => {
                // One server-side size rejection (terminal, never retried)
                mockControls.failuresRemaining = 1;
                mockControls.failCode = 'invalid-argument';
                mockControls.failMessage = 'Document exceeds the maximum allowed size';
            },
        },
    ] as const;

    for (const scenario of scenarios) {
        it(`edits saved after a '${scenario.name}' rejection are integrable by a fresh peer`, async () => {
            const path = `integration-tests/save-drop-clock-gap-${getStableDate()}-${counter++}`;

            const docA = new Y.Doc();
            const providerA = createProvider(docA, path, { maxWaitTime: 30 });
            await waitForConditionTruthy(() => providerA.synced, {
                timeout: 30000,
                message: 'Provider A should complete initial sync',
            });

            // 1. Baseline edit commits normally.
            const firstSaved = nextEvent(providerA, 'saved');
            docA.getText('t').insert(0, 'x');
            await firstSaved;

            // 2. The next batch hits a terminal save failure.
            scenario.arm();
            const rejected = nextEvent(providerA, 'save-rejected');
            docA.getText('t').insert(1, 'y');
            const [rejection] = await rejected;
            expect(rejection.code).toBe(scenario.name);
            // The failure condition is over: every later write succeeds.
            expect(mockControls.failuresRemaining).toBe(0);

            // 3. A later edit to an unrelated shared type in the same session.
            let savedAfterFailure = false;
            providerA.on('saved', () => {
                savedAfterFailure = true;
            });
            docA.getMap('m').set('k', 'later');

            // Deterministic quiescence point: destroy() waits out any
            // in-flight save and flushes everything still buffered, so
            // every write A will ever make has committed once it resolves.
            await providerA.destroy();

            // 4. A fresh peer loads the document from Firestore.
            const docC = new Y.Doc();
            const providerC = createProvider(docC, path, { maxWaitTime: 30 });
            try {
                await waitForConditionTruthy(() => providerC.synced, {
                    timeout: 30000,
                    message: 'Fresh peer C should complete initial sync',
                });

                const peerView = {
                    t: docC.getText('t').toString(),
                    k: docC.getMap('m').get('k'),
                };
                const store = docC.store as unknown as { pendingStructs: unknown; pendingDs: unknown };

                if (savedAfterFailure) {
                    // A reported the later edit as committed: a peer must be
                    // able to integrate it (and therefore the contiguous
                    // history it depends on).
                    expect.soft(peerView).toEqual({ t: 'xy', k: 'later' });
                }

                // Whatever was persisted must form a contiguous history: no
                // structs from A may be stuck behind a clock gap on a peer.
                expect(store.pendingStructs, 'peer has structs parked behind a clock gap').toBeNull();
            } finally {
                await providerC.destroy();
            }
        }, 60000);
    }
});
