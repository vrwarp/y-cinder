/**
 * Regression test: local edits made on the squashing client while
 * squash() is uploading / committing must not be silently lost.
 *
 * squashDocument() clones the live doc synchronously, then awaits the
 * Cloud Storage upload of the new snapshot and the commit transaction.
 * An edit made on the squashing device in that window (e.g. versicle
 * writing reading progress during a maintenance squash) is not in the
 * clone. On the buggy code it then either
 *
 *  - sits in the provider's pending buffer when the commit lands, and
 *    _stopSyncing() blocks it from ever being saved, or
 *  - is saved by the debounce before the commit as an OLD-epoch update
 *    document the commit transaction does not delete — every new-epoch
 *    client ignores it and compaction later deletes it as stale.
 *
 * Either way squash() reports success, the new epoch lacks the edit, and
 * the 'squashed' event carries only `{ epoch }` (unlike 'epoch-changed',
 * which carries localState), so the application has nothing to re-apply.
 *
 * The contract asserted here holds for any reasonable fix: after squash()
 * resolves, the window edit is recoverable — either it is in what a fresh
 * client loads from the server (squash aborted and the edit was persisted
 * in the old epoch, or squash retried and the new epoch includes it), or
 * the 'squashed' event / squash result carries state that, applied by the
 * application, yields the edit.
 *
 * Determinism: the window edit is injected from a hook on the snapshot
 * upload (uploadBytes for the `snapshot_e<N>_` path), i.e. strictly after
 * the clone and strictly before the commit transaction.
 *
 * @file squash_window_edits.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { hooks } = vi.hoisted(() => ({
    hooks: {
        /** Runs before the squash snapshot upload (after the clone) */
        onSquashSnapshotUpload: null as null | (() => Promise<void>),
        /** Runs before a storage object is deleted (squash: post-commit) */
        onDeleteObject: null as null | ((fullPath: string) => void),
    },
}));

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        uploadBytes: async (storageRef: any, data: any, metadata?: any) => {
            const fullPath: string = storageRef?.fullPath ?? String(storageRef);
            if (/\/snapshot_e\d+_/.test(fullPath) && hooks.onSquashSnapshotUpload) {
                const hook = hooks.onSquashSnapshotUpload;
                hooks.onSquashSnapshotUpload = null; // fire once
                await hook();
            }
            return actual.uploadBytes(storageRef, data, metadata);
        },
        deleteObject: async (storageRef: any) => {
            hooks.onDeleteObject?.(storageRef?.fullPath ?? String(storageRef));
            return actual.deleteObject(storageRef);
        },
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** Resolves on the provider's next 'saved' event. */
function nextSaved(provider: FireProvider): Promise<void> {
    return new Promise<void>(resolve => {
        const onSaved = () => { provider.off('saved', onSaved); resolve(); };
        provider.on('saved', onSaved);
    });
}

async function withTimeout<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
        return await Promise.race([
            p,
            new Promise<never>((_, reject) => {
                timer = setTimeout(() => reject(new Error(`timed out waiting for ${what}`)), ms);
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

/** Every Uint8Array found (recursively) in an event payload / result. */
function collectBytes(value: unknown, out: Uint8Array[] = [], depth = 0): Uint8Array[] {
    if (value instanceof Uint8Array) {
        out.push(value);
    } else if (value && typeof value === 'object' && depth < 4) {
        for (const v of Object.values(value as Record<string, unknown>)) {
            collectBytes(v, out, depth + 1);
        }
    }
    return out;
}

/**
 * Whether any of `blobs`, applied on top of one of `bases` (or an empty
 * doc), materializes the window edit. Generous on purpose: accepts a full
 * local state, a post-clone delta, or a new-epoch delta.
 */
function blobsRecoverEdit(blobs: Uint8Array[], bases: Uint8Array[]): boolean {
    for (const blob of blobs) {
        for (const base of [null, ...bases]) {
            const d = new Y.Doc();
            try {
                if (base) Y.applyUpdate(d, base);
                Y.applyUpdate(d, blob);
                if (d.getMap('m').get('during') === 2) return true;
            } catch {
                /* not applicable on this base */
            } finally {
                d.destroy();
            }
        }
    }
    return false;
}

describe('squash(): edits made on the squashing client during upload/commit', () => {
    let app: any;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        hooks.onSquashSnapshotUpload = null;
        hooks.onDeleteObject = null;
    });

    /**
     * Runs one squash with an edit injected after the clone, then checks
     * the edit is recoverable by the application.
     *
     * @param maxWaitTime - provider debounce for client A
     * @param waitForDebouncedSave - whether the hook waits until the window
     *        edit has been saved by the debounce before letting the
     *        squash upload/commit continue
     */
    async function runWindowEditScenario(maxWaitTime: number, waitForDebouncedSave: boolean): Promise<void> {
        const path = `tests/squash-window-${getStableDate()}-${Date.now()}-${counter++}`;
        const ydocA = new Y.Doc();
        const providerA = new FireProvider({
            firebaseApp: app,
            ydoc: ydocA,
            path,
            maxUpdatesThreshold: 1000,
            maxWaitTime,
        });
        let ydocF: Y.Doc | null = null;
        let providerF: FireProvider | null = null;
        let providerADestroyed = false;

        try {
            await waitForConditionTruthy(() => providerA.synced, { timeout: 30000, message: 'A synced' });

            const beforeSaved = nextSaved(providerA);
            ydocA.getMap('m').set('before', 1);
            if (waitForDebouncedSave) {
                // Short debounce: let the 'before' save land first so it
                // cannot race squash()'s own compaction / flush preamble.
                await withTimeout(beforeSaved, 20000, "'before' persisted");
            }
            // (Long debounce: squash() itself flushes 'before'.)

            // Inject the window edit strictly after the clone, before commit
            let cloneTimeState: Uint8Array | null = null;
            let hookRan = false;
            hooks.onSquashSnapshotUpload = async () => {
                hookRan = true;
                cloneTimeState = Y.encodeStateAsUpdate(ydocA);
                const duringSaved = waitForDebouncedSave ? nextSaved(providerA) : null;
                ydocA.getMap('m').set('during', 2);
                if (duringSaved) {
                    // Hold the upload until the debounce has persisted the
                    // edit (bounded: a fix may legitimately defer saves
                    // while a squash is in progress).
                    await Promise.race([duringSaved, new Promise(r => setTimeout(r, 5000))]);
                }
            };

            const squashedPayloads: unknown[] = [];
            providerA.on('squashed', (e: unknown) => squashedPayloads.push(e));

            const result = await providerA.squash();
            expect(hookRan, `squash reached the snapshot upload (result=${JSON.stringify({ ...result, error: result.error?.message })})`).toBe(true);
            // The edit is in the live doc of the squashing client
            expect(ydocA.getMap('m').get('during')).toBe(2);

            // Let A flush whatever it still may persist (relevant when a
            // fix aborts the squash and keeps syncing the old epoch)
            await providerA.destroy();
            providerADestroyed = true;

            // What a fresh client loads from the server, whatever the epoch
            ydocF = new Y.Doc();
            providerF = new FireProvider({
                firebaseApp: app, ydoc: ydocF, path, maxUpdatesThreshold: 1000,
            });
            const pf = providerF;
            await waitForConditionTruthy(() => pf.synced, { timeout: 30000, message: 'fresh client synced' });
            const fresh = ydocF.getMap('m').toJSON();
            expect(fresh.before, 'pre-squash content survives').toBe(1);

            const onServer = fresh.during === 2;
            const surfaced = blobsRecoverEdit(
                collectBytes([squashedPayloads, result]),
                [cloneTimeState!, Y.encodeStateAsUpdate(ydocF)],
            );

            expect(
                onServer || surfaced,
                `edit made during squash was lost: squash result=${JSON.stringify({ success: result.success, epoch: result.epoch, skippedReason: result.skippedReason, error: result.error?.message })}, ` +
                `fresh client (epoch ${providerF.epoch}) sees ${JSON.stringify(fresh)}, ` +
                `'squashed' payload keys=${JSON.stringify(squashedPayloads.map(p => Object.keys(p as object)))}`,
            ).toBe(true);
        } finally {
            hooks.onSquashSnapshotUpload = null;
            if (!providerADestroyed) await providerA.destroy();
            if (providerF) await providerF.destroy();
            ydocA.destroy();
            ydocF?.destroy();
        }
    }

    it('an edit still buffered when the squash commits is not silently dropped', { timeout: 120000 }, async () => {
        // Long debounce: the window edit is still in the pending buffer
        // when the commit lands (squash() itself flushed 'before').
        await runWindowEditScenario(60000, false);
    });

    it('an edit saved by the debounce before the squash commits is not left as old-epoch garbage', { timeout: 120000 }, async () => {
        // Short debounce, and the upload waits until the window edit has
        // been saved: it lands as an old-epoch update document the commit
        // transaction never read.
        await runWindowEditScenario(50, true);
    });

    it('an edit that races the commit itself is surfaced in the \'squashed\' payload', { timeout: 120000 }, async () => {
        // Past the commit no check can abort the squash any more: the edit
        // is missing from the new epoch and the provider is about to fence,
        // so the 'squashed' event is the only place it can surface.
        const path = `tests/squash-window-${getStableDate()}-${Date.now()}-${counter++}`;
        const docs: Y.Doc[] = [];
        const providers: FireProvider[] = [];
        const open = async (maxWaitTime?: number) => {
            const ydoc = new Y.Doc();
            const provider = new FireProvider({
                firebaseApp: app, ydoc, path, maxUpdatesThreshold: 1000, maxWaitTime,
            });
            docs.push(ydoc);
            providers.push(provider);
            await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: 'provider synced' });
            return { ydoc, provider };
        };

        try {
            // A first squash moves the snapshot into Cloud Storage, so the
            // second squash deletes that blob right after its commit.
            const a = await open();
            a.ydoc.getMap('m').set('before', 1);
            expect((await a.provider.squash()).epoch).toBe(1);
            await a.provider.destroy();

            const b = await open(60000);
            expect(b.ydoc.getMap('m').get('before')).toBe(1);

            let hookRan = false;
            hooks.onDeleteObject = (fullPath) => {
                if (hookRan || !/\/snapshot_e1_/.test(fullPath)) return;
                hookRan = true;
                b.ydoc.getMap('m').set('during', 2);
            };
            const squashedPayloads: unknown[] = [];
            b.provider.on('squashed', (e: unknown) => squashedPayloads.push(e));

            const result = await b.provider.squash();
            expect(result.success).toBe(true);
            expect(result.epoch).toBe(2);
            expect(hookRan, 'the edit was made after the commit').toBe(true);
            await b.provider.destroy();

            const f = await open();
            const fresh = f.ydoc.getMap('m').toJSON();
            expect(fresh.before, 'pre-squash content survives').toBe(1);

            const onServer = fresh.during === 2;
            const surfaced = blobsRecoverEdit(collectBytes([squashedPayloads, result]), [Y.encodeStateAsUpdate(f.ydoc)]);
            expect(
                onServer || surfaced,
                `edit that raced the commit was lost: fresh client (epoch ${f.provider.epoch}) sees ${JSON.stringify(fresh)}, ` +
                `'squashed' payload keys=${JSON.stringify(squashedPayloads.map(p => Object.keys(p as object)))}`,
            ).toBe(true);
        } finally {
            hooks.onDeleteObject = null;
            for (const p of providers) await p.destroy();
            for (const d of docs) d.destroy();
        }
    });
});
