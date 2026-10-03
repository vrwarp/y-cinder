/**
 * Regression test: deletions carried by a delta history segment must reach
 * every client that loads the document.
 *
 * Bug: between folds, delta compaction merges the pending update documents
 * into ONE history segment and stores, as the segment's `stateVector`, the
 * clock ends of the STRUCTS it contains. Deletions add no structs, so:
 *
 *  (a) a segment built only from deletions gets an empty state vector,
 *      which every local state vector vacuously "covers" — a fresh client
 *      skips the segment as redundant and resurrects the deleted content;
 *  (b) a segment that mixes structs a returning client already holds with
 *      deletions it lacks is judged redundant the same way, so the client
 *      never learns about the deletions.
 *
 * Before compaction the same delete-only update document had no clocks and
 * was always applied; only the next FOLD (refreshing the main document's
 * delete-set fingerprint) would deliver the deletions, up to
 * historyFoldThreshold cycles later.
 *
 * Contract asserted here: after a client syncs, it shows the post-deletion
 * state, identical to the device that made the deletions.
 *
 * @file delta_segment_deletions.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

describe('Delta history segments carrying deletions', () => {
    let app: any;
    let path: string;
    let counter = 0;
    const cleanups: (() => Promise<void> | void)[] = [];

    const createProvider = (ydoc: Y.Doc) => {
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxWaitTime: 50,
            maxUpdatesThreshold: 1000, // compaction only via explicit compact()
            historyFoldThreshold: 8, // default; the second compaction runs in DELTA mode
        });
        cleanups.push(() => provider.destroy());
        return provider;
    };

    const waitSynced = (provider: FireProvider, label: string) =>
        waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: `${label} initial sync` });

    /** Runs `edit` and resolves once the resulting update document is committed. */
    const editAndSave = async (provider: FireProvider, edit: () => void) => {
        const saved = new Promise<void>(resolve => provider.once('saved', () => resolve()));
        edit();
        await saved;
    };

    /**
     * Gives the client a bounded grace period to converge (so a fix that
     * delivers the deletions slightly after 'synced' still passes) without
     * throwing — the assertions that follow report the actual state.
     */
    const settle = async (done: () => boolean, ms = 3000) => {
        const start = Date.now();
        while (!done() && Date.now() - start < ms) {
            await new Promise(r => setTimeout(r, 50));
        }
    };

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        path = `integration-tests/delta-segment-deletions-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    afterEach(async () => {
        while (cleanups.length > 0) {
            try {
                await cleanups.pop()!();
            } catch {
                // best-effort teardown
            }
        }
    });

    it('a fresh client applies a delete-only delta segment (deleted keys and text stay deleted)', { timeout: 90000 }, async () => {
        const docA = new Y.Doc();
        cleanups.push(() => docA.destroy());
        const providerA = createProvider(docA);
        await waitSynced(providerA, 'A');

        const mapA = docA.getMap('books');
        const textA = docA.getText('note');

        // 1. Content, folded into the base snapshot (first compaction folds).
        await editAndSave(providerA, () => {
            docA.transact(() => {
                mapA.set('book1', 'Dune');
                mapA.set('book2', 'Emma');
                textA.insert(0, 'hello world');
            });
        });
        await providerA.compact();

        // 2. Pure deletions, then a delta compaction (base exists, few segments).
        await editAndSave(providerA, () => {
            docA.transact(() => {
                mapA.delete('book1');
                textA.delete(5, 6); // 'hello world' -> 'hello'
            });
        });
        await providerA.compact();

        expect(mapA.toJSON()).toEqual({ book2: 'Emma' });
        expect(textA.toString()).toBe('hello');

        // 3. A brand-new device loads the document.
        const docB = new Y.Doc();
        cleanups.push(() => docB.destroy());
        const providerB = createProvider(docB);
        await waitSynced(providerB, 'B');

        const mapB = docB.getMap('books');
        const textB = docB.getText('note');
        await settle(() => !mapB.has('book1') && textB.toString() === 'hello');

        // The deleted book and text must not come back on the new device.
        expect(mapB.toJSON()).toEqual({ book2: 'Emma' });
        expect(textB.toString()).toBe('hello');
    });

    it('a returning client that already holds the segment structs still receives its deletions', { timeout: 90000 }, async () => {
        const docA = new Y.Doc();
        cleanups.push(() => docA.destroy());
        const providerA = createProvider(docA);
        await waitSynced(providerA, 'A');

        const textA = docA.getText('note');

        // 1. Seed + fold so that a base snapshot exists.
        await editAndSave(providerA, () => {
            docA.getMap('meta').set('title', 'Notes');
        });
        await providerA.compact();

        // 2. A inserts 'abc' (an uncompacted update document).
        await editAndSave(providerA, () => {
            textA.insert(0, 'abc');
        });

        // 3. Device B syncs and receives 'abc', then goes offline.
        const docB = new Y.Doc();
        cleanups.push(() => docB.destroy());
        const providerB1 = createProvider(docB);
        await waitSynced(providerB1, 'B (first session)');
        await waitForConditionTruthy(() => docB.getText('note').toString() === 'abc', {
            timeout: 30000,
            message: "B receives 'abc'",
        });
        await providerB1.destroy();

        // 4. While B is offline, A deletes 'a' and delta-compacts: the new
        //    segment holds the 'abc' structs (which B has) plus the deletion
        //    (which B lacks).
        await editAndSave(providerA, () => {
            textA.delete(0, 1);
        });
        await providerA.compact();
        expect(textA.toString()).toBe('bc');

        // 5. B comes back online with its persisted local state.
        const providerB2 = createProvider(docB);
        await waitSynced(providerB2, 'B (second session)');

        const textB = docB.getText('note');
        await settle(() => textB.toString() === 'bc');

        expect(textB.toString()).toBe('bc');
        expect(docB.getMap('meta').toJSON()).toEqual({ title: 'Notes' });
    });
});
