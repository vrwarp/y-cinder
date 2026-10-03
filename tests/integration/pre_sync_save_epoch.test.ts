/**
 * Regression test: saves that start while the initial-sync push is in
 * flight must belong to the document's epoch.
 *
 * Bug: FireProvider tags every save with `this._epoch`, which is read from
 * the document once in the constructor and refreshed only after
 * performInitialSync() has fully returned. An app that constructs the
 * provider before local persistence (y-indexeddb style) hydrates an
 * epoch-N document therefore holds `_epoch === 0` for the whole initial
 * sync even though the doc already carries epoch-N content. When initial
 * sync has local data to push, it computes the diff and then awaits the
 * push write; a save that starts in that window carries edits made after
 * the diff and is written WITHOUT an epoch field. Every epoch-N client
 * drops that document as foreign-epoch, and the client's later (correctly
 * tagged) saves depend on its clocks, so peers park them in pendingStructs:
 * from then on they silently stop seeing this client's edits.
 *
 * The push write is held open deterministically (addDoc wrapper) while the
 * client edits and its debounced save starts; nothing races real timers
 * for the outcome.
 *
 * @file pre_sync_save_epoch.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { writeHook } = vi.hoisted(() => ({
    writeHook: {
        /** Runs before every addDoc reaches Firestore; may delay it. */
        intercept: null as null | ((collectionPath: string, data: any) => Promise<void>),
    },
}));

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        addDoc: async (collectionRef: any, data: any) => {
            if (writeHook.intercept) {
                await writeHook.intercept(collectionRef.path, data);
            }
            return actual.addDoc(collectionRef, data);
        },
    };
});

import { FireProvider } from '../../src/provider';
import { readDocEpoch } from '../../src/squash';
import * as Y from 'yjs';
import { collection, getDocs } from 'firebase/firestore';
import { setupEmulator } from '../utils/emulator';
import { waitFor, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** Whether the server holds a write by `uid` covering `clientID` up to `minEnd`. */
async function serverHasClientClock(
    db: any, path: string, uid: string, clientID: number, minEnd: number,
): Promise<boolean> {
    const snap = await getDocs(collection(db, path, 'updates'));
    return snap.docs.some(d => {
        const data = d.data();
        if (data.createdBy !== uid || !data.update) return false;
        const ends = Y.parseUpdateMeta(data.update.toUint8Array()).to;
        return (ends.get(clientID) ?? 0) >= minEnd;
    });
}

describe('Saves during the initial-sync push on a squashed document', () => {
    let app: any;
    let db: any;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        writeHook.intercept = null;
    });

    it('edits saved while the initial-sync push is in flight reach epoch-1 peers and later joiners', { timeout: 120000 }, async () => {
        const path = `tests/pre-sync-save-epoch-${getStableDate()}-${Date.now()}-${counter++}`;
        const opts = { firebaseApp: app, path, maxUpdatesThreshold: 1000, maxWaitTime: 20 };
        const providers: FireProvider[] = [];

        try {
            // --- 1. Squash the document into epoch 1 ---
            const docS = new Y.Doc();
            const providerS = new FireProvider({ ...opts, ydoc: docS });
            providers.push(providerS);
            await waitForConditionTruthy(() => providerS.synced, { timeout: 30000, message: 'S synced' });
            docS.getMap('data').set('base', 'squashed');
            await waitForConditionTruthy(async () =>
                (await getDocs(collection(db, path, 'updates'))).size >= 1,
                { timeout: 20000, message: 'S update persisted' });
            const squashed = await providerS.squash();
            expect(squashed.success).toBe(true);
            expect(squashed.epoch).toBe(1);

            // --- 2. C's previous session: bootstraps epoch 1, then edits
            // offline. Its full state is what local persistence holds. ---
            const docPrev = new Y.Doc();
            const providerPrev = new FireProvider({ ...opts, ydoc: docPrev });
            await waitForConditionTruthy(() => providerPrev.synced, { timeout: 30000, message: 'previous session synced' });
            expect(readDocEpoch(docPrev)).toBe(1);
            await providerPrev.destroy();
            docPrev.getMap('data').set('offline', 'from-previous-session');
            const persisted = Y.encodeStateAsUpdate(docPrev);

            // --- 3. A live epoch-1 peer ---
            const docP = new Y.Doc();
            const providerP = new FireProvider({ ...opts, ydoc: docP });
            providers.push(providerP);
            await waitForConditionTruthy(() => providerP.synced, { timeout: 30000, message: 'P synced' });
            expect(providerP.epoch).toBe(1);
            expect(docP.getMap('data').get('base')).toBe('squashed');

            // --- 4. Client C: provider constructed before the persisted
            // epoch-1 state is applied (y-indexeddb ordering). ---
            const docC = new Y.Doc();
            let providerC: FireProvider | undefined;
            const writesByC: any[] = [];
            let heldWrite = false;

            writeHook.intercept = async (collectionPath, data) => {
                if (!providerC || !collectionPath.endsWith('/updates') || data?.createdBy !== providerC.uid) {
                    return;
                }
                writesByC.push(data);
                // Hold C's first epoch-1 write made before sync completes —
                // the initial-sync push of the offline edit, whose diff is
                // already computed — and edit while it is in flight.
                if (heldWrite || providerC.synced || data.epoch !== 1) return;
                heldWrite = true;

                docC.getMap('data').set('window', 'edited-during-push');
                const seen = writesByC.length;
                // Release once a save carrying this edit has started (or
                // after 5 s, should the provider defer saves until sync
                // completes — that is a valid fix, not a failure).
                await waitFor(
                    () => writesByC.slice(seen).some(w => w.clientIDs?.includes(docC.clientID)),
                    started => started,
                    { timeout: 5000, interval: 10 },
                ).catch(() => undefined);
            };

            providerC = new FireProvider({ ...opts, ydoc: docC });
            providers.push(providerC);
            Y.applyUpdate(docC, persisted, 'idb');

            await waitForConditionTruthy(() => providerC!.synced, { timeout: 30000, message: 'C synced' });
            expect(heldWrite).toBe(true);
            expect(providerC.epoch).toBe(1);

            // C keeps editing after sync completes.
            docC.getMap('data').set('after', 'edited-after-sync');
            // Both of C's edits (clocks 0 and 1 of docC.clientID) are on the server.
            await waitForConditionTruthy(
                () => serverHasClientClock(db, path, providerC!.uid, docC.clientID, 2),
                { timeout: 20000, message: "C's edits persisted" });

            const expected = {
                base: 'squashed',
                offline: 'from-previous-session',
                window: 'edited-during-push',
                after: 'edited-after-sync',
            };
            expect(docC.getMap('data').toJSON()).toEqual(expected);

            // --- 5. What other epoch-1 clients see ---
            const peerView = await waitFor(
                () => docP.getMap('data').toJSON(),
                view => Object.keys(view).length === Object.keys(expected).length
                    && Object.entries(expected).every(([k, v]) => view[k] === v),
                { timeout: 10000, interval: 100 },
            ).catch(() => docP.getMap('data').toJSON());

            const docL = new Y.Doc();
            const providerL = new FireProvider({ ...opts, ydoc: docL });
            providers.push(providerL);
            await waitForConditionTruthy(() => providerL.synced, { timeout: 30000, message: 'late joiner synced' });
            const lateView = docL.getMap('data').toJSON();

            const untaggedByC = (await getDocs(collection(db, path, 'updates'))).docs
                .map(d => d.data())
                .filter(d => d.createdBy === providerC!.uid && d.epoch === undefined).length;
            console.log(
                `[pre-sync-save-epoch] untagged update docs by C: ${untaggedByC}; ` +
                `peer pendingStructs: ${docP.store.pendingStructs !== null}; ` +
                `late joiner pendingStructs: ${docL.store.pendingStructs !== null}`,
            );

            expect({ peer: peerView, lateJoiner: lateView }).toEqual({ peer: expected, lateJoiner: expected });
        } finally {
            writeHook.intercept = null;
            for (const provider of providers) {
                await provider.destroy();
            }
        }
    });
});
