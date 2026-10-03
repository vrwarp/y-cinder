/**
 * Regression test: a deletion made while initial sync's push is
 * unacknowledged must reach peers and fresh loads.
 *
 * A save that comes due during initial sync is held, up to
 * maxAggregationTime from the first buffered edit. When that ceiling
 * expires after initial sync captured the local doc but while its push is
 * still unacknowledged, the released save takes the whole buffer: an edit
 * the push already carries (inserting 'Z') together with a deletion-only
 * edit made after the capture (deleting 'q'). The push commits first, so
 * every reader already holds the save's structs when the save arrives.
 * Readers must still apply the deletion it carries.
 *
 * The writer's update writes are handed to the SDK in call order, each one
 * after the previous one commits (the order the SDK commits them in), and
 * none before the test opens the gate. Each client has a Firebase app of
 * its own, like separate devices. Outcomes are read from a live peer and a
 * fresh client; timeouts are generous upper bounds, never timing
 * assertions.
 *
 * @file save_hold_deletion_skipped.test.ts
 */

import { describe, it, expect, beforeAll, afterEach, vi } from 'vitest';

const { ctl } = vi.hoisted(() => ({
    ctl: {
        /** Update writes by this uid are stalled until `stallGate` opens. */
        stallUid: null as string | null,
        stallGate: null as Promise<void> | null,
        /** Serializes the stalled writes: each enters the SDK after the previous commits. */
        chain: Promise.resolve() as Promise<unknown>,
        /** Update documents handed to the SDK, in order. */
        writes: [] as { createdBy?: string }[],
        /** Update documents the server acknowledged, in order. */
        committed: [] as { createdBy?: string }[],
        /** Main-document reads of `watchPath` that returned. */
        watchPath: null as string | null,
        mainReads: 0,
    },
}));

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        addDoc: async (collectionRef: any, data: any) => {
            if (!String(collectionRef.path).endsWith('/updates')) {
                return actual.addDoc(collectionRef, data);
            }
            const entry = { createdBy: data?.createdBy };
            ctl.writes.push(entry);
            let write: Promise<any>;
            if (ctl.stallUid !== null && data?.createdBy === ctl.stallUid) {
                const previous = ctl.chain;
                const gate = ctl.stallGate;
                write = (async () => {
                    await previous;
                    await gate;
                    return actual.addDoc(collectionRef, data);
                })();
                ctl.chain = write.catch(() => {});
            } else {
                write = actual.addDoc(collectionRef, data);
            }
            const result = await write;
            ctl.committed.push(entry);
            return result;
        },
        getDoc: async (ref: any) => {
            const snap = await actual.getDoc(ref);
            if (ctl.watchPath !== null && ref.path === ctl.watchPath) ctl.mainReads++;
            return snap;
        },
    };
});

import * as Y from 'yjs';
import { initializeApp, getApps, FirebaseApp } from 'firebase/app';
import { getFirestore, connectFirestoreEmulator } from 'firebase/firestore';
import { getStorage, connectStorageEmulator } from 'firebase/storage';
import { FireProvider } from '../../src/provider';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** A Firebase app of its own per device: own Firestore client, own connection. */
function deviceApp(name: string): FirebaseApp {
    const existing = getApps().find(app => app.name === name);
    if (existing) return existing;
    const app = initializeApp({
        projectId: 'demo-test-project',
        apiKey: 'fake-api-key',
        storageBucket: 'demo-test-project.appspot.com',
    }, name);
    connectFirestoreEmulator(getFirestore(app), '127.0.0.1', 8080);
    connectStorageEmulator(getStorage(app), '127.0.0.1', 9199);
    return app;
}

describe('Deletion batched by the initial-sync save hold', () => {
    let counter = 0;
    const live: FireProvider[] = [];

    beforeAll(async () => {
        await setupEmulator();
    });

    afterEach(async () => {
        ctl.stallUid = null;
        ctl.stallGate = null;
        ctl.chain = Promise.resolve();
        ctl.watchPath = null;
        ctl.writes = [];
        ctl.committed = [];
        ctl.mainReads = 0;
        while (live.length > 0) {
            await live.pop()!.destroy();
        }
    });

    const newPath = () => `tests/save-hold-deletion-${getStableDate()}-${Date.now()}-${counter++}`;

    const createProvider = (device: string, ydoc: Y.Doc, path: string, extra: Record<string, unknown> = {}) => {
        const provider = new FireProvider({
            firebaseApp: deviceApp(device),
            ydoc,
            path,
            // Skip the clock-skew probe.
            cachedClockOffset: 0,
            // Keep compaction out of the picture: a fold would mask the
            // outcome of the update documents themselves.
            maxUpdatesThreshold: 1000,
            ...extra,
        });
        live.push(provider);
        return provider;
    };

    const release = async (provider: FireProvider) => {
        live.splice(live.indexOf(provider), 1);
        await provider.destroy();
    };

    const waitSynced = (provider: FireProvider, label: string) =>
        waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: `${label} synced` });

    const writesBy = (provider: FireProvider) => ctl.writes.filter(w => w.createdBy === provider.uid).length;
    const committedBy = (provider: FireProvider) => ctl.committed.filter(w => w.createdBy === provider.uid).length;

    /** What a brand-new device reads; nothing may be left waiting on a gap. */
    const readFromFreshClient = async (path: string): Promise<string> => {
        const fresh = new Y.Doc();
        const reader = createProvider('fresh', fresh, path);
        await waitSynced(reader, 'fresh client');
        expect((fresh.store as any).pendingStructs).toBeNull();
        const text = fresh.getText('t').toString();
        await release(reader);
        fresh.destroy();
        return text;
    };

    it('peers and fresh loads apply a deletion made while the initial-sync push is unacknowledged', { timeout: 90000 }, async () => {
        const path = newPath();
        const HOLD_MS = 1500;

        // A peer holds 'abcq' on the server and stays online.
        const peerDoc = new Y.Doc();
        peerDoc.getText('t').insert(0, 'abcq');
        const peer = createProvider('peer', peerDoc, path, { maxWaitTime: 30 });
        await waitSynced(peer, 'peer');

        // The writer's local persistence already holds the same state.
        const writerDoc = new Y.Doc();
        Y.applyUpdate(writerDoc, Y.encodeStateAsUpdate(peerDoc));
        let markLocalReady!: () => void;
        const localReady = new Promise<void>(resolve => { markLocalReady = resolve; });
        let openWrites!: () => void;
        ctl.stallGate = new Promise<void>(resolve => { openWrites = resolve; });
        ctl.watchPath = path;
        ctl.mainReads = 0;
        const writer = createProvider('writer', writerDoc, path, {
            maxWaitTime: 30,
            maxAggregationTime: HOLD_MS,
            localReady,
        });
        ctl.stallUid = writer.uid;

        // Initial sync has read the server and waits for local persistence.
        await waitForConditionTruthy(() => ctl.mainReads > 0, {
            timeout: 30000, message: "the writer's initial sync reads the main document",
        });

        // The user edits before initial sync captures the doc: the due save
        // is held until maxAggregationTime from now.
        writerDoc.getText('t').insert(0, 'Z');
        markLocalReady();

        // Initial sync captures the doc and hands its push (carrying 'Z')
        // to the SDK, where it stays unacknowledged.
        await waitForConditionTruthy(() => writesBy(writer) === 1, {
            timeout: 10000, message: "the writer's initial-sync push is handed to the SDK",
        });

        // A deletion-only edit after the capture, before the hold expires.
        const text = writerDoc.getText('t');
        text.delete(text.length - 1, 1);
        expect(text.toString()).toBe('Zabc');
        expect(writesBy(writer)).toBe(1);

        // The held save goes out at maxAggregationTime while the push is
        // still unacknowledged.
        await waitForConditionTruthy(() => writesBy(writer) === 2, {
            timeout: 10000, message: 'the held save is handed to the SDK while the push is unacknowledged',
        });
        expect(committedBy(writer)).toBe(0);

        // The connection recovers: the push commits, then the save.
        ctl.stallGate = null;
        openWrites();
        await waitForConditionTruthy(() => committedBy(writer) === 2, {
            timeout: 30000, message: "both of the writer's update documents commit",
        });
        await waitSynced(writer, 'writer');
        expect(writerDoc.getText('t').toString()).toBe('Zabc');

        // A fresh load reads both update documents; the live peer received
        // both through its listener.
        const fresh = await readFromFreshClient(path);
        await waitForConditionTruthy(() => peerDoc.getText('t').toString() === 'Zabc', {
            timeout: 10000, message: 'the peer converges',
        }).catch(() => {});
        expect({ fresh, peer: peerDoc.getText('t').toString() }).toEqual({ fresh: 'Zabc', peer: 'Zabc' });
    });
});
