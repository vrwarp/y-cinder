/**
 * Regression test: a storage-backed update whose pointer write commits but
 * whose ack is lost must not leave a pointer document behind whose blob is
 * gone.
 *
 * Saves and initial-sync pushes larger than INLINE_UPDATE_LIMIT upload the
 * update to Cloud Storage (`{path}/large_updates/`) and then write a
 * pointer document with `addDoc`. The web SDK re-sends every
 * unacknowledged mutation when its write stream reconnects, and `addDoc` is
 * a set with the precondition `exists: false`. So when the connection drops
 * after the server committed the pointer but before the ack arrived:
 *
 *  1. the re-send fails ALREADY_EXISTS and `addDoc` REJECTS although the
 *     pointer is live. Deleting "the blob nobody references" on that
 *     rejection deletes the payload of a live pointer;
 *  2. if a peer's compaction consumed the pointer (and reclaimed its blob)
 *     before the re-send, the re-send passes the precondition and
 *     RE-CREATES the pointer under the same id, now referencing a blob that
 *     compaction already deleted. The re-send can also land between the
 *     compaction's commit and its blob delete, so the writer, checking
 *     right after its write, still finds the blob.
 *
 * Either way the document is left with an unreadable update document:
 * every later compaction aborts on storage/object-not-found (it cannot
 * compact around a missing payload) and the updates backlog never shrinks.
 *
 * The lost ack is produced for real, not mocked: the writer's Firestore
 * instance talks to the emulator through a TCP proxy that can withhold
 * every server-to-client byte (the ack) and then cut the connection, so the
 * SDK reconnects and re-sends exactly as it would on a flaky mobile
 * network. (Browsers use WebChannel, which can absorb a very short blip,
 * but going offline, a network restart or a suspended tab tear the stream
 * down the same way.) Cloud Storage is not proxied. Ordering is explicit,
 * not timed: the proxy freezes right after the writer's blob upload
 * resolves (before the pointer write), the test waits until the pointer is
 * visible on the server through an unproxied client, and only then cuts
 * the connection.
 *
 * Contract asserted (user-visible, independent of how a fix works):
 *  - once the write has settled, every update document on the server has a
 *    readable payload (duplicates from the re-send are fine);
 *  - a compaction then succeeds: no 'compaction-failed', backlog drained;
 *  - the peer converges on the content.
 *
 * Run alone (through the emulator isolation wrapper):
 *   isolated.sh bash scripts/test.sh tests/integration/lost_ack_large_update_pointer.test.ts
 *
 * @file lost_ack_large_update_pointer.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { storageCtl } = vi.hoisted(() => ({
    storageCtl: {
        /**
         * When set, the next upload made through the Firebase app with this
         * name calls `onArmedUpload` once it has resolved (i.e. after the
         * blob is in Storage, before the code under test writes its pointer).
         */
        armedApp: null as string | null,
        onArmedUpload: null as ((fullPath: string) => void) | null,
        /** Storage deletes started by the code under test, still in flight. */
        inflightDeletes: new Set<Promise<unknown>>(),
        /** When set, awaited before each delete of the code under test is sent. */
        beforeDelete: null as ((fullPath: string) => Promise<void>) | null,
    },
}));

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        uploadBytes: async (storageRef: any, data: any, metadata?: any) => {
            const result = await actual.uploadBytes(storageRef, data, metadata);
            const appName: string | undefined = storageRef?.storage?.app?.name;
            if (storageCtl.armedApp !== null && appName === storageCtl.armedApp) {
                storageCtl.armedApp = null;
                const hook = storageCtl.onArmedUpload;
                storageCtl.onArmedUpload = null;
                hook?.(storageRef?.fullPath ?? String(storageRef));
            }
            return result;
        },
        deleteObject: (storageRef: any) => {
            const hook = storageCtl.beforeDelete;
            const p: Promise<unknown> = hook
                ? hook(storageRef?.fullPath ?? String(storageRef)).then(() => actual.deleteObject(storageRef))
                : actual.deleteObject(storageRef);
            storageCtl.inflightDeletes.add(p);
            p.then(
                () => storageCtl.inflightDeletes.delete(p),
                () => storageCtl.inflightDeletes.delete(p),
            );
            return p;
        },
    };
});

import * as net from 'net';
import * as Y from 'yjs';
import { initializeApp, deleteApp, FirebaseApp } from 'firebase/app';
import {
    getFirestore, connectFirestoreEmulator, collection, getDocs, doc, setDoc, Firestore,
} from 'firebase/firestore';
import { getStorage, connectStorageEmulator, ref, getBytes } from '@firebase/storage';
import { FireProvider } from '../../src/provider';
import { FIRESTORE_PATHS } from '../../src/types';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

const PROJECT_ID = 'demo-test-project';
const EMULATOR_HOST = '127.0.0.1';
const FIRESTORE_PORT = 8080;
const STORAGE_PORT = 9199;
/** ~1.1 MB once encoded: above INLINE_UPDATE_LIMIT, so offloaded to Storage. */
const BIG_CHARS = 1_100_000;

interface LossyProxy {
    port: number;
    /** Withhold (and later drop) every server-to-client byte from now on. */
    freeze(): void;
    /** Cut every connection (dropping what was withheld) and stop withholding. */
    cutAndThaw(): void;
    close(): Promise<void>;
}

/**
 * TCP proxy in front of the Firestore emulator. While frozen, requests
 * still reach the server (writes commit) but no response reaches the
 * client: the ack of a committed write is lost once the connection is cut.
 */
function startLossyProxy(targetPort: number): Promise<LossyProxy> {
    const sockets = new Set<net.Socket>();
    let frozen = false;
    const server = net.createServer((client) => {
        const upstream = net.connect(targetPort, EMULATOR_HOST);
        sockets.add(client);
        sockets.add(upstream);
        client.on('data', (chunk) => upstream.write(chunk));
        upstream.on('data', (chunk) => {
            if (!frozen) client.write(chunk);
        });
        const close = () => {
            client.destroy();
            upstream.destroy();
            sockets.delete(client);
            sockets.delete(upstream);
        };
        client.on('error', close);
        upstream.on('error', close);
        client.on('close', close);
        upstream.on('close', close);
    });
    return new Promise((resolve) => {
        server.listen(0, EMULATOR_HOST, () => {
            resolve({
                port: (server.address() as net.AddressInfo).port,
                freeze: () => { frozen = true; },
                cutAndThaw: () => {
                    for (const s of sockets) s.destroy();
                    sockets.clear();
                    frozen = false;
                },
                close: () => new Promise<void>((done) => {
                    for (const s of sockets) s.destroy();
                    sockets.clear();
                    server.close(() => done());
                }),
            });
        });
    });
}

let appCounter = 0;

/** A second Firebase app whose Firestore goes through a lossy proxy. */
async function lossyApp(): Promise<{ app: FirebaseApp; db: Firestore; proxy: LossyProxy }> {
    const proxy = await startLossyProxy(FIRESTORE_PORT);
    const app = initializeApp({
        projectId: PROJECT_ID,
        apiKey: 'fake-api-key',
        storageBucket: `${PROJECT_ID}.appspot.com`,
    }, `lossy-ack-${getStableDate()}-${appCounter++}`);
    const db = getFirestore(app);
    connectFirestoreEmulator(db, EMULATOR_HOST, proxy.port);
    connectStorageEmulator(getStorage(app), EMULATOR_HOST, STORAGE_PORT);
    return { app, db, proxy };
}

/** Waits until every Storage delete started so far has settled. */
async function settleStorageDeletes(): Promise<void> {
    while (storageCtl.inflightDeletes.size > 0) {
        await Promise.allSettled([...storageCtl.inflightDeletes]);
    }
}

describe('Lost ack of a storage-backed update pointer', () => {
    let app: any;
    let db: any;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        await clearFirestore(db);
        storageCtl.armedApp = null;
        storageCtl.onArmedUpload = null;
        storageCtl.beforeDelete = null;
    });

    const createProvider = (firebaseApp: any, ydoc: Y.Doc, path: string) => new FireProvider({
        firebaseApp,
        ydoc,
        path,
        maxWaitTime: 50,
        // Compaction runs only when the test asks for it.
        maxUpdatesThreshold: 1000,
    });

    const updateCount = async (path: string) =>
        (await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES))).size;

    /**
     * Update documents on the server whose payload cannot be read, as
     * `{id}: {error code}` (read through the unproxied default app).
     */
    async function unreadableUpdates(path: string): Promise<string[]> {
        const storage = getStorage(app);
        const snap = await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES));
        const unreadable: string[] = [];
        for (const d of snap.docs) {
            const data = d.data();
            if (data.update) continue; // inline payload
            if (typeof data.updateStoragePath !== 'string') {
                unreadable.push(`${d.id}: no payload`);
                continue;
            }
            try {
                await getBytes(ref(storage, data.updateStoragePath));
            } catch (e: any) {
                unreadable.push(`${d.id}: ${e?.code ?? e?.message ?? String(e)}`);
            }
        }
        return unreadable;
    }

    /**
     * Arms the proxy to freeze right after the writer's next blob upload,
     * i.e. just before it writes the pointer. The write stream must already
     * be open (handshake done) or the pointer would never reach the server:
     * a throwaway write through the writer's app opens it.
     */
    async function armLostAck(writer: { app: FirebaseApp; db: Firestore; proxy: LossyProxy }, path: string) {
        await setDoc(doc(writer.db, 'integration-tests', `${path.split('/').pop()}-warm`), { warm: true });
        storageCtl.armedApp = writer.app.name;
        storageCtl.onArmedUpload = () => writer.proxy.freeze();
    }

    /** Waits until the pointer has committed on the server while its ack is withheld. */
    async function waitForPointerCommittedWithoutAck(path: string) {
        await waitForConditionTruthy(async () => (await updateCount(path)) >= 1, {
            timeout: 30000, interval: 50, message: 'pointer committed while its ack is withheld',
        });
        // Precondition: the proxy really froze before the pointer write.
        expect(storageCtl.armedApp).toBeNull();
    }

    /**
     * The shared contract once the writer's write has settled: every update
     * document is readable, a compaction succeeds and drains the backlog,
     * and the peer has the content.
     */
    async function expectDocumentStillCompacts(path: string, peer: FireProvider, peerDoc: Y.Doc) {
        await settleStorageDeletes();
        // Soft, so a failure also reports what it does to compaction below.
        expect.soft(await unreadableUpdates(path), 'update documents whose payload cannot be read').toEqual([]);

        const failures: string[] = [];
        const onFailed = (e: { error: any }) => failures.push(String(e.error?.code ?? e.error?.message ?? e.error));
        peer.on('compaction-failed', onFailed);
        try {
            await peer.compact();
            await settleStorageDeletes();
        } finally {
            peer.off('compaction-failed', onFailed);
        }
        expect.soft(failures, "'compaction-failed' events").toEqual([]);
        expect(await updateCount(path), 'update documents left after compaction').toBe(0);

        await waitForConditionEquals(() => peerDoc.getText('t').length, BIG_CHARS, {
            timeout: 30000, interval: 100, message: 'peer converges on the content',
        });
    }

    it('save: a pointer committed with its ack lost keeps a readable blob, and compaction still works', async () => {
        const path = `integration-tests/lost-ack-save-${getStableDate()}-${counter++}`;
        const lossy = await lossyApp();
        const writerDoc = new Y.Doc();
        const peerDoc = new Y.Doc();
        const writer = createProvider(lossy.app, writerDoc, path);
        const peer = createProvider(app, peerDoc, path);

        try {
            await waitForConditionTruthy(() => writer.synced && peer.synced, {
                timeout: 30000, message: 'both providers synced',
            });

            await armLostAck(lossy, path);
            const saved = new Promise<void>(resolve => writer.once('saved', () => resolve()));
            writerDoc.getText('t').insert(0, 'x'.repeat(BIG_CHARS));

            await waitForPointerCommittedWithoutAck(path);
            lossy.proxy.cutAndThaw();
            // The SDK reconnects and re-sends the pointer write; the save
            // settles one way or another (here: rejected, then retried).
            await saved;

            await expectDocumentStillCompacts(path, peer, peerDoc);
        } finally {
            lossy.proxy.cutAndThaw();
            await writer.destroy();
            await peer.destroy();
            writerDoc.destroy();
            peerDoc.destroy();
            await deleteApp(lossy.app).catch(() => { });
            await lossy.proxy.close();
        }
    }, 120000);

    it('initial-sync push: a pointer committed with its ack lost keeps a readable blob, and compaction still works', async () => {
        const path = `integration-tests/lost-ack-push-${getStableDate()}-${counter++}`;
        const lossy = await lossyApp();
        const peerDoc = new Y.Doc();
        const peer = createProvider(app, peerDoc, path);
        // A local-first document: its content predates the provider, so the
        // initial sync pushes it as one oversized diff.
        const writerDoc = new Y.Doc();
        writerDoc.getText('t').insert(0, 'x'.repeat(BIG_CHARS));
        let writer: FireProvider | null = null;

        try {
            await waitForConditionTruthy(() => peer.synced, { timeout: 30000, message: 'peer synced' });

            await armLostAck(lossy, path);
            writer = createProvider(lossy.app, writerDoc, path);

            await waitForPointerCommittedWithoutAck(path);
            lossy.proxy.cutAndThaw();
            // The SDK re-sends the pointer write; initial sync completes
            // one way or another (here: rejected, then the sync retried).
            const w = writer;
            await waitForConditionTruthy(() => w.synced, {
                timeout: 60000, interval: 50, message: 'writer initial sync completed',
            });

            await expectDocumentStillCompacts(path, peer, peerDoc);
        } finally {
            lossy.proxy.cutAndThaw();
            await writer?.destroy();
            await peer.destroy();
            writerDoc.destroy();
            peerDoc.destroy();
            await deleteApp(lossy.app).catch(() => { });
            await lossy.proxy.close();
        }
    }, 120000);

    it('save: a pointer re-sent after a peer compacted it away keeps a readable blob, and compaction still works', async () => {
        const path = `integration-tests/lost-ack-resend-${getStableDate()}-${counter++}`;
        const lossy = await lossyApp();
        const writerDoc = new Y.Doc();
        const peerDoc = new Y.Doc();
        const writer = createProvider(lossy.app, writerDoc, path);
        const peer = createProvider(app, peerDoc, path);

        try {
            await waitForConditionTruthy(() => writer.synced && peer.synced, {
                timeout: 30000, message: 'both providers synced',
            });

            await armLostAck(lossy, path);
            let savedCount = 0;
            writer.on('saved', () => { savedCount++; });
            writerDoc.getText('t').insert(0, 'x'.repeat(BIG_CHARS));

            await waitForPointerCommittedWithoutAck(path);
            // While the writer is still waiting for its ack, a peer compacts
            // the pointer away (and whatever it does with the blob settles).
            await peer.compact();
            await settleStorageDeletes();
            expect(await updateCount(path)).toBe(0);

            lossy.proxy.cutAndThaw();
            // The SDK re-sends the pointer write, which now passes addDoc's
            // exists:false precondition: the save resolves. No rejection is
            // involved, so this needs more than handling ALREADY_EXISTS.
            await waitForConditionTruthy(() => savedCount > 0, {
                timeout: 60000, interval: 50, message: 'writer save settled',
            });

            await expectDocumentStillCompacts(path, peer, peerDoc);
        } finally {
            lossy.proxy.cutAndThaw();
            await writer.destroy();
            await peer.destroy();
            writerDoc.destroy();
            peerDoc.destroy();
            await deleteApp(lossy.app).catch(() => { });
            await lossy.proxy.close();
        }
    }, 120000);

    it('compaction: a pointer re-sent between the commit and the blob delete gets its blob back', async () => {
        const path = `integration-tests/lost-ack-reclaim-race-${getStableDate()}-${counter++}`;
        const lossy = await lossyApp();
        const writerDoc = new Y.Doc();
        const peerDoc = new Y.Doc();
        const writer = createProvider(lossy.app, writerDoc, path);
        const peer = createProvider(app, peerDoc, path);

        try {
            await waitForConditionTruthy(() => writer.synced && peer.synced, {
                timeout: 30000, message: 'both providers synced',
            });

            await armLostAck(lossy, path);
            let savedCount = 0;
            writer.on('saved', () => { savedCount++; });
            writerDoc.getText('t').insert(0, 'x'.repeat(BIG_CHARS));

            await waitForPointerCommittedWithoutAck(path);
            // A peer compacts the pointer away. After its commit but before
            // its blob delete is sent, the writer reconnects: the re-send
            // re-creates the pointer, and the writer's save settles while
            // the blob is still there. Only then does the delete go out.
            storageCtl.beforeDelete = async (fullPath) => {
                if (!fullPath.includes('/large_updates/')) return;
                storageCtl.beforeDelete = null;
                lossy.proxy.cutAndThaw();
                await waitForConditionTruthy(() => savedCount > 0, {
                    timeout: 60000, interval: 50, message: 'writer save settled',
                });
            };
            await peer.compact();
            // Precondition: the re-send really landed inside the reclaim.
            expect(storageCtl.beforeDelete).toBeNull();

            await expectDocumentStillCompacts(path, peer, peerDoc);
        } finally {
            storageCtl.beforeDelete = null;
            lossy.proxy.cutAndThaw();
            await writer.destroy();
            await peer.destroy();
            writerDoc.destroy();
            peerDoc.destroy();
            await deleteApp(lossy.app).catch(() => { });
            await lossy.proxy.close();
        }
    }, 120000);
});
