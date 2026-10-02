/**
 * Regression test: a client must receive the data folded by its OWN
 * compaction when its local document is missing some of it.
 *
 * The snapshot listener used to return early for any main-document write
 * stamped `origin === this client's uid`, on the assumption that the
 * compacting client already holds everything it merged. It does not have
 * to: compaction reads the update documents straight from the server and
 * downloads storage-backed payloads itself, while the local doc can be
 * missing some of them — here, a storage-backed update whose listener
 * download hit a transient Cloud Storage failure and was quarantined. The
 * same transaction deletes the source update documents, so the snapshot is
 * the only path left for that data.
 *
 * When ANOTHER client compacts, the snapshot listener notices the local doc
 * does not cover the snapshot's state vector and downloads it — the
 * quarantined data (and every update parked behind it as a missing
 * dependency) arrives. When the stuck client compacts itself, the
 * own-origin skip bypassed that check, and the client stayed diverged for
 * the rest of the session while every other client (and any fresh load)
 * saw the folded content.
 *
 * @file own_compaction_snapshot_recovery.test.ts
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

const { storageOutage } = vi.hoisted(() => ({
    storageOutage: {
        /** While true, downloads of storage-backed update blobs fail. */
        active: false,
        failedDownloads: 0,
    },
}));

// Simulate a transient Cloud Storage outage for storage-backed update blobs
// (`<path>/large_updates/...`). Snapshot downloads are never affected.
vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getBytes: async (storageRef: any, ...rest: any[]) => {
            const fullPath: string = storageRef?.fullPath ?? String(storageRef);
            if (storageOutage.active && fullPath.includes('/large_updates/')) {
                storageOutage.failedDownloads++;
                const err: any = new Error('Simulated transient Cloud Storage outage');
                err.code = 'storage/retry-limit-exceeded';
                throw err;
            }
            return actual.getBytes(storageRef, ...rest);
        },
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { doc as fsDoc, getDoc } from '@firebase/firestore';
import { waitFor, waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** Comfortably above DEFAULTS.INLINE_UPDATE_LIMIT (~1 MB) → storage-backed save. */
const OVERSIZED_PAYLOAD = 'x'.repeat(1_200_000);

describe('Own compaction: snapshot recovery of data the compactor was missing', () => {
    let app: any;
    let db: any;
    let counter = 0;
    const providers: FireProvider[] = [];

    const createProvider = (ydoc: Y.Doc, path: string) => {
        const p = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxWaitTime: 50,
            maxUpdatesThreshold: 1000, // compaction only via explicit compact()
        });
        providers.push(p);
        return p;
    };

    /** Polls until `getter()` equals `expected` or the timeout elapses; never throws. */
    const settle = async <T>(getter: () => T, expected: T, timeout: number) => {
        try {
            await waitFor(getter, (v) => v === expected, { timeout, interval: 100 });
        } catch {
            /* fall through to the caller's assertion */
        }
        return getter();
    };

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storageOutage.active = false;
        storageOutage.failedDownloads = 0;
    });

    afterEach(async () => {
        storageOutage.active = false;
        await Promise.all(providers.splice(0).map((p) => p.destroy().catch(() => { /* ignore */ })));
    });

    /**
     * Writer A saves one oversized (storage-backed) update — text 'big' plus
     * a large attachment — while reader B's download of that blob fails, so
     * B quarantines it. A then types '+more' (inline update) which B
     * receives but cannot integrate (it depends on the quarantined 'big').
     */
    async function setUpQuarantinedReader(path: string) {
        const docA = new Y.Doc();
        const docB = new Y.Doc();
        const pA = createProvider(docA, path);
        const pB = createProvider(docB, path);
        await waitForConditionTruthy(() => pA.synced && pB.synced, { timeout: 30000, message: 'providers should sync' });

        const quarantined: string[] = [];
        pB.on('corrupted-document', (e: { docId: string }) => quarantined.push(e.docId));

        storageOutage.active = true;

        let saves = 0;
        pA.on('saved', () => { saves++; });

        docA.transact(() => {
            docA.getText('t').insert(0, 'big');
            docA.getMap('attachments').set('blob', OVERSIZED_PAYLOAD);
        });
        await waitForConditionTruthy(() => saves >= 1, { timeout: 30000, message: 'A should save the oversized update' });
        await waitForConditionTruthy(() => quarantined.length >= 1, {
            timeout: 30000,
            message: "B should quarantine the storage-backed update after its download fails",
        });
        expect(storageOutage.failedDownloads).toBeGreaterThan(0);

        docA.getText('t').insert(3, '+more');
        await waitForConditionTruthy(() => saves >= 2, { timeout: 30000, message: 'A should save the inline update' });

        // B is missing the quarantined 'big' (and therefore cannot show '+more').
        expect(docB.getText('t').toString()).toBe('');

        // The outage is over before anyone compacts.
        storageOutage.active = false;

        return { docA, docB, pA, pB };
    }

    it('control: when ANOTHER client compacts, the quarantined client recovers via the snapshot', async () => {
        const path = `integration-tests/own-compaction-recovery-control-${getStableDate()}-${counter++}`;
        const { docB, pA } = await setUpQuarantinedReader(path);

        await pA.compact();

        const bText = await settle(() => docB.getText('t').toString(), 'big+more', 10000);
        expect(bText).toBe('big+more');
        expect((docB.getMap('attachments').get('blob') as string | undefined)?.length).toBe(OVERSIZED_PAYLOAD.length);
    }, 120000);

    it('when the quarantined client compacts ITSELF, it still receives the data its compaction folded in', async () => {
        const path = `integration-tests/own-compaction-recovery-${getStableDate()}-${counter++}`;
        const { docB, pB } = await setUpQuarantinedReader(path);

        // B folds the server backlog — including the update it quarantined —
        // into a snapshot. The source update documents are deleted.
        await pB.compact();

        // Pin the path under test: B's OWN fold into a snapshot. (An update
        // over ~1 MB never fits a delta segment; a delta segment would reach
        // B through the history listener instead.)
        const main = (await getDoc(fsDoc(db, path))).data();
        expect(main?.origin).toBe(pB.uid);
        expect(main?.snapshotStoragePath).toBeTruthy();

        // Control: the server state is complete — a fresh client loads it.
        const docC = new Y.Doc();
        createProvider(docC, path);
        await waitForConditionEquals(() => docC.getText('t').toString(), 'big+more', {
            timeout: 30000,
            interval: 100,
            message: 'a fresh client should load the folded content',
        });

        // The compacting client must converge too.
        const bText = await settle(() => docB.getText('t').toString(), 'big+more', 10000);
        expect(bText).toBe('big+more');
        expect((docB.getMap('attachments').get('blob') as string | undefined)?.length).toBe(OVERSIZED_PAYLOAD.length);
    }, 120000);
});
