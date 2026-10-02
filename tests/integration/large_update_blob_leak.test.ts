/**
 * Storage leak benchmark + regression: storage-backed update blobs
 * (`large_updates/*.bin`) must not outlive the pointer documents that
 * reference them.
 *
 * Saves and initial-sync pushes larger than INLINE_UPDATE_LIMIT upload the
 * update to Cloud Storage (`{path}/large_updates/{uid}_{millis}.bin`) and
 * write a pointer document carrying `updateStoragePath`. Compaction (fold,
 * and the stale-epoch cleanup) and squash delete those pointer documents
 * inside their transactions, but never the blob: once the pointer is gone
 * nothing references the object, and it stays in billed Storage forever.
 * A save or initial-sync push whose `addDoc` fails AFTER a successful
 * upload is retried under a fresh `Date.now()` path, so every failed
 * attempt orphans another full copy before compaction even runs.
 *
 * This file measures the leak with deterministic counters instead of
 * timings:
 *  - uploads to / deletes from `large_updates/` (call count and bytes),
 *    recorded by wrapping the Storage SDK the provider uses;
 *  - the Storage ledger: every object under `{path}/large_updates/`
 *    (`listAll`, sized from the recorded uploads) versus the
 *    `updateStoragePath` of the pointer documents that still exist. An
 *    object no pointer references is an orphan: nothing will ever read or
 *    delete it.
 *
 * Each scenario prints one `[blob-leak]` JSON line with its counters, then
 * asserts the contract: once the operation that consumed (or abandoned) a
 * blob has committed, no orphaned `large_updates/` object remains. A short
 * grace period (GRACE_MS) allows a fix that deletes asynchronously after the
 * commit; the fold scenario additionally tolerates ONE cycle of deferral (a
 * fix that reclaims the previous cycle's blobs on the next fold) by
 * asserting the orphan backlog stays bounded instead of growing per cycle.
 *
 * Run alone (through the emulator isolation wrapper):
 *   isolated.sh bash scripts/test.sh tests/integration/large_update_blob_leak.test.ts
 *
 * @file large_update_blob_leak.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { mockControls } = vi.hoisted(() => ({
    mockControls: {
        /** Upcoming pointer-document addDoc calls to fail (transient error). */
        pointerAddDocFailures: 0,
        /** Pointer-document addDoc calls that were failed on purpose. */
        pointerAddDocFailed: 0,
        /** uploadBytes calls made by the code under test. */
        uploads: [] as { path: string; bytes: number }[],
        /** deleteObject calls made by the code under test. */
        deletes: [] as string[],
        /**
         * Size of every object uploaded in this test, by full path (never
         * reset mid-test). The ledger sizes objects from here instead of a
         * getMetadata round trip per object: the Storage emulator
         * occasionally drops one of several parallel metadata requests,
         * which hangs the SDK call until the test times out.
         */
        objectBytes: new Map<string, number>(),
    },
}));

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        uploadBytes: async (storageRef: any, data: any, metadata?: any) => {
            const path: string = storageRef?.fullPath ?? String(storageRef);
            const bytes: number = data?.byteLength ?? data?.size ?? 0;
            mockControls.uploads.push({ path, bytes });
            mockControls.objectBytes.set(path, bytes);
            return actual.uploadBytes(storageRef, data, metadata);
        },
        deleteObject: async (storageRef: any) => {
            mockControls.deletes.push(storageRef?.fullPath ?? String(storageRef));
            return actual.deleteObject(storageRef);
        },
    };
});

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        addDoc: async (collectionRef: any, data: any) => {
            // Fail only the pointer write of a storage-backed update, i.e.
            // AFTER its blob has been uploaded: a dropped connection, an
            // expired token, a backend 'unavailable' blip.
            if (data?.updateStoragePath && mockControls.pointerAddDocFailures > 0) {
                mockControls.pointerAddDocFailures--;
                mockControls.pointerAddDocFailed++;
                const err: any = new Error('Simulated transient Firestore failure');
                err.code = 'unavailable';
                throw err;
            }
            return actual.addDoc(collectionRef, data);
        },
    };
});

import * as Y from 'yjs';
import { addDoc, collection, doc, getDoc, getDocs, serverTimestamp } from '@firebase/firestore';
import { getMetadata, listAll, ref, uploadBytes, FirebaseStorage } from '@firebase/storage';
import { FireProvider } from '../../src/provider';
import { compact, CompactionContext } from '../../src/compaction';
import { squashDocument } from '../../src/squash';
import { largeUpdatePath } from '../../src/sync-policy';
import { aggregateClockEnds, extractClockEnds } from '../../src/update-metadata';
import { DEFAULTS, FIRESTORE_PATHS } from '../../src/types';
import { setupEmulator, clearFirestore } from '../utils/emulator';
import { waitForConditionEquals, waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** Characters per oversized write: ~1.1 MB once encoded, above INLINE_UPDATE_LIMIT. */
const BIG_CHARS = 1_100_000;
/** How long an asynchronous post-commit delete may take to land. */
const GRACE_MS = 5000;
const LARGE_PREFIX = 'large_updates/';

interface Ledger {
    /** Objects under {path}/large_updates/. */
    objects: number;
    objectBytes: number;
    /** Pointer documents that still reference a blob. */
    referenced: number;
    /** Objects no surviving pointer document references. */
    orphans: number;
    orphanBytes: number;
}

describe('Storage-backed update blobs are reclaimed (large_updates/*.bin)', () => {
    let app: any;
    let db: any;
    let storage: FirebaseStorage;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage as unknown as FirebaseStorage;
        await clearFirestore(db);
        mockControls.pointerAddDocFailures = 0;
        mockControls.pointerAddDocFailed = 0;
        mockControls.uploads = [];
        mockControls.deletes = [];
        mockControls.objectBytes = new Map();
    });

    const newPath = (name: string) =>
        `tests/blob-leak-${name}-${getStableDate()}-${Date.now()}-${counter++}`;

    const createProvider = (ydoc: Y.Doc, path: string) => new FireProvider({
        firebaseApp: app,
        ydoc,
        path,
        maxWaitTime: 50,
        // Compaction runs only when a test asks for it.
        maxUpdatesThreshold: 1000,
    });

    const compactionCtx = (path: string, uid = 'compactor'): CompactionContext => ({
        db,
        path,
        uid,
        lockTTL: 60000,
        compactionLimit: DEFAULTS.COMPACTION_LIMIT,
        isDestroyed: () => false,
        storage,
    });

    /**
     * Runs one Storage emulator request with a deadline, retrying a request
     * the emulator dropped (seen under CPU load: the SDK call then never
     * settles) instead of letting it hang the test.
     */
    async function storageCall<T>(fn: () => Promise<T>, what: string): Promise<T> {
        for (let attempt = 1; ; attempt++) {
            let timer: ReturnType<typeof setTimeout> | undefined;
            const deadline = new Promise<never>((_, reject) => {
                timer = setTimeout(() => reject(new Error(`${what}: no response in 10s`)), 10000);
            });
            try {
                return await Promise.race([fn(), deadline]);
            } catch (e) {
                if (attempt >= 3) throw e;
            } finally {
                clearTimeout(timer);
            }
        }
    }

    /** Storage objects under the document's large_updates/ prefix vs. surviving pointers. */
    async function ledger(path: string): Promise<Ledger> {
        const listing = await storageCall(() => listAll(ref(storage, `${path}/large_updates`)), 'listAll');
        const objects: { path: string; bytes: number }[] = [];
        for (const item of listing.items) {
            const known = mockControls.objectBytes.get(item.fullPath);
            objects.push({
                path: item.fullPath,
                bytes: known ?? (await storageCall(() => getMetadata(item), 'getMetadata')).size,
            });
        }
        const updatesSnap = await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES));
        const referenced = new Set<string>(
            updatesSnap.docs
                .map(d => d.data().updateStoragePath)
                .filter((p): p is string => typeof p === 'string'),
        );
        const orphans = objects.filter(o => !referenced.has(o.path));
        return {
            objects: objects.length,
            objectBytes: objects.reduce((s, o) => s + o.bytes, 0),
            referenced: referenced.size,
            orphans: orphans.length,
            orphanBytes: orphans.reduce((s, o) => s + o.bytes, 0),
        };
    }

    /**
     * The ledger once post-commit cleanup had GRACE_MS to land: returns as
     * soon as no orphan is left, otherwise the state at the deadline.
     */
    async function settledLedger(path: string): Promise<Ledger> {
        const deadline = Date.now() + GRACE_MS;
        let current = await ledger(path);
        while (current.orphans > 0 && Date.now() < deadline) {
            await new Promise(r => setTimeout(r, 500));
            current = await ledger(path);
        }
        return current;
    }

    /** uploadBytes / deleteObject traffic on large_updates/ since the last reset. */
    function transferCounters() {
        const uploads = mockControls.uploads.filter(u => u.path.includes(LARGE_PREFIX));
        return {
            largeUploads: uploads.length,
            largeUploadBytes: uploads.reduce((s, u) => s + u.bytes, 0),
            largeDeletes: mockControls.deletes.filter(p => p.includes(LARGE_PREFIX)).length,
        };
    }

    const report = (scenario: string, data: Record<string, unknown>) => {
        console.log(`[blob-leak] ${JSON.stringify({ scenario, ...data })}`);
    };

    /** Replaces the document's text with BIG_CHARS copies of `ch` in one transaction. */
    function writeBig(ydoc: Y.Doc, ch: string) {
        const text = ydoc.getText('content');
        ydoc.transact(() => {
            if (text.length > 0) text.delete(0, text.length);
            text.insert(0, ch.repeat(BIG_CHARS));
        });
    }

    /** Resolves after the provider's next committed save. */
    function nextSaved(provider: FireProvider): Promise<void> {
        return new Promise(resolve => provider.once('saved', () => resolve()));
    }

    async function updateDocCount(path: string): Promise<number> {
        return (await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES))).size;
    }

    it('fold compaction reclaims each consumed oversized save\'s blob (no per-cycle growth)', { timeout: 180000 }, async () => {
        const CYCLES = 3;
        const path = newPath('fold');
        const ydoc = new Y.Doc();
        const provider = createProvider(ydoc, path);
        await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: 'provider synced' });

        const perCycle: Ledger[] = [];
        let lastVersion = 0;
        for (let cycle = 0; cycle < CYCLES; cycle++) {
            const saved = nextSaved(provider);
            writeBig(ydoc, String.fromCharCode(97 + cycle)); // 'a', 'b', 'c'
            await saved;

            const beforeFold = await ledger(path);
            expect(beforeFold.referenced, 'the save went to Storage behind a pointer').toBe(1);

            await provider.compact();

            // Precondition: the fold committed and consumed the pointer.
            const main = (await getDoc(doc(db, path))).data()!;
            expect(main.version).toBeGreaterThan(lastVersion);
            lastVersion = main.version;
            expect(await updateDocCount(path)).toBe(0);

            perCycle.push(await settledLedger(path));
        }

        await provider.destroy();
        ydoc.destroy();

        const counters = transferCounters();
        const maxBlobBytes = Math.max(
            ...mockControls.uploads.filter(u => u.path.includes(LARGE_PREFIX)).map(u => u.bytes),
        );
        report('fold', {
            cycles: CYCLES,
            blobBytes: maxBlobBytes,
            ...counters,
            orphansPerCycle: perCycle.map(l => l.orphans),
            orphanBytesPerCycle: perCycle.map(l => l.orphanBytes),
        });

        const last = perCycle[CYCLES - 1];
        // Contract: nothing references a consumed blob, so the backlog of
        // unreferenced blobs must stay bounded — at most one blob still
        // awaiting a deferred delete — instead of one more full copy per
        // fold, forever.
        expect(last.orphans, `orphaned large_updates blobs after ${CYCLES} folds`).toBeLessThanOrEqual(1);
        expect(last.orphanBytes, `orphaned large_updates bytes after ${CYCLES} folds`).toBeLessThanOrEqual(maxBlobBytes);
    });

    it('squash reclaims the blob of the oversized-save pointer it deletes', { timeout: 120000 }, async () => {
        const path = newPath('squash');
        const ydoc = new Y.Doc();
        const provider = createProvider(ydoc, path);
        await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: 'provider synced' });

        const saved = nextSaved(provider);
        writeBig(ydoc, 's');
        await saved;
        await provider.destroy();

        const beforeSquash = await ledger(path);
        expect(beforeSquash.referenced).toBe(1);
        expect(beforeSquash.orphans).toBe(0);

        // squashDocument directly: provider.squash() folds first, which
        // would consume the pointer on the compaction path instead.
        const result = await squashDocument({
            db, path, uid: 'squasher', lockTTL: 60000, storage,
            isDestroyed: () => false, doc: ydoc,
        });
        expect(result.error).toBeUndefined();
        expect(result.success).toBe(true);
        expect(await updateDocCount(path)).toBe(0);

        const after = await settledLedger(path);
        ydoc.destroy();
        report('squash', { ...transferCounters(), before: beforeSquash, after });

        expect(after.orphans, 'large_updates blobs orphaned by squash').toBe(0);
        expect(after.orphanBytes).toBe(0);
    });

    it('stale-epoch cleanup reclaims the blob of the old-epoch pointer it deletes', { timeout: 120000 }, async () => {
        const path = newPath('stale');

        // Epoch 1: a small document squashed once.
        const ydoc = new Y.Doc();
        const provider = createProvider(ydoc, path);
        await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: 'provider synced' });
        const saved = nextSaved(provider);
        ydoc.getMap('m').set('k', 'v');
        await saved;
        await provider.destroy();
        const squashed = await squashDocument({
            db, path, uid: 'squasher', lockTTL: 60000, storage,
            isDestroyed: () => false, doc: ydoc,
        });
        expect(squashed.success).toBe(true);
        ydoc.destroy();

        // An epoch-0 device's oversized save whose pointer landed after the
        // squash committed (its 1 MB upload was still in flight): written
        // exactly as FireProvider writes one, with no epoch tag.
        const oldDoc = new Y.Doc();
        oldDoc.getText('content').insert(0, 'o'.repeat(BIG_CHARS));
        const update = Y.encodeStateAsUpdate(oldDoc);
        oldDoc.destroy();
        const storagePath = largeUpdatePath(path, 'old-epoch-device', Date.now());
        await uploadBytes(ref(storage, storagePath), update);
        await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
            updateStoragePath: storagePath,
            createdAt: serverTimestamp(),
            createdBy: 'old-epoch-device',
            ...aggregateClockEnds(extractClockEnds(update)),
        });
        mockControls.uploads = [];
        mockControls.deletes = [];

        const before = await ledger(path);
        expect(before.referenced).toBe(1);

        const result = await compact(compactionCtx(path));
        expect(result.success).toBe(true);
        expect(await updateDocCount(path), 'stale pointer deleted').toBe(0);

        const after = await settledLedger(path);
        report('stale-epoch', { ...transferCounters(), before, after });

        expect(after.orphans, 'large_updates blobs orphaned by stale-epoch cleanup').toBe(0);
        expect(after.orphanBytes).toBe(0);
    });

    it('save retries do not orphan a re-uploaded copy per failed pointer write', { timeout: 120000 }, async () => {
        const FAILURES = 3;
        const path = newPath('save-retry');
        const ydoc = new Y.Doc();
        const provider = createProvider(ydoc, path);
        await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: 'provider synced' });

        mockControls.pointerAddDocFailures = FAILURES;
        const saved = nextSaved(provider);
        writeBig(ydoc, 'r');
        await saved;
        expect(mockControls.pointerAddDocFailed).toBe(FAILURES);

        const afterSave = await settledLedger(path);
        const counters = transferCounters();

        // The referenced blob is consumed by the next fold as well.
        await provider.compact();
        expect(await updateDocCount(path)).toBe(0);
        const afterFold = await settledLedger(path);

        await provider.destroy();
        ydoc.destroy();
        report('save-retry', { failures: FAILURES, ...counters, afterSave, afterFold });

        expect(afterSave.referenced, 'exactly one pointer committed').toBe(1);
        // Contract: an abandoned attempt's upload is reclaimed (or reused),
        // so only the committed pointer's blob is in Storage.
        expect(afterSave.orphans, 'blobs orphaned by failed save attempts').toBe(0);
        expect(afterSave.orphanBytes).toBe(0);
    });

    it('initial-sync push retries do not orphan each attempt\'s upload', { timeout: 120000 }, async () => {
        const FAILURES = 2;
        const path = newPath('push');
        // Local-first: >1 MB authored before the document ever synced.
        const ydoc = new Y.Doc();
        ydoc.getText('content').insert(0, 'p'.repeat(BIG_CHARS));

        mockControls.pointerAddDocFailures = FAILURES;
        const provider = createProvider(ydoc, path);
        await waitForConditionTruthy(() => provider.synced, { timeout: 60000, message: 'provider synced after push retries' });
        expect(mockControls.pointerAddDocFailed).toBe(FAILURES);
        await waitForConditionEquals(() => updateDocCount(path), 1, { timeout: 10000, message: 'push pointer committed' });

        const afterPush = await settledLedger(path);
        const counters = transferCounters();

        await provider.compact();
        expect(await updateDocCount(path)).toBe(0);
        const afterFold = await settledLedger(path);

        await provider.destroy();
        ydoc.destroy();
        report('initial-sync-push', { failures: FAILURES, ...counters, afterPush, afterFold });

        expect(afterPush.referenced, 'exactly one pointer committed').toBe(1);
        expect(afterPush.orphans, 'blobs orphaned by failed push attempts').toBe(0);
        expect(afterPush.orphanBytes).toBe(0);
    });
});
