/**
 * Regression: compaction must not paper over a failed storage-backed download.
 *
 * Scenario: one client writes three sequential edits, X[0,3) X[3,6) X[6,9).
 * The middle one was large enough to be offloaded to Cloud Storage, and its
 * blob cannot be downloaded while compaction runs (transient Storage error,
 * simulated here by the blob not existing yet). Compaction used to log the
 * failure, drop that update and merge the SAME client's later update anyway,
 * committing a snapshot (fold) or history segment (delta) that holds
 * X[0,3) + X[6,9) with a hole at X[3,6) but whose stored state vector claims
 * X:9. Clients trust that vector, so the update document that still carries
 * X[3,6) is skipped as redundant, and a fresh client ends up with only the
 * text before the gap; the parked X[6,9) structs also keep GC disabled.
 *
 * Contract pinned here (valid for "abort and retry later" and for "leave out
 * every later update of that client" fixes alike):
 *  1. The compacted tiers (snapshot + history segments) are self-contained:
 *     applying them leaves nothing parked in pendingStructs, and no stored
 *     state vector claims a clock beyond what they actually integrate.
 *  2. Once the blob is downloadable again, a fresh client loads every edit.
 *
 * @file compaction_download_gap.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import * as Y from 'yjs';
import { fromBase64 } from 'lib0/buffer';
import {
    Firestore,
    collection,
    addDoc,
    getDoc,
    getDocs,
    doc,
    serverTimestamp,
    Bytes,
} from 'firebase/firestore';
import { FirebaseStorage, ref, uploadBytes, getBytes } from 'firebase/storage';
import { setupEmulator } from '../utils/emulator';
import { getStableDate } from '../unit/prng';
import { compact, CompactionContext } from '../../src/compaction';
import { performInitialSync } from '../../src/sync';
import { extractClockEnds, aggregateClockEnds } from '../../src/update-metadata';
import { FIRESTORE_PATHS } from '../../src/types';

const WRITER_CLIENT_ID = 4242;

/** Three sequential edits from one client: 'AAA' X[0,3), 'BBB' X[3,6), 'CCC' X[6,9). */
function makeSequentialUpdates(parts: string[]): Uint8Array[] {
    const src = new Y.Doc();
    src.clientID = WRITER_CLIENT_ID;
    const updates: Uint8Array[] = [];
    src.on('update', (u: Uint8Array) => updates.push(u));
    const text = src.getText('content');
    for (const part of parts) {
        text.insert(text.length, part);
    }
    src.destroy();
    return updates;
}

describe('Compaction with a failed storage-backed download', () => {
    let db: Firestore;
    let storage: FirebaseStorage;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        db = setup.db as unknown as Firestore;
        storage = setup.storage as unknown as FirebaseStorage;
        path = `tests/compaction-download-gap-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    const ctx = (): CompactionContext => ({
        db,
        path,
        uid: 'compactor',
        lockTTL: 60000,
        compactionLimit: 500,
        isDestroyed: () => false,
        storage,
        historyFoldThreshold: 8,
    });

    async function addInlineUpdate(update: Uint8Array): Promise<void> {
        await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
            update: Bytes.fromUint8Array(update),
            createdAt: serverTimestamp(),
            createdBy: 'writer',
            ...aggregateClockEnds(extractClockEnds(update)),
        });
    }

    /**
     * Writes the pointer document of a storage-backed update WITHOUT the blob
     * (the download fails while compaction runs). Returns the storage path.
     *
     * Real writers upload the blob before the pointer; a missing blob is just
     * a deterministic way to make getBytes reject, standing in for any
     * download failure (quota, 403, retry-limit-exceeded, Storage outage).
     */
    async function addStorageBackedUpdatePointer(update: Uint8Array): Promise<string> {
        const storagePath = `${path}/large_updates/writer_${Date.now()}.bin`;
        await addDoc(collection(db, path, FIRESTORE_PATHS.UPDATES), {
            updateStoragePath: storagePath,
            createdAt: serverTimestamp(),
            createdBy: 'writer',
            ...aggregateClockEnds(extractClockEnds(update)),
        });
        return storagePath;
    }

    /**
     * Applies everything compaction committed (snapshot + history segments)
     * to a fresh doc and checks it is self-contained and that no stored
     * state vector claims clocks the compacted data does not integrate.
     */
    async function expectCompactedTiersSelfContained(): Promise<void> {
        const blobs: Uint8Array[] = [];
        const claims: { source: string; sv: Map<number, number> }[] = [];

        const mainSnap = await getDoc(doc(db, path));
        const main = mainSnap.exists() ? mainSnap.data() : undefined;
        if (main?.snapshotStoragePath) {
            blobs.push(new Uint8Array(await getBytes(ref(storage, main.snapshotStoragePath))));
        }
        if (typeof main?.stateVector === 'string') {
            claims.push({ source: 'snapshot', sv: Y.decodeStateVector(fromBase64(main.stateVector)) });
        }
        const historySnap = await getDocs(collection(db, path, FIRESTORE_PATHS.HISTORY));
        for (const h of historySnap.docs) {
            const data = h.data();
            if (data.segment) blobs.push((data.segment as Bytes).toUint8Array());
            if (typeof data.stateVector === 'string') {
                claims.push({ source: `history segment ${h.id}`, sv: Y.decodeStateVector(fromBase64(data.stateVector)) });
            }
        }

        const probe = new Y.Doc();
        try {
            for (const blob of blobs) Y.applyUpdate(probe, blob);
            const integrated = Y.decodeStateVector(Y.encodeStateVector(probe));

            for (const { source, sv } of claims) {
                for (const [client, clock] of sv) {
                    expect(
                        clock,
                        `${source} stateVector claims client ${client} up to clock ${clock}, ` +
                        `but the compacted data only integrates up to clock ${integrated.get(client) ?? 0}`
                    ).toBeLessThanOrEqual(integrated.get(client) ?? 0);
                }
            }

            const store = probe.store as unknown as { pendingStructs: unknown };
            expect(
                store.pendingStructs,
                'compacted snapshot/history holds structs past a clock gap (parked in pendingStructs)'
            ).toBeNull();
        } finally {
            probe.destroy();
        }
    }

    /** Whether the update document carrying this storage pointer still exists. */
    async function pointerDocExists(storagePath: string): Promise<boolean> {
        const updatesSnap = await getDocs(collection(db, path, FIRESTORE_PATHS.UPDATES));
        return updatesSnap.docs.some(d => d.data().updateStoragePath === storagePath);
    }

    async function freshClientText(): Promise<string> {
        const fresh = new Y.Doc();
        try {
            const result = await performInitialSync({
                db,
                path,
                doc: fresh,
                uid: 'fresh-reader',
                maxUpdatesThreshold: 1000,
                isDestroyed: () => false,
                storage,
            });
            expect(result.success).toBe(true);
            return fresh.getText('content').toString();
        } finally {
            fresh.destroy();
        }
    }

    it('fold: does not commit a snapshot that skips the undownloadable update but keeps the same client\'s later one', async () => {
        const [u1, u2, u3] = makeSequentialUpdates(['AAA', 'BBB', 'CCC']);

        await addInlineUpdate(u1);
        const u2Path = await addStorageBackedUpdatePointer(u2); // blob missing during compaction
        await addInlineUpdate(u3);

        // No base yet -> this cycle folds into a snapshot.
        await compact(ctx());

        await expectCompactedTiersSelfContained();
        // The only copy of X[3,6) must not be deleted while it is unreadable.
        expect(await pointerDocExists(u2Path)).toBe(true);

        // The Storage outage ends; the blob becomes downloadable.
        await uploadBytes(ref(storage, u2Path), u2);

        expect(await freshClientText()).toBe('AAABBBCCC');

        // The next cycle compacts the leftovers.
        await compact(ctx());

        await expectCompactedTiersSelfContained();
        expect(await pointerDocExists(u2Path)).toBe(false);
        expect(await freshClientText()).toBe('AAABBBCCC');
    }, 60000);

    it('delta: does not commit a history segment that skips the undownloadable update but keeps the same client\'s later one', async () => {
        const [u1, u2, u3] = makeSequentialUpdates(['AAA', 'BBB', 'CCC']);

        // Establish a base snapshot holding X[0,3).
        await addInlineUpdate(u1);
        const first = await compact(ctx());
        expect(first.type).toBe('snapshot');

        await expectCompactedTiersSelfContained();

        const u2Path = await addStorageBackedUpdatePointer(u2); // blob missing during compaction
        await addInlineUpdate(u3);

        // Base exists and history is below the fold threshold -> delta cycle.
        await compact(ctx());

        await expectCompactedTiersSelfContained();
        expect(await pointerDocExists(u2Path)).toBe(true);

        await uploadBytes(ref(storage, u2Path), u2);

        expect(await freshClientText()).toBe('AAABBBCCC');

        await compact(ctx());

        await expectCompactedTiersSelfContained();
        expect(await pointerDocExists(u2Path)).toBe(false);
        expect(await freshClientText()).toBe('AAABBBCCC');
    }, 60000);
});
