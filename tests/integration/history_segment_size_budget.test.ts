/**
 * Integration test: a delta-compaction history segment must stay under
 * Firestore's 1 MiB entity limit once its state vector is counted.
 *
 * DELTA mode writes the merged segment AND its base64 state vector (~8
 * bytes per client in the segment) onto one history document. The
 * inline check only looked at the segment, so a segment just under the
 * inline limit plus a many-client state vector produced a document
 * Firestore rejects with INVALID_ARGUMENT ("maximum entity size"). That
 * error is not retryable, and the same pending updates re-merge to the
 * same oversized segment on every cycle.
 *
 * Contract: compaction succeeds (a segment that cannot fit falls back to
 * a fold, or is otherwise kept under the limit), the pending updates are
 * consumed, and the data stays readable.
 *
 * @file history_segment_size_budget.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import * as Y from 'yjs';
import {
    collection,
    getDocs,
    addDoc,
    getDoc,
    doc,
    serverTimestamp,
    Bytes,
    Firestore,
} from 'firebase/firestore';
import { FirebaseStorage } from 'firebase/storage';
import { compact, CompactionContext } from '../../src/compaction';
import { FireProvider } from '../../src/provider';
import { DEFAULTS } from '../../src/types';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** Distinct clients in the pending updates (~32 KB of base64 state vector). */
const CLIENTS = 4_000;
/** Realistic 32-bit clientIDs (5-byte varints, like Yjs' random uint32). */
const CLIENT_BASE = 0x40000000;
/** Merged segment size: under the inline limit, but not by a state vector. */
const TARGET_SEGMENT_BYTES = DEFAULTS.INLINE_UPDATE_LIMIT - 4_096;

/** One update in which CLIENTS sessions each overwrote progress.position. */
function buildManyClientUpdate(): Uint8Array {
    const d = new Y.Doc();
    const progress = d.getMap('progress');
    // clientID switched per write inside one transaction: keeps generation
    // O(n) instead of copying the state vector once per client.
    d.transact(() => {
        for (let i = 0; i < CLIENTS; i++) {
            d.clientID = CLIENT_BASE + i;
            progress.set('position', i);
        }
    });
    const update = Y.encodeStateAsUpdate(d);
    d.destroy();
    return update;
}

/** One single-client update inserting `length` characters of text. */
function buildPaddingUpdate(length: number): Uint8Array {
    const d = new Y.Doc();
    d.clientID = 7;
    d.getText('notes').insert(0, 'x'.repeat(length));
    const update = Y.encodeStateAsUpdate(d);
    d.destroy();
    return update;
}

describe('Compaction: history-segment size budget on many-client updates', () => {
    let app: any;
    let db: Firestore;
    let storage: FirebaseStorage;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const emulator = await setupEmulator();
        app = emulator.app;
        db = emulator.db;
        storage = emulator.storage;
        path = `tests/history-segment-size-budget-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    it('keeps compacting when segment + state vector exceed 1 MiB', { timeout: 180_000 }, async () => {
        const ctx: CompactionContext = {
            db,
            path,
            uid: 'compactor',
            lockTTL: 60_000,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
            // Default threshold: with a base and no history, the next
            // cycle runs in DELTA mode.
        };

        // --- A base snapshot, so the next cycle is a delta.
        const baseDoc = new Y.Doc();
        baseDoc.getMap('meta').set('title', 'aged');
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(baseDoc)),
            createdAt: serverTimestamp(),
        });
        baseDoc.destroy();
        const baseResult = await compact(ctx);
        expect(baseResult.error?.message).toBeUndefined();
        expect(baseResult.type).toBe('snapshot');

        // --- Pending updates whose merged segment sits just under the
        // inline limit while its state vector pushes the document over.
        const manyClients = buildManyClientUpdate();
        let padLength = TARGET_SEGMENT_BYTES - manyClients.byteLength;
        let padding = buildPaddingUpdate(padLength);
        padLength += TARGET_SEGMENT_BYTES - Y.mergeUpdates([manyClients, padding]).byteLength;
        padding = buildPaddingUpdate(padLength);

        const segment = Y.mergeUpdates([manyClients, padding]);
        const svB64Bytes = Buffer.from(Y.encodeStateVectorFromUpdate(segment)).toString('base64').length;
        expect(segment.byteLength).toBeLessThanOrEqual(DEFAULTS.INLINE_UPDATE_LIMIT);
        expect(segment.byteLength + svB64Bytes).toBeGreaterThan(DEFAULTS.FIRESTORE_DOC_LIMIT);

        for (const u of [manyClients, padding]) {
            await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(u),
                createdAt: serverTimestamp(),
            });
        }

        const result = await compact(ctx);
        expect(result.error?.message).toBeUndefined();
        expect(result.success).toBe(true);
        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);
        expect((await getDoc(doc(db, path))).exists()).toBe(true);

        // --- A fresh client converges from what compaction persisted.
        const fresh = new Y.Doc();
        const provider = new FireProvider({ firebaseApp: app, ydoc: fresh, path, maxUpdatesThreshold: 1000 });
        try {
            await waitForConditionTruthy(
                () => provider.synced
                    && fresh.getMap('progress').get('position') === CLIENTS - 1
                    && fresh.getText('notes').length === padLength
                    && fresh.getMap('meta').get('title') === 'aged',
                { timeout: 60_000, message: 'fresh client sees the compacted state' }
            );
        } finally {
            await provider.destroy();
            fresh.destroy();
        }
    });
});
