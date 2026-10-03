/**
 * Integration test: the main document must stay under Firestore's 1 MiB
 * entity limit on aged, many-client documents.
 *
 * A fold writes the base64 state vector (~8 bytes per client ever seen)
 * AND the inline delete-set fingerprint (up to MAX_DELETE_SET_FIELD_BYTES)
 * onto the same main document. The inline/offload decision only looks at
 * the delete-set size, so a document whose fingerprint is under the inline
 * cap but whose fingerprint + state vector exceed 1 MiB produces a main
 * document Firestore rejects with INVALID_ARGUMENT ("maximum entity size").
 * That error is not retryable and every later fold recomputes the same
 * oversized fields, so compaction stops for good.
 *
 * Scenario: a long-lived document opened in ~75k sessions. Every page load
 * gets a fresh Yjs clientID and overwrites a Y.Map key (reading progress),
 * which deletes the previous session's item, so every client contributes
 * one state-vector entry AND one delete-set entry:
 *   state vector (base64) ~600 KB + delete-set ~600 KB  >  1,048,576 B
 * while the delete-set alone stays under the 700 KB inline cap.
 *
 * Contract: folding keeps working (the main document is kept under the
 * limit however the fix chooses to do it) and the data stays readable.
 *
 * Not covered: the state vector ALONE passes 1 MiB at roughly 130k
 * clients and has no offload path; epoch squash is the remedy there.
 *
 * @file main_doc_size_budget.test.ts
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
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

/** Distinct sessions (clientIDs) the aged document has seen. */
const SESSIONS = 75_000;
/** Pending update documents the sessions are spread over (each < 1 MiB). */
const UPDATE_DOCS = 4;
/** Realistic 32-bit clientIDs (5-byte varints, like Yjs' random uint32). */
const CLIENT_BASE = 0x40000000;

/**
 * Builds UPDATE_DOCS independent updates that together contain SESSIONS
 * clients, each of which overwrote the 'position' key of the 'progress'
 * map (deleting the previous session's value).
 *
 * The clientID is switched per write inside one transaction purely to keep
 * generation O(n): a transaction per client would copy the whole state
 * vector each time.
 */
function buildAgedSessionUpdates(): Uint8Array[] {
    const perDoc = Math.ceil(SESSIONS / UPDATE_DOCS);
    const updates: Uint8Array[] = [];
    for (let c = 0; c < UPDATE_DOCS; c++) {
        const d = new Y.Doc();
        const progress = d.getMap('progress');
        const lo = c * perDoc;
        const hi = Math.min(SESSIONS, lo + perDoc);
        d.transact(() => {
            for (let i = lo; i < hi; i++) {
                d.clientID = CLIENT_BASE + i;
                progress.set('position', i);
            }
        });
        updates.push(Y.encodeStateAsUpdate(d));
        d.destroy();
    }
    return updates;
}

describe('Compaction: main-document size budget on many-client documents', () => {
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
        path = `tests/main-doc-size-budget-${getStableDate()}-${Date.now()}-${counter++}`;
    });

    it('keeps folding once state vector + inline delete-set fingerprint exceed 1 MiB', { timeout: 180_000 }, async () => {
        const sessionUpdates = buildAgedSessionUpdates();

        // Precondition: the scenario sits exactly in the gap the bug
        // describes — fingerprint under the inline cap, fingerprint +
        // base64 state vector over the Firestore document limit.
        const reference = new Y.Doc();
        sessionUpdates.forEach(u => Y.applyUpdate(reference, u));
        const sv = Y.encodeStateVector(reference);
        const svB64Bytes = Buffer.from(sv).toString('base64').length;
        const dsBytes = Y.encodeStateAsUpdate(reference, sv).byteLength;
        expect(dsBytes).toBeLessThanOrEqual(700_000);
        expect(svB64Bytes + dsBytes).toBeGreaterThan(1_048_576);
        for (const u of sessionUpdates) {
            expect(u.byteLength).toBeLessThan(1_000_000);
        }

        for (const u of sessionUpdates) {
            await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(u),
                createdAt: serverTimestamp(),
            });
        }

        const ctx: CompactionContext = {
            db,
            path,
            uid: 'compactor',
            lockTTL: 60_000,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
            historyFoldThreshold: 1, // always fold
        };

        // --- Fold #1: no base yet -> everything folds into a snapshot.
        const first = await compact(ctx);
        expect(first.error?.message).toBeUndefined();
        expect(first.success).toBe(true);
        expect(first.type).toBe('snapshot');

        let mainSnap = await getDoc(doc(db, path));
        expect(mainSnap.exists()).toBe(true);
        expect(mainSnap.data()!.version).toBe(1);
        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);

        // --- A later session (fresh clientID) overwrites the position again.
        const nextSession = new Y.Doc();
        Y.applyUpdate(nextSession, Y.encodeStateAsUpdate(reference));
        nextSession.clientID = CLIENT_BASE + SESSIONS;
        const laterSessionUpdates: Uint8Array[] = [];
        nextSession.on('update', (u: Uint8Array) => { laterSessionUpdates.push(u); });
        nextSession.getMap('progress').set('position', SESSIONS);
        expect(laterSessionUpdates).toHaveLength(1);
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(laterSessionUpdates[0]),
            createdAt: serverTimestamp(),
        });
        nextSession.destroy();
        reference.destroy();

        // --- Fold #2: base + new update -> new snapshot. Folding must keep
        // working on the aged document, not just once.
        const second = await compact(ctx);
        expect(second.error?.message).toBeUndefined();
        expect(second.success).toBe(true);
        expect(second.type).toBe('snapshot');

        mainSnap = await getDoc(doc(db, path));
        expect(mainSnap.data()!.version).toBe(2);
        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);

        // --- A fresh client converges from what compaction persisted.
        const fresh = new Y.Doc();
        const provider = new FireProvider({ firebaseApp: app, ydoc: fresh, path, maxUpdatesThreshold: 1000 });
        try {
            await waitForConditionTruthy(
                () => provider.synced && fresh.getMap('progress').get('position') === SESSIONS,
                { timeout: 60_000, message: 'fresh client sees the folded state' }
            );
        } finally {
            await provider.destroy();
            fresh.destroy();
        }
    });
});
