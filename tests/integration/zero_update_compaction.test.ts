/**
 * Performance regression: a compaction that finds no pending update
 * documents must not run a full O(snapshot) FOLD.
 *
 * compact() returns early only when the updates AND history queries are
 * both empty. With zero update documents but one or more current-epoch
 * history segments, shouldUseDelta() returns false ("nothing new to add")
 * and the cycle folds the whole base: download the base snapshot from
 * Cloud Storage, merge + GC it (worker CPU, O(snapshot)), upload a new
 * snapshot, and rewrite the main document — which fans out to every
 * connected client's snapshot listener (each re-applies the delete-set
 * fingerprint). It does so even with ONE segment against a fold threshold
 * of 8, although folding early is never needed for correctness: readers
 * apply history, and the fold happens anyway at the threshold.
 *
 * Callers that reach compaction with nothing new:
 *
 *  - a late lock winner: its trigger was decided on a stale count, and
 *    another device already drained every update into a delta segment;
 *  - squash(): it runs compact() "to fold the backlog" unconditionally,
 *    although the squash transaction only needs <= MAX_COMPACTION_UPDATES
 *    updates and <= MAX_COMPACTION_HISTORY segments, and its new-epoch
 *    snapshot supersedes the fold moments later. The same waste happens
 *    when history sits at historyFoldThreshold - 1 (a normal fold is due);
 *  - app-driven compact() calls.
 *
 * The document is a versicle-shaped aged document (benchmarks/
 * versicle-workload.ts: ten Y.Map roots, page-turn / TTS overwrites, a
 * fresh clientID per session), aged to two sizes 4x apart, so the logged
 * numbers show the O(snapshot) scaling of the wasted work while the new
 * data stays at zero.
 *
 * Costs are counted, never timed: Cloud Storage getBytes/uploadBytes
 * (calls and bytes, via a wrapped @firebase/storage), compaction merges
 * (calls and input bytes, via a wrapped merge-utils), and main-document
 * rewrites (the snapshot `version` delta).
 *
 * Run (always through the isolation wrapper):
 *   isolated.sh bash scripts/test.sh tests/integration/zero_update_compaction.test.ts
 * Set ZERO_UPDATE_AGES=24,96,240 to also log the 14.4k-event size.
 *
 * @file zero_update_compaction.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { io } = vi.hoisted(() => ({
    io: {
        downloads: [] as { path: string; bytes: number }[],
        uploads: [] as { path: string; bytes: number }[],
        merges: [] as { inputs: number; inputBytes: number; gc: boolean; ms: number }[],
    },
}));

vi.mock('@firebase/storage', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getBytes: async (storageRef: any, maxDownloadSizeBytes?: number) => {
            const buffer: ArrayBuffer = await actual.getBytes(storageRef, maxDownloadSizeBytes);
            io.downloads.push({ path: storageRef?.fullPath ?? String(storageRef), bytes: buffer.byteLength });
            return buffer;
        },
        uploadBytes: async (storageRef: any, data: Uint8Array, metadata?: any) => {
            io.uploads.push({ path: storageRef?.fullPath ?? String(storageRef), bytes: data.byteLength });
            return actual.uploadBytes(storageRef, data, metadata);
        },
    };
});

vi.mock('../../src/merge-utils', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        mergeUpdatesWithMetaAsync: async (updates: Uint8Array[], options?: { gc?: boolean }) => {
            const t0 = performance.now();
            const result = await actual.mergeUpdatesWithMetaAsync(updates, options);
            io.merges.push({
                inputs: updates.length,
                inputBytes: updates.reduce((n, u) => n + u.byteLength, 0),
                gc: options?.gc === true,
                ms: performance.now() - t0,
            });
            return result;
        },
    };
});

import * as Y from 'yjs';
import {
    collection,
    addDoc,
    doc,
    getDoc,
    getDocs,
    serverTimestamp,
    Bytes,
    Firestore,
} from 'firebase/firestore';
import { FirebaseStorage, ref, uploadBytes } from 'firebase/storage';
import { compact, CompactionContext } from '../../src/compaction';
import { FireProvider } from '../../src/provider';
import { DEFAULTS } from '../../src/types';
import { extractClockEnds, aggregateClockEnds } from '../../src/update-metadata';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';
import { createSim, runSession, clientIdForSession, VersicleSimState } from '../../benchmarks/versicle-workload';

const SEED = 20260820; // the versicle-aging benchmarks' seed
/** n and 4n sessions (60 events each): ~400 KB and ~1.45 MB GC'd snapshots */
const AGES = (process.env.ZERO_UPDATE_AGES ?? '24,96').split(',').map(Number);

const VERSICLE_ROOTS = ['library', 'progress', 'annotations', 'reading-list', 'vocabulary', 'lexicon', 'contentAnalysis', 'devices', 'searchHistory', 'meta'];

/** A fold commits a snapshot named snapshot_v{version}_{attempt}.bin */
const FOLD_SNAPSHOT = /\/snapshot_v\d+_[^/]*\.bin$/;
/** squash() commits a snapshot named snapshot_e{epoch}_v{version}_{attempt}.bin */
const SQUASH_SNAPSHOT = /\/snapshot_e\d+_v\d+_[^/]*\.bin$/;

function resetIo(): void {
    io.downloads.length = 0;
    io.uploads.length = 0;
    io.merges.length = 0;
}

function sum(rows: { bytes: number }[]): number {
    return rows.reduce((n, r) => n + r.bytes, 0);
}

function kb(n: number): string {
    return n >= 1024 * 1024 ? `${(n / 1024 / 1024).toFixed(2)} MB` : `${(n / 1024).toFixed(1)} KB`;
}

/** What one measured operation cost, from the counters. */
function costSince(versionBefore: number, versionAfter: number) {
    const baseDownloads = io.downloads.filter(d => FOLD_SNAPSHOT.test(d.path) || SQUASH_SNAPSHOT.test(d.path));
    const foldUploads = io.uploads.filter(u => FOLD_SNAPSHOT.test(u.path));
    return {
        baseDownloads: baseDownloads.length,
        bytesDown: sum(io.downloads),
        foldUploads: foldUploads.length,
        squashUploads: io.uploads.filter(u => SQUASH_SNAPSHOT.test(u.path)).length,
        bytesUp: sum(io.uploads),
        foldMerges: io.merges.filter(m => m.gc).length,
        foldMergeInputBytes: io.merges.filter(m => m.gc).reduce((n, m) => n + m.inputBytes, 0),
        foldMergeMs: io.merges.filter(m => m.gc).reduce((n, m) => n + m.ms, 0),
        mainDocRewrites: versionAfter - versionBefore,
    };
}

async function mainVersion(db: Firestore, path: string): Promise<number> {
    const snap = await getDoc(doc(db, path));
    const v = snap.exists() ? snap.data()?.version : undefined;
    return typeof v === 'number' ? v : 0;
}

/**
 * Builds a versicle-shaped document aged `sessions` sessions, one fresh
 * clientID per session (like versicle's fresh Y.Doc per page load).
 */
function agedDoc(sessions: number): { doc: Y.Doc; sim: VersicleSimState } {
    const sim = createSim({ seed: SEED });
    const ydoc = new Y.Doc();
    for (let s = 0; s < sessions; s++) {
        ydoc.clientID = clientIdForSession(SEED, s);
        runSession(sim, ydoc);
    }
    return { doc: ydoc, sim };
}

/** Runs one more session on `ydoc` and returns its debounced-save blobs. */
function nextSession(sim: VersicleSimState, ydoc: Y.Doc): Uint8Array[] {
    ydoc.clientID = clientIdForSession(SEED, sim.sessionCount);
    return runSession(sim, ydoc).blobs;
}

/**
 * Types the versicle roots as the app does before squashing: a root only
 * hydrated from the server is a generic AbstractType that squash refuses
 * to clone.
 */
function typeVersicleRoots(ydoc: Y.Doc): void {
    for (const name of VERSICLE_ROOTS) ydoc.getMap(name);
}

/**
 * Writes update documents shaped exactly like a provider save (inline
 * payload + clientIDs/clientClocks metadata), oldest first.
 */
async function addInlineUpdates(db: Firestore, path: string, blobs: Uint8Array[], uid: string): Promise<void> {
    // Sequential: compaction drains in createdAt order.
    for (const blob of blobs) {
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(blob),
            createdAt: serverTimestamp(),
            createdBy: uid,
            ...aggregateClockEnds(extractClockEnds(blob)),
        });
    }
}

/** Merges a session's blobs into about `docs` update documents. */
function batchBlobs(blobs: Uint8Array[], docs: number): Uint8Array[] {
    const out: Uint8Array[] = [];
    const per = Math.ceil(blobs.length / docs);
    for (let i = 0; i < blobs.length; i += per) {
        out.push(Y.mergeUpdates(blobs.slice(i, i + per)));
    }
    return out;
}

describe('Performance: zero-update compaction folds', () => {
    let db: Firestore;
    let storage: FirebaseStorage;
    let app: any;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        storage = setup.storage;
        resetIo();
    });

    function ctxFor(path: string, uid: string): CompactionContext {
        return {
            db,
            path,
            uid,
            lockTTL: 60000,
            compactionLimit: DEFAULTS.COMPACTION_LIMIT,
            isDestroyed: () => false,
            storage,
        };
    }

    /**
     * Server state: a folded base snapshot of the aged document plus
     * `segments` delta history segments, each produced by a real delta
     * compaction of one more session's saves. No update documents remain.
     * Returns the local doc holding exactly the server's content.
     */
    async function seedBasePlusHistory(path: string, sessions: number, segments: number) {
        const { doc: ydoc, sim } = agedDoc(sessions);

        // The aged state as ONE Storage-backed update document; the first
        // compaction (no base yet) folds it into snapshot v1.
        const seedPath = `${path}/seed_state.bin`;
        await uploadBytes(ref(storage, seedPath), Y.encodeStateAsUpdate(ydoc));
        await addDoc(collection(db, path, 'updates'), {
            updateStoragePath: seedPath,
            createdAt: serverTimestamp(),
        });
        const fold = await compact(ctxFor(path, 'seeder'));
        expect(fold.type).toBe('snapshot');

        // Each further session's saves -> a few update docs -> one delta
        // segment (device B drains everything).
        for (let i = 0; i < segments; i++) {
            await addInlineUpdates(db, path, batchBlobs(nextSession(sim, ydoc), 6), 'device-B');
            const delta = await compact(ctxFor(path, 'device-B'));
            expect(delta.type).toBe('history');
        }

        expect((await getDocs(collection(db, path, 'updates'))).size).toBe(0);
        expect((await getDocs(collection(db, path, 'history'))).size).toBe(segments);
        return { ydoc, sim };
    }

    /*
     * The late-lock-winner / manual compact() case. Device B already
     * delta-compacted every pending update into ONE segment; device A's
     * compaction (triggered on a stale count, or called by the app) then
     * finds zero update documents and one segment — far below the fold
     * threshold of 8 — and must do nothing.
     */
    it('a compaction with no pending updates and history below the fold threshold transfers nothing', { timeout: 300000 }, async () => {
        const rows: string[] = [];
        const costs: ReturnType<typeof costSince>[] = [];

        for (const sessions of AGES) {
            const path = `tests/zero-update-${getStableDate()}-${Date.now()}-${counter++}`;
            const { ydoc } = await seedBasePlusHistory(path, sessions, 1);
            const snapshotBytes = Y.encodeStateAsUpdate(ydoc).byteLength;
            ydoc.destroy();

            const vBefore = await mainVersion(db, path);
            resetIo();
            const result = await compact(ctxFor(path, 'device-A'));
            const cost = costSince(vBefore, await mainVersion(db, path));
            costs.push(cost);

            rows.push(
                `  ${String(sessions * 60).padStart(6)} events | doc state ${kb(snapshotBytes).padStart(9)} | ` +
                `result ${String(result.type).padEnd(8)} | down ${kb(cost.bytesDown).padStart(9)} (${cost.baseDownloads} base) | ` +
                `up ${kb(cost.bytesUp).padStart(9)} (${cost.foldUploads} fold) | ` +
                `fold merge ${kb(cost.foldMergeInputBytes).padStart(9)} in ${cost.foldMergeMs.toFixed(0)} ms | ` +
                `main-doc rewrites ${cost.mainDocRewrites}`,
            );
        }

        console.log(
            '[zero-update compaction] 0 update docs, 1 history segment, historyFoldThreshold 8:\n' + rows.join('\n'),
        );

        // Nothing new exists, so the cycle must not touch the base: no
        // snapshot transfer, no O(snapshot) merge, no main-doc broadcast.
        for (const cost of costs) {
            expect(cost.baseDownloads, 'base snapshot downloads').toBe(0);
            expect(cost.foldUploads, 'fold snapshot uploads').toBe(0);
            expect(cost.foldMerges, 'O(snapshot) fold merges').toBe(0);
            expect(cost.mainDocRewrites, 'main-document rewrites').toBe(0);
        }
    });

    /*
     * squash() on a quiescent document: zero pending updates, one segment
     * (the state right after any delta compaction). The squash transaction
     * can delete that segment itself and its new-epoch snapshot replaces
     * the base, so the pre-squash compact() must not fold.
     */
    it('squash() with no pending updates does not fold before squashing', { timeout: 300000 }, async () => {
        const path = `tests/zero-update-squash-${getStableDate()}-${Date.now()}-${counter++}`;
        const { ydoc: seeded } = await seedBasePlusHistory(path, AGES[0], 1);
        const snapshotBytes = Y.encodeStateAsUpdate(seeded).byteLength;
        seeded.destroy();

        const ydocA = new Y.Doc();
        const providerA = new FireProvider({ firebaseApp: app, ydoc: ydocA, path, maxWaitTime: 50 });
        try {
            await waitForConditionTruthy(() => providerA.synced, { timeout: 60000, message: 'A synced' });
            typeVersicleRoots(ydocA);

            const vBefore = await mainVersion(db, path);
            resetIo();
            const result = await providerA.squash();
            const cost = costSince(vBefore, await mainVersion(db, path));

            console.log(
                `[zero-update compaction] squash(), 0 update docs, 1 segment, doc state ${kb(snapshotBytes)}: ` +
                `down ${kb(cost.bytesDown)} (${cost.baseDownloads} base), up ${kb(cost.bytesUp)} ` +
                `(${cost.foldUploads} fold + ${cost.squashUploads} squash), fold merge ${kb(cost.foldMergeInputBytes)} ` +
                `in ${cost.foldMergeMs.toFixed(0)} ms, main-doc rewrites ${cost.mainDocRewrites}; ` +
                `squash ${JSON.stringify({ success: result.success, skippedReason: result.skippedReason })}`,
            );

            expect(result.error).toBeUndefined();
            expect(result.success).toBe(true);
            expect(cost.squashUploads).toBe(1);
            expect(cost.baseDownloads, 'base snapshot downloads').toBe(0);
            expect(cost.foldUploads, 'fold snapshot uploads').toBe(0);
            expect(cost.foldMerges, 'O(snapshot) fold merges').toBe(0);
            // Only the squash commit itself rewrites the main document
            expect(cost.mainDocRewrites, 'main-document rewrites').toBe(1);
        } finally {
            await providerA.destroy();
        }
    });

    /*
     * squash() when history sits at historyFoldThreshold - 1 with a few
     * pending updates (about 1 in 8 squashes): the backlog is far inside
     * the squash transaction's limits, so the "due" fold is pure waste —
     * the squash snapshot supersedes it moments later.
     */
    it('squash() with history at historyFoldThreshold - 1 does not run the fold it supersedes', { timeout: 300000 }, async () => {
        const path = `tests/zero-update-squash-due-${getStableDate()}-${Date.now()}-${counter++}`;
        const foldThreshold = DEFAULTS.HISTORY_FOLD_THRESHOLD;
        const { ydoc: seeded, sim } = await seedBasePlusHistory(path, AGES[0], foldThreshold - 1);
        const pending = batchBlobs(nextSession(sim, seeded), 10);
        await addInlineUpdates(db, path, pending, 'device-B');
        const snapshotBytes = Y.encodeStateAsUpdate(seeded).byteLength;
        seeded.destroy();

        const ydocA = new Y.Doc();
        const providerA = new FireProvider({ firebaseApp: app, ydoc: ydocA, path, maxWaitTime: 50 });
        try {
            await waitForConditionTruthy(() => providerA.synced, { timeout: 60000, message: 'A synced' });
            typeVersicleRoots(ydocA);

            const vBefore = await mainVersion(db, path);
            resetIo();
            const result = await providerA.squash();
            const cost = costSince(vBefore, await mainVersion(db, path));

            console.log(
                `[zero-update compaction] squash(), ${pending.length} update docs, ${foldThreshold - 1} segments, ` +
                `doc state ${kb(snapshotBytes)}: down ${kb(cost.bytesDown)} (${cost.baseDownloads} base), ` +
                `up ${kb(cost.bytesUp)} (${cost.foldUploads} fold + ${cost.squashUploads} squash), ` +
                `fold merge ${kb(cost.foldMergeInputBytes)} in ${cost.foldMergeMs.toFixed(0)} ms, ` +
                `main-doc rewrites ${cost.mainDocRewrites}; ` +
                `squash ${JSON.stringify({ success: result.success, skippedReason: result.skippedReason })}`,
            );

            expect(result.error).toBeUndefined();
            expect(result.success).toBe(true);
            expect(cost.squashUploads).toBe(1);
            expect(cost.baseDownloads, 'base snapshot downloads').toBe(0);
            expect(cost.foldUploads, 'fold snapshot uploads').toBe(0);
            expect(cost.foldMerges, 'O(snapshot) fold merges').toBe(0);
            expect(cost.mainDocRewrites, 'main-document rewrites').toBe(1);
        } finally {
            await providerA.destroy();
        }
    });
});
