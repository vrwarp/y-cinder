/**
 * Benchmark Helpers
 *
 * Simulates a Yjs document that has been "used for a while": many editing
 * sessions from different clients, each producing incremental update blobs
 * (like the provider's debounced saves), with periodic compaction whenever
 * the pending-update count crosses a threshold — mirroring FireProvider's
 * maxUpdatesThreshold behavior.
 *
 * The live content length is kept roughly constant (targetLiveChars) while
 * churn accumulates, which is the pathological case for compaction
 * strategies that never garbage-collect: the snapshot grows with total
 * historical churn instead of live content.
 */
import * as Y from 'yjs';
import { SeededRandom } from '../tests/unit/prng';

export interface CompactionSample {
    /** 1-based compaction number */
    index: number;
    /** Total edits performed when this compaction ran */
    editsSoFar: number;
    /** Snapshot size after this compaction */
    snapshotBytes: number;
    /** Time spent in the merge strategy */
    mergeMs: number;
}

export interface SimulationResult {
    samples: CompactionSample[];
    /** Final compacted snapshot (null if no compaction ever ran) */
    snapshot: Uint8Array | null;
    /** Updates still pending after the last session */
    pending: Uint8Array[];
    totalEdits: number;
    /** Length of the live text at the end */
    liveChars: number;
    /** Total time spent compacting across the whole simulation */
    totalMergeMs: number;
    /** Final document text (for cross-strategy equivalence checks) */
    finalText: string;
}

/** A compaction strategy: merge snapshot + pending blobs into a new snapshot */
export type CompactStrategy = (blobs: Uint8Array[]) => Uint8Array;

export interface SimulationOptions {
    seed: number;
    /** Number of editing sessions (each gets a fresh client ID) */
    sessions: number;
    editsPerSession: number;
    /** Pending update count that triggers compaction (cf. maxUpdatesThreshold) */
    compactionThreshold: number;
    /** Approximate live document size to maintain (churn keeps it bounded) */
    targetLiveChars: number;
    compact: CompactStrategy;
}

export function simulateAgedDocument(opts: SimulationOptions): SimulationResult {
    const rng = new SeededRandom(opts.seed);
    const samples: CompactionSample[] = [];
    let snapshot: Uint8Array | null = null;
    let pending: Uint8Array[] = [];
    let totalEdits = 0;
    let totalMergeMs = 0;
    let liveChars = 0;
    let finalText = '';

    for (let session = 0; session < opts.sessions; session++) {
        // Fresh client per session, deterministic ID for reproducible sizes
        const doc = new Y.Doc();
        doc.clientID = opts.seed * 100_000 + session + 1;

        // Bootstrap the session from persisted state (snapshot + pending),
        // the same way a client syncs before editing.
        if (snapshot) Y.applyUpdate(doc, snapshot);
        for (const u of pending) Y.applyUpdate(doc, u);

        const captured: Uint8Array[] = [];
        const onUpdate = (u: Uint8Array) => captured.push(u);
        doc.on('update', onUpdate);

        // Realistic editing model: words are typed at a cursor that mostly
        // advances (occasionally jumping elsewhere), and stale content is
        // removed as contiguous ranges — like rewriting sentences and
        // paragraphs. Uniformly-random single-word inserts would instead
        // maximally fragment the item structure, which no editor produces.
        const text = doc.getText('content');
        let cursor = rng.int(0, text.length);
        for (let e = 0; e < opts.editsPerSession; e++) {
            totalEdits++;
            if (rng.next() < 0.1) cursor = rng.int(0, text.length);
            cursor = Math.min(cursor, text.length);
            const word = rng.string(rng.int(3, 10)) + ' ';
            text.insert(cursor, word);
            cursor += word.length;
            // Churn: keep live size bounded, like editing a document section
            if (text.length > opts.targetLiveChars) {
                const delLen = rng.int(100, 400);
                const delPos = rng.int(0, Math.max(0, text.length - delLen));
                text.delete(delPos, Math.min(delLen, text.length - delPos));
                cursor = Math.min(cursor, text.length);
            }

            // One captured blob per edit ~ one debounced save
            const blob = captured.length === 1 ? captured[0] : Y.mergeUpdates(captured);
            captured.length = 0;
            pending.push(blob);

            if (pending.length >= opts.compactionThreshold) {
                const blobs = [...(snapshot ? [snapshot] : []), ...pending];
                const t0 = performance.now();
                snapshot = opts.compact(blobs);
                const mergeMs = performance.now() - t0;
                totalMergeMs += mergeMs;
                pending = [];
                samples.push({
                    index: samples.length + 1,
                    editsSoFar: totalEdits,
                    snapshotBytes: snapshot.byteLength,
                    mergeMs,
                });
            }
        }

        doc.off('update', onUpdate);
        liveChars = text.length;
        finalText = text.toString();
        doc.destroy();
    }

    return { samples, snapshot, pending, totalEdits, liveChars, totalMergeMs, finalText };
}

export interface ObjectWorkloadOptions {
    seed: number;
    /** Number of live objects (nested Y.Maps under a root Y.Map) */
    objects: number;
    sessions: number;
    /** High-level operations per session (a drag burst counts as one) */
    opsPerSession: number;
    compactionThreshold: number;
    compact: CompactStrategy;
    /**
     * 'numeric': position/size overwrites (tiny values — worst case for GC,
     * tombstone structure dominates).
     * 'text': label/data string rewrites (content dominates — best case).
     */
    valueStyle: 'numeric' | 'text';
}

export interface ObjectWorkloadResult {
    samples: CompactionSample[];
    snapshot: Uint8Array | null;
    pending: Uint8Array[];
    totalOps: number;
    totalMergeMs: number;
    /** JSON of the final object tree (for cross-strategy equivalence) */
    finalJson: string;
    /** Bytes of a fresh doc containing only the live state (no history) */
    liveStateFloorBytes: number;
}

/**
 * Simulates an object-modification-heavy document: N objects stored as
 * nested Y.Maps under a root Y.Map, mutated by drag bursts (sequential x/y
 * overwrites), property tweaks, and occasional delete+recreate — the shape
 * of whiteboard/canvas/board apps. Every Y.Map.set on an existing key
 * tombstones the previous item, so churn accrues per *modification*, not
 * per object.
 */
export function simulateObjectWorkload(opts: ObjectWorkloadOptions): ObjectWorkloadResult {
    const rng = new SeededRandom(opts.seed);
    const samples: CompactionSample[] = [];
    let snapshot: Uint8Array | null = null;
    let pending: Uint8Array[] = [];
    let totalOps = 0;
    let totalMergeMs = 0;

    const maybeCompact = () => {
        while (pending.length >= opts.compactionThreshold) {
            const blobs = [...(snapshot ? [snapshot] : []), ...pending.splice(0, pending.length)];
            const t0 = performance.now();
            snapshot = opts.compact(blobs);
            const mergeMs = performance.now() - t0;
            totalMergeMs += mergeMs;
            samples.push({
                index: samples.length + 1,
                editsSoFar: totalOps,
                snapshotBytes: snapshot.byteLength,
                mergeMs,
            });
        }
    };

    for (let session = 0; session < opts.sessions; session++) {
        const doc = new Y.Doc();
        doc.clientID = opts.seed * 100_000 + session + 1;
        if (snapshot) Y.applyUpdate(doc, snapshot);
        for (const u of pending) Y.applyUpdate(doc, u);

        const captured: Uint8Array[] = [];
        doc.on('update', (u: Uint8Array) => captured.push(u));
        const flush = () => {
            if (captured.length > 0) {
                pending.push(captured.length === 1 ? captured[0] : Y.mergeUpdates(captured));
                captured.length = 0;
                maybeCompact();
            }
        };

        const root = doc.getMap('objects');
        if (session === 0) {
            doc.transact(() => {
                for (let i = 0; i < opts.objects; i++) {
                    createObject(root, `obj${i}`, rng, opts.valueStyle);
                }
            });
            flush();
        }

        for (let op = 0; op < opts.opsPerSession; op++) {
            totalOps++;
            const key = `obj${rng.int(0, opts.objects - 1)}`;
            const obj = root.get(key) as Y.Map<any> | undefined;
            if (!obj) {
                doc.transact(() => createObject(root, key, rng, opts.valueStyle));
                flush();
                continue;
            }
            const kind = rng.next();
            if (kind < 0.02) {
                // Object replaced (delete + recreate)
                doc.transact(() => {
                    root.delete(key);
                    createObject(root, key, rng, opts.valueStyle);
                });
                flush();
            } else if (opts.valueStyle === 'numeric' && kind < 0.62) {
                // Drag burst: each step is a separate transaction and a
                // separate persisted blob, like debounce-window saves
                const steps = rng.int(5, 15);
                const x = obj.get('x') as number, y = obj.get('y') as number;
                for (let s = 0; s < steps; s++) {
                    doc.transact(() => {
                        obj.set('x', x + s);
                        obj.set('y', y + s);
                    });
                    flush();
                }
            } else {
                // Property tweak
                doc.transact(() => {
                    if (opts.valueStyle === 'numeric') {
                        obj.set('color', rng.int(0, 0xffffff));
                    } else if (rng.bool()) {
                        obj.set('label', rng.string(rng.int(40, 80)));
                    } else {
                        obj.set('data', rng.string(rng.int(80, 160)));
                    }
                });
                flush();
            }
        }
        doc.destroy();
    }

    // Materialize final state for equivalence checks and the live-state floor
    const ref = new Y.Doc();
    if (snapshot) Y.applyUpdate(ref, snapshot);
    for (const u of pending) Y.applyUpdate(ref, u);
    const json = ref.getMap('objects').toJSON();
    const finalJson = JSON.stringify(json);
    ref.destroy();

    const floor = new Y.Doc();
    floor.transact(() => {
        const r = floor.getMap('objects');
        for (const [k, v] of Object.entries(json)) {
            const m = new Y.Map();
            r.set(k, m);
            for (const [kk, vv] of Object.entries(v as Record<string, unknown>)) {
                m.set(kk, vv);
            }
        }
    });
    const liveStateFloorBytes = Y.encodeStateAsUpdate(floor).byteLength;
    floor.destroy();

    return { samples, snapshot, pending, totalOps, totalMergeMs, finalJson, liveStateFloorBytes };
}

function createObject(root: Y.Map<any>, key: string, rng: SeededRandom, style: 'numeric' | 'text'): void {
    const o = new Y.Map();
    root.set(key, o);
    o.set('x', rng.int(0, 2000));
    o.set('y', rng.int(0, 2000));
    o.set('w', rng.int(20, 400));
    o.set('h', rng.int(20, 400));
    if (style === 'numeric') {
        o.set('color', rng.int(0, 0xffffff));
    } else {
        o.set('label', rng.string(60));
        o.set('data', rng.string(120));
    }
}

/**
 * Rebuilds a document from persisted state (snapshot + pending updates)
 * and returns its text content — used to verify strategy equivalence.
 */
export function materializeText(snapshot: Uint8Array | null, pending: Uint8Array[]): string {
    const doc = new Y.Doc();
    if (snapshot) Y.applyUpdate(doc, snapshot);
    for (const u of pending) Y.applyUpdate(doc, u);
    const s = doc.getText('content').toString();
    doc.destroy();
    return s;
}

/** Runs fn `iterations` times and returns the median duration in ms. */
export function medianMs(fn: () => void, iterations: number = 15): number {
    const times: number[] = [];
    for (let i = 0; i < iterations; i++) {
        const t0 = performance.now();
        fn();
        times.push(performance.now() - t0);
    }
    times.sort((a, b) => a - b);
    return times[Math.floor(times.length / 2)];
}

export function fmtBytes(n: number): string {
    if (n >= 1024 * 1024) return (n / (1024 * 1024)).toFixed(2) + ' MB';
    if (n >= 1024) return (n / 1024).toFixed(1) + ' KB';
    return n + ' B';
}

export function fmtMs(n: number): string {
    return n >= 100 ? n.toFixed(0) + ' ms' : n.toFixed(2) + ' ms';
}
