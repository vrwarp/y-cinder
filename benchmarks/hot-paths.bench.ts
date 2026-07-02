/**
 * Benchmark: per-operation hot paths that scale with document age.
 *
 * 1. Metadata extraction on the save path
 *    extractAllMetadata (full Y.decodeUpdate: allocates every struct +
 *    content) vs extractClockEnds (lazy state-vector walk).
 *
 * 2. Initial-sync application of many pending blobs
 *    One Y.applyUpdate per blob (a Yjs transaction + observer flush +
 *    'update' event each) vs the same applies wrapped in a single
 *    ydoc.transact.
 *
 * 3. Reconnect push check (diffCarriesNewData)
 *    Legacy behavior decoded EVERY server blob — including the
 *    multi-megabyte snapshot — to prove delete-set coverage before
 *    concluding "nothing to push". The current implementation checks
 *    smallest blobs first and exits early, so the snapshot's inline
 *    delete-set fingerprint usually settles it alone.
 */
import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import {
    extractAllMetadata,
    extractClockEnds,
    diffCarriesNewData,
} from '../src/update-metadata';
import { simulateAgedDocument, medianMs, fmtBytes, fmtMs } from './helpers';

/** Legacy (pre-optimization) implementation, kept verbatim for comparison. */
function legacyDiffCarriesNewData(diff: Uint8Array, getServerBlobs: () => Uint8Array[]): boolean {
    let localDs: ReturnType<typeof Y.decodeUpdate>['ds'];
    try {
        const decoded = Y.decodeUpdate(diff);
        if (decoded.structs.length > 0) return true;
        localDs = decoded.ds;
    } catch (e) {
        return true;
    }
    const serverDeleteSets = [];
    for (const blob of getServerBlobs()) {
        try {
            serverDeleteSets.push(Y.decodeUpdate(blob).ds);
        } catch (e) { /* ignore */ }
    }
    const serverDs = Y.mergeDeleteSets(serverDeleteSets);
    const unionDs = Y.mergeDeleteSets([serverDs, localDs]);
    return !Y.equalDeleteSets(serverDs, unionDs);
}

/** Build an aged, un-GC'd snapshot: large blob with heavy churn history. */
function buildAgedSnapshot() {
    const sim = simulateAgedDocument({
        seed: 424242,
        sessions: 30,
        editsPerSession: 150,
        compactionThreshold: 50,
        targetLiveChars: 2_000,
        compact: (blobs) => Y.mergeUpdates(blobs), // no GC: worst-case size
    });
    return sim;
}

describe('Hot paths on aged documents', () => {
    const sim = buildAgedSnapshot();
    const snapshot = sim.snapshot!;

    it('metadata extraction: full decode vs lazy state-vector walk', () => {
        // The worst realistic case for the save path: a large merged update
        // (e.g. flushing a long offline session).
        const fullDecode = medianMs(() => extractAllMetadata(snapshot));
        const lazyWalk = medianMs(() => extractClockEnds(snapshot));

        // Equivalence: same clientID -> clockEnd pairs
        const viaDecode = new Map(extractAllMetadata(snapshot).map(m => [m.clientID, m.clockEnd]));
        const viaLazy = extractClockEnds(snapshot);
        expect([...viaLazy.entries()].sort()).toEqual([...viaDecode.entries()].sort());

        console.log(`\nMetadata extraction on ${fmtBytes(snapshot.byteLength)} update:`);
        console.log(`  Y.decodeUpdate (extractAllMetadata): ${fmtMs(fullDecode)}`);
        console.log(`  lazy SV walk  (extractClockEnds):    ${fmtMs(lazyWalk)}  (${(fullDecode / Math.max(lazyWalk, 0.001)).toFixed(1)}x faster)\n`);

        // Regression guard (generous: CI machines vary)
        expect(lazyWalk).toBeLessThanOrEqual(fullDecode * 1.5);
    });

    it('initial sync: per-blob transactions vs one wrapping transaction', () => {
        // Pending blobs a reconnecting client must apply: snapshot + updates
        const blobs: Uint8Array[] = [snapshot, ...sim.pending];
        // Pad with extra independent update blobs to reach a realistic
        // "many pending updates" backlog
        const extra = new Y.Doc();
        extra.clientID = 999_999;
        const extraText = extra.getText('content');
        const captured: Uint8Array[] = [];
        extra.on('update', (u: Uint8Array) => captured.push(u));
        for (let i = 0; i < 300; i++) extraText.insert(0, `pending-${i} `);
        blobs.push(...captured);

        const runIndividually = () => {
            const doc = new Y.Doc();
            let events = 0;
            doc.on('update', () => events++);
            doc.getText('content').observe(() => { /* editor binding stand-in */ });
            for (const b of blobs) Y.applyUpdate(doc, b, 'bench');
            const text = doc.getText('content').toString();
            doc.destroy();
            return { events, text };
        };
        const runTransacted = () => {
            const doc = new Y.Doc();
            let events = 0;
            doc.on('update', () => events++);
            doc.getText('content').observe(() => { /* editor binding stand-in */ });
            doc.transact(() => {
                for (const b of blobs) Y.applyUpdate(doc, b, 'bench');
            }, 'bench');
            const text = doc.getText('content').toString();
            doc.destroy();
            return { events, text };
        };

        const individual = runIndividually();
        const transacted = runTransacted();
        expect(transacted.text).toBe(individual.text);

        const individualMs = medianMs(() => runIndividually(), 9);
        const transactedMs = medianMs(() => runTransacted(), 9);

        console.log(`\nApplying ${blobs.length} pending blobs on initial sync:`);
        console.log(`  one transaction per blob: ${fmtMs(individualMs)}  (${individual.events} update events fired)`);
        console.log(`  single wrapped transact:  ${fmtMs(transactedMs)}  (${transacted.events} update event fired)\n`);

        expect(transacted.events).toBe(1);
        expect(individual.events).toBe(blobs.length);
    });

    it('reconnect push check: decode-everything vs early-exit fingerprint', () => {
        // Server state as seen by a reconnecting, fully-synced client:
        // the big snapshot, its tiny delete-set fingerprint, some updates.
        const sv = Y.encodeStateVectorFromUpdate(snapshot);
        const fingerprint = Y.diffUpdate(snapshot, sv); // structs-empty, DS only
        const serverBlobs = [snapshot, fingerprint, ...sim.pending];

        // The client's diff against the server SV: no new structs, but Yjs
        // embeds the full local delete-set (the spurious-push trigger).
        const client = new Y.Doc();
        Y.applyUpdate(client, snapshot);
        for (const u of sim.pending) Y.applyUpdate(client, u);
        const serverSV = Y.encodeStateVector(client); // fully synced client == server coverage
        const localDiff = Y.encodeStateAsUpdate(client, serverSV);

        const legacy = legacyDiffCarriesNewData(localDiff, () => serverBlobs);
        const current = diffCarriesNewData(localDiff, () => serverBlobs);
        expect(legacy).toBe(false);
        expect(current).toBe(false);

        const legacyMs = medianMs(() => legacyDiffCarriesNewData(localDiff, () => serverBlobs));
        const currentMs = medianMs(() => diffCarriesNewData(localDiff, () => serverBlobs));

        console.log(`\nReconnect no-op push check (server: ${serverBlobs.length} blobs, snapshot ${fmtBytes(snapshot.byteLength)}, fingerprint ${fmtBytes(fingerprint.byteLength)}):`);
        console.log(`  decode all blobs (legacy):  ${fmtMs(legacyMs)}`);
        console.log(`  early-exit smallest-first:  ${fmtMs(currentMs)}  (${(legacyMs / Math.max(currentMs, 0.001)).toFixed(1)}x faster)\n`);

        client.destroy();
        expect(currentMs).toBeLessThanOrEqual(legacyMs * 1.5);
    });
});
