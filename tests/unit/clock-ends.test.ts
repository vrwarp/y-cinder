/**
 * Unit tests for the lazy metadata fast path (extractClockEnds /
 * aggregateClockEnds), which replaced full Y.decodeUpdate parsing on the
 * save and sync hot paths. The contract: identical clientID -> clockEnd
 * output to extractAllMetadata, identical error behavior (empty result on
 * garbage), identical Firestore payload shape from aggregation.
 */

import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import {
    extractAllMetadata,
    extractClockEnds,
    aggregateMetadata,
    aggregateClockEnds,
} from '../../src/update-metadata';

function multiClientUpdate(clients: number): Uint8Array {
    const blobs: Uint8Array[] = [];
    for (let c = 0; c < clients; c++) {
        const doc = new Y.Doc();
        doc.clientID = 1000 + c;
        doc.getText('t').insert(0, `client ${c} content `);
        blobs.push(Y.encodeStateAsUpdate(doc));
        doc.destroy();
    }
    return Y.mergeUpdates(blobs);
}

describe('extractClockEnds', () => {
    it('matches extractAllMetadata clockEnd values exactly', () => {
        const update = multiClientUpdate(5);
        const viaDecode = new Map(
            extractAllMetadata(update).map(m => [m.clientID, m.clockEnd])
        );
        const viaLazy = extractClockEnds(update);
        expect([...viaLazy.entries()].sort((a, b) => a[0] - b[0]))
            .toEqual([...viaDecode.entries()].sort((a, b) => a[0] - b[0]));
    });

    it('REGRESSION: reports clock ends for mid-life updates (structs not starting at clock 0)', () => {
        // Every real-world incremental save looks like this: the client
        // already has history, so the update's structs start at clock > 0.
        // The previous implementation (encodeStateVectorFromUpdate) returned
        // an EMPTY map here — the leading gap zeroes that function's answer —
        // which silently stripped the redundancy-skip metadata from every
        // update document after a client's very first save.
        const doc = new Y.Doc();
        doc.clientID = 777;
        const captured: Uint8Array[] = [];
        doc.on('update', (u: Uint8Array) => captured.push(u));
        doc.getMap('m').set('first', 1); // clock 0
        captured.length = 0;
        doc.getMap('m').set('second', 2); // clock 1 — a mid-life save
        doc.getMap('m').set('third', 3); // clock 2

        const midLife = Y.mergeUpdates(captured);
        const viaLazy = extractClockEnds(midLife);
        const viaDecode = new Map(
            extractAllMetadata(midLife).map(m => [m.clientID, m.clockEnd])
        );
        expect(viaLazy.size).toBe(1);
        expect(viaLazy.get(777)).toBe(3);
        expect([...viaLazy.entries()]).toEqual([...viaDecode.entries()]);
        doc.destroy();
    });

    it('returns an empty map on garbage input (parity with extractAllMetadata)', () => {
        expect(extractClockEnds(new Uint8Array([7, 7, 7, 7]))).toEqual(new Map());
        expect(extractClockEnds(new Uint8Array(0))).toEqual(new Map());
    });

    it('returns an empty map for a structs-empty (deletion-only) diff', () => {
        const doc = new Y.Doc();
        doc.getText('t').insert(0, 'hello');
        doc.getText('t').delete(0, 3);
        // Diff against own SV: no structs, delete-set only
        const dsOnly = Y.diffUpdate(Y.encodeStateAsUpdate(doc), Y.encodeStateVector(doc));
        doc.destroy();

        expect(extractClockEnds(dsOnly)).toEqual(new Map());
        expect(extractAllMetadata(dsOnly)).toEqual([]);
    });
});

describe('aggregateClockEnds', () => {
    it('produces the same payload as aggregateMetadata', () => {
        const update = multiClientUpdate(4);
        const legacy = aggregateMetadata(extractAllMetadata(update));
        const fast = aggregateClockEnds(extractClockEnds(update));

        const normalize = (p: { clientIDs?: number[]; clientClocks?: number[] }) => {
            const pairs = (p.clientIDs ?? []).map((id, i) => [id, p.clientClocks![i]]);
            return pairs.sort((a, b) => a[0] - b[0]);
        };
        expect(normalize(fast)).toEqual(normalize(legacy));
    });

    it('returns empty object for empty input', () => {
        expect(aggregateClockEnds(new Map())).toEqual({});
    });

    it('skips metadata beyond the client cap (parity with aggregateMetadata)', () => {
        const big = new Map<number, number>();
        for (let i = 0; i < 51; i++) big.set(i, i + 1); // MAX_METADATA_CLIENTS = 50
        expect(aggregateClockEnds(big)).toEqual({});

        const atCap = new Map<number, number>();
        for (let i = 0; i < 50; i++) atCap.set(i, i + 1);
        expect(aggregateClockEnds(atCap).clientIDs).toHaveLength(50);
    });
});
