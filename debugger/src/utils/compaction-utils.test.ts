import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { computeCompactedState } from './compaction-utils';

describe('computeCompactedState', () => {
    it('returns null when no updates are provided', async () => {
        const result = await computeCompactedState({
            useBaseDoc: false,
            baseDoc: null,
            history: [],
            updates: [],
            selectedUpdateIds: new Set()
        });

        expect(result).toBeNull();
    });

    it('compacts the base document properly', async () => {
        const doc = new Y.Doc();
        const map = doc.getMap('state');
        map.set('active', true);

        const updateBytes = Y.encodeStateAsUpdate(doc);
        const baseDoc = { content: updateBytes };

        const result = await computeCompactedState({
            useBaseDoc: true,
            baseDoc,
            history: [],
            updates: [],
            selectedUpdateIds: new Set()
        });

        expect(result).not.toBeNull();
        expect(result.state).toEqual({ active: true });
    });

    it('combines history and selected updates accurately', async () => {
        // Create base data
        const doc1 = new Y.Doc();
        doc1.getText('text').insert(0, 'Hello');
        const update1 = Y.encodeStateAsUpdate(doc1);

        // Create history data extending base data
        const doc2 = new Y.Doc();
        Y.applyUpdate(doc2, update1);
        doc2.getText('text').insert(5, ' World');
        const update2 = Y.encodeStateAsUpdate(doc2);
        // Difference from doc1 to doc2:
        const delta1 = Y.encodeStateAsUpdate(doc2, Y.encodeStateVector(doc1));

        // Create update data
        const doc3 = new Y.Doc();
        Y.applyUpdate(doc3, update2);
        doc3.getText('text').insert(11, '!');
        const update3 = Y.encodeStateAsUpdate(doc3);
        const delta2 = Y.encodeStateAsUpdate(doc3, Y.encodeStateVector(doc2));

        const result = await computeCompactedState({
            useBaseDoc: true,
            baseDoc: { content: update1 },
            history: [{ segment: delta1 }],
            updates: [
                { id: 'up1', update: delta2 },
                { id: 'unselected', update: new Uint8Array([0, 0, 0]) } // junk that should be ignored
            ],
            selectedUpdateIds: new Set(['up1'])
        });

        expect(result).not.toBeNull();
        expect(result.text).toBe('Hello World!');
    });
});
