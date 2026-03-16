import { describe, it, expect } from 'vitest';
import * as Y from 'yjs';
import { getUint8Array, validateBlob, extractYDocState, formatDataForDisplay } from './yjs-utils';

describe('Yjs Utils', () => {
    describe('getUint8Array', () => {
        it('returns null for empty input', () => {
            expect(getUint8Array(null)).toBeNull();
            expect(getUint8Array('')).toBeNull();
            expect(getUint8Array(undefined)).toBeNull();
        });

        it('returns Uint8Array natively', () => {
            const arr = new Uint8Array([1, 2, 3]);
            expect(getUint8Array(arr)).toBe(arr);
        });

        it('extracts from Node Buffer-like object', () => {
            const arr = getUint8Array({ type: 'Buffer', data: [1, 2, 3] });
            expect(arr).toBeInstanceOf(Uint8Array);
            expect(arr?.length).toBe(3);
            expect(arr?.[0]).toBe(1);
        });

        it('extracts using toUint8Array if available', () => {
            const mock = {
                toUint8Array: () => new Uint8Array([4, 5])
            };
            const arr = getUint8Array(mock);
            expect(arr).toBeInstanceOf(Uint8Array);
            expect(arr?.length).toBe(2);
            expect(arr?.[0]).toBe(4);
        });

        it('extracts Firestore base64 formatted bytes', () => {
            // "SGVsbG8=" is base64 for "Hello"
            const firestoreBytes = { type: 'firestore/bytes/1.0', bytes: 'SGVsbG8=' };
            const arr = getUint8Array(firestoreBytes);
            expect(arr).toBeInstanceOf(Uint8Array);
            // 'H' = 72, 'e' = 101, 'l' = 108, 'l' = 108, 'o' = 111
            expect(arr?.[0]).toBe(72);
            expect(arr?.[1]).toBe(101);
        });
    });

    describe('validateBlob', () => {
        it('returns error if missing or empty', () => {
            expect(validateBlob(null)).toBe('Empty or missing blob');
        });

        it('returns valid for actual Yjs update', () => {
            const doc = new Y.Doc();
            const update = Y.encodeStateAsUpdate(doc);
            expect(validateBlob(update)).toBeNull();
        });

        it('returns error message for invalid bytes', () => {
            const badBytes = new Uint8Array([255, 255, 255, 255]); // Invalid Yjs byte sequence
            const err = validateBlob(badBytes);
            expect(typeof err).toBe('string');
            expect(err).not.toBeNull();
            expect(err?.length).toBeGreaterThan(0);
        });
    });

    describe('extractYDocState', () => {
        it('extracts state from standard Y.Doc types', () => {
            const doc = new Y.Doc();
            const map = doc.getMap('metadata');
            map.set('title', 'Test Document');

            const arr = doc.getArray('list');
            arr.insert(0, ['item1', 'item2']);

            const text = doc.getText('content');
            text.insert(0, 'Hello world');

            const state = extractYDocState(doc);

            expect(state.metadata).toEqual({ title: 'Test Document' });
            expect(state.list).toEqual(['item1', 'item2']);
            expect(state.content).toBe('Hello world');
        });

        it('ignores empty documents safely', () => {
            const doc = new Y.Doc();
            const state = extractYDocState(doc);
            expect(state).toEqual({});
        });

        it('extracts pending structs when update application is deferred', () => {
            const targetDoc = new Y.Doc();

            // Manually craft what Yjs internal pendingStructs.update looks like
            // A basic Yjs update with 1 struct is [1, 1, 0, ...]
            // For testing the utility, we will use a real Y.Doc to encode an update
            const tempDoc = new Y.Doc();
            tempDoc.clientID = 1000;
            tempDoc.getText('test').insert(0, 'pending data');
            const updateBytes = Y.encodeStateAsUpdate(tempDoc);

            // Inject directly into the internal store to mock pending state
            (targetDoc.store as any).pendingStructs = {
                missing: new Map([[999, 0]]),
                update: updateBytes
            };

            const state = extractYDocState(targetDoc);

            expect(state.__pendingStructs).toBeDefined();
            expect(state.__pendingStructs.count).toBeGreaterThan(0);
            expect(state.__pendingStructs.preview.length).toBeGreaterThan(0);
            expect(state.__pendingStructs.preview[0]).toHaveProperty('client');
            expect(state.__pendingStructs.preview[0]).toHaveProperty('clock');
            expect(state.__pendingStructs.preview[0]).toHaveProperty('class');
            expect(state.__pendingStructs.note).toContain('missing dependencies');
        });

        it('truncates pending structs preview gracefully', () => {
            const targetDoc = new Y.Doc();

            const tempDoc = new Y.Doc();
            for (let i = 0; i < 60; i++) {
                // Changing client ID forces a new struct
                tempDoc.clientID = i + 1000;
                tempDoc.getText('test').insert(i, 'X');
            }
            const updateBytes = Y.encodeStateAsUpdate(tempDoc);

            (targetDoc.store as any).pendingStructs = {
                missing: new Map([[999, 0]]),
                update: updateBytes // Over 60 distinct structs now
            };

            const state = extractYDocState(targetDoc);

            expect(state.__pendingStructs).toBeDefined();
            expect(state.__pendingStructs.count).toBeGreaterThan(50);
            expect(state.__pendingStructs.preview.length).toBe(50); // Hardcapped at 50
        });
    });

    describe('formatDataForDisplay', () => {
        it('formats dates properly', () => {
            const data = {
                createdAt: { seconds: 1672531200, nanoseconds: 0 } // Jan 1, 2023
            };
            const formatted = formatDataForDisplay(data);
            expect(formatted).toContain('2023-01-01T00:00:00.000Z');
        });

        it('formats Yjs bytes and decodes structs', () => {
            const doc = new Y.Doc();
            doc.getText('test').insert(0, 'A');
            const update = Y.encodeStateAsUpdate(doc);

            const data = {
                myUpdate: update
            };

            const formatted = formatDataForDisplay(data);
            const parsed = JSON.parse(formatted);
            expect(parsed.myUpdate.__yjs_update_bytes).toBeGreaterThan(0);
            expect(parsed.myUpdate.decoded.structs.length).toBeGreaterThan(0);
        });

        it('truncates when exceeding struct limit', () => {
            const doc = new Y.Doc();
            // create independent structs by forcing different client IDs
            for (let i = 0; i < 5; i++) {
                doc.clientID = i + 1;
                doc.getText('test').insert(i, 'A');
            }
            const update = Y.encodeStateAsUpdate(doc);

            const data = { update };
            // Limit to 2 structs
            const formatted = formatDataForDisplay(data, 2);
            const parsed = JSON.parse(formatted);

            expect(parsed.update.decoded.structs.length).toBe(2);
            expect(parsed.update.decoded.__warning__).toContain('Showing 2 of');
        });
    });
});
