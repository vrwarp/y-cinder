/**
 * Regression test: destroy() while a save is in-flight must not drop updates.
 *
 * Bug: saveToFirestore() early-returned while another save was in flight.
 * If destroy() was called at that moment, the flush was silently skipped and
 * the in-flight save's completion refused to reschedule (provider destroyed),
 * permanently dropping everything buffered during the in-flight save.
 *
 * @file destroy_inflight_save.test.ts
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const { mockControls } = vi.hoisted(() => ({
    mockControls: {
        addDocDelayMs: 0,
    }
}));

vi.mock('@firebase/firestore', async (importOriginal: () => Promise<any>) => {
    const actual = await importOriginal();
    return {
        ...actual,
        addDoc: async (collectionRef: any, data: any) => {
            if (mockControls.addDocDelayMs > 0 && collectionRef.path.includes('updates')) {
                await new Promise(r => setTimeout(r, mockControls.addDocDelayMs));
            }
            return actual.addDoc(collectionRef, data);
        },
    };
});

import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator } from '../utils/emulator';
import { getDocs, collection, query } from '@firebase/firestore';
import { getStableDate } from '../unit/prng';

describe('Destroy during in-flight save', () => {
    let app: any;
    let db: any;
    let counter = 0;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        mockControls.addDocDelayMs = 0;
    });

    it('flushes updates that arrived while a save was in flight', async () => {
        const path = `integration-tests/destroy-inflight-${getStableDate()}-${counter++}`;
        const doc = new Y.Doc();
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path,
            maxWaitTime: 50,
        });

        // Let initial sync settle
        await new Promise(r => setTimeout(r, 1500));

        // First edit triggers a save; make that save slow
        mockControls.addDocDelayMs = 800;
        doc.getText('t').insert(0, 'FIRST');

        // Wait past the debounce so the save is in flight
        await new Promise(r => setTimeout(r, 300));

        // Second edit lands in the pending buffer while the save is in flight
        doc.getText('t').insert(5, '-SECOND');

        // destroy() must wait out the in-flight save and flush the buffer
        await provider.destroy();
        mockControls.addDocDelayMs = 0;

        // Reconstruct the document from what reached Firestore
        const snap = await getDocs(query(collection(db, path, 'updates')));
        const remote = new Y.Doc();
        snap.forEach(d => {
            const u = d.data().update;
            if (u) Y.applyUpdate(remote, u.toUint8Array());
        });

        expect(remote.getText('t').toString()).toBe('FIRST-SECOND');
    }, 20000);
});
