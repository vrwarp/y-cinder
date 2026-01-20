/**
 * Flush on Destroy Tests
 *
 * Tests that pending updates are correctly flushed to Firestore when
 * destroy() is called. Verifies no data loss occurs during provider
 * shutdown, even when updates are still in the debounce buffer.
 *
 * @file flush_on_destroy.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator'; // Assuming these exist from reading previous tests
import { waitForCondition } from '../utils/wait';
import { getDocs, collection, query, orderBy } from '@firebase/firestore';

describe('FireProvider Destroy Flush (Emulator)', () => {
    let app: any;
    let db: any;

    const createProvider = (doc: Y.Doc, path: string, config: any = {}) => {
        return new FireProvider({
            firebaseApp: app,
            ydoc: doc,
            path,
            ...config
        });
    }

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
    });

    it('should flush pending updates on destroy', async () => {
        const path = `integration-tests/flush-destroy-${Date.now()}`;
        const doc = new Y.Doc();
        // Long debounce to ensure it doesn't auto-save before we destroy
        const provider = createProvider(doc, path, { maxWaitTime: 1000 });

        // Make a change
        doc.getText('content').insert(0, 'Flushed Content');

        // Verify not yet saved (best effort check, immediate might be too fast so we just proceed to destroy)
        // Actually, we can check internal state if we wanted, but blackbox is better.

        // Destroy immediately
        provider.destroy();

        // Wait a bit for the async write triggered by destroy to land
        await new Promise(r => setTimeout(r, 500));

        // Verify data exists in Firestore
        const updatesQ = query(collection(db, path, 'updates'), orderBy('createdAt', 'asc'));
        const snap = await getDocs(updatesQ);

        expect(snap.empty).toBe(false);
        const data = snap.docs[0].data();

        // Decode update to verify content
        const update = data.update.toUint8Array();
        const verifyDoc = new Y.Doc();
        Y.applyUpdate(verifyDoc, update);
        expect(verifyDoc.getText('content').toString()).toBe('Flushed Content');
    });
});
