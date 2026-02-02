import { describe, it, expect, vi, beforeEach } from 'vitest';
import { FireProvider } from '../src/provider';
import * as Y from 'yjs';
import * as firestore from '@firebase/firestore';

// Mock Firestore
vi.mock('@firebase/firestore', () => ({
    getFirestore: vi.fn(),
    doc: vi.fn((db, path, ...segments) => ({ path: [path, ...segments].join('/') })),
    collection: vi.fn((db, path, ...segments) => ({ path: [path, ...segments].join('/') })),
    query: vi.fn(),
    orderBy: vi.fn(),
    limit: vi.fn(),
    startAfter: vi.fn(),
    limitToLast: vi.fn(),
    getDocs: vi.fn(() => Promise.resolve({ docs: [], empty: true, forEach: () => { } })),
    getDoc: vi.fn(() => Promise.resolve({ exists: () => false, data: () => ({ t: { toMillis: () => Date.now() } }) })),
    setDoc: vi.fn(() => Promise.resolve()),
    deleteDoc: vi.fn(() => Promise.resolve()),
    onSnapshot: vi.fn(() => vi.fn()), // Returns unsubscribe fn
    addDoc: vi.fn(),
    writeBatch: vi.fn(() => ({ commit: vi.fn() })),
    runTransaction: vi.fn(async (db, updateFunction) => {
        // Mock Transaction Object
        const transaction = {
            get: vi.fn(() => Promise.resolve({ exists: () => false })),
            set: vi.fn(),
            update: vi.fn(),
            delete: vi.fn(),
        };
        return updateFunction(transaction);
    }),
    serverTimestamp: vi.fn(() => 'TIMESTAMP'),
    Bytes: {
        fromUint8Array: (arr) => ({ toUint8Array: () => arr }),
    },
    Timestamp: {}
}));

describe('FireProvider', () => {
    let ydoc: Y.Doc;
    let firebaseApp: any = {};
    const path = 'test-collection/doc-id';

    beforeEach(() => {
        ydoc = new Y.Doc();
        vi.clearAllMocks();
    });

    it('should initialize and start sync', async () => {
        const provider = new FireProvider({ firebaseApp, ydoc, path });

        // Sync is async in constructor. Wait for microtasks and clock skew measurement.
        await new Promise(resolve => setTimeout(resolve, 50));

        expect(firestore.getFirestore).toHaveBeenCalledWith(firebaseApp);
        // Sync should trigger getDoc (base) and getDocs (history) and onSnapshot (updates)
        expect(firestore.getDoc).toHaveBeenCalled();
        expect(firestore.getDocs).toHaveBeenCalled();
        expect(firestore.onSnapshot).toHaveBeenCalled();

        provider.destroy();
    });

    it('should debounce updates and save to firestore', async () => {
        vi.useFakeTimers();
        const provider = new FireProvider({ firebaseApp, ydoc, path, maxWaitTime: 100 });

        // Mock addDoc
        const addDocSpy = vi.spyOn(firestore, 'addDoc');

        // Simulate update
        const update = new Uint8Array([1, 2, 3]);
        provider.handleUpdate(update, null);

        // Should not save immediately
        expect(addDocSpy).not.toHaveBeenCalled();

        // Fast forward
        vi.advanceTimersByTime(110);

        expect(addDocSpy).toHaveBeenCalled();

        vi.useRealTimers();
        provider.destroy();
    });

    it('should trigger compaction when updates exceed threshold', async () => {
        const provider = new FireProvider({ firebaseApp, ydoc, path, maxUpdatesThreshold: 5 });
        const compactSpy = vi.spyOn(provider, 'compact');

        // Wait for sync to reach onSnapshot subscribe (includes clock skew measurement)
        await new Promise(resolve => setTimeout(resolve, 50));

        // Mock onSnapshot callback
        const onSnapshotMock = firestore.onSnapshot as any;

        // Setup provider (already called sync and onSnapshot)
        expect(onSnapshotMock).toHaveBeenCalled();
        const callback = onSnapshotMock.mock.calls[0][1];

        // Invoke callback with "large" snapshot
        const mockSnapshot = {
            size: 10,
            docChanges: () => []
        };

        await callback(mockSnapshot);

        expect(compactSpy).toHaveBeenCalled();

        provider.destroy();
    });
});
