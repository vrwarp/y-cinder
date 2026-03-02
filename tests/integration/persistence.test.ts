import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import * as firestore from '@firebase/firestore';

// Mock the firestore module
vi.mock('@firebase/firestore', async (importOriginal) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getFirestore: vi.fn(() => ({ type: 'firestore-mock' })),
        initializeFirestore: vi.fn(() => ({ type: 'firestore-mock-initialized' })),
        // Mock persistentLocalCache to return a recognizable object
        persistentLocalCache: vi.fn((settings) => ({ type: 'persistent-cache', settings })),
    };
});

vi.mock('@firebase/storage', async (importOriginal) => {
    const actual = await importOriginal();
    return {
        ...actual,
        getStorage: vi.fn(() => ({ type: 'storage-mock' })),
    };
});

describe('FireProvider Persistence', () => {
    let mockApp: any;
    let doc: Y.Doc;

    beforeEach(() => {
        mockApp = { name: '[DEFAULT]' };
        doc = new Y.Doc();
        vi.clearAllMocks();
    });

    it('should use getFirestore by default (persistence disabled)', () => {
        new FireProvider({
            firebaseApp: mockApp,
            ydoc: doc,
            path: 'test/doc'
        });

        expect(firestore.getFirestore).toHaveBeenCalledWith(mockApp);
        expect(firestore.initializeFirestore).not.toHaveBeenCalled();
    });

    it('should use initializeFirestore when persistence is enabled', () => {
        new FireProvider({
            firebaseApp: mockApp,
            ydoc: doc,
            path: 'test/doc',
            persistence: { enabled: true }
        });

        expect(firestore.initializeFirestore).toHaveBeenCalledWith(mockApp, expect.objectContaining({
            localCache: expect.objectContaining({ type: 'persistent-cache' })
        }));
        expect(firestore.getFirestore).not.toHaveBeenCalled();
    });

    it('should fallback to getFirestore if initializeFirestore fails with failed-precondition', () => {
        // Mock initializeFirestore to throw
        vi.mocked(firestore.initializeFirestore).mockImplementationOnce(() => {
            const err: any = new Error('Already initialized');
            err.code = 'failed-precondition';
            throw err;
        });

        new FireProvider({
            firebaseApp: mockApp,
            ydoc: doc,
            path: 'test/doc',
            persistence: { enabled: true }
        });

        expect(firestore.initializeFirestore).toHaveBeenCalled();
        expect(firestore.getFirestore).toHaveBeenCalledWith(mockApp);
    });

    it('should rethrow other errors from initializeFirestore', () => {
        // Mock initializeFirestore to throw unknown error
        vi.mocked(firestore.initializeFirestore).mockImplementationOnce(() => {
            throw new Error('Unknown error');
        });

        expect(() => {
            new FireProvider({
                firebaseApp: mockApp,
                ydoc: doc,
                path: 'test/doc',
                persistence: { enabled: true }
            });
        }).toThrow('Unknown error');
    });
});
