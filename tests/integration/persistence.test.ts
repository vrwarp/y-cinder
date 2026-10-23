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
    const providers: FireProvider[] = [];

    const createProvider = (config: Record<string, unknown> = {}) => {
        const provider = new FireProvider({
            firebaseApp: mockApp,
            ydoc: doc,
            path: 'test/doc',
            ...config,
        });
        providers.push(provider);
        return provider;
    };

    beforeEach(() => {
        mockApp = { name: '[DEFAULT]' };
        doc = new Y.Doc();
        vi.clearAllMocks();
    });

    // The constructor starts an initial sync against the mock db; stop it so
    // its retry timers do not outlive the test.
    afterEach(async () => {
        for (const p of providers.splice(0)) {
            await p.destroy().catch(() => { /* sync never started */ });
        }
    });

    it('should use getFirestore by default (persistence disabled)', () => {
        createProvider();

        expect(firestore.getFirestore).toHaveBeenCalledWith(mockApp);
        expect(firestore.initializeFirestore).not.toHaveBeenCalled();
    });

    it('should use initializeFirestore when persistence is enabled', () => {
        createProvider({ persistence: { enabled: true } });

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

        createProvider({ persistence: { enabled: true } });

        expect(firestore.initializeFirestore).toHaveBeenCalled();
        expect(firestore.getFirestore).toHaveBeenCalledWith(mockApp);
    });

    it('should rethrow other errors from initializeFirestore', () => {
        // Mock initializeFirestore to throw unknown error
        vi.mocked(firestore.initializeFirestore).mockImplementationOnce(() => {
            throw new Error('Unknown error');
        });

        expect(() => {
            createProvider({ persistence: { enabled: true } });
        }).toThrow('Unknown error');
    });
});
