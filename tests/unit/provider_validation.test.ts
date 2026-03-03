import { describe, it, expect, vi } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';

// Mock dependencie to avoid full initialization
vi.mock('firebase/app', () => ({
    initializeApp: vi.fn(),
}));
vi.mock('firebase/firestore', () => ({
    getFirestore: vi.fn(),
    collection: vi.fn(),
    onSnapshot: vi.fn(),
    addDoc: vi.fn(),
    query: vi.fn(),
}));
vi.mock('firebase/storage', () => ({
    getStorage: vi.fn(),
}));

describe('FireProvider Configuration Validation', () => {
    const mockApp: any = {};
    const ydoc = new Y.Doc();

    it('should throw error for empty path', () => {
        expect(() => {
            new FireProvider({
                firebaseApp: mockApp,
                ydoc,
                path: ''
            });
        }).toThrow(/Invalid Firestore path/);
    });

    it('should throw error for path containing "//"', () => {
        expect(() => {
            new FireProvider({
                firebaseApp: mockApp,
                ydoc,
                path: 'docs//my-doc'
            });
        }).toThrow(/Invalid Firestore path/);
    });

    it('should throw error for path starting with "/"', () => {
        expect(() => {
            new FireProvider({
                firebaseApp: mockApp,
                ydoc,
                path: '/docs/my-doc'
            });
        }).toThrow(/Invalid Firestore path/);
    });

    it('should throw error for invalid maxUpdatesThreshold', () => {
        expect(() => {
            new FireProvider({
                firebaseApp: mockApp,
                ydoc,
                path: 'valid/path',
                maxUpdatesThreshold: 0
            });
        }).toThrow(/Invalid maxUpdatesThreshold/);
    });

    it('should throw error for invalid depth', () => {
        expect(() => {
            new FireProvider({
                firebaseApp: mockApp,
                ydoc,
                path: 'valid/path',
                depth: -1
            });
        }).toThrow(/Invalid depth/);

        expect(() => {
            new FireProvider({
                firebaseApp: mockApp,
                ydoc,
                path: 'valid/path',
                depth: 101
            });
        }).toThrow(/Invalid depth/);
    });

    it('should accept valid configuration', () => {
        // We expect this might fail further down due to mocks, but validation should pass
        // If it throws "Invalid ...", test fails.
        // We wrap in try-catch to ignore downstream errors
        try {
            new FireProvider({
                firebaseApp: mockApp,
                ydoc,
                path: 'valid/path',
                maxUpdatesThreshold: 10,
                depth: 5
            });
        } catch (e: any) {
            // Should NOT throw validation error
            expect(e.message).not.toMatch(/Invalid/);
        }
    });
});
