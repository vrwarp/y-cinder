import { describe, it, expect, beforeEach, vi } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { setupEmulator, clearFirestore } from '../utils/emulator';

describe('FireProvider Recursion Depth Guard (Emulator)', () => {
    let app: any;
    let db: any;

    beforeEach(async () => {
        const setup = await setupEmulator();
        app = setup.app;
        db = setup.db;
        await clearFirestore(db);
    });

    it('should limit subdocument recursion depth to 50', async () => {
        const path = `depth-tests/nested-${Date.now()}`;
        const rootDoc = new Y.Doc();
        const rootProvider = new FireProvider({
            firebaseApp: app,
            ydoc: rootDoc,
            path: path
        });

        // Create a chain of subdocs
        let currentDoc = rootDoc;
        const depth = 60;

        for (let i = 0; i < depth; i++) {
            const subdoc = new Y.Doc();
            currentDoc.getArray('subdocs').push([subdoc]);
            currentDoc = subdoc;
            // Spread the load to avoid overloading emulator
            await new Promise(r => setTimeout(r, 20));
        }

        // rootProvider will automatically try to create sub-providers because of 'subdocs' event
        // We wait a bit for the async propagation if any (though it's synchronous event)
        // Actually startSubdocProvider is called in handleSubdocs which is triggered by Yjs 'subdocs' event.

        // Let's verify the depth of the deepest provider in the chain
        let count = 0;
        let p: any = rootProvider;

        // We need to traverse the subProviders map. 
        // Since we only added one subdoc per level, it should be a single branch.
        const calculateMaxDepth = (provider: FireProvider): number => {
            let max = (provider as any).depth;
            (provider as any).subProviders.forEach((subP: FireProvider) => {
                max = Math.max(max, calculateMaxDepth(subP));
            });
            return max;
        };

        // Give it a moment to initialize providers
        await new Promise(r => setTimeout(r, 500));

        const reachedDepth = calculateMaxDepth(rootProvider);
        console.log(`Reached depth: ${reachedDepth}`);

        expect(reachedDepth).toBeLessThanOrEqual(50);

        rootProvider.destroy();
    });
    it('should emit connection-error when recursion depth limit is reached', async () => {
        const path = `depth-tests/signal-${Date.now()}`;
        const rootDoc = new Y.Doc();

        // Initialize at depth 49
        const rootProvider = new FireProvider({
            firebaseApp: app,
            ydoc: rootDoc,
            path: path,
            depth: 49
        });

        // 1. Add first subdoc (Level 50)
        const subdoc1 = new Y.Doc();
        rootDoc.getArray('subdocs').push([subdoc1]);

        // Wait for handler
        await new Promise(r => setTimeout(r, 100));

        // Get the child provider
        const childProvider = rootProvider.subProviders.values().next().value;
        expect(childProvider).toBeDefined();
        if (!childProvider) return; // TS Guard
        expect(childProvider.depth).toBe(50);

        const errorSpy = vi.fn();
        childProvider.on('connection-error', errorSpy);

        // 2. Add second subdoc (Level 51) - should trigger signal
        const subdoc2 = new Y.Doc();
        subdoc1.getArray('subdocs').push([subdoc2]);

        // Wait for handler
        await new Promise(r => setTimeout(r, 100));

        expect(errorSpy).toHaveBeenCalled();
        const errorArgs = errorSpy.mock.calls[0][0];
        expect(errorArgs.code).toBe('recursion-limit');
        expect(errorArgs.path).toContain(subdoc2.guid);

        rootProvider.destroy();
    });
});
