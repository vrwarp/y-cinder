import { describe, it, expect, beforeEach } from 'vitest';
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
});
