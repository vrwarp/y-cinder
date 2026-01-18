import { FirebaseApp } from "@firebase/app";
import { Firestore } from "@firebase/firestore";
import * as Y from "yjs";
import { ObservableV2 } from "lib0/observable";
export interface FireProviderConfig {
    firebaseApp: FirebaseApp;
    ydoc: Y.Doc;
    path: string;
    maxUpdatesThreshold?: number;
    maxWaitTime?: number;
}
export declare class FireProvider extends ObservableV2<any> {
    doc: Y.Doc;
    path: string;
    db: Firestore;
    firebaseApp: FirebaseApp;
    uid: string;
    subProviders: Map<string, FireProvider>;
    isCompacting: boolean;
    updateCache: Uint8Array | null;
    maxUpdatesThreshold: number;
    maxWaitTime: number;
    private _unsubscribeUpdates;
    private _debouncedSave;
    constructor({ firebaseApp, ydoc, path, maxUpdatesThreshold, maxWaitTime, }: FireProviderConfig);
    private debounce;
    /**
     * Sync Mechanism
     * 1. Load Base Snapshot
     * 2. Load History Segments
     * 3. Subscribe to Live Updates
     */
    sync(): Promise<void>;
    handleUpdate: (update: Uint8Array, origin: any) => void;
    saveToFirestore(): Promise<void>;
    /**
     * Compaction Logic (Tiered)
     * Merges updates into History Segments or Base Snapshot
     */
    compact(): Promise<void>;
    handleSubdocs: ({ added, removed, loaded }: {
        added: Set<Y.Doc>;
        removed: Set<Y.Doc>;
        loaded: Set<Y.Doc>;
    }) => void;
    startSubdocProvider(subdoc: Y.Doc): void;
    destroy(): void;
}
//# sourceMappingURL=provider.d.ts.map