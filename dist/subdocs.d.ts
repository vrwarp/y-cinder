/**
 * Subdocument Management Module
 *
 * Manages the lifecycle of Yjs subdocuments, creating and destroying
 * FireProvider instances for each subdocument as they are added/removed
 * from parent documents.
 *
 * ## Subdocument Storage
 *
 * Subdocuments are stored using a flat path structure to avoid Firestore's
 * path depth limitations:
 * ```
 * {parentPath}/subdocs/{subdocGuid}/
 * ```
 *
 * ## Recursion Limiting
 *
 * To prevent infinite recursion (and potential DoS), subdocuments have a
 * maximum depth limit (MAX_SUBDOC_DEPTH). When exceeded, the subdocument
 * is not synced and a 'connection-error' event is emitted.
 *
 * @module subdocs
 */
import * as Y from "yjs";
import { FirebaseApp } from "@firebase/app";
/**
 * Event emitted when subdocuments change.
 */
export interface SubdocsEvent {
    added: Set<Y.Doc>;
    removed: Set<Y.Doc>;
    loaded: Set<Y.Doc>;
}
/**
 * Context for subdocument management.
 */
export interface SubdocContext {
    /** Firebase app instance */
    firebaseApp: FirebaseApp;
    /** Parent document path */
    parentPath: string;
    /** Current recursion depth */
    depth: number;
    /** Maximum updates threshold (inherited) */
    maxUpdatesThreshold: number;
    /** Maximum wait time (inherited) */
    maxWaitTime: number;
    /** Compaction probability (inherited) */
    compactionProbability: number;
    /** Lock TTL (inherited) */
    lockTTL: number;
    /** Compaction limit (inherited) */
    compactionLimit: number;
    /** Factory to create new providers */
    createProvider: (config: any) => any;
    /** Callback to emit connection errors */
    onConnectionError?: (error: SubdocError) => void;
}
/**
 * Error emitted when subdocument operations fail.
 */
export interface SubdocError {
    code: string;
    message: string;
    path: string;
    doc: Y.Doc;
}
/**
 * Map of subdocument GUIDs to their providers.
 */
export type SubProviderMap = Map<string, any>;
/**
 * Handles subdocument events (added, removed, loaded).
 *
 * This function manages the lifecycle of subdocument providers:
 * - For added/loaded subdocs: Creates a new provider
 * - For removed subdocs: Destroys the existing provider
 *
 * @param event - The subdocs event from Y.Doc
 * @param ctx - Subdocument context
 * @param subProviders - Map of existing subdocument providers
 *
 * @example
 * ```typescript
 * doc.on('subdocs', (event) => {
 *   handleSubdocs(event, context, subProviders);
 * });
 * ```
 */
export declare function handleSubdocs(event: SubdocsEvent, ctx: SubdocContext, subProviders: SubProviderMap): void;
/**
 * Starts a provider for a subdocument.
 *
 * Checks recursion depth limit and creates a new provider if allowed.
 * Emits a connection error if the depth limit is exceeded.
 *
 * @param subdoc - The subdocument to sync
 * @param ctx - Subdocument context
 * @param subProviders - Map of existing subdocument providers
 * @returns The created provider, or null if depth limit exceeded
 *
 * @example
 * ```typescript
 * const provider = startSubdocProvider(subdoc, context, subProviders);
 * if (!provider) {
 *   console.warn('Subdoc depth limit exceeded');
 * }
 * ```
 */
export declare function startSubdocProvider(subdoc: Y.Doc, ctx: SubdocContext, subProviders: SubProviderMap): any | null;
/**
 * Destroys all subdocument providers.
 *
 * @param subProviders - Map of subdocument providers
 * @returns Promise that resolves when all providers are destroyed
 *
 * @example
 * ```typescript
 * await destroyAllSubdocs(subProviders);
 * ```
 */
export declare function destroyAllSubdocs(subProviders: SubProviderMap): Promise<void>;
/**
 * Gets statistics about subdocument providers.
 *
 * @param subProviders - Map of subdocument providers
 * @returns Object with subdoc statistics
 */
export declare function getSubdocStats(subProviders: SubProviderMap): {
    count: number;
    guids: string[];
};
//# sourceMappingURL=subdocs.d.ts.map