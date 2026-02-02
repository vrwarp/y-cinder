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
import { DEFAULTS } from "./types";

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
export function handleSubdocs(
    event: SubdocsEvent,
    ctx: SubdocContext,
    subProviders: SubProviderMap
): void {
    const { added, removed, loaded } = event;

    // Handle added subdocs
    added.forEach(subdoc => {
        startSubdocProvider(subdoc, ctx, subProviders);
    });

    // Handle loaded subdocs
    loaded.forEach(subdoc => {
        startSubdocProvider(subdoc, ctx, subProviders);
    });

    // Handle removed subdocs
    removed.forEach(subdoc => {
        const guid = subdoc.guid;
        const provider = subProviders.get(guid);
        if (provider) {
            provider.destroy();
            subProviders.delete(guid);
        }
    });
}

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
export function startSubdocProvider(
    subdoc: Y.Doc,
    ctx: SubdocContext,
    subProviders: SubProviderMap
): any | null {
    const guid = subdoc.guid;

    // Already have a provider for this subdoc
    if (subProviders.has(guid)) {
        return subProviders.get(guid);
    }

    const subPath = `${ctx.parentPath}/subdocs/${guid}`;

    // Check recursion depth limit
    if (ctx.depth >= DEFAULTS.MAX_SUBDOC_DEPTH) {
        console.warn(`Max subdocument depth exceeded at ${ctx.parentPath}`);

        ctx.onConnectionError?.({
            code: 'recursion-limit',
            message: 'Max subdocument recursion depth exceeded',
            path: subPath,
            doc: subdoc,
        });

        return null;
    }

    // Create new provider with inherited configuration
    const provider = ctx.createProvider({
        firebaseApp: ctx.firebaseApp,
        ydoc: subdoc,
        path: subPath,
        maxUpdatesThreshold: ctx.maxUpdatesThreshold,
        maxWaitTime: ctx.maxWaitTime,
        depth: ctx.depth + 1,
        lockTTL: ctx.lockTTL,
        compactionLimit: ctx.compactionLimit,
    });

    subProviders.set(guid, provider);
    return provider;
}

/**
 * Destroys all subdocument providers.
 * 
 * P0.6 FIX: Uses Promise.allSettled instead of Promise.all to ensure
 * all subdocs are destroyed even if one fails. Logs individual failures.
 * 
 * @param subProviders - Map of subdocument providers
 * @returns Promise that resolves when all providers are destroyed
 * 
 * @example
 * ```typescript
 * await destroyAllSubdocs(subProviders);
 * ```
 */
export async function destroyAllSubdocs(subProviders: SubProviderMap): Promise<void> {
    const destroyPromises = Array.from(subProviders.entries()).map(
        async ([guid, provider]) => {
            try {
                await provider.destroy();
            } catch (err) {
                console.error(`Failed to destroy subdoc provider ${guid}:`, err);
                throw err; // Rethrow so allSettled records it as rejected
            }
        }
    );

    // P0.6 FIX: Use allSettled to ensure all subdocs attempt destruction
    const results = await Promise.allSettled(destroyPromises);

    // Log any failures (but don't throw - we want cleanup to continue)
    const failures = results.filter(r => r.status === 'rejected');
    if (failures.length > 0) {
        console.warn(`${failures.length} subdoc(s) failed to destroy properly`);
    }

    subProviders.clear();
}

/**
 * Gets statistics about subdocument providers.
 * 
 * @param subProviders - Map of subdocument providers
 * @returns Object with subdoc statistics
 */
export function getSubdocStats(subProviders: SubProviderMap): {
    count: number;
    guids: string[];
} {
    return {
        count: subProviders.size,
        guids: Array.from(subProviders.keys()),
    };
}
