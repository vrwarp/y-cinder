/**
 * y-fire - Yjs Persistence Provider for Firebase Firestore
 *
 * This module exports the public API for the y-fire library.
 *
 * @module y-fire
 *
 * @example
 * ```typescript
 * import { FireProvider } from 'y-fire';
 *
 * const provider = new FireProvider({
 *   firebaseApp: myApp,
 *   ydoc: myDoc,
 *   path: 'documents/my-doc'
 * });
 * ```
 */

export { FireProvider } from "./provider";
export type { FireProviderConfig } from "./provider";
export { mergeUpdatesAsync } from "./merge-utils";
export {
    readDocEpoch,
    docHasContent,
    buildSquashedDoc,
    PROVIDER_META_KEY,
    EPOCH_KEY,
} from "./squash";
export type { SquashResult } from "./squash";
