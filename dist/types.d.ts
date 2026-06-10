/**
 * Type Definitions and Constants
 *
 * This module contains shared type definitions, interfaces, and constants
 * used throughout the y-fire library. Includes:
 * - Configuration interfaces (FireProviderConfig)
 * - Internal interfaces (UpdateMetadata, TestHooks)
 * - Firestore path constants
 * - Default configuration values
 *
 * @module types
 */
import { FirebaseApp } from "@firebase/app";
import * as Y from "yjs";
/**
 * Metadata extracted from a Yjs update blob.
 * Represents the clock range for a single client's operations within an update.
 */
export interface UpdateMetadata {
    /** The Yjs client ID that generated these operations */
    clientID: number;
    /** The starting clock value (inclusive) */
    clockStart: number;
    /** The ending clock value (exclusive) */
    clockEnd: number;
}
/**
 * Test hooks for dependency injection during testing.
 * @internal
 */
export interface TestHooks {
    /** Called before compaction transaction begins */
    beforeTransaction?: () => Promise<void>;
}
/**
 * Configuration options for FireProvider.
 */
export interface FireProviderConfig {
    /** Firebase app instance */
    firebaseApp: FirebaseApp;
    /** The Yjs document to sync */
    ydoc: Y.Doc;
    /** Firestore document path for this document */
    path: string;
    /**
     * Number of updates that triggers compaction consideration.
     * @default 50
     */
    maxUpdatesThreshold?: number;
    /**
     * Debounce wait time in milliseconds before saving updates.
     * @default 500
     */
    maxWaitTime?: number;
    /**
     * Current subdocument depth. Used internally for recursion limiting.
     * @default 0
     */
    depth?: number;
    /**
     * Time-to-live for distributed locks in milliseconds.
     * @default 60000 (60 seconds)
     */
    lockTTL?: number;
    /**
     * Maximum number of updates to process in a single compaction run.
     * Prevents unbounded memory usage.
     * @default 500
     */
    compactionLimit?: number;
    /**
     * Test hooks for dependency injection.
     * @internal
     */
    testHooks?: TestHooks;
    /**
     * Whether to enable Firestore offline persistence.
     */
    persistence?: {
        enabled: boolean;
    };
}
/**
 * Origins used to tag updates from Firebase.
 * Used to prevent echo/loops when applying remote updates.
 */
export declare const FIREBASE_ORIGINS: {
    readonly SNAPSHOT: "origin:firebase/snapshot";
    readonly HISTORY: "origin:firebase/history";
    readonly UPDATE: "origin:firebase/update";
};
/**
 * Firestore path constants.
 */
export declare const FIRESTORE_PATHS: {
    readonly UPDATES: "updates";
    readonly HISTORY: "history";
    readonly MAINTENANCE: "maintenance";
    readonly LOCK_COMPACTION: "metadata/lock_compaction";
};
/**
 * Default configuration values.
 */
export declare const DEFAULTS: {
    readonly MAX_UPDATES_THRESHOLD: 50;
    readonly MAX_WAIT_TIME: 500;
    readonly DEPTH: 0;
    readonly LOCK_TTL: 60000;
    readonly COMPACTION_LIMIT: 200;
    readonly MAX_SUBDOC_DEPTH: 50;
    readonly TARGET_SNAPSHOT_SIZE: 900000;
    readonly MAX_RETRIES: 5;
    /** Maximum docs to fetch per batch during initial sync (P0.1 fix) */
    readonly SYNC_BATCH_SIZE: 100;
    /** Pending-update count that forces a compaction trigger, bypassing the trigger cooldown */
    readonly REALTIME_LIMIT: 200;
    /** Minimum time between compaction triggers from a single client's listener */
    readonly COMPACTION_TRIGGER_COOLDOWN_MS: 10000;
    /** Firestore maximum document size in bytes (1MB) */
    readonly FIRESTORE_DOC_LIMIT: 1048576;
    /**
     * Updates larger than this are offloaded to Cloud Storage instead of
     * being inlined in a Firestore document. Leaves headroom below
     * FIRESTORE_DOC_LIMIT for field names, metadata arrays, and overhead.
     */
    readonly INLINE_UPDATE_LIMIT: 1040384;
    /** Maximum consecutive save failures before emitting save-rejected */
    readonly MAX_SAVE_RETRIES: 5;
    /**
     * Caps on documents deleted per compaction transaction. Firestore
     * transactions allow at most 500 writes; updates + history + 1 snapshot
     * set must stay within that budget (400 + 99 + 1 = 500).
     */
    readonly MAX_COMPACTION_UPDATES: 400;
    readonly MAX_COMPACTION_HISTORY: 99;
    /**
     * Maximum size of the delete-set fingerprint stored inline on the main
     * document. Larger delete-sets are simply not stored (clients fall back
     * to a redundant-but-idempotent push), keeping the main document well
     * under the Firestore size limit.
     */
    readonly MAX_DELETE_SET_FIELD_BYTES: 700000;
};
//# sourceMappingURL=types.d.ts.map