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
     * Upper bound in milliseconds on how long buffered local updates may be
     * deferred. The debounce timer resets on every local edit, so without
     * this cap a user typing continuously would never trigger a save: the
     * buffer grows unboundedly and nothing is persisted until they pause.
     * Once the oldest buffered update is older than this, a save is forced
     * even while edits keep arriving.
     * @default maxWaitTime * 10
     */
    maxAggregationTime?: number;
    /**
     * Whether compaction garbage-collects the content of deleted items when
     * building the snapshot. Without GC, snapshots produced by
     * Y.mergeUpdates grow with the document's total historical churn rather
     * than its live content, so long-lived documents pay ever-growing
     * download/merge/upload costs. GC preserves state vectors and
     * delete-sets (only tombstone *content* is dropped) and matches the
     * default behavior of live Y.Doc instances.
     * @default true
     */
    gcCompaction?: boolean;
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
     * Pre-measured clock offset (serverTime - clientTime, ms) to reuse for
     * distributed locking. Passed by parent providers to their subdocument
     * providers: clock skew is a property of the client, not the document,
     * so re-measuring it per subdoc costs 3 Firestore ops each for no
     * benefit — with hundreds of object subdocs that is a startup storm.
     * @internal
     */
    cachedClockOffset?: number;
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
export const FIREBASE_ORIGINS = {
    SNAPSHOT: 'origin:firebase/snapshot',
    HISTORY: 'origin:firebase/history',
    UPDATE: 'origin:firebase/update',
} as const;

/**
 * Firestore path constants.
 */
export const FIRESTORE_PATHS = {
    UPDATES: 'updates',
    HISTORY: 'history',
    MAINTENANCE: 'maintenance',
    LOCK_COMPACTION: 'metadata/lock_compaction',
} as const;

/**
 * Default configuration values.
 */
export const DEFAULTS = {
    MAX_UPDATES_THRESHOLD: 50,
    MAX_WAIT_TIME: 500,
    /** maxAggregationTime = maxWaitTime * this, unless configured explicitly */
    MAX_AGGREGATION_MULTIPLIER: 10,
    DEPTH: 0,
    LOCK_TTL: 60000,
    COMPACTION_LIMIT: 200, // P0: Reduced from 500 to stay under Firestore 500 op limit
    MAX_SUBDOC_DEPTH: 50,
    TARGET_SNAPSHOT_SIZE: 900000, // 900KB
    MAX_RETRIES: 5,
    /** Maximum docs to fetch per batch during initial sync (P0.1 fix) */
    SYNC_BATCH_SIZE: 100,
    /** Pending-update count that forces a compaction trigger, bypassing the trigger cooldown */
    REALTIME_LIMIT: 200,
    /** Minimum time between compaction triggers from a single client's listener */
    COMPACTION_TRIGGER_COOLDOWN_MS: 10_000,
    /** Firestore maximum document size in bytes (1MB) */
    FIRESTORE_DOC_LIMIT: 1_048_576,
    /**
     * Updates larger than this are offloaded to Cloud Storage instead of
     * being inlined in a Firestore document. Leaves headroom below
     * FIRESTORE_DOC_LIMIT for field names, metadata arrays, and overhead.
     */
    INLINE_UPDATE_LIMIT: 1_040_384, // FIRESTORE_DOC_LIMIT - 8KB
    /** Maximum consecutive save failures before emitting save-rejected */
    MAX_SAVE_RETRIES: 5,
    /**
     * Caps on documents deleted per compaction transaction. Firestore
     * transactions allow at most 500 writes; updates + history + 1 snapshot
     * set must stay within that budget (400 + 99 + 1 = 500).
     */
    MAX_COMPACTION_UPDATES: 400,
    MAX_COMPACTION_HISTORY: 99,
    /**
     * Maximum size of the delete-set fingerprint stored inline on the main
     * document. Larger delete-sets are simply not stored (clients fall back
     * to a redundant-but-idempotent push), keeping the main document well
     * under the Firestore size limit.
     */
    MAX_DELETE_SET_FIELD_BYTES: 700_000,
} as const;
