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
/**
 * Origins used to tag updates from Firebase.
 * Used to prevent echo/loops when applying remote updates.
 */
export const FIREBASE_ORIGINS = {
    SNAPSHOT: 'origin:firebase/snapshot',
    HISTORY: 'origin:firebase/history',
    UPDATE: 'origin:firebase/update',
};
/**
 * Firestore path constants.
 */
export const FIRESTORE_PATHS = {
    UPDATES: 'updates',
    HISTORY: 'history',
    MAINTENANCE: 'maintenance',
    LOCK_COMPACTION: 'metadata/lock_compaction',
};
/**
 * Default configuration values.
 */
export const DEFAULTS = {
    MAX_UPDATES_THRESHOLD: 50,
    MAX_WAIT_TIME: 500,
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
    COMPACTION_TRIGGER_COOLDOWN_MS: 10000,
    /** Firestore maximum document size in bytes (1MB) */
    FIRESTORE_DOC_LIMIT: 1048576,
    /**
     * Updates larger than this are offloaded to Cloud Storage instead of
     * being inlined in a Firestore document. Leaves headroom below
     * FIRESTORE_DOC_LIMIT for field names, metadata arrays, and overhead.
     */
    INLINE_UPDATE_LIMIT: 1040384, // FIRESTORE_DOC_LIMIT - 8KB
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
    MAX_DELETE_SET_FIELD_BYTES: 700000,
};
