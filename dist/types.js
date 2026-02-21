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
    /** Maximum updates to track in real-time listener (P0.2 fix) */
    REALTIME_LIMIT: 200,
    /** Firestore maximum document size in bytes (1MB) */
    FIRESTORE_DOC_LIMIT: 1048576,
    /** Maximum consecutive save failures before emitting save-rejected */
    MAX_SAVE_RETRIES: 5,
};
