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
    FIRESTORE_DOC_LIMIT: 1_048_576,
    /** Maximum consecutive save failures before emitting save-rejected */
    MAX_SAVE_RETRIES: 5,
} as const;

// Type augmentation for internal Yjs API
declare module 'yjs' {
    export function decodeUpdate(update: Uint8Array): {
        structs: Array<{ id: { client: number; clock: number }; length: number }>;
    };
}
