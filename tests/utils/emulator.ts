/**
 * Firebase Emulator Setup Utilities
 *
 * Provides helper functions for setting up and managing Firebase Firestore
 * emulator connections in test environments. Handles singleton initialization
 * to prevent "already connected" errors across test files.
 *
 * @module tests/utils/emulator
 *
 * @example
 * ```typescript
 * import { setupEmulator, clearFirestore } from './utils/emulator';
 *
 * beforeEach(async () => {
 *   const { app, db } = await setupEmulator();
 *   await clearFirestore(db);
 * });
 * ```
 */

import { initializeApp, getApps, getApp } from "firebase/app";
import {
    getFirestore,
    connectFirestoreEmulator,
    terminate,
    clearIndexedDbPersistence,
    setLogLevel,
} from "firebase/firestore";

/** Project ID for the demo Firebase project (emulator only) */
const PROJECT_ID = "demo-test-project";

/** Hostname where the Firestore emulator is running */
const EMULATOR_HOST = "127.0.0.1";

/** Port number for the Firestore emulator */
const FIRESTORE_PORT = 8080;

/** Tracks whether emulator connection has been established (singleton pattern) */
let emulatorConnected = false;

/** Tracks whether log level has been set */
let logLevelConfigured = false;

/**
 * Configure Firestore log level for debugging.
 * 
 * Set FIREBASE_LOG_LEVEL environment variable to control logging:
 * - 'debug': Verbose logging for debugging
 * - 'error': Only log errors
 * - 'silent': No logging (default)
 * 
 * @example
 * FIREBASE_LOG_LEVEL=debug npm test
 */
function configureLogLevel(): void {
    if (logLevelConfigured) return;
    logLevelConfigured = true;

    const logLevel = process.env.FIREBASE_LOG_LEVEL || 'error';

    if (logLevel === 'debug' || logLevel === 'error' || logLevel === 'silent') {
        setLogLevel(logLevel);
        if (logLevel === 'debug') {
            console.log('[Firebase] Log level set to:', logLevel);
        }
    } else {
        // Default to 'error' for cleaner test output
        setLogLevel('error');
    }
}

/**
 * Initializes or retrieves the Firebase app and connects to the Firestore emulator.
 *
 * Uses a singleton pattern to ensure the emulator is only connected once per
 * process, avoiding "already started" errors when running multiple test files.
 *
 * @returns Promise resolving to an object containing the Firebase app and Firestore instance
 *
 * @example
 * ```typescript
 * const { app, db } = await setupEmulator();
 * // Use db for Firestore operations
 * ```
 */
export const setupEmulator = async () => {
    // Configure logging first
    configureLogLevel();

    let app;
    if (getApps().length > 0) {
        app = getApp();
    } else {
        app = initializeApp({
            projectId: PROJECT_ID,
            apiKey: "fake-api-key",
            storageBucket: `${PROJECT_ID}.appspot.com`,
        });
    }

    const db = getFirestore(app);

    const { getStorage, connectStorageEmulator } = await import("firebase/storage");
    const storage = getStorage(app);

    // Only connect once per process to avoid "already started" error
    if (!emulatorConnected) {
        connectFirestoreEmulator(db, EMULATOR_HOST, FIRESTORE_PORT);
        connectStorageEmulator(storage, EMULATOR_HOST, 9199);
        emulatorConnected = true;
    }

    return { app, db, storage };
};

/**
 * Clears all documents from the Firestore emulator database.
 *
 * Makes an HTTP DELETE request to the emulator's REST API to remove all
 * documents. This should be called between tests to ensure isolation.
 *
 * @param db - The Firestore instance (used for type consistency, not actually required)
 *
 * @example
 * ```typescript
 * beforeEach(async () => {
 *   await clearFirestore(db);
 * });
 * ```
 */
export const clearFirestore = async (db: any) => {
    try {
        await fetch(`http://${EMULATOR_HOST}:${FIRESTORE_PORT}/emulator/v1/projects/${PROJECT_ID}/databases/(default)/documents`, {
            method: 'DELETE'
        });
    } catch (e) {
        console.warn("Failed to clear emulator database (is it running?)", e);
    }
};

/**
 * Placeholder for emulator teardown.
 *
 * Currently a no-op because terminating the Firebase app can break shared
 * state across test files running in the same process. The emulator handles
 * cleanup automatically when the test process exits.
 *
 * @param app - The Firebase app instance (unused)
 */
export const teardownEmulator = async (app: any) => {
    // No-op to avoid breaking shared state
};
