import { initializeApp, getApps, getApp } from "firebase/app";
import { getFirestore, connectFirestoreEmulator, terminate, clearIndexedDbPersistence } from "firebase/firestore";

const PROJECT_ID = "demo-test-project";
const EMULATOR_HOST = "127.0.0.1";
const FIRESTORE_PORT = 8080;

let emulatorConnected = false;

export const setupEmulator = async () => {
    let app;
    if (getApps().length > 0) {
        app = getApp();
    } else {
        app = initializeApp({
            projectId: PROJECT_ID,
            apiKey: "fake-api-key",
        });
    }

    const db = getFirestore(app);

    // Only connect once per process to avoid "already started" error
    if (!emulatorConnected) {
        connectFirestoreEmulator(db, EMULATOR_HOST, FIRESTORE_PORT);
        emulatorConnected = true;
    }

    return { app, db };
};

export const clearFirestore = async (db: any) => {
    try {
        await fetch(`http://${EMULATOR_HOST}:${FIRESTORE_PORT}/emulator/v1/projects/${PROJECT_ID}/databases/(default)/documents`, {
            method: 'DELETE'
        });
    } catch (e) {
        console.warn("Failed to clear emulator database (is it running?)", e);
    }
};

export const teardownEmulator = async (app: any) => {
    // No-op to avoid breaking shared state
};
