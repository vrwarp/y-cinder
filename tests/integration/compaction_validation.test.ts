/**
 * Integration test: Compaction candidate validation
 *
 * Verifies that the compaction pipeline rejects invalid/corrupted updates
 * before they can overwrite the canonical snapshot. Tests call compact()
 * directly to bypass FireProvider constructor overhead.
 *
 * @file compaction_validation.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import * as Y from 'yjs';
import { seedFromString, getStableDate } from '../unit/prng';
import {
    collection,
    getDocs,
    addDoc,
    getDoc,
    doc,
    setDoc,
    serverTimestamp,
    Bytes,
    Firestore,
} from 'firebase/firestore';
import { FirebaseStorage, ref, uploadBytes } from 'firebase/storage';
import { compact, CompactionContext } from '../../src/compaction';

describe('Data Integrity: Compaction candidate validation', () => {
    let app: any;
    let db: Firestore;
    let storage: FirebaseStorage;
    let path: string;
    let counter = 0;

    beforeEach(async () => {
        const seed = `compaction-validation-${getStableDate()}-${counter++}`;
        const emulator = await import("../utils/emulator").then(m => m.setupEmulator());
        app = emulator.app;
        db = emulator.db;
        storage = emulator.storage;
        path = `tests/${seed}`;
    });

    it('should succeed with valid updates', async () => {
        // Seed valid updates
        for (let i = 0; i < 3; i++) {
            const d = new Y.Doc();
            d.getText('content').insert(0, `Update ${i}`);
            await addDoc(collection(db, path, 'updates'), {
                update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(d)),
                createdAt: serverTimestamp()
            });
            d.destroy();
        }

        const ctx: CompactionContext = {
            db,
            path,
            uid: 'test-client',
            lockTTL: 60000,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
        };

        const result = await compact(ctx);

        expect(result.success).toBe(true);
        expect(result.type).toBe('snapshot');

        // All updates should be compacted away
        const updatesSnap = await getDocs(collection(db, path, 'updates'));
        expect(updatesSnap.size).toBe(0);
    }, 30000);

    it('should reject compaction when only a corrupted update exists in storage', async () => {
        // Seed a single corrupted update (random garbage bytes)
        const garbage = new Uint8Array([255, 254, 253, 252, 251, 250, 249, 248]);
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(garbage),
            createdAt: serverTimestamp()
        });

        const ctx: CompactionContext = {
            db,
            path,
            uid: 'test-client',
            lockTTL: 60000,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
        };

        const result = await compact(ctx);

        // Compaction should fail (either mergeUpdatesAsync or decodeUpdate throws)
        expect(result.success).toBe(false);

        // The corrupted update should still exist (not wiped)
        const updatesSnap = await getDocs(collection(db, path, 'updates'));
        expect(updatesSnap.size).toBe(1);
    }, 30000);

    it('should reject compaction when a mix of valid and corrupted updates exist', async () => {
        // Seed one valid update
        const d = new Y.Doc();
        d.getText('content').insert(0, 'Valid data');
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(Y.encodeStateAsUpdate(d)),
            createdAt: serverTimestamp()
        });
        d.destroy();

        // Seed one corrupted update
        const truncated = new Uint8Array([1, 1, 200, 150]); // truncated/invalid structure
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(truncated),
            createdAt: serverTimestamp()
        });

        const ctx: CompactionContext = {
            db,
            path,
            uid: 'test-client',
            lockTTL: 60000,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
        };

        const result = await compact(ctx);

        // If Y.mergeUpdates or Y.decodeUpdate catches corruption, compaction fails
        expect(result.success).toBe(false);

        // Both updates should remain (nothing wiped)
        const updatesSnap = await getDocs(collection(db, path, 'updates'));
        expect(updatesSnap.size).toBe(2);
    }, 30000);

    it('should not overwrite an existing valid snapshot with corrupted data', async () => {
        // Seed an existing valid base snapshot via Cloud Storage
        const baseDoc = new Y.Doc();
        baseDoc.getText('content').insert(0, 'Precious data that must survive');
        const baseUpdate = Y.encodeStateAsUpdate(baseDoc);
        baseDoc.destroy();

        const storagePath = `${path}/snapshot_v1.bin`;
        const storageRef = ref(storage, storagePath);
        await uploadBytes(storageRef, baseUpdate);

        await setDoc(doc(db, path), {
            snapshotStoragePath: storagePath,
            version: 1,
        });

        // Add a corrupted update
        const garbage = new Uint8Array([255, 254, 253, 252, 251]);
        await addDoc(collection(db, path, 'updates'), {
            update: Bytes.fromUint8Array(garbage),
            createdAt: serverTimestamp()
        });

        const ctx: CompactionContext = {
            db,
            path,
            uid: 'test-client',
            lockTTL: 60000,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
        };

        const result = await compact(ctx);

        // Compaction should fail
        expect(result.success).toBe(false);

        // Original snapshot should remain intact
        const mainSnap = await getDoc(doc(db, path));
        const mainData = mainSnap.data();
        expect(mainData?.version).toBe(1);
        expect(mainData?.snapshotStoragePath).toBe(storagePath);
    }, 30000);

    it('should successfully compact oversized storage-backed updates', async () => {
        // Create an "oversized" update content
        const d = new Y.Doc();
        d.getText('content').insert(0, 'Giant payload ');
        const largeUpdate = Y.encodeStateAsUpdate(d);
        d.destroy();

        // 1. Upload binary to Cloud Storage directly (simulating sync.ts large update logic)
        const storagePath = `${path}/large_updates/test_123.bin`;
        const storageRef = ref(storage, storagePath);
        await uploadBytes(storageRef, largeUpdate);

        // 2. Write lightweight pointer document to FIRESTORE_PATHS.UPDATES
        await addDoc(collection(db, path, 'updates'), {
            updateStoragePath: storagePath,
            createdAt: serverTimestamp()
        });

        const ctx: CompactionContext = {
            db,
            path,
            uid: 'test-client',
            lockTTL: 60000,
            compactionLimit: 500,
            isDestroyed: () => false,
            storage,
        };

        const result = await compact(ctx);

        // Should successfully download from Storage and merge it into candidate
        expect(result.success).toBe(true);
        expect(result.type).toBe('snapshot');

        // The update pointer should be properly deleted from Firestore
        const updatesSnap = await getDocs(collection(db, path, 'updates'));
        expect(updatesSnap.size).toBe(0);

        // Verify the extracted snapshot actually contains the data
        const mainSnap = await getDoc(doc(db, path));
        const mainData = mainSnap.data();
        expect(mainData?.snapshotStoragePath).toBeDefined();

        // Download the compacted snapshot to verify data made it in
        const snapRef = ref(storage, mainData!.snapshotStoragePath);
        const buffer = await import('firebase/storage').then(m => m.getBytes(snapRef));
        
        const finalDoc = new Y.Doc();
        Y.applyUpdate(finalDoc, new Uint8Array(buffer));
        expect(finalDoc.getText('content').toString()).toBe('Giant payload ');
    }, 30000);
});

