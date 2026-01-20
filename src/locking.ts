/**
 * Distributed Locking Module
 *
 * Provides distributed locking primitives for coordinating exclusive operations
 * (like compaction) across multiple clients. Uses Firestore transactions and
 * server timestamps to ensure correctness despite client clock skew.
 *
 * ## Lock Semantics
 *
 * - **TTL-based expiry**: Locks automatically expire after lockTTL milliseconds
 * - **Re-entrant**: A client can re-acquire its own unexpired lock
 * - **Clock-skew tolerant**: Uses server timestamps for all time comparisons
 *
 * ## Implementation
 *
 * The lock is stored as a Firestore document with:
 * - `owner`: Client ID that holds the lock
 * - `createdAt`: Server timestamp when lock was acquired
 *
 * Before acquiring, we measure clock skew by round-tripping through the server,
 * ensuring accurate TTL calculations even when client clocks are wrong.
 *
 * @module locking
 */

import {
    Firestore,
    doc,
    collection,
    getDoc,
    setDoc,
    deleteDoc,
    runTransaction,
    serverTimestamp,
} from "@firebase/firestore";
import { FIRESTORE_PATHS } from "./types";

/**
 * Measures the difference between client clock and server clock.
 * 
 * This is critical for distributed locking to work correctly when clients
 * have clock skew. The returned offset can be used to estimate server time:
 * `serverTime ≈ Date.now() + offset`
 * 
 * Implementation:
 * 1. Write a document with serverTimestamp()
 * 2. Read it back to get the server's timestamp
 * 3. Calculate the difference from local time
 * 4. Clean up the temporary document
 * 
 * @param db - Firestore instance
 * @param path - Base document path
 * @param uid - Unique client ID (used for temp doc naming)
 * @returns The offset in milliseconds (ServerTime - ClientTime).
 *          Positive means server is ahead of client.
 *          Returns 0 if measurement fails.
 * 
 * @example
 * ```typescript
 * const offset = await measureClockSkew(db, 'docs/abc', 'client123');
 * const serverNow = Date.now() + offset;
 * ```
 */
export async function measureClockSkew(
    db: Firestore,
    path: string,
    uid: string
): Promise<number> {
    const tempId = `skew_${uid}_${Math.random().toString(36).substring(2)}`;
    const ref = doc(collection(db, path, FIRESTORE_PATHS.MAINTENANCE), tempId);

    try {
        await setDoc(ref, { t: serverTimestamp() });
        const snap = await getDoc(ref);
        const data = snap.data();

        if (data && data.t && typeof data.t.toMillis === 'function') {
            const serverTime = data.t.toMillis();
            // P1.6 FIX: Cleanup in finally ensures doc removed even on errors
            return serverTime - Date.now();
        }

        return 0;
    } catch (e) {
        // If we can't write/read, assume 0 skew (best effort)
        return 0;
    } finally {
        // P1.6 FIX: Always attempt cleanup to prevent orphaned docs
        deleteDoc(ref).catch(() => { });
    }
}

/**
 * Configuration for lock operations.
 */
export interface LockConfig {
    /** Firestore instance */
    db: Firestore;
    /** Base document path */
    path: string;
    /** Unique client ID */
    uid: string;
    /** Lock time-to-live in milliseconds */
    lockTTL: number;
    /** 
     * P0.3 FIX: Pre-measured clock offset to avoid measuring on every lock attempt.
     * If provided, measureClockSkew is skipped (saves 3 Firestore ops).
     */
    cachedClockOffset?: number;
}

/**
 * Attempts to acquire a distributed lock for exclusive operations.
 * 
 * The lock uses a TTL-based expiry mechanism that's resilient to client
 * clock skew by using server timestamps for the createdAt field.
 * 
 * Lock acquisition succeeds if:
 * - No lock exists
 * - Existing lock is expired (age > TTL)
 * - Existing lock is owned by us (re-entrant)
 * 
 * @param config - Lock configuration
 * @returns true if lock was successfully acquired, false otherwise
 * 
 * @example
 * ```typescript
 * const hasLock = await acquireLock({ db, path, uid, lockTTL: 60000 });
 * if (hasLock) {
 *   try {
 *     // Do exclusive work
 *   } finally {
 *     await releaseLock({ db, path, uid });
 *   }
 * }
 * ```
 */
export async function acquireLock(config: LockConfig): Promise<boolean> {
    const { db, path, uid, lockTTL, cachedClockOffset } = config;

    // P0.3 FIX: Use cached offset if provided, otherwise measure (only on first call)
    let serverOffset = cachedClockOffset ?? 0;
    if (cachedClockOffset === undefined) {
        try {
            serverOffset = await measureClockSkew(db, path, uid);
        } catch (e) {
            console.warn("Failed to measure clock skew, defaulting to 0:", e);
        }
    }

    // Estimated Server Time
    const serverNow = Date.now() + serverOffset;
    const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);

    try {
        return await runTransaction(db, async (transaction) => {
            const lockSnap = await transaction.get(lockRef);

            if (lockSnap.exists()) {
                const data = lockSnap.data();

                // Use estimated server time for check
                // If createdAt is valid timestamp (millis), use it.
                // Fallback: If createdAt is missing/invalid, treat as 0 (expired).
                const createdAt = (data.createdAt && typeof data.createdAt.toMillis === 'function')
                    ? data.createdAt.toMillis()
                    : (typeof data.createdAt === 'number' ? data.createdAt : 0);

                const lockAge = serverNow - createdAt;

                if (lockAge < lockTTL && data.owner !== uid) {
                    return false; // Lock is busy
                }
            }

            // Lock is free, expired, or owned by us (re-entrant). Claim it.
            transaction.set(lockRef, {
                owner: uid,
                createdAt: serverTimestamp(), // Write authoritative Server Time
                expiresAt: serverTimestamp() // Debug only
            });

            return true;
        });
    } catch (e) {
        console.warn("Failed to acquire lock (contention):", e);
        return false;
    }
}

/**
 * Releases a lock only if we still own it.
 * 
 * Uses a transaction to safely check ownership before deleting,
 * preventing accidental deletion of another client's lock.
 * 
 * @param config - Lock configuration (only db, path, uid needed)
 * 
 * @example
 * ```typescript
 * await releaseLock({ db, path, uid, lockTTL: 0 }); // lockTTL not used
 * ```
 */
export async function releaseLock(config: Pick<LockConfig, 'db' | 'path' | 'uid'>): Promise<void> {
    const { db, path, uid } = config;
    const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);

    try {
        await runTransaction(db, async (transaction) => {
            const lockSnap = await transaction.get(lockRef);
            if (lockSnap.exists() && lockSnap.data().owner === uid) {
                transaction.delete(lockRef);
            }
        });
    } catch (e) {
        console.warn("Failed to release lock:", e);
    }
}

/**
 * Checks if a lock is currently held and unexpired.
 * 
 * P1.1 FIX: Uses cachedClockOffset for accurate age calculation on
 * clients with clock skew. Without offset, the age may be incorrect.
 * 
 * Note: This is primarily used for debugging/diagnostics.
 * 
 * @param config - Lock configuration
 * @returns Object with lock status information
 */
export async function checkLockStatus(config: LockConfig): Promise<{
    exists: boolean;
    owner?: string;
    isExpired?: boolean;
    ageMs?: number;
}> {
    const { db, path, uid, lockTTL, cachedClockOffset } = config;
    const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);

    try {
        const lockSnap = await getDoc(lockRef);

        if (!lockSnap.exists()) {
            return { exists: false };
        }

        const data = lockSnap.data();
        const createdAt = (data.createdAt && typeof data.createdAt.toMillis === 'function')
            ? data.createdAt.toMillis()
            : 0;

        // P1.1 FIX: Use cached clock offset for accurate age calculation
        const serverNow = Date.now() + (cachedClockOffset ?? 0);
        const ageMs = serverNow - createdAt;

        return {
            exists: true,
            owner: data.owner,
            isExpired: ageMs >= lockTTL,
            ageMs,
        };
    } catch (e) {
        return { exists: false };
    }
}
