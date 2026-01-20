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
import { Firestore } from "@firebase/firestore";
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
export declare function measureClockSkew(db: Firestore, path: string, uid: string): Promise<number>;
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
export declare function acquireLock(config: LockConfig): Promise<boolean>;
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
export declare function releaseLock(config: Pick<LockConfig, 'db' | 'path' | 'uid'>): Promise<void>;
/**
 * Checks if a lock is currently held and unexpired.
 *
 * @param config - Lock configuration
 * @returns Object with lock status information
 */
export declare function checkLockStatus(config: LockConfig): Promise<{
    exists: boolean;
    owner?: string;
    isExpired?: boolean;
    ageMs?: number;
}>;
//# sourceMappingURL=locking.d.ts.map