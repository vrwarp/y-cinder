/**
 * Pure decision logic for the distributed compaction lock.
 *
 * locking.ts makes these judgements inside Firestore transactions, so they
 * were reachable only through the emulator suite and invisible to mutation
 * testing. Nothing here touches the Firestore SDK.
 *
 * Getting these wrong is expensive in both directions: too eager and two
 * clients compact the same document concurrently, too reluctant and
 * compaction stops happening at all once a lock is orphaned by a crash.
 */

/**
 * Reads a lock's creation time in epoch millis.
 *
 * Accepts a Firestore Timestamp (the normal case) or a raw number (written
 * by older clients). Anything missing or unrecognised reads as 0, which
 * makes the lock look infinitely old and therefore expired — the safe
 * direction, since the alternative is a lock nobody can ever break.
 *
 * @param createdAt - The document's createdAt field.
 * @returns Creation time in epoch millis, or 0 when unusable.
 */
export function readLockCreatedAt(createdAt: unknown): number {
    if (createdAt && typeof (createdAt as { toMillis?: unknown }).toMillis === 'function') {
        return (createdAt as { toMillis: () => number }).toMillis();
    }
    if (typeof createdAt === 'number') {
        return createdAt;
    }

    return 0;
}

/**
 * The estimated server time, given the measured client/server offset.
 *
 * Lock ages must be judged on server time: a client whose clock is minutes
 * fast would otherwise consider every lock expired and stampede.
 *
 * @param nowMs - The local clock reading.
 * @param serverOffsetMs - Measured offset (server minus local).
 * @returns Estimated server-side now.
 */
export function estimateServerNow(nowMs: number, serverOffsetMs: number): number {
    return nowMs + serverOffsetMs;
}

/**
 * Whether an existing lock blocks this client from acquiring it.
 *
 * A lock is only an obstacle while it is BOTH unexpired AND held by
 * someone else. Our own lock is re-entrant, and an expired lock is
 * reclaimable by anyone — that is what stops a crashed client from
 * blocking compaction forever.
 *
 * @param params - The lock's fields, our identity, the TTL and server now.
 * @returns true when acquisition must fail.
 */
export function isLockBusy(params: {
    createdAt: unknown;
    owner: unknown;
    uid: string;
    serverNow: number;
    lockTTL: number;
}): boolean {
    const { createdAt, owner, uid, serverNow, lockTTL } = params;
    const lockAge = serverNow - readLockCreatedAt(createdAt);

    return lockAge < lockTTL && owner !== uid;
}

/**
 * Whether a lock has aged past its TTL.
 *
 * Expiry is inclusive of the boundary: a lock exactly at its TTL is
 * expired, matching isLockBusy's strict `age < ttl` test for "still
 * blocking" so the two can never both be true.
 *
 * @param ageMs - The lock's age in millis.
 * @param lockTTL - Time to live in millis.
 * @returns true when the lock is expired.
 */
export function isLockExpired(ageMs: number, lockTTL: number): boolean {
    return ageMs >= lockTTL;
}

/**
 * Extracts the server timestamp a clock-skew probe read back.
 *
 * @param data - The probe document's data.
 * @returns The server time in millis, or null when the field is unusable.
 */
export function readProbeServerTime(data: Record<string, any> | null | undefined): number | null {
    if (data && data.t && typeof data.t.toMillis === 'function') {
        return data.t.toMillis();
    }

    return null;
}

/**
 * The clock offset implied by a probe, or 0 when it could not be read.
 *
 * @param serverTime - Server time from the probe, or null.
 * @param localNow - The local clock at read time.
 * @returns Offset in millis (server minus local); 0 when unknown.
 */
export function offsetFromProbe(serverTime: number | null, localNow: number): number {
    return serverTime === null ? 0 : serverTime - localNow;
}
