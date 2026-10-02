/**
 * Pure policy for the provider.
 *
 * FireProvider is a long-lived object wired to Firestore listeners, timers
 * and the window lifecycle, so these decisions were only reachable by
 * standing one up against the emulator. Nothing here touches the Firestore
 * SDK or any timer.
 */
import { FIREBASE_ORIGINS } from './types';

/**
 * Validates the provider configuration, throwing the same messages the
 * constructor used to build inline.
 *
 * This runs before any Firebase SDK call on purpose: getFirestore() on a
 * malformed app fails with a cryptic error, and a bad path only surfaces
 * much later as a permission denial.
 *
 * @param config - The fields that have constraints.
 * @throws Error when any constraint is violated.
 */
export function validateProviderConfig(config: {
    path: string;
    maxUpdatesThreshold: number;
    maxWaitTime: number;
    maxAggregationTime: number;
    depth: number;
}): void {
    const { path, maxUpdatesThreshold, maxWaitTime, maxAggregationTime, depth } = config;

    if (!path || path.includes('//') || path.startsWith('/') || path.endsWith('/')) {
        throw new Error(`Invalid Firestore path: '${path}'. Path must not be empty, start/end with '/', or contain '//'`);
    }
    if (maxUpdatesThreshold <= 0) {
        throw new Error(`Invalid maxUpdatesThreshold: ${maxUpdatesThreshold}. Must be positive.`);
    }
    // Checked before maxAggregationTime, whose default derives from it.
    if (maxWaitTime < 0) {
        throw new Error(`Invalid maxWaitTime: ${maxWaitTime}. Must not be negative.`);
    }
    if (maxAggregationTime <= 0) {
        throw new Error(`Invalid maxAggregationTime: ${maxAggregationTime}. Must be positive.`);
    }
    if (depth < 0 || depth > 100) {
        throw new Error(`Invalid depth: ${depth}. Must be between 0 and 100.`);
    }
}

/**
 * Whether an update came back from the server rather than from a local
 * edit, and so must not be written back.
 *
 * @param origin - The Yjs transaction origin.
 * @returns true for an update this provider applied from Firestore.
 */
export function isRemoteOrigin(origin: unknown): boolean {
    return origin === FIREBASE_ORIGINS.SNAPSHOT
        || origin === FIREBASE_ORIGINS.HISTORY
        || origin === FIREBASE_ORIGINS.UPDATE;
}

/**
 * How long the debounced save should wait.
 *
 * The debounce slides on every keystroke, so on its own it would defer a
 * save indefinitely during continuous typing. `maxAggregationTime` is a
 * hard ceiling measured from the FIRST buffered update: once that deadline
 * passes the delay collapses to 0 and the save goes out. An explicit delay
 * (a retry backoff) bypasses the ceiling, because it is not aggregating
 * anything.
 *
 * @param params - Explicit delay, the two limits, buffer start and now.
 * @returns The delay in milliseconds, never negative.
 */
export function computeSaveDelay(params: {
    explicitDelayMs?: number;
    maxWaitTime: number;
    maxAggregationTime: number;
    pendingSince: number | null;
    now: number;
}): number {
    const { explicitDelayMs, maxWaitTime, maxAggregationTime, pendingSince, now } = params;

    if (explicitDelayMs !== undefined) {
        return explicitDelayMs;
    }
    if (pendingSince === null) {
        return maxWaitTime;
    }

    const deadline = pendingSince + maxAggregationTime;

    return Math.max(0, Math.min(maxWaitTime, deadline - now));
}

/**
 * How much longer a due save should wait for the initial sync in flight.
 *
 * Initial sync's push covers every update buffered before it reads the
 * local doc. On a cold start that includes local persistence's hydration
 * (the whole document), so saving the buffer meanwhile would upload it a
 * second time. The save still goes out at `maxAggregationTime` from the
 * first buffered update, the same ceiling as for continuous typing: the
 * sync can stall (offline, its push is never acknowledged), and buffered
 * edits must not wait on it indefinitely.
 *
 * @param params - Whether initial sync is running, the ceiling, buffer start and now.
 * @returns 0 to save now, else the delay until the ceiling.
 */
export function initialSyncSaveHold(params: {
    syncInFlight: boolean;
    maxAggregationTime: number;
    pendingSince: number | null;
    now: number;
}): number {
    const { syncInFlight, maxAggregationTime, pendingSince, now } = params;

    if (!syncInFlight || pendingSince === null) {
        return 0;
    }

    return Math.max(0, pendingSince + maxAggregationTime - now);
}

/**
 * How to recover from a real-time listener error.
 *
 * Firestore ends a listener for good once its error callback fires, so the
 * provider re-syncs to attach fresh ones. Listeners that stayed up for
 * `healthyMs` were healthy, so the count starts over. Errors in quick
 * succession (the re-sync's reads succeed but the listen keeps being
 * rejected) escalate the backoff and, like initial sync, give up after
 * `maxRetries` instead of re-reading the whole document forever.
 *
 * @param state - Errors so far, when the listeners were attached, now and the limits.
 * @returns The new error count, and whether to stop re-syncing.
 */
export function planListenerRecovery(state: {
    retryCount: number;
    attachedAt: number;
    now: number;
    healthyMs: number;
    maxRetries: number;
}): { retryCount: number; giveUp: boolean } {
    const healthy = state.now - state.attachedAt >= state.healthyMs;
    const retryCount = (healthy ? 0 : state.retryCount) + 1;

    return { retryCount, giveUp: retryCount >= state.maxRetries };
}

/** Why a squash cannot run right now. */
export type SquashBlock =
    | { kind: 'destroyed' }
    | { kind: 'local-behind' }
    | { kind: 'subdocs-unsupported' }
    | null;

/**
 * Whether the provider is in a state where squashing is safe.
 *
 * Each block exists for a different reason: a destroyed provider has no
 * connection, an unsynced or epoch-fenced one would squash a document it
 * does not fully hold (silently dropping other clients' data), and
 * subdocuments would be orphaned in the old epoch. A subdocument counts
 * whether or not a provider was started for it (lazy mode leaves
 * unloaded subdocuments without one).
 *
 * @param state - The provider's current lifecycle flags.
 * @returns The blocking reason, or null when a squash may proceed.
 */
export function squashBlockedBy(state: {
    isDestroyed: boolean;
    synced: boolean;
    epochFenced: boolean;
    subProviderCount: number;
    subdocCount: number;
    depth: number;
}): SquashBlock {
    if (state.isDestroyed) {
        return { kind: 'destroyed' };
    }
    if (!state.synced) {
        return { kind: 'local-behind' };
    }
    if (state.epochFenced) {
        return { kind: 'local-behind' };
    }
    if (state.subProviderCount > 0 || state.subdocCount > 0 || state.depth > 0) {
        return { kind: 'subdocs-unsupported' };
    }

    return null;
}
