/**
 * Pure preconditions and derivations for the squash protocol.
 *
 * A squash rebuilds the document into a brand-new id space, so its
 * preconditions are unusually unforgiving: squashing while the local
 * client is behind the server silently DROPS whatever it had not yet
 * received, with no error anywhere. Those checks lived inside
 * squashDocument between Firestore reads and so were reachable only
 * through the emulator suite. Nothing here touches the Firestore SDK.
 */
import * as Y from 'yjs';
import { fromBase64 } from 'lib0/buffer';

/**
 * Whether the local state vector covers a remote one encoded as base64.
 *
 * An absent state vector counts as covered — there is nothing to be behind
 * of. An unparseable one counts as NOT covered, which blocks the squash:
 * the safe direction, since proceeding would risk dropping data we cannot
 * prove we hold.
 *
 * @param localSV - The local document's state vector.
 * @param svB64 - The remote state vector, base64 encoded.
 * @returns true when the local document is at or ahead of the remote.
 */
export function stateVectorCovers(localSV: Map<number, number>, svB64: string | undefined): boolean {
    if (!svB64) {
        return true;
    }
    try {
        const remote = Y.decodeStateVector(fromBase64(svB64));

        for (const [client, clock] of remote) {
            if ((localSV.get(client) || 0) < clock) {
                return false;
            }
        }

        return true;
    } catch {
        return false;
    }
}

/**
 * Whether the local document already holds everything one pending
 * update/history document contributes.
 *
 * Three cases in priority order: explicit per-client clocks (the cheapest
 * and most precise), a state vector, or neither — and "neither" is
 * deliberately conservative. A document carrying a payload we cannot
 * verify against blocks the squash rather than being assumed known.
 *
 * @param localSV - The local document's state vector.
 * @param data - The pending document's data.
 * @returns true when the local document provably covers it.
 */
export function localCoversPendingDoc(
    localSV: Map<number, number>,
    data: Record<string, any> | null | undefined,
): boolean {
    const ids: number[] = data?.clientIDs || [];
    const clocks: number[] = data?.clientClocks || [];

    if (ids.length === clocks.length && ids.length > 0) {
        for (let i = 0; i < ids.length; i += 1) {
            if ((localSV.get(ids[i]) || 0) < clocks[i]) {
                return false;
            }
        }

        return true;
    }

    if (data?.stateVector) {
        return stateVectorCovers(localSV, data.stateVector);
    }

    if (data?.update || data?.segment || data?.updateStoragePath) {
        // Carries data but offers no way to verify it: refuse to squash.
        return false;
    }

    return true;
}

/**
 * Whether the server holds more pending work than one transaction can
 * fold, meaning a normal compaction must run first.
 *
 * @param params - Pending counts and their per-transaction ceilings.
 * @returns true when the document is not quiescent enough to squash.
 */
export function isNotQuiescent(params: {
    updateCount: number;
    historyCount: number;
    maxUpdates: number;
    maxHistory: number;
}): boolean {
    const { updateCount, historyCount, maxUpdates, maxHistory } = params;

    return updateCount > maxUpdates || historyCount > maxHistory;
}

/** The main document's optimistic-concurrency fields. */
export interface VersionEpoch {
    version: number;
    epoch: number;
}

/**
 * Reads version and epoch, defaulting both to 0 for a document that
 * predates them or does not exist.
 *
 * @param data - Main document data.
 * @returns The version and epoch.
 */
export function readVersionEpoch(data: Record<string, any> | null | undefined): VersionEpoch {
    return {
        version: typeof data?.version === 'number' ? data.version : 0,
        epoch: typeof data?.epoch === 'number' ? data.epoch : 0,
    };
}

/**
 * Whether the main document changed under us between the pre-upload read
 * and the commit transaction.
 *
 * Either field moving means another client compacted or squashed while we
 * were uploading, and committing would clobber their work.
 *
 * @param observed - What the transaction just read.
 * @param expected - What we read before uploading.
 * @returns true when the squash must abort.
 */
export function isSquashPreempted(observed: VersionEpoch, expected: VersionEpoch): boolean {
    return observed.version !== expected.version || observed.epoch !== expected.epoch;
}

/**
 * Whether the lock document still shows us as the owner.
 *
 * @param lockData - The lock document's data, or undefined when absent.
 * @param uid - This client's id.
 * @returns true when we still hold the lock.
 */
export function stillHoldsLock(lockData: Record<string, any> | null | undefined, uid: string): boolean {
    return Boolean(lockData) && lockData?.owner === uid;
}

/**
 * The Cloud Storage path for a squashed snapshot.
 *
 * Epoch and version both appear so a squash never overwrites the blob a
 * previous epoch's readers may still be fetching.
 *
 * @param basePath - The document's base path.
 * @param epoch - The new epoch.
 * @param version - The new version.
 * @returns The storage object path.
 */
export function squashSnapshotPath(basePath: string, epoch: number, version: number): string {
    return `${basePath}/snapshot_e${epoch}_v${version}.bin`;
}
