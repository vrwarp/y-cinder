/**
 * Epoch Squash Module
 *
 * THE aged-document floor reset. Garbage-collected compaction keeps
 * deleted *content* out of the snapshot, but three things still grow
 * forever with total historical churn, because CRDT convergence requires
 * them:
 *
 *  - tombstone *structure* (one struct per overwritten map key survives
 *    every merge — versicle-style documents add thousands per month),
 *  - the delete-set (one range per fragmented deletion),
 *  - the state vector (one entry per client that ever wrote; a new client
 *    is born on every page load).
 *
 * Squashing rebuilds the document CONTENT into a brand-new Yjs document
 * (fresh id space: no tombstones, empty delete-set, one client in the
 * state vector) and publishes it as the start of a new EPOCH. This is a
 * deliberate break of CRDT history: edits made concurrently across the
 * squash boundary can no longer merge automatically. Epoch fencing makes
 * the break explicit and safe:
 *
 *  - the main document carries an `epoch` counter (absent = 0);
 *  - every update / history segment is tagged with the epoch it belongs
 *    to; clients ignore data from foreign epochs, and compaction deletes
 *    it;
 *  - the squashed document itself carries its epoch in a well-known
 *    shared map (`__ycinder.epoch`), so any locally persisted copy knows
 *    which epoch it came from;
 *  - a client that finds the server ahead of its local epoch STOPS
 *    syncing and surfaces an `epoch-changed` event with its full local
 *    state — the application decides what (if anything) to merge, then
 *    rebuilds its local document from the new snapshot.
 *
 * Use it for single-user / small-team documents (multi-device note or
 * library sync, like versicle) where "last synced state wins across the
 * squash boundary" is acceptable. Avoid it for high-concurrency real-time
 * collaboration where peers are routinely offline with unsynced edits.
 *
 * @module squash
 */

import {
    Firestore,
    doc,
    collection,
    Bytes,
    runTransaction,
    query,
    orderBy,
    getDocs,
    getDoc,
    serverTimestamp,
    deleteField,
    limit,
} from "@firebase/firestore";
import { ref, uploadBytes, deleteObject, FirebaseStorage } from "@firebase/storage";
import { toBase64, fromBase64 } from "lib0/buffer";
import * as Y from "yjs";
import { DEFAULTS, FIRESTORE_PATHS } from "./types";
import { acquireLock, releaseLock } from "./locking";

/** Name of the shared map that carries provider metadata inside the doc. */
export const PROVIDER_META_KEY = "__ycinder";

/** Key of the epoch marker within the provider metadata map. */
export const EPOCH_KEY = "epoch";

/**
 * Reads the epoch a Yjs document belongs to (0 when it predates squashing).
 */
export function readDocEpoch(ydoc: Y.Doc): number {
    const meta = ydoc.getMap(PROVIDER_META_KEY);
    const epoch = meta.get(EPOCH_KEY);
    return typeof epoch === "number" ? epoch : 0;
}

/**
 * Whether a document holds any integrated structs (i.e. has real state).
 * Fresh, never-hydrated documents return false.
 */
export function docHasContent(ydoc: Y.Doc): boolean {
    return (ydoc.store as any).clients.size > 0;
}

/**
 * Deep-clones the CONTENT of `source` into a fresh Y.Doc with a brand-new
 * id space, and stamps the target epoch into the provider metadata map.
 *
 * Throws when a root shared type was never concretely typed on this client
 * (its constructor is still AbstractType) — squashing such a document
 * would silently drop that root's content.
 *
 * @param source - The fully-synced document to squash
 * @param epoch - Epoch number to stamp into the clone
 * @returns The squashed document (caller owns destruction)
 */
export function buildSquashedDoc(source: Y.Doc, epoch: number): Y.Doc {
    // Validate every root first so we fail before allocating the clone.
    // A root that was only ever hydrated (never accessed through a typed
    // getter) is still a generic AbstractType: it cannot be cloned. When
    // it holds live content that would be data loss — refuse. When it is
    // dead weight (every entry deleted — e.g. versicle's emptied legacy
    // "husk" shares), it is skipped: squash is exactly the mechanism that
    // finally sheds those permanently-stuck root shares.
    const skippedDeadRoots = new Set<string>();
    source.share.forEach((type: any, name: string) => {
        if (name === PROVIDER_META_KEY) return; // rewritten below regardless
        if (type.constructor === Y.AbstractType) {
            if (abstractRootHasLiveContent(type)) {
                throw new Error(
                    `Cannot squash: root share '${name}' has no concrete type on this client`
                );
            }
            skippedDeadRoots.add(name);
        }
    });

    const target = new Y.Doc({ gc: true });
    try {
        target.transact(() => {
            source.share.forEach((type: any, name: string) => {
                if (name === PROVIDER_META_KEY) return; // rewritten below
                if (skippedDeadRoots.has(name)) return; // tombstones only
                if (type instanceof Y.Map) {
                    const t = target.getMap(name);
                    type.forEach((v: unknown, k: string) => {
                        t.set(k, cloneValue(v));
                    });
                } else if (type instanceof Y.Array) {
                    const t = target.getArray(name);
                    t.insert(0, type.toArray().map(cloneValue));
                } else if (type instanceof Y.XmlFragment && !(type instanceof Y.XmlElement)) {
                    const t = target.getXmlFragment(name);
                    t.insert(0, (type.toArray() as any[]).map((c) => c.clone()));
                } else if (type instanceof Y.Text) {
                    const t = target.getText(name);
                    t.applyDelta(type.toDelta());
                } else {
                    throw new Error(
                        `Cannot squash: root share '${name}' has unsupported type ${type.constructor?.name}`
                    );
                }
            });
            target.getMap(PROVIDER_META_KEY).set(EPOCH_KEY, epoch);
        });
    } catch (e) {
        target.destroy();
        throw e;
    }
    return target;
}

function cloneValue(v: unknown): unknown {
    return v instanceof Y.AbstractType ? (v as any).clone() : v;
}

/**
 * Whether a generically-typed (AbstractType) root still holds any live
 * (undeleted) content. Dead roots — every map entry overwritten/deleted,
 * every list item deleted — carry only tombstone structure and are safe
 * to drop from the squashed document.
 */
function abstractRootHasLiveContent(type: any): boolean {
    for (const item of type._map?.values() ?? []) {
        if (!item.deleted) return true;
    }
    for (let item = type._start; item != null; item = item.right) {
        if (!item.deleted) return true;
    }
    return false;
}

/**
 * Context for the squash operation.
 */
export interface SquashContext {
    db: Firestore;
    path: string;
    uid: string;
    lockTTL: number;
    cachedClockOffset?: number;
    storage: FirebaseStorage;
    isDestroyed: () => boolean;
    /** The live, fully-synced document to squash */
    doc: Y.Doc;
}

export interface SquashResult {
    success: boolean;
    /** The new epoch when success is true */
    epoch?: number;
    /**
     * Why the squash was skipped (no error): another client holds the
     * lock, too much unfolded data, or the local doc is behind the server.
     */
    skippedReason?: 'lock-unavailable' | 'not-quiescent' | 'local-behind';
    error?: Error;
}

/**
 * Rebuilds the document into a new epoch on the server.
 *
 * Preconditions (validated here):
 *  - caller is fully synced (local state vector covers everything stored
 *    server-side, checked against the snapshot state vector and pending
 *    update/segment metadata);
 *  - pending updates + history fit one Firestore transaction — run a
 *    normal compaction first to fold the backlog.
 *
 * The new snapshot is uploaded to Cloud Storage; the transaction bumps
 * `epoch` and `version`, resets the delete-set fingerprint (a squashed
 * document has no deletions), and deletes the old-epoch update/history
 * documents it verified.
 */
export async function squashDocument(ctx: SquashContext): Promise<SquashResult> {
    const { db, path, uid, lockTTL, cachedClockOffset, storage, isDestroyed, doc: ydoc } = ctx;

    const hasLock = await acquireLock({ db, path, uid, lockTTL, cachedClockOffset });
    if (!hasLock) {
        return { success: false, skippedReason: 'lock-unavailable' };
    }

    try {
        // Snapshot of what exists server-side right now
        const updatesSnap = await getDocs(query(
            collection(db, path, FIRESTORE_PATHS.UPDATES),
            orderBy('createdAt', 'asc'),
            limit(DEFAULTS.MAX_COMPACTION_UPDATES + 1)
        ));
        const historySnap = await getDocs(query(
            collection(db, path, FIRESTORE_PATHS.HISTORY),
            orderBy('startTime', 'asc'),
            limit(DEFAULTS.MAX_COMPACTION_HISTORY + 1)
        ));
        if (isDestroyed()) return { success: false, error: new Error('destroyed') };

        if (
            updatesSnap.docs.length > DEFAULTS.MAX_COMPACTION_UPDATES ||
            historySnap.docs.length > DEFAULTS.MAX_COMPACTION_HISTORY
        ) {
            return { success: false, skippedReason: 'not-quiescent' };
        }

        const mainRef = doc(db, path);
        const mainSnap = await getDoc(mainRef);
        const mainData = mainSnap.exists() ? mainSnap.data() : undefined;
        const currentVersion = typeof mainData?.version === 'number' ? mainData.version : 0;
        const currentEpoch = typeof mainData?.epoch === 'number' ? mainData.epoch : 0;

        // The squasher must hold everything the server holds — otherwise
        // the squashed doc would silently drop other clients' data.
        const localSV = Y.decodeStateVector(Y.encodeStateVector(ydoc));
        const coveredBy = (svB64: string | undefined): boolean => {
            if (!svB64) return true;
            try {
                const remote = Y.decodeStateVector(fromBase64(svB64));
                for (const [client, clock] of remote) {
                    if ((localSV.get(client) || 0) < clock) return false;
                }
                return true;
            } catch {
                return false;
            }
        };
        if (!coveredBy(mainData?.stateVector)) {
            return { success: false, skippedReason: 'local-behind' };
        }
        for (const snap of [...updatesSnap.docs, ...historySnap.docs]) {
            const data = snap.data();
            const ids: number[] = data.clientIDs || [];
            const clocks: number[] = data.clientClocks || [];
            if (ids.length === clocks.length && ids.length > 0) {
                for (let i = 0; i < ids.length; i++) {
                    if ((localSV.get(ids[i]) || 0) < clocks[i]) {
                        return { success: false, skippedReason: 'local-behind' };
                    }
                }
            } else if (data.stateVector) {
                if (!coveredBy(data.stateVector)) {
                    return { success: false, skippedReason: 'local-behind' };
                }
            } else if (data.update || data.segment || data.updateStoragePath) {
                // No metadata to verify against — be conservative
                return { success: false, skippedReason: 'local-behind' };
            }
        }

        // Build the new-epoch document
        const newEpoch = currentEpoch + 1;
        const squashed = buildSquashedDoc(ydoc, newEpoch);
        let candidate: Uint8Array;
        let stateVectorB64: string;
        let dsUpdate: Uint8Array;
        try {
            candidate = Y.encodeStateAsUpdate(squashed);
            const sv = Y.encodeStateVector(squashed);
            stateVectorB64 = toBase64(sv);
            dsUpdate = Y.encodeStateAsUpdate(squashed, sv); // structs-empty, empty DS
        } finally {
            squashed.destroy();
        }

        const nextVersion = currentVersion + 1;
        const storagePath = `${path}/snapshot_e${newEpoch}_v${nextVersion}.bin`;
        await uploadBytes(ref(storage, storagePath), candidate);

        const result = await runTransaction(db, async (transaction) => {
            const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);
            const lockSnap = await transaction.get(lockRef);
            if (!lockSnap.exists() || lockSnap.data().owner !== uid) {
                throw new Error("Lock lost or expired during squash - aborting.");
            }

            const mainCheck = await transaction.get(mainRef);
            const checkData = mainCheck.exists() ? mainCheck.data() : undefined;
            const versionNow = typeof checkData?.version === 'number' ? checkData.version : 0;
            const epochNow = typeof checkData?.epoch === 'number' ? checkData.epoch : 0;
            if (versionNow !== currentVersion || epochNow !== currentEpoch) {
                throw new Error("Document changed during squash upload. Aborting.");
            }

            const [updateChecks, historyChecks] = await Promise.all([
                Promise.all(updatesSnap.docs.map(d => transaction.get(d.ref))),
                Promise.all(historySnap.docs.map(d => transaction.get(d.ref))),
            ]);

            transaction.set(mainRef, {
                snapshotStoragePath: storagePath,
                content: deleteField(),
                stateVector: stateVectorB64,
                // A squashed document has no deletions yet
                deleteSet: Bytes.fromUint8Array(dsUpdate),
                deleteSetStoragePath: deleteField(),
                version: nextVersion,
                epoch: newEpoch,
                updatedAt: serverTimestamp(),
                origin: uid,
            }, { merge: true });

            for (const s of [...updateChecks, ...historyChecks]) {
                if (s.exists()) transaction.delete(s.ref);
            }

            return { success: true as const, epoch: newEpoch };
        });

        // Best-effort cleanup of the previous epoch's blobs
        if (result.success && typeof mainData?.snapshotStoragePath === 'string') {
            try {
                await deleteObject(ref(storage, mainData.snapshotStoragePath));
            } catch { /* orphaned blob is harmless */ }
        }
        if (result.success && typeof mainData?.deleteSetStoragePath === 'string') {
            try {
                await deleteObject(ref(storage, mainData.deleteSetStoragePath));
            } catch { /* orphaned blob is harmless */ }
        }

        return result;
    } catch (e: any) {
        return { success: false, error: e instanceof Error ? e : new Error(String(e)) };
    } finally {
        await releaseLock({ db, path, uid });
    }
}
