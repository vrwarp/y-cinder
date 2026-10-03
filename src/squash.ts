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
import { ref, deleteObject, getBytes, FirebaseStorage } from "@firebase/storage";
import { toBase64, fromBase64 } from "lib0/buffer";
import * as Y from "yjs";
import { DEFAULTS, FIRESTORE_PATHS } from "./types";
import { acquireLock, releaseLock } from "./locking";
import { generateSessionId } from "./utils";
import { readMainDocState, updateBlobPath } from "./compaction-policy";
import { reclaimUpdateBlobs } from "./compaction";
import { uploadBlob, UpdateBlobReclaim } from "./storage-blobs";
import {
    isNotQuiescent,
    isSquashPreempted,
    localCoversDeletions,
    localCoversPendingDoc,
    readVersionEpoch,
    squashSnapshotPath,
    stateVectorCovers,
    stillHoldsLock,
} from './squash-policy';

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
 * `source` is only read: its subdocuments and embedded types are copied,
 * never moved, so destroying the result leaves `source` intact.
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
                    t.insert(0, type.toArray().map(cloneValue) as any[]);
                } else if (type instanceof Y.Text) {
                    const t = target.getText(name);
                    t.applyDelta(cloneDelta(type.toDelta()));
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

/**
 * Deep-clones a value for the squashed document.
 *
 * Nothing integrated in the live doc may reach the clone: Yjs re-parents
 * an integrated type or subdocument into whichever document it is
 * inserted into, detaching it from the live doc — and destroying the
 * clone then destroys a live subdocument. Yjs's own clone() passes
 * subdocuments, Y.Text embeds and Y.XmlHook values through as the live
 * instances, so the recursion is done here for every built-in type.
 */
function cloneValue(v: unknown): unknown {
    if (v instanceof Y.Doc) {
        // A subdocument is a guid reference; its content syncs on its own
        return new Y.Doc({ guid: v.guid, gc: v.gc, autoLoad: v.autoLoad, meta: v.meta, shouldLoad: false });
    }
    if (v instanceof Y.Map) {
        const m: Y.Map<unknown> = v instanceof Y.XmlHook ? new Y.XmlHook(v.hookName) : new Y.Map();
        v.forEach((x: unknown, k: string) => m.set(k, cloneValue(x)));
        return m;
    }
    if (v instanceof Y.Array) {
        const a = new Y.Array();
        a.insert(0, v.toArray().map(cloneValue));
        return a;
    }
    if (v instanceof Y.Text) {
        const t = v instanceof Y.XmlText ? new Y.XmlText() : new Y.Text();
        t.applyDelta(cloneDelta(v.toDelta()));
        return t;
    }
    if (v instanceof Y.XmlElement) {
        const el = new Y.XmlElement(v.nodeName);
        Object.entries(v.getAttributes()).forEach(([k, x]) => el.setAttribute(k, cloneValue(x) as any));
        el.insert(0, v.toArray().map(cloneValue) as any[]);
        return el;
    }
    if (v instanceof Y.XmlFragment) {
        const f = new Y.XmlFragment();
        f.insert(0, v.toArray().map(cloneValue) as any[]);
        return f;
    }
    return v instanceof Y.AbstractType ? (v as any).clone() : v;
}

/** Clones the embedded values of a Y.Text delta (strings stay as-is). */
function cloneDelta(delta: any[]): any[] {
    return delta.map((op) => ({ ...op, insert: cloneValue(op.insert) }));
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
    /** See CompactionContext.deferredUpdateBlobs */
    deferredUpdateBlobs?: UpdateBlobReclaim[];
}

export interface SquashResult {
    success: boolean;
    /** The new epoch when success is true */
    epoch?: number;
    /**
     * Why the squash was skipped (no error): another client holds the
     * lock, too much unfolded data, the local doc is behind the server,
     * or the local doc changed while the squash was uploading/committing.
     */
    skippedReason?: 'lock-unavailable' | 'not-quiescent' | 'local-behind' | 'local-changed';
    error?: Error;
}

/**
 * Rebuilds the document into a new epoch on the server.
 *
 * Preconditions (validated here):
 *  - caller is fully synced (local state vector covers everything stored
 *    server-side, checked against the snapshot state vector and pending
 *    update/segment metadata, and every server-side deletion of a struct
 *    it holds is applied locally, checked against the snapshot's
 *    delete-set fingerprint and the pending updates/segments);
 *  - pending updates + history fit one Firestore transaction — run a
 *    normal compaction first to fold the backlog.
 *
 * The new snapshot is uploaded to Cloud Storage; the transaction bumps
 * `epoch` and `version`, resets the delete-set fingerprint (a squashed
 * document has no deletions), and deletes the old-epoch update/history
 * documents it verified. It commits nothing when the live doc changed
 * after it was cloned (skippedReason 'local-changed'): the snapshot would
 * lack that change, and the squashing client stops syncing on success.
 */
export async function squashDocument(ctx: SquashContext): Promise<SquashResult> {
    const { db, path, uid, lockTTL, cachedClockOffset, storage, isDestroyed, doc: ydoc } = ctx;

    const hasLock = await acquireLock({ db, path, uid, lockTTL, cachedClockOffset });
    if (!hasLock) {
        return { success: false, skippedReason: 'lock-unavailable' };
    }

    // Set by any change to the live doc after the clone below (e.g. the
    // user editing while the snapshot uploads).
    let changedSinceClone = false;
    const onLiveUpdate = () => { changedSinceClone = true; };

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

        if (isNotQuiescent({
            updateCount: updatesSnap.docs.length,
            historyCount: historySnap.docs.length,
            maxUpdates: DEFAULTS.MAX_COMPACTION_UPDATES,
            maxHistory: DEFAULTS.MAX_COMPACTION_HISTORY,
        })) {
            return { success: false, skippedReason: 'not-quiescent' };
        }

        const mainRef = doc(db, path);
        const mainSnap = await getDoc(mainRef);
        const mainData = mainSnap.exists() ? mainSnap.data() : undefined;
        const { version: currentVersion, epoch: currentEpoch } = readVersionEpoch(mainData);

        // The squasher must hold everything the server holds — otherwise
        // the squashed doc would silently drop other clients' data.
        const localSV = Y.decodeStateVector(Y.encodeStateVector(ydoc));

        if (!stateVectorCovers(localSV, mainData?.stateVector)) {
            return { success: false, skippedReason: 'local-behind' };
        }
        const pendingDocs = [...updatesSnap.docs, ...historySnap.docs].map(snap => snap.data());
        for (const data of pendingDocs) {
            if (!localCoversPendingDoc(localSV, data)) {
                return { success: false, skippedReason: 'local-behind' };
            }
        }

        // State vectors do not move on deletion, so the squasher must also
        // have applied every deletion the server holds: the snapshot's
        // delete-set fingerprint (or, predating fingerprints, the snapshot
        // itself) and each pending update/segment.
        const deletionBlobs = await Promise.all([
            mainData?.deleteSet || mainData?.deleteSetStoragePath
                ? readBlob(storage, mainData.deleteSet, mainData.deleteSetStoragePath)
                : readBlob(storage, mainData?.content, mainData?.snapshotStoragePath),
            ...pendingDocs.map(data => readBlob(storage, data.update ?? data.segment, data.updateStoragePath)),
        ]);
        for (const blob of deletionBlobs) {
            if (blob && !localCoversDeletions(ydoc, blob)) {
                return { success: false, skippedReason: 'local-behind' };
            }
        }

        // Build the new-epoch document
        const newEpoch = currentEpoch + 1;
        const squashed = buildSquashedDoc(ydoc, newEpoch);
        ydoc.on('update', onLiveUpdate);
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
        const storagePath = squashSnapshotPath(path, newEpoch, nextVersion, generateSessionId());
        await uploadBlob(storage, storagePath, candidate);

        // Blobs of the pointer documents the committed attempt deleted
        // (reset per attempt: the body re-runs on contention).
        let deletedBlobs: string[] = [];
        const result = await runTransaction(db, async (transaction) => {
            deletedBlobs = [];
            const lockRef = doc(db, path, FIRESTORE_PATHS.LOCK_COMPACTION);
            const lockSnap = await transaction.get(lockRef);
            if (!stillHoldsLock(lockSnap.exists() ? lockSnap.data() : undefined, uid)) {
                throw new Error("Lock lost or expired during squash - aborting.");
            }

            const mainCheck = await transaction.get(mainRef);
            const checkData = mainCheck.exists() ? mainCheck.data() : undefined;
            if (isSquashPreempted(readVersionEpoch(checkData), { version: currentVersion, epoch: currentEpoch })) {
                throw new Error("Document changed during squash upload. Aborting.");
            }

            // Checked last, right before the writes: a change the clone
            // lacks would be silently dropped from the new epoch. Commit
            // nothing and let the provider keep syncing it in this epoch.
            if (changedSinceClone) {
                return { success: false as const, skippedReason: 'local-changed' as const };
            }

            transaction.set(mainRef, {
                snapshotStoragePath: storagePath,
                content: deleteField(),
                stateVector: stateVectorB64,
                // A squashed document has no deletions yet
                deleteSet: Bytes.fromUint8Array(dsUpdate),
                deleteSetStoragePath: deleteField(),
                // Recorded for the same reason as a fold's (see
                // MainDocState.orphanedBlobPaths)
                snapshotBlobPaths: [storagePath],
                // A fold's tail belongs to the old epoch's id space
                foldTailStoragePath: deleteField(),
                foldTailBaseClocks: deleteField(),
                foldTailVersion: deleteField(),
                version: nextVersion,
                epoch: newEpoch,
                updatedAt: serverTimestamp(),
                origin: uid,
            }, { merge: true });

            // Deleted without re-reading: the squashed doc covers every one
            // of them (checked above), and only lock holders delete
            // update/history documents (see "Deletion" in compaction.ts).
            for (const d of [...updatesSnap.docs, ...historySnap.docs]) {
                transaction.delete(d.ref);
            }
            deletedBlobs = updatesSnap.docs
                .map(d => updateBlobPath(d.data()))
                .filter((p): p is string => p !== null);

            return { success: true as const, epoch: newEpoch };
        });

        // Nothing references a deleted pointer's blob any more (see
        // updateBlobPath), but a peer may still be downloading one: it is
        // reclaimed later, like compaction's (see reclaimUpdateBlobs). A
        // re-sent pointer write may re-create a pointer, but tagged with
        // the old epoch: listeners drop it, initial sync skips a missing
        // blob, and compaction deletes it unread.
        if (result.success) {
            await reclaimUpdateBlobs(ctx, deletedBlobs.map(blobPath => ({ path: blobPath })));
        }

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
        if (result.success && typeof mainData?.foldTailStoragePath === 'string') {
            try {
                await deleteObject(ref(storage, mainData.foldTailStoragePath));
            } catch { /* orphaned blob is harmless */ }
        }
        // ...and those an older client's fold replaced without deleting
        // them: this commit replaced the record of them
        if (result.success) {
            for (const blobPath of readMainDocState(mainData).orphanedBlobPaths) {
                try {
                    await deleteObject(ref(storage, blobPath));
                } catch { /* orphaned blob is harmless */ }
            }
        }

        return result;
    } catch (e: any) {
        return { success: false, error: e instanceof Error ? e : new Error(String(e)) };
    } finally {
        ydoc.off('update', onLiveUpdate);
        await releaseLock({ db, path, uid });
    }
}

/**
 * Reads a blob a document stores either inline (Firestore Bytes) or in
 * Cloud Storage.
 *
 * @returns The bytes, or null when the document carries neither.
 */
async function readBlob(storage: FirebaseStorage, inline: unknown, storagePath: unknown): Promise<Uint8Array | null> {
    if (inline) {
        return (inline as Bytes).toUint8Array();
    }
    if (typeof storagePath === 'string') {
        return new Uint8Array(await getBytes(ref(storage, storagePath)));
    }
    return null;
}
