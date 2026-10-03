/**
 * Storage Blobs Module
 *
 * Uploads the blobs y-cinder keeps in Cloud Storage: fold and squash
 * snapshots, fold tails, offloaded large updates and offloaded delete-set
 * fingerprints. Also keeps an offloaded large update's blob readable
 * while its pointer is live: the writer's restoreMissingBlob and the
 * reclaimer's deleteUpdateBlobs.
 *
 * ## Compression
 *
 * Yjs V1 updates of map-heavy documents compress about 3x with gzip
 * (repeated key strings and client ids, varint clocks), and every snapshot
 * transfer (each cold start, each fold's download and upload) is billed
 * Storage egress and the user's bandwidth. Blobs are therefore uploaded
 * gzip-compressed and stored with `Content-Encoding: gzip`. Cloud Storage
 * serves such an object compressed to clients that accept gzip (browsers
 * and Node's fetch always do) and the HTTP stack inflates it, so `getBytes`
 * still returns the raw V1 bytes: readers need no change, clients that
 * predate compression read new blobs, and raw blobs written before it stay
 * readable. A client that does not accept gzip gets the object inflated by
 * Cloud Storage instead (decompressive transcoding).
 *
 * Readers must therefore never gunzip a downloaded blob themselves.
 *
 * @module storage-blobs
 */

import { DocumentReference, getDocFromServer } from "@firebase/firestore";
import { FirebaseStorage, deleteObject, getMetadata, ref, uploadBytes, UploadMetadata } from "@firebase/storage";
import { gzipBlobAsync } from "./merge-utils";
import { isPermanentDownloadError } from "./sync-policy";

/**
 * Metadata of a gzip-compressed blob. The content type describes the
 * inflated bytes, as Cloud Storage's transcoding expects.
 */
const GZIP_METADATA: UploadMetadata = {
    contentType: 'application/octet-stream',
    contentEncoding: 'gzip',
};

/**
 * Uploads a Yjs blob to Cloud Storage, gzip-compressed when possible.
 *
 * Compression runs in the merge Web Worker when available. When it is
 * unavailable or fails, the blob is uploaded raw (without
 * Content-Encoding) — compression never fails or blocks a save, fold or
 * squash.
 *
 * @param storage - Firebase Storage instance
 * @param path - The blob's path
 * @param blob - The raw Yjs V1 blob; callers derive all metadata from it
 */
export async function uploadBlob(storage: FirebaseStorage, path: string, blob: Uint8Array): Promise<void> {
    let compressed: Uint8Array | null = null;
    try {
        compressed = await gzipBlobAsync(blob);
    } catch (e) {
        console.debug(`Uploading ${path} uncompressed:`, e);
    }

    if (compressed) {
        await uploadBytes(ref(storage, path), compressed, GZIP_METADATA);
    } else {
        await uploadBytes(ref(storage, path), blob);
    }
}

/**
 * Uploads an offloaded update's blob again if it is gone once the
 * writer's pointer write has settled.
 *
 * The SDK re-sends a write whose ack was lost when its stream reconnects.
 * If a compaction consumed the pointer in between, it also deleted the
 * blob, and the re-send re-creates the pointer under the same id behind a
 * missing blob. Compaction re-reads each merged pointer after deleting
 * its blob (see reclaimUpdateBlobs) and the writer checks the blob after
 * its write, so whichever comes second sees the other and puts the blob
 * back.
 *
 * @param storage - Firebase Storage instance
 * @param path - The blob's path
 * @param blob - The raw Yjs V1 blob the pointer was written for
 * @throws When the blob's existence cannot be checked.
 */
export async function restoreMissingBlob(storage: FirebaseStorage, path: string, blob: Uint8Array): Promise<void> {
    try {
        await getMetadata(ref(storage, path));
    } catch (e) {
        if (!isPermanentDownloadError(e)) throw e;
        console.warn(`Update blob ${path} was reclaimed behind a re-sent pointer; uploading it again`);
        await uploadBlob(storage, path, blob);
    }
}

/**
 * A reclaimed update blob awaiting deletion (see reclaimUpdateBlobs in
 * compaction). `pointer` and `payload` are set for a pointer this client
 * merged, so that deleteUpdateBlobs can give the blob back to a pointer a
 * lost-ack re-send re-created.
 */
export interface UpdateBlobReclaim {
    /** The blob's storage path. */
    path: string;
    /** The merged pointer document the commit deleted. */
    pointer?: DocumentReference;
    /** The payload merged from the blob (raw Yjs V1). */
    payload?: Uint8Array;
}

/**
 * Deletes reclaimed update blobs whose pointers are gone, in parallel.
 *
 * A writer whose ack was lost re-sends its pointer write on reconnect,
 * which re-creates a deleted pointer under the same id, possibly while
 * its blob still awaits deletion here. So each merged pointer is re-read
 * after its blob is deleted, and the blob uploaded again from the merged
 * payload (kept in its entry until then) if the pointer is back; the
 * writer checks its blob after its write (restoreMissingBlob), so one of
 * the two always sees the other. A stale-epoch pointer is never merged,
 * so one re-created behind a missing blob is harmless: compaction deletes
 * it unread.
 *
 * Best effort, like snapshot garbage collection: a failure (including a
 * 404 from a double delete) only leaves an orphan, never fails the cycle.
 */
export async function deleteUpdateBlobs(storage: FirebaseStorage, blobs: UpdateBlobReclaim[]): Promise<void> {
    await Promise.all(blobs.map(async ({ path: blobPath, pointer, payload }) => {
        try {
            await deleteObject(ref(storage, blobPath));
        } catch (err) {
            console.warn(`Failed to delete update blob ${blobPath}`, err);
            return;
        }
        if (pointer === undefined || payload === undefined) return;
        try {
            if ((await getDocFromServer(pointer)).exists()) {
                console.warn(`Update pointer ${pointer.id} was re-sent after its blob was reclaimed; uploading it again`);
                await uploadBlob(storage, blobPath, payload);
            }
        } catch (err) {
            console.warn(`Failed to check update blob ${blobPath} for a re-sent pointer`, err);
        }
    }));
}
