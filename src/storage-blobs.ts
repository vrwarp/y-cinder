/**
 * Storage Blobs Module
 *
 * Uploads the blobs y-cinder keeps in Cloud Storage: fold and squash
 * snapshots, fold tails, offloaded large updates and offloaded delete-set
 * fingerprints.
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

import { FirebaseStorage, ref, uploadBytes, UploadMetadata } from "@firebase/storage";
import { gzipBlobAsync } from "./merge-utils";

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
