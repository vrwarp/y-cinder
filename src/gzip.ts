/**
 * Gzip Module
 *
 * gzip for the blobs y-cinder uploads to Cloud Storage (see
 * storage-blobs). Runs inside the merge Web Worker when available and on
 * the main thread otherwise; this module is bundled into the worker blob
 * by scripts/bundle-worker.js, so it must stay dependency-free.
 *
 * Uses the web Compression Streams API (browsers, Web Workers, Node 18+).
 *
 * @module gzip
 */

/**
 * gzip-compresses a blob for a Cloud Storage upload.
 *
 * The result is verified before it is returned: it must be smaller than
 * the input and inflate back to it byte for byte. Readers get the inflated
 * bytes from the HTTP stack and apply them as a Yjs update, so a stream
 * that does not round-trip would leave a committed snapshot unreadable.
 *
 * @param blob - The raw Yjs V1 blob
 * @returns The gzip stream
 * @throws When the blob must be uploaded raw instead: the Compression
 *         Streams API is unavailable (Safari < 16.4, older WebViews),
 *         compression failed, or its result is not smaller or does not
 *         round-trip
 */
export async function gzipBlob(blob: Uint8Array): Promise<Uint8Array> {
    if (typeof CompressionStream === 'undefined' || typeof DecompressionStream === 'undefined') {
        throw new Error('Compression Streams API unavailable');
    }
    const compressed = await pipe(blob, new CompressionStream('gzip'));
    if (compressed.byteLength >= blob.byteLength) {
        throw new Error(`gzip does not shrink this blob (${blob.byteLength} -> ${compressed.byteLength} bytes)`);
    }
    if (!bytesEqual(await pipe(compressed, new DecompressionStream('gzip')), blob)) {
        throw new Error('gzip round trip does not reproduce the blob');
    }
    return compressed;
}

/** Runs `bytes` through a compression or decompression stream. */
async function pipe(bytes: Uint8Array, transform: CompressionStream | DecompressionStream): Promise<Uint8Array> {
    const stream = new Blob([bytes]).stream().pipeThrough(transform);
    return new Uint8Array(await new Response(stream).arrayBuffer());
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.byteLength !== b.byteLength) return false;
    for (let i = 0; i < a.byteLength; i++) {
        if (a[i] !== b[i]) return false;
    }
    return true;
}
