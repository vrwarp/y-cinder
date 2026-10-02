/**
 * Cloud Storage uploads are gzip-compressed, with a raw fallback.
 *
 * uploadBlob stores a compressible blob as a gzip stream with
 * Content-Encoding: gzip (the HTTP stack inflates it on download, so
 * readers still receive the raw bytes), and uploads the raw blob without
 * Content-Encoding whenever compression is unavailable or does not pay
 * off. The emulator round trip is covered by
 * tests/integration/storage_blob_compression.test.ts.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as Y from 'yjs';
import { gunzipSync } from 'node:zlib';

const uploads = vi.hoisted(() => [] as Array<{ path: string; bytes: Uint8Array; metadata?: Record<string, unknown> }>);

vi.mock('@firebase/storage', () => ({
    ref: (_storage: unknown, path: string) => ({ fullPath: path }),
    uploadBytes: async (r: { fullPath: string }, bytes: Uint8Array, metadata?: Record<string, unknown>) => {
        uploads.push({ path: r.fullPath, bytes: bytes.slice(), metadata });
        return {};
    },
}));

import { uploadBlob } from '../../src/storage-blobs';
import { gzipBlob } from '../../src/gzip';

/** A map-heavy snapshot: repeated keys and client ids, like versicle's stores. */
function mapHeavySnapshot(): Uint8Array {
    const doc = new Y.Doc();
    for (let session = 0; session < 20; session++) {
        doc.clientID = 1000 + session;
        const progress = doc.getMap('progress');
        for (let i = 0; i < 50; i++) {
            progress.set(`book-${i}`, { cfi: `epubcfi(/6/${session * 2}!/4/2/${i})`, percentage: (session + i) / 100 });
        }
    }
    const snapshot = Y.encodeStateAsUpdate(doc);
    doc.destroy();
    return snapshot;
}

function randomBytes(n: number): Uint8Array {
    const bytes = new Uint8Array(n);
    let x = 0x2545f491;
    for (let i = 0; i < n; i++) {
        x ^= x << 13; x ^= x >>> 17; x ^= x << 5;
        bytes[i] = x & 0xff;
    }
    return bytes;
}

describe('gzipBlob', () => {
    it('returns a smaller gzip stream that inflates to the input', async () => {
        const raw = mapHeavySnapshot();
        const compressed = await gzipBlob(raw);
        expect(compressed.byteLength).toBeLessThan(raw.byteLength / 2);
        expect(new Uint8Array(gunzipSync(compressed))).toEqual(raw);
    });

    it('rejects a blob gzip does not shrink', async () => {
        await expect(gzipBlob(randomBytes(4096))).rejects.toThrow(/does not shrink/);
    });
});

describe('uploadBlob', () => {
    beforeEach(() => {
        uploads.length = 0;
        vi.spyOn(console, 'debug').mockImplementation(() => { });
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        vi.restoreAllMocks();
    });

    it('uploads a compressible blob gzipped, with Content-Encoding: gzip', async () => {
        const raw = mapHeavySnapshot();
        await uploadBlob({} as any, 'docs/a/snapshot_v1_x.bin', raw);

        expect(uploads).toHaveLength(1);
        expect(uploads[0].path).toBe('docs/a/snapshot_v1_x.bin');
        expect(uploads[0].metadata).toEqual({ contentType: 'application/octet-stream', contentEncoding: 'gzip' });
        expect(uploads[0].bytes.byteLength).toBeLessThan(raw.byteLength / 2);
        // What a reader receives once the HTTP stack has inflated it
        expect(new Uint8Array(gunzipSync(uploads[0].bytes))).toEqual(raw);
    });

    it('uploads the raw blob without metadata when gzip does not shrink it', async () => {
        const raw = randomBytes(4096);
        await uploadBlob({} as any, 'docs/a/large_updates/u.bin', raw);

        expect(uploads).toHaveLength(1);
        expect(uploads[0].metadata).toBeUndefined();
        expect(uploads[0].bytes).toEqual(raw);
    });

    it('uploads the raw blob without metadata when the Compression Streams API is unavailable', async () => {
        vi.stubGlobal('CompressionStream', undefined);
        const raw = mapHeavySnapshot();
        await uploadBlob({} as any, 'docs/a/snapshot_v1_x.bin', raw);

        expect(uploads).toHaveLength(1);
        expect(uploads[0].metadata).toBeUndefined();
        expect(uploads[0].bytes).toEqual(raw);
    });
});
