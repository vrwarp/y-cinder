/**
 * Regression: a save batch the server rejects while a (re)sync runs must
 * stay queued and reach the server; the sync must not retire it.
 *
 * Scenario: save S1 has taken batch B1 (edit E1) and its addDoc is
 * unacknowledged when a sync attempt starts — the retry of a failed
 * initial sync, or the re-sync after a listener error. Firestore's latency
 * compensation makes S1's pending document visible to that attempt's
 * getDocs(updates), so the server coverage the push is diffed against
 * counts B1's clocks and the push leaves B1 out. The server then
 * permanently rejects S1 (e.g. permission-denied from security rules that
 * depend on changing state, or App Check) and the save's catch puts
 * B1 back at the front of the buffer. When the sync succeeds,
 * _retireSyncedUpdates splices the captured count of entries from the
 * front of that buffer — B1 included, whether it was put back before the
 * capture (counted) or after it (sitting at index 0) — and emits 'saved'.
 * B1 is never sent again, and every later edit of this client starts past
 * its clock range, so no peer can integrate them.
 *
 * Contract asserted (valid for any reasonable fix, in provider.ts or
 * sync.ts): once the save retries have had time to run, the committed
 * server documents rebuild the full local document, with no struct left
 * waiting on a missing clock range.
 *
 * Firestore is faked at the SDK boundary with the one behavior the bug
 * depends on: an unacknowledged write is visible to getDocs as a pending
 * document (hasPendingWrites) until the server accepts or rejects it. The
 * REAL provider and performInitialSync run against it; time is controlled
 * with fake timers and the rejection is triggered from explicit hooks.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as Y from 'yjs';

// ---------------------------------------------------------------------------
// Firestore / Storage fake (SDK boundary only — Bytes stays real)
// ---------------------------------------------------------------------------

type Row = { id: string; data: Record<string, any> };
type HeldWrite = Row & { reject: (err: unknown) => void };

const fake = vi.hoisted(() => ({
    /** Update documents the server has committed, in commit order. */
    committed: [] as Row[],
    /** Writes sent but not yet acknowledged (visible locally as pending). */
    held: [] as HeldWrite[],
    /** Number of addDoc calls so far. */
    writes: 0,
    /** Number of getDoc(main) calls so far. */
    mainReads: 0,
    /** Hold (leave unacknowledged) the write with this 1-based index. */
    holdWrite: 0,
    /** Fail this many getDocs calls (from the first one on). */
    failReads: 0,
    /** Runs when the write with this index is sent, before it commits. */
    beforeWrite: new Map<number, () => Promise<void>>(),
    /** Runs when the main document read with this index is served. */
    beforeMainRead: new Map<number, () => Promise<void>>(),
    /** Error callbacks of the live onSnapshot listeners. */
    listenerErrors: new Set<(err: unknown) => void>(),
}));

function permissionDenied(): Error {
    return Object.assign(new Error('Missing or insufficient permissions.'), { code: 'permission-denied' });
}

vi.mock('@firebase/firestore', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/firestore')>();
    const join = (parts: unknown[]) => parts.filter(p => typeof p === 'string').join('/');
    const asQuerySnapshot = (rows: { id: string; data: Record<string, any>; pending: boolean }[]) => {
        const docs = rows.map(row => ({
            id: row.id,
            ref: { path: row.id },
            metadata: { hasPendingWrites: row.pending, fromCache: false },
            // A pending serverTimestamp() reads as null, like the SDK's default
            data: () => ({ ...row.data, createdAt: row.pending ? null : row.data.createdAt }),
        }));
        return {
            docs,
            empty: docs.length === 0,
            size: docs.length,
            metadata: { fromCache: false, hasPendingWrites: rows.some(r => r.pending) },
            forEach: (fn: (d: unknown) => void) => docs.forEach(fn),
        };
    };
    return {
        ...actual,
        getFirestore: vi.fn(() => ({ __fake: 'firestore' })),
        initializeFirestore: vi.fn(() => ({ __fake: 'firestore' })),
        collection: (_db: unknown, ...parts: unknown[]) => ({ kind: 'collection', path: join(parts) }),
        doc: (_db: unknown, ...parts: unknown[]) => ({ kind: 'doc', path: join(parts) }),
        query: (ref: any, ...constraints: any[]) => ({ ...ref, constraints }),
        orderBy: (field: string) => ({ orderBy: field }),
        startAfter: (cursor: any) => ({ startAfter: cursor }),
        limit: (n: number) => ({ limit: n }),
        serverTimestamp: () => ({ serverTimestamp: true }),
        getDocs: async (q: any) => {
            if (fake.failReads > 0) {
                fake.failReads--;
                throw permissionDenied();
            }
            if (!q.path.endsWith('/updates')) return asQuerySnapshot([]);
            // Latency compensation: committed documents plus this client's
            // unacknowledged writes, which sort last (local estimate).
            const rows = [
                ...fake.committed.map(r => ({ ...r, pending: false })),
                ...fake.held.map(r => ({ id: r.id, data: r.data, pending: true })),
            ];
            const after = q.constraints?.find((c: any) => c.startAfter)?.startAfter;
            const start = after ? rows.findIndex(r => r.id === after.id) + 1 : 0;
            const max = q.constraints?.find((c: any) => c.limit)?.limit ?? rows.length;
            return asQuerySnapshot(rows.slice(start, start + max));
        },
        getDoc: async () => {
            const index = ++fake.mainReads;
            const hook = fake.beforeMainRead.get(index);
            if (hook) await hook();
            return {
                exists: () => false,
                data: () => undefined,
                metadata: { fromCache: false, hasPendingWrites: false },
            };
        },
        addDoc: async (_ref: unknown, data: Record<string, any>) => {
            const index = ++fake.writes;
            const id = `update-${index}`;
            const hook = fake.beforeWrite.get(index);
            if (hook) await hook();
            if (index === fake.holdWrite) {
                return new Promise((_resolve, reject) => {
                    fake.held.push({ id, data, reject });
                });
            }
            fake.committed.push({ id, data: { ...data, createdAt: { seconds: index } } });
            return { id };
        },
        onSnapshot: (_ref: unknown, _next: unknown, error?: (err: unknown) => void) => {
            if (error) fake.listenerErrors.add(error);
            return () => {
                if (error) fake.listenerErrors.delete(error);
            };
        },
    };
});

vi.mock('@firebase/storage', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@firebase/storage')>();
    return {
        ...actual,
        getStorage: vi.fn(() => ({ __fake: 'storage' })),
        ref: (_storage: unknown, path: string) => ({ fullPath: path }),
        getBytes: async () => { throw new Error('no Storage objects in this test'); },
        uploadBytes: async () => undefined,
        deleteObject: async () => undefined,
    };
});

import { FireProvider } from '../../src/provider';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Lets promise continuations (the save's catch block) run. */
async function flushMicrotasks(): Promise<void> {
    for (let i = 0; i < 50; i++) await Promise.resolve();
}

/** The server permanently rejects the held save. */
async function rejectHeldSave(): Promise<void> {
    for (const write of fake.held.splice(0)) {
        write.reject(permissionDenied());
    }
    await flushMicrotasks();
}

/** Rebuilds a document from every committed update document. */
function serverState(): Y.Doc {
    const doc = new Y.Doc();
    for (const { data } of fake.committed) {
        Y.applyUpdate(doc, data.update.toUint8Array());
    }
    return doc;
}

type Trigger = 'retry of a failed initial sync' | 'listener-error re-sync';
type RejectedAt = 'while the sync reads the main document (before its capture)'
    | 'while the sync push is in flight (after its capture)';

describe('a save rejected during a (re)sync is not retired by it', () => {
    const created: FireProvider[] = [];

    beforeEach(() => {
        vi.useFakeTimers();
        fake.committed.length = 0;
        fake.held.length = 0;
        fake.writes = 0;
        fake.mainReads = 0;
        fake.holdWrite = 0;
        fake.failReads = 0;
        fake.beforeWrite.clear();
        fake.beforeMainRead.clear();
        fake.listenerErrors.clear();
        vi.spyOn(console, 'log').mockImplementation(() => {});
        vi.spyOn(console, 'debug').mockImplementation(() => {});
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(console, 'warn').mockImplementation(() => {});
    });

    afterEach(async () => {
        await rejectHeldSave();
        for (const p of created.splice(0)) {
            const destroyed = p.destroy();
            await vi.runOnlyPendingTimersAsync();
            await destroyed;
        }
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    it.each<[Trigger, RejectedAt]>([
        ['retry of a failed initial sync', 'while the sync reads the main document (before its capture)'],
        ['retry of a failed initial sync', 'while the sync push is in flight (after its capture)'],
        ['listener-error re-sync', 'while the sync reads the main document (before its capture)'],
        ['listener-error re-sync', 'while the sync push is in flight (after its capture)'],
    ])('%s: a save rejected %s still reaches the server', async (trigger, rejectedAt) => {
        const ydoc = new Y.Doc();
        const text = ydoc.getText('t');

        // The first save (S1, edit E1) stays unacknowledged until the
        // server rejects it from inside the sync attempt that follows.
        fake.holdWrite = 1;
        if (trigger === 'retry of a failed initial sync') {
            // The first initial sync's read is denied
            fake.failReads = 1;
        }
        // The attempt that runs while S1 is pending: initial sync's second
        // attempt (its first one failed before reading the main document),
        // or the first re-sync after the listener error.
        const resyncMainRead = trigger === 'retry of a failed initial sync' ? 1 : 2;
        if (rejectedAt.startsWith('while the sync reads')) {
            fake.beforeMainRead.set(resyncMainRead, rejectHeldSave);
        } else {
            // Write 2 is the sync push of what the server lacks
            fake.beforeWrite.set(2, rejectHeldSave);
        }

        const provider = new FireProvider({
            firebaseApp: {} as any,
            ydoc,
            path: 'docs/retire-rejected-save',
            maxWaitTime: 50,
            cachedClockOffset: 0,
        });
        created.push(provider);
        await vi.advanceTimersByTimeAsync(0);

        if (trigger === 'listener-error re-sync') {
            expect(provider.synced).toBe(true);
        } else {
            expect(provider.synced).toBe(false);
            expect(fake.mainReads).toBe(0);
        }

        // E1: its debounced save S1 goes out and stays pending
        text.insert(0, 'first ');
        await vi.advanceTimersByTimeAsync(50);
        expect(fake.writes).toBe(1);
        expect(fake.held).toHaveLength(1);

        // E2, typed while S1 is in flight
        text.insert(text.length, 'second');

        if (trigger === 'listener-error re-sync') {
            // A listener error follows; a re-sync is scheduled
            expect(fake.listenerErrors.size).toBeGreaterThan(0);
            const [listenerError] = fake.listenerErrors;
            listenerError(permissionDenied());
            expect(provider.synced).toBe(false);
        }

        // The (re)sync attempt runs while S1 is pending; the server rejects
        // S1 at the hook point inside it. Should a fix keep the attempt from
        // reaching that point while a save is in flight, the server still
        // rejects S1 here.
        await vi.advanceTimersByTimeAsync(5_000);
        await rejectHeldSave();
        await vi.advanceTimersByTimeAsync(5_000);
        expect(provider.synced).toBe(true);

        // Give the save retries ample time (later writes are accepted)
        await vi.advanceTimersByTimeAsync(60_000);

        expect(text.toString()).toBe('first second');
        const server = serverState();
        // The server holds every local edit, E1 included...
        expect(server.getText('t').toString()).toBe('first second');
        // ...and no struct of this client is stuck behind a missing range
        expect((server as any).store.pendingStructs).toBeNull();
    });

    // A deletion-only batch has no clocks to count, but S1's pending blob
    // must not prove its deletion to the push either: neither to the
    // guard (E2 another deletion) nor to the trimming of the pushed diff
    // (E2 an insertion).
    it.each<[RejectedAt, 'an insertion' | 'another deletion']>([
        ['while the sync reads the main document (before its capture)', 'an insertion'],
        ['while the sync push is in flight (after its capture)', 'an insertion'],
        ['while the sync reads the main document (before its capture)', 'another deletion'],
        ['while the sync push is in flight (after its capture)', 'another deletion'],
    ])('listener-error re-sync: a deletion-only save rejected %s, followed by %s, still reaches the server', async (rejectedAt, followup) => {
        const ydoc = new Y.Doc();
        const text = ydoc.getText('t');

        const provider = new FireProvider({
            firebaseApp: {} as any,
            ydoc,
            path: 'docs/retire-rejected-save',
            maxWaitTime: 50,
            cachedClockOffset: 0,
        });
        created.push(provider);
        await vi.advanceTimersByTimeAsync(0);
        expect(provider.synced).toBe(true);

        // Committed content to delete from (write 1)
        text.insert(0, 'first second');
        await vi.advanceTimersByTimeAsync(50);
        expect(fake.committed).toHaveLength(1);

        // S1 (write 2) carries only E1's deletion and stays pending
        fake.holdWrite = 2;
        if (rejectedAt.startsWith('while the sync reads')) {
            // Main document read 1 was the initial sync's
            fake.beforeMainRead.set(2, rejectHeldSave);
        } else {
            // Write 3 is the re-sync's push of what the server lacks
            fake.beforeWrite.set(3, rejectHeldSave);
        }
        text.delete(0, 'first '.length);
        await vi.advanceTimersByTimeAsync(50);
        expect(fake.writes).toBe(2);
        expect(fake.held).toHaveLength(1);

        // E2, made while S1 is in flight
        if (followup === 'an insertion') {
            text.insert(text.length, '!');
        } else {
            text.delete(text.length - 1, 1);
        }
        const expected = followup === 'an insertion' ? 'second!' : 'secon';

        const [listenerError] = fake.listenerErrors;
        listenerError(permissionDenied());
        expect(provider.synced).toBe(false);

        await vi.advanceTimersByTimeAsync(5_000);
        await rejectHeldSave();
        await vi.advanceTimersByTimeAsync(5_000);
        expect(provider.synced).toBe(true);

        // Give the save retries ample time (later writes are accepted)
        await vi.advanceTimersByTimeAsync(60_000);

        expect(text.toString()).toBe(expected);
        // The server holds every local edit, E1's deletion included
        expect(serverState().getText('t').toString()).toBe(expected);
    });
});
