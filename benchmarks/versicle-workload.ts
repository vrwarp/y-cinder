/**
 * Versicle-shaped aging workload
 *
 * Simulates the document shape and write cadence of versicle (the primary
 * consumer of y-cinder + y-idb): ONE long-lived Y.Doc holding ten top-level
 * Y.Maps, written by an ebook reader across many sessions and devices.
 *
 * Faithfulness notes (derived from versicle's stores as of schema v9):
 *
 * - One Y.Doc for the whole app; a NEW random clientID per session (versicle
 *   constructs a fresh Y.Doc on every page load and never pins clientID).
 *   The simulator gives every session a fresh deterministic clientID, which
 *   reproduces the unbounded state-vector growth of real long-term use.
 * - `progress` is the hot map: every page turn / TTS sentence rewrites keys
 *   of progress[bookId][deviceId] (currentCfi, percentage, lastRead, ...)
 *   and appends to a readingSessions Y.Array that saw-tooths 500 -> 300.
 * - `library.books` holds one nested Y.Map per book (~12 fields, tags
 *   Y.Array, coverPalette Y.Array, perceptualPalette Y.Map).
 * - Deleting a book removes whole nested trees (library entry + its
 *   contentAnalysis sections) — the churn pattern that produces GC ranges.
 * - `devices` gets a heartbeat overwrite of the same key every ~5 minutes.
 * - `searchHistory` arrays are rebuilt wholesale on every query (versicle
 *   runs that store without scoped diffing).
 * - Strings are plain values (versicle sets disableYText: true) — no Y.Text.
 *
 * Each high-level event becomes one persisted update blob, mirroring the
 * provider's debounced save (real page turns / TTS sentences are further
 * apart than the 2 s debounce window, so 1 event ~ 1 save).
 *
 * The same model is mirrored in y-idb's `benchmarks/versicle-workload.mjs`
 * (plain-JS port) so both libraries measure the same aging profile — keep
 * the two in sync when changing the model.
 */
import * as Y from 'yjs';
import { SeededRandom } from '../tests/unit/prng';

export interface VersicleSimOptions {
    seed: number;
    /** Books in the library at t=0 */
    initialBooks: number;
    /** Library size ceiling (imports stop, removals continue to churn) */
    maxBooks: number;
    /** High-level events per session (page turns, TTS sentences, ...) */
    eventsPerSession: number;
    /** Number of simulated devices (sessions round-robin across them) */
    devices: number;
}

export const DEFAULT_SIM: Omit<VersicleSimOptions, 'seed'> = {
    initialBooks: 20,
    maxBooks: 60,
    eventsPerSession: 60,
    devices: 2,
};

/** Mutable simulator state that survives across sessions. */
export interface VersicleSimState {
    rng: SeededRandom;
    opts: VersicleSimOptions;
    sessionCount: number;
    bookIds: string[];
    nextBookNum: number;
    totalEvents: number;
    /** Cumulative bytes of update blobs produced (provider write volume) */
    bytesProduced: number;
}

export function createSim(opts: Partial<VersicleSimOptions> & { seed: number }): VersicleSimState {
    const full: VersicleSimOptions = { ...DEFAULT_SIM, ...opts };
    return {
        rng: new SeededRandom(full.seed),
        opts: full,
        sessionCount: 0,
        bookIds: [],
        nextBookNum: 0,
        totalEvents: 0,
        bytesProduced: 0,
    };
}

function cfi(rng: SeededRandom): string {
    return `epubcfi(/6/${rng.int(2, 40)}!/4/${rng.int(2, 200)}/${rng.int(1, 30)}:${rng.int(0, 900)})`;
}

function nowFor(state: VersicleSimState): number {
    // Deterministic pseudo-time: one session ~ one half-day
    return 1_700_000_000_000 + state.sessionCount * 43_200_000 + state.totalEvents * 5_000;
}

function importBook(state: VersicleSimState, doc: Y.Doc): void {
    const { rng } = state;
    const bookId = `book-${state.nextBookNum++}`;
    state.bookIds.push(bookId);
    const books = (doc.getMap('library').get('books') as Y.Map<unknown>);
    const item = new Y.Map<unknown>();
    books.set(bookId, item);
    item.set('bookId', bookId);
    item.set('title', 'Title ' + rng.string(rng.int(8, 30)));
    item.set('author', 'Author ' + rng.string(rng.int(6, 20)));
    item.set('addedAt', nowFor(state));
    item.set('lastInteraction', nowFor(state));
    item.set('sourceFilename', bookId + '.epub');
    item.set('status', 'unread');
    item.set('language', rng.bool(0.3) ? 'zh' : 'en');
    const tags = new Y.Array<string>();
    item.set('tags', tags);
    tags.push([rng.string(6)]);
    const palette = new Y.Array<number>();
    item.set('coverPalette', palette);
    palette.push([rng.int(0, 65535), rng.int(0, 65535), rng.int(0, 65535), rng.int(0, 65535), rng.int(0, 65535)]);
    const perceptual = new Y.Map<unknown>();
    item.set('perceptualPalette', perceptual);
    perceptual.set('standout', rng.int(0, 0xffffff));
    perceptual.set('background', rng.int(0, 0xffffff));
    perceptual.set('deltaE', rng.next() * 100);

    // Content analysis for ~10 chapters (created as the book is processed)
    const sections = doc.getMap('contentAnalysis').get('sections') as Y.Map<unknown>;
    const chapters = rng.int(6, 14);
    for (let c = 0; c < chapters; c++) {
        const s = new Y.Map<unknown>();
        sections.set(`${bookId}/sec-${c}`, s);
        s.set('title', 'Chapter ' + c + ' ' + rng.string(10));
        s.set('generatedAt', nowFor(state));
        s.set('status', 'done');
        if (rng.bool(0.2)) {
            s.set('referenceStartCfi', cfi(rng));
        }
    }
    // Reading-list entry appears once the book is opened
    const entries = doc.getMap('reading-list').get('entries') as Y.Map<unknown>;
    const e = new Y.Map<unknown>();
    entries.set(bookId + '.epub', e);
    e.set('filename', bookId + '.epub');
    e.set('bookId', bookId);
    e.set('title', item.get('title'));
    e.set('author', item.get('author'));
    e.set('percentage', 0);
    e.set('lastUpdated', nowFor(state));
}

function removeBook(state: VersicleSimState, doc: Y.Doc): void {
    const { rng } = state;
    if (state.bookIds.length <= 5) return;
    const idx = rng.int(0, state.bookIds.length - 1);
    const bookId = state.bookIds.splice(idx, 1)[0];
    const books = doc.getMap('library').get('books') as Y.Map<unknown>;
    books.delete(bookId);
    // versicle deletes the book's contentAnalysis sections on removal
    const sections = doc.getMap('contentAnalysis').get('sections') as Y.Map<unknown>;
    const toDelete: string[] = [];
    sections.forEach((_v: unknown, k: string) => {
        if (k.startsWith(bookId + '/')) toDelete.push(k);
    });
    toDelete.forEach(k => sections.delete(k));
    // progress and reading-list entries are NOT pruned (matches versicle)
}

function ensureRoots(doc: Y.Doc): void {
    // versicle's ten top-level maps, each with its container key(s)
    const ensure = (map: string, key: string, mk: () => unknown) => {
        const m = doc.getMap(map);
        if (!m.has(key)) m.set(key, mk());
    };
    ensure('library', 'books', () => new Y.Map());
    ensure('progress', 'progress', () => new Y.Map());
    ensure('annotations', 'annotations', () => new Y.Map());
    ensure('reading-list', 'entries', () => new Y.Map());
    ensure('vocabulary', 'knownCharacters', () => new Y.Map());
    ensure('lexicon', 'rules', () => new Y.Map());
    ensure('contentAnalysis', 'sections', () => new Y.Map());
    ensure('devices', 'devices', () => new Y.Map());
    ensure('searchHistory', 'recentQueries', () => new Y.Array());
    ensure('searchHistory', 'savedQueries', () => new Y.Array());
    const meta = doc.getMap('meta');
    if (!meta.has('schemaVersion')) meta.set('schemaVersion', 9);
}

function deviceIdFor(state: VersicleSimState): string {
    return `device-${state.sessionCount % state.opts.devices}`;
}

function bootDevice(state: VersicleSimState, doc: Y.Doc): void {
    const devices = doc.getMap('devices').get('devices') as Y.Map<unknown>;
    const id = deviceIdFor(state);
    let dev = devices.get(id) as Y.Map<unknown> | undefined;
    if (!dev) {
        dev = new Y.Map<unknown>();
        devices.set(id, dev);
        dev.set('id', id);
        dev.set('name', 'Device ' + id);
        dev.set('platform', id.endsWith('0') ? 'android' : 'web');
        dev.set('browser', 'chrome');
        dev.set('userAgent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 versicle/' + state.rng.string(8));
        dev.set('created', nowFor(state));
        const profile = new Y.Map<unknown>();
        dev.set('profile', profile);
        profile.set('theme', 'dark');
        profile.set('fontSize', 18);
    }
    // registerCurrentDevice rewrites volatile fields on every boot
    dev.set('lastActive', nowFor(state));
    dev.set('appVersion', '1.' + (100 + (state.sessionCount % 40)) + '.0');
}

function progressMapFor(state: VersicleSimState, doc: Y.Doc, bookId: string): Y.Map<unknown> {
    const root = doc.getMap('progress').get('progress') as Y.Map<unknown>;
    let perBook = root.get(bookId) as Y.Map<unknown> | undefined;
    if (!perBook) {
        perBook = new Y.Map<unknown>();
        root.set(bookId, perBook);
    }
    const deviceId = deviceIdFor(state);
    let perDevice = perBook.get(deviceId) as Y.Map<unknown> | undefined;
    if (!perDevice) {
        perDevice = new Y.Map<unknown>();
        perBook.set(deviceId, perDevice);
        perDevice.set('bookId', bookId);
        perDevice.set('percentage', 0);
        perDevice.set('completedRanges', new Y.Array<string>());
        perDevice.set('readingSessions', new Y.Array<Y.Map<unknown>>());
    }
    return perDevice;
}

/** Sawtooth cap from useReadingStateStore: 500 -> keep last 300 */
const MAX_READING_SESSIONS = 500;
const PRUNED_READING_SESSIONS = 300;

function pushReadingSession(state: VersicleSimState, perDevice: Y.Map<unknown>, type: string): void {
    const { rng } = state;
    const sessions = perDevice.get('readingSessions') as Y.Array<Y.Map<unknown>>;
    const s = new Y.Map<unknown>();
    // Yjs requires inserting the map before setting nested keys in the same
    // transaction only when parent is attached; push first then set.
    sessions.push([s]);
    s.set('cfiRange', cfi(rng) + ',' + cfi(rng));
    s.set('startTime', nowFor(state) - rng.int(5_000, 90_000));
    s.set('endTime', nowFor(state));
    s.set('type', type);
    if (rng.bool(0.7)) s.set('label', 'Chapter ' + rng.int(1, 30) + ': ' + rng.string(rng.int(10, 40)));
    if (sessions.length > MAX_READING_SESSIONS) {
        sessions.delete(0, sessions.length - PRUNED_READING_SESSIONS);
    }
}

function pageTurn(state: VersicleSimState, doc: Y.Doc, bookId: string): void {
    const { rng } = state;
    const perDevice = progressMapFor(state, doc, bookId);
    perDevice.set('currentCfi', cfi(rng));
    perDevice.set('percentage', Math.min(1, (perDevice.get('percentage') as number || 0) + rng.next() * 0.01));
    perDevice.set('lastRead', nowFor(state));
    pushReadingSession(state, perDevice, rng.bool(0.8) ? 'page' : 'scroll');
    const ranges = perDevice.get('completedRanges') as Y.Array<string>;
    if (rng.bool(0.6) && ranges.length > 0) {
        // merged with the previous range (sequential reading fast path)
        ranges.delete(ranges.length - 1, 1);
        ranges.push([cfi(rng) + ',' + cfi(rng)]);
    } else {
        ranges.push([cfi(rng) + ',' + cfi(rng)]);
    }
    // Every page turn also rewrites the reading-list entry
    const entries = doc.getMap('reading-list').get('entries') as Y.Map<unknown>;
    const entry = entries.get(bookId + '.epub') as Y.Map<unknown> | undefined;
    if (entry) {
        entry.set('percentage', perDevice.get('percentage'));
        entry.set('lastUpdated', nowFor(state));
    }
}

function ttsSentence(state: VersicleSimState, doc: Y.Doc, bookId: string): void {
    const { rng } = state;
    const perDevice = progressMapFor(state, doc, bookId);
    perDevice.set('lastPlayedCfi', cfi(rng));
    perDevice.set('currentQueueIndex', rng.int(0, 400));
    perDevice.set('lastRead', nowFor(state));
    const ranges = perDevice.get('completedRanges') as Y.Array<string>;
    if (ranges.length > 0 && rng.bool(0.8)) {
        ranges.delete(ranges.length - 1, 1);
    }
    ranges.push([cfi(rng) + ',' + cfi(rng)]);
    if (rng.bool(0.05)) pushReadingSession(state, perDevice, 'tts');
}

function heartbeat(state: VersicleSimState, doc: Y.Doc): void {
    const devices = doc.getMap('devices').get('devices') as Y.Map<unknown>;
    const dev = devices.get(deviceIdFor(state)) as Y.Map<unknown> | undefined;
    if (dev) dev.set('lastActive', nowFor(state));
}

function addAnnotation(state: VersicleSimState, doc: Y.Doc, bookId: string): void {
    const { rng } = state;
    const annotations = doc.getMap('annotations').get('annotations') as Y.Map<unknown>;
    const id = 'ann-' + state.sessionCount + '-' + rng.string(6);
    const a = new Y.Map<unknown>();
    annotations.set(id, a);
    a.set('id', id);
    a.set('bookId', bookId);
    a.set('cfiRange', cfi(rng) + ',' + cfi(rng));
    a.set('text', rng.string(rng.int(40, 300)));
    a.set('type', rng.bool(0.8) ? 'highlight' : 'note');
    a.set('color', 'yellow');
    if (rng.bool(0.3)) a.set('note', rng.string(rng.int(20, 120)));
    a.set('created', nowFor(state));
}

function deleteAnnotation(state: VersicleSimState, doc: Y.Doc): void {
    const { rng } = state;
    const annotations = doc.getMap('annotations').get('annotations') as Y.Map<unknown>;
    const keys: string[] = [];
    annotations.forEach((_v: unknown, k: string) => { keys.push(k); });
    if (keys.length > 20) annotations.delete(keys[rng.int(0, keys.length - 1)]);
}

function searchQuery(state: VersicleSimState, doc: Y.Doc): void {
    const { rng } = state;
    // versicle rebuilds the whole recentQueries array per query without
    // scoped diffing — modelled as full delete + reinsert of <= 20 entries
    const recent = doc.getMap('searchHistory').get('recentQueries') as Y.Array<Y.Map<unknown>>;
    const existing: { query: string; lastUsedAt: number }[] = [];
    recent.forEach((m: Y.Map<unknown>) => {
        existing.push({ query: m.get('query') as string, lastUsedAt: m.get('lastUsedAt') as number });
    });
    existing.unshift({ query: rng.string(rng.int(3, 18)), lastUsedAt: nowFor(state) });
    if (existing.length > 20) existing.length = 20;
    recent.delete(0, recent.length);
    for (const q of existing) {
        const m = new Y.Map<unknown>();
        recent.push([m]);
        m.set('query', q.query);
        m.set('lastUsedAt', q.lastUsedAt);
        m.set('isSaved', false);
    }
}

function vocabulary(state: VersicleSimState, doc: Y.Doc): void {
    const { rng } = state;
    const known = doc.getMap('vocabulary').get('knownCharacters') as Y.Map<unknown>;
    const ch = String.fromCharCode(0x4e00 + rng.int(0, 3000));
    known.set(ch, nowFor(state));
}

export interface SessionResult {
    /** One blob per debounced save */
    blobs: Uint8Array[];
    clientID: number;
}

/**
 * Runs one versicle session against a doc hydrated by the caller.
 * Each high-level event is captured as ONE update blob (one debounced save).
 */
export function runSession(state: VersicleSimState, doc: Y.Doc): SessionResult {
    const { rng, opts } = state;

    const blobs: Uint8Array[] = [];
    const captured: Uint8Array[] = [];
    const onUpdate = (u: Uint8Array) => captured.push(u);
    doc.on('update', onUpdate);
    const save = () => {
        if (captured.length > 0) {
            const blob = captured.length === 1 ? captured[0] : Y.mergeUpdates(captured);
            captured.length = 0;
            blobs.push(blob);
            state.bytesProduced += blob.byteLength;
        }
    };

    // Root containers must be captured too — losing them from the persisted
    // stream would give every later blob a missing dependency (Yjs then
    // parks everything in pendingStructs and nothing ever integrates).
    doc.transact(() => ensureRoots(doc));
    save();

    // First session bootstraps the library
    if (state.sessionCount === 0) {
        doc.transact(() => {
            for (let i = 0; i < opts.initialBooks; i++) importBook(state, doc);
        });
        save();
    }

    doc.transact(() => bootDevice(state, doc));
    save();

    // Books being read this session
    const active: string[] = [];
    const activeCount = Math.min(state.bookIds.length, rng.int(1, 3));
    for (let i = 0; i < activeCount; i++) active.push(rng.choice(state.bookIds));
    const ttsSession = rng.bool(0.3);

    for (let e = 0; e < opts.eventsPerSession; e++) {
        state.totalEvents++;
        const r = rng.next();
        if (r < 0.02 && state.bookIds.length < opts.maxBooks) {
            doc.transact(() => importBook(state, doc));
        } else if (r < 0.03) {
            doc.transact(() => removeBook(state, doc));
        } else if (r < 0.06) {
            doc.transact(() => heartbeat(state, doc));
        } else if (r < 0.09) {
            doc.transact(() => searchQuery(state, doc));
        } else if (r < 0.12) {
            doc.transact(() => addAnnotation(state, doc, rng.choice(active)));
        } else if (r < 0.13) {
            doc.transact(() => deleteAnnotation(state, doc));
        } else if (r < 0.18) {
            doc.transact(() => vocabulary(state, doc));
        } else if (ttsSession && r < 0.6) {
            doc.transact(() => ttsSentence(state, doc, active[0]));
        } else {
            doc.transact(() => pageTurn(state, doc, rng.choice(active)));
        }
        save();
    }

    doc.off('update', onUpdate);
    state.sessionCount++;
    return { blobs, clientID: doc.clientID };
}

/**
 * Deterministic clientID for a session — a fresh one per session, like
 * versicle's fresh Y.Doc per page load.
 */
export function clientIdForSession(seed: number, session: number): number {
    return (seed % 1000) * 1_000_000 + session + 1;
}

/** Aggregate struct statistics of a doc's store (for instrumentation). */
export function docStructStats(doc: Y.Doc): {
    items: number; gcStructs: number; deletedItems: number; dsRanges: number; svClients: number;
} {
    let items = 0, gcStructs = 0, deletedItems = 0;
    (doc.store as any).clients.forEach((arr: any[]) => {
        for (const s of arr) {
            if (s instanceof Y.GC) gcStructs++;
            else {
                items++;
                if ((s as any).deleted) deletedItems++;
            }
        }
    });
    const ds = Y.createDeleteSetFromStructStore((doc as any).store);
    let dsRanges = 0;
    (ds as any).clients.forEach((arr: any[]) => { dsRanges += arr.length; });
    const svClients = Y.decodeStateVector(Y.encodeStateVector(doc)).size;
    return { items, gcStructs, deletedItems, dsRanges, svClients };
}

/** Materializes all versicle maps to JSON (equivalence checks). */
export function materializeVersicleDoc(snapshot: Uint8Array | null, pending: Uint8Array[]): string {
    const doc = new Y.Doc();
    if (snapshot) Y.applyUpdate(doc, snapshot);
    for (const u of pending) Y.applyUpdate(doc, u);
    const out: Record<string, unknown> = {};
    for (const name of ['library', 'progress', 'annotations', 'reading-list', 'vocabulary', 'lexicon', 'contentAnalysis', 'devices', 'searchHistory', 'meta']) {
        out[name] = doc.getMap(name).toJSON();
    }
    const s = JSON.stringify(out);
    doc.destroy();
    return s;
}
