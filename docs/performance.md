# Performance on Long-Lived Documents

This document describes the performance bottlenecks that appear when a
y-cinder document has been edited for a long time (many accumulated /
aggregated changes), the fixes for them, and the benchmark suite that
measures them.

Run the benchmarks with:

```bash
npm run bench
```

The suite (`benchmarks/`) is pure Yjs — no Firebase emulator required. It
simulates a document edited across many sessions with bounded live content
but high historical churn (words typed at a cursor, stale ranges deleted),
compacting every `maxUpdatesThreshold` updates exactly like `FireProvider`
does.

## The core problem: churn accumulates forever

A Yjs document's update history never shrinks by itself. `Y.mergeUpdates` —
which compaction previously used to build snapshots — deduplicates and
concatenates, but **never garbage-collects**: the full content of every
deleted character rides along in every snapshot, forever.

For a document whose live content stays around 2 KB but which has seen a
few thousand edits, the measured effect (from `benchmarks/aged-doc.bench.ts`):

| compaction # | edits | snapshot (plain merge) | snapshot (GC) |
| ---: | ---: | ---: | ---: |
| 1   | 50    | 1.2 KB   | 570 B   |
| 41  | 2050  | 51.1 KB  | 11.0 KB |
| 81  | 4050  | 100.0 KB | 18.8 KB |
| 120 | 6000  | 147.9 KB | 26.7 KB |

Plain merge grows **linearly with total churn** (~25 KB per 1000 edits
here) with no upper bound. That size is paid repeatedly:

- every compaction downloads the snapshot from Cloud Storage, merges it,
  and re-uploads it (every ~50 updates);
- every *fresh* client downloads it on first sync;
- every compaction validates and re-encodes it on the CPU.

### Fix: GC-aware compaction (`src/merge-core.ts`)

Compaction now merges with `mergeUpdatesAsync(blobs, { gc: true })`: after
`Y.mergeUpdates`, the result is rewritten through a temporary `Y.Doc` with
garbage collection enabled, which replaces deleted-item content with tiny
tombstone ranges. This happens in the merge Web Worker when available, so
the main thread is not blocked.

Safety properties (tested in `tests/unit/merge-core.test.ts` and asserted
by the benchmark on every run):

- **State vector preserved** — the sync layer's redundancy checks are
  unaffected.
- **Delete-set preserved** — the reconnect push guard and delete-set
  fingerprint are unaffected.
- **Convergence preserved** — clients holding full un-GC'd history still
  converge with clients bootstrapped from a GC'd snapshot. This matches
  what live clients already do: `Y.Doc` defaults to `gc: true`, so every
  connected client GCs its own in-memory state anyway.
- **Missing dependencies detected** — if the merged blob has gaps (Yjs
  queues `pendingStructs`), rebuilding would silently drop the queued
  data, so the plain merged result is returned unchanged.
- **Never larger** — if a document's shape gives GC nothing to collect
  (pathologically fragmented inserts), the plain merge result is kept.

The behavior can be disabled with `gcCompaction: false` in
`FireProviderConfig`.

Note: GC also makes compaction *cheaper over time* — in the benchmark the
total compaction CPU dropped from 391 ms to 226 ms because each cycle
re-processes a much smaller base snapshot.

## Hot paths that scale with document age

Measured in `benchmarks/hot-paths.bench.ts` against an aged (~110 KB,
un-GC'd) snapshot:

### 1. Metadata extraction on every save

Every debounced save extracted per-client clocks with
`extractAllMetadata`, which calls `Y.decodeUpdate` — materializing every
struct *and its content* on the main thread. For a normal typing batch
that's fine; for the large merged updates produced by long offline
sessions it's a UI stall on the save path.

`extractClockEnds` now uses `Y.encodeStateVectorFromUpdate`, Yjs's lazy
walker (no struct/content allocation), producing identical
`clientID → clockEnd` metadata ~1.4× faster with far less allocation.
Used by the provider save path, the initial-sync push, and the sync
metadata fallbacks.

### 2. Initial sync applies one transaction per blob

`performInitialSync` applied each pending snapshot/history/update blob
with its own top-level `Y.applyUpdate` — one Yjs transaction, one observer
flush, and one `'update'` event *per blob*. An editor binding re-renders
on each of those. With 301 pending blobs:

| | time | update events |
| --- | ---: | ---: |
| one transaction per blob | 13.1 ms | 301 |
| single wrapped `transact` | 6.7 ms | 1 |

The apply loop is now wrapped in a single `ydoc.transact(...)`, so a
client loading a busy document fires one document update event instead of
hundreds.

### 3. Reconnect no-op push check decoded the whole snapshot

`diffCarriesNewData` guards against spurious pushes on reconnect (Yjs
embeds the full local delete-set in every diff). To prove the server
already covers the local delete-set it decoded **every** server blob —
including the multi-hundred-KB/MB snapshot — with `Y.decodeUpdate`, on
every reconnect of every client.

It now checks blobs smallest-first with an early exit. The snapshot's
inline delete-set fingerprint (a few hundred bytes, written by compaction
exactly for this purpose) usually proves coverage alone:

| | time |
| --- | ---: |
| decode all blobs (legacy) | 1.93 ms |
| early-exit smallest-first | 0.11 ms (17×) |

The gap widens with snapshot size — the legacy path was `O(snapshot)`,
the new path is `O(fingerprint)` in the common case.

### 4. Compaction validation double-walked the candidate

Before committing, compaction validated the merged candidate with
`Y.decodeUpdate` (full struct materialization on the main thread) and then
*separately* walked it again for the state vector and delete-set
fingerprint — and recomputed the state vector inside the Firestore
transaction body, which re-runs on contention.

Validation now reuses the two lazy passes that were already needed:
`Y.encodeStateVectorFromUpdate` (walks and validates every struct) and
`Y.diffUpdate` (walks and validates the delete-set). Both reject the same
corruption `Y.decodeUpdate` did — covered by
`tests/unit/data-integrity.test.ts` and the fuzz suites — and the state
vector is computed once, outside the transaction.

These passes were subsequently moved off the main thread entirely
(`mergeUpdatesWithMeta`, worker-side): see
[Very large single documents](#very-large-single-documents-multi-megabyte-snapshots)
for why that matters at multi-MB scale.

## Object-modification-heavy documents (Y.Map overwrites)

Canvas/whiteboard/board-style apps store objects as nested `Y.Map`s and
mutate them constantly — a drag is a burst of `x`/`y` overwrites. Every
`map.set` on an existing key tombstones the previous item, so history
grows with the number of *modifications*, not the number of objects.
`benchmarks/object-heavy.bench.ts` measures this workload (150 objects,
2400 operations including drag bursts):

| value style | plain merge | GC | live-state floor |
| --- | ---: | ---: | ---: |
| numeric (positions/colors) | 369.0 KB | 226.3 KB (1.6×) | 14.2 KB |
| strings (labels/serialized props) | 287.9 KB | 60.2 KB (4.8×) | 42.8 KB |

Two structural takeaways:

- **GC removes tombstone content, not tombstone structure.** Item ids, key
  names, and origin references of overwritten entries must survive for
  CRDT convergence. With string values, content dominates and GC lands
  near the live-state floor. With tiny numeric values the win is real but
  bounded — the residual (~212 KB above) is inherent to Yjs map semantics,
  not something a provider can compact away. Interleaved key overwrites
  (`x`,`y`,`x`,`y`…) also fragment clock ranges so tombstones can't merge.

  *App-level guidance:* batch object mutations into one transaction per
  gesture frame (one `doc.transact` setting both `x` and `y`, or a single
  `position` value), and prefer replacing one serialized value over many
  scalar keys for high-churn properties. Fewer distinct overwrites →
  fewer tombstones.

- **Remote modification bursts are delivered as many small updates.** The
  update and history listeners now batch each Firestore delivery into a
  single Yjs transaction. For a 150-update remote drag burst: 150 observer
  flushes / `'update'` events → 1, and 2.94 ms → 0.98 ms apply time. For
  an editor or canvas bound to the document, that's one re-render per
  network delivery instead of one per remote mutation.

## Array-heavy documents (Y.Array)

Lists, kanban boards, layer stacks, and row collections churn through
arrays, and array churn has its own shapes
(`benchmarks/array-heavy.bench.ts`, 1000–1500 operations each):

| pattern | plain merge | GC | ratio |
| --- | ---: | ---: | ---: |
| reorder-heavy (kanban moves) | 231.5 KB | 34.4 KB | 6.7× |
| full-array rewrite (anti-pattern) | 1.46 MB | 7.9 KB | 190× |
| nested rows, delete+recreate | 155.4 KB | 34.9 KB | 4.5× |

Why arrays benefit the most from GC compaction:

- **Move = delete + re-insert.** Yjs has no native array move, so every
  reorder duplicates the item's full content in history. GC reclaims the
  old copy; without it a kanban board's snapshot grows by one card-size
  per drag, forever.
- **Deleting a nested type GCs its whole subtree.** When a `Y.Map` row is
  removed from an array, all of its items are replaced by plain GC
  id-ranges (`parentGCd`), which merge with adjacent ranges. This is the
  *opposite* trade-off from in-place map-key overwrites (whose tombstone
  structure must be kept individually) — bulk delete/replace of array
  entries compacts nearly to the live-state floor.
- **The full-rewrite anti-pattern is fatal without GC.** Apps that sync
  external state with `arr.delete(0, len); arr.insert(0, rows)` tombstone
  every element on every save: after only 600 such rewrites of a 50-row
  array the un-GC'd snapshot passes Firestore's 1 MB document limit. With
  GC it stays at ~8 KB. (Prefer minimal diffs over full rewrites anyway —
  every rewrite also makes concurrent edits from other clients conflict
  spuriously.)

Correctness of the nested-type GC path (subtree → GC ranges) is pinned by
`tests/unit/merge-core.test.ts`: identical materialized JSON, identical
state vectors, and convergence with clients holding full un-GC'd history
after concurrent edits.

## Very large single documents (multi-megabyte snapshots)

Profiled on realistic fragmented documents (~60-char items, 10 clients,
scattered churn — item *count*, not byte count, is what drives Yjs costs;
see `benchmarks/large-doc.bench.ts` for the 1–4 MB tier, prototype numbers
at 10 MB / 300k structs):

| operation | ~1 MB | ~5 MB | ~10 MB |
| --- | ---: | ---: | ---: |
| merge + GC (worker-side) | ~150 ms | ~1 s | ~3.1 s |
| old main-thread meta walks (SV + DS fingerprint) | 40 ms | 138 ms | ~670 ms |
| delete-set fingerprint size | 22 KB | 89 KB | 178 KB |

The finding: compaction already merged in the Web Worker, but then
**walked the multi-MB result twice on the main thread** — once for
validation + state vector, once for the delete-set fingerprint. At 10 MB
that's ~670 ms of UI stall per compaction (every ~50 updates).

`mergeUpdatesWithMeta` now derives the result, state vector, and
delete-set fingerprint in a single worker-side call, so compaction's
main-thread Yjs cost is ~zero at any document size. On the GC path the
metadata is nearly free: the rebuilt `Y.Doc` is already in hand, so the
state vector is `O(clients)` and the delete-set encodes straight from the
store instead of re-walking the binary (total CPU drops too — 912 ms vs
1098 ms at ~5 MB). Applying cleanly to the doc doubles as the structural
validation the old `Y.decodeUpdate` guard provided; the non-GC/fallback
path keeps the lazy-walk validation, also worker-side.

Also verified at scale: the delete-set fingerprint grows slowly (~178 KB
at 10 MB of fragmented churn), staying well under the 700 KB inline cap —
so reconnecting clients keep the early-exit fast path even on very old
documents.

## Subdocument fan-out at scale

Every subdocument gets its own `FireProvider`: one initial sync (three
paginated queries + main doc read) and **three Firestore listeners**, all
started eagerly the moment the subdoc appears — whether or not the app
ever renders it. A board with 500 object-subdocs opens 1,500 listeners at
startup.

`subdocLoadingMode: 'lazy'` follows the Yjs lazy-loading convention:
remote-arriving subdocs (`shouldLoad === false`) are not synced until the
app calls `subdoc.load()`; locally created and `autoLoad` subdocs still
sync immediately. Measured at just 20 subdocs on the emulator, a client
reaches parent-synced in ~200 ms in lazy mode vs ~470 ms for a full eager
sync — and the gap on production Firestore is larger, since each avoided
subdoc sync is several network round-trips and each avoided listener is
ongoing read/broadcast cost. Covered by
`tests/integration/subdoc_lazy.test.ts`.

The default remains `'eager'` for backward compatibility: lazy mode
requires the app to call `subdoc.load()` (which is also what makes it
fast).

### Many objects as subdocuments

Apps that model each object as a Yjs subdocument get one `FireProvider`
per object. Two fixed costs were removed:

- **Clock-skew measurement is now shared.** Skew is a property of the
  client, not the document, but each subdoc provider used to measure it
  independently — 3 Firestore ops (write + read + delete) per subdoc at
  startup, i.e. 1,500 wasted ops for a 500-object board. Subdoc providers
  now inherit the parent's measured offset.
- **`gcCompaction` and `maxAggregationTime` are now inherited** by subdoc
  providers along with the other settings (they previously silently reset
  to defaults).

## Unbounded update aggregation while typing

`FireProvider`'s save debounce reset its timer on **every** local update.
A user typing continuously (keystroke interval < `maxWaitTime`) never
triggered a save at all: `_pendingUpdates` grew without bound, nothing was
persisted until they paused, and the eventual flush was a single giant
merged update (risking the Cloud Storage offload path, and losing the
whole session on a tab crash).

Saves are now additionally capped by `maxAggregationTime` (default
`maxWaitTime × 10`, configurable): once the oldest buffered update has
waited that long, the save fires even mid-burst. Normal debouncing is
unchanged when edits pause before the cap. Covered by
`tests/integration/debounce_cap.test.ts`.

## Configuration added

```typescript
new FireProvider({
  // ...
  maxAggregationTime: 5000, // default: maxWaitTime * 10
  gcCompaction: true,       // default: true
});
```

---

# Extended-use degradation (documents used for years)

A second round of work targeted what happens AFTER all of the above: a
document written daily for years through versicle-style workloads (one
long-lived doc; ten `Y.Map` roots; page-turn/TTS same-key overwrites; a
fresh random `clientID` per app launch). The suite
`benchmarks/versicle-aging.bench.ts` ages one document through 240
sessions / 14,400 events with the exact provider persistence shape and
samples every age-sensitive hot path; `versicle-aging-fixed.bench.ts`
replays the identical workload through the remediated pipeline.

## What still grew without bound (baseline, seed 20260820)

GC compaction removes deleted *content*, but three things grow with total
historical churn forever, because CRDT convergence requires them: dead
item *structure* (a map-key overwrite's tombstone can never merge away
when writes interleave keys — ~7 bytes and one struct per overwrite,
forever), the delete-set, and the state vector (one entry per client that
ever wrote — and versicle mints a new client every page load).

| metric | @1,440 events | @14,400 events | growth /1k events |
| --- | ---: | ---: | ---: |
| snapshot size | 394 KB | 3.31 MB | +230 KB |
| live+dead structs | 16.5k (7.1k dead) | 148k (74k dead) | +10k |
| fresh-client load (apply) | 21 ms | 372 ms | +25 ms |
| compaction merge CPU (every ~50 updates) | 64 ms | 631 ms | +45 ms |
| compaction transfer (every ~50 updates) | ~0.8 MB | ~6.6 MB | unbounded |
| reconnect diff encode | 0.3 ms | 5.6 ms | +0.4 ms |
| push guard, fingerprint dropped | 11 ms | 236 ms | +14 ms |
| fingerprint re-apply per snapshot delivery | 0.4 ms | 12 ms | +1.0 ms |

Every row is linear in age with no plateau. The same document measured
through y-idb (its own `benchmarks/aging.mjs`, fake-indexeddb): hydration
on every boot grew 37 ms → 1,004 ms, and the trim's full-document
re-encode drove write amplification from 1.8× to 9.1× and climbing.

## Fix 1: delta compaction (`historyFoldThreshold`)

Compaction previously folded snapshot + updates into a NEW snapshot every
`maxUpdatesThreshold` updates: download O(snapshot), merge O(snapshot),
upload O(snapshot) — all costs above scale with document age and repeat
every ~50 updates.

Compaction now has two modes. DELTA (the steady-state cycle): merge only
the pending update documents into ONE history segment — O(new data) CPU
and bandwidth, the base snapshot is not touched. FOLD: the legacy
everything-into-the-snapshot merge, run once history reaches
`historyFoldThreshold` (default 8) segments. Initial sync and the history
listener already consumed the history tier; segments carry a `stateVector`
of true clock ends so every redundancy check works unchanged. A state
vector cannot show deletions, though (they add no structs), so a segment
that carries any is also flagged `hasDeletions` and always applied rather
than skipped as covered — otherwise deleted content would reappear on
fresh and returning clients until the next fold.

Measured (240 sessions, identical workload): steady-state cycle cost
631 ms → **0.6 ms**; per-cycle transfer ~6.6 MB → **~6 KB**; total
compaction CPU 85.7 s → **8.7 s**; total compaction transfer ~990 MB →
**110 MB**.

## Fix 2: the delete-set fingerprint no longer dies of old age

The reconnect fast paths depend on the snapshot's delete-set fingerprint.
It was silently DROPPED once it outgrew its 700 KB inline cap — from that
day on, every reconnect of every client failed the coverage proof and
wrote a spurious O(delete-set) update document, forever. Oversized
fingerprints are now offloaded to Cloud Storage
(`deleteSetStoragePath`) and downloaded when needed — O(delete-set), once,
instead of the spurious-push spiral.

## Fix 3: clean reconnects no longer encode or decode anything big

`performInitialSync` encoded `Y.encodeStateAsUpdate(ydoc, serverSV)` on
every boot (Yjs embeds the FULL delete-set in every diff → O(churn)), then
`diffCarriesNewData` decoded that diff again. The push decision is now
made without either step when the server state vector covers every local
struct: local deletions come straight from
`Y.createDeleteSetFromStructStore` and are checked against the fingerprint
(`deleteSetCoveredByBlobs`). Measured guard cost at 14.4k events: 4.5 ms
(and growing) → **0.04 ms (flat)**. The O(document) encode only runs when
there is actually something to push.

The server state vector the guard compares against is, per client, the
clock up to which the server holds *every* struct: the snapshot's state
vector extended by the `[from, to)` ranges of the update and history
blobs (`buildServerCoverage`, a lazy `Y.parseUpdateMeta` walk of the two
tiers compaction keeps small). It is never the highest end clock in the
metadata: that hides a server-side gap — e.g. a save that committed
before initial sync pushed the doc's pre-existing content — and the guard
would never push the missing range.

The snapshot listener also re-applied the fingerprint on every delivery —
including the attach-delivery of every reconnect, whose state initial sync
had *just* processed. Deliveries now carry a version gate
(`SyncResult.snapshotVersion` → `createSnapshotListener`), so the
re-apply runs only when a compaction actually produced a new fold.

## Fix 4 (latent correctness bug): partial-update metadata was empty

`extractClockEnds` — the save-path metadata extractor introduced in the
first optimization round — was built on `Y.encodeStateVectorFromUpdate`,
which answers "what state does this update produce from scratch": for any
update whose structs do not start at clock 0, i.e. **every incremental
save after a client's first**, the leading gap makes the answer empty.
Consequences: the clientIDs/clientClocks redundancy metadata was silently
missing from every mid-life update document (every client re-applied
every remote update), initial sync under-counted the server state vector
(spurious re-pushes on reconnect against uncompacted updates), and
delta-compaction segments would have carried empty state vectors (skipped
as vacuously redundant — caught by the integration suite). All clock-ends
extraction now uses `Y.parseUpdateMeta(update).to`, which reports the true
per-client clock ranges with the same lazy walker. Pinned by regression
tests in `tests/unit/clock-ends.test.ts` and `tests/unit/merge-core.test.ts`.

## Fix 5: cold starts no longer round-trip the snapshot through `Bytes`

`performInitialSync` wrapped every blob it downloaded from Cloud Storage
(the snapshot, storage-backed updates, an offloaded fingerprint) in
Firestore `Bytes` just so it could sit in the field an inline blob uses.
In @firebase/firestore 4.x, `Bytes.fromUint8Array` builds a binary
string with one `String.fromCharCode` concatenation per byte, in the
browser and node builds alike. Every reader then copied the blob back
out with `toUint8Array()`: the snapshot twice (apply, push guard), each
inline update or segment three times (coverage, apply, push guard), and
a fingerprint four times. Downloads now stay the Uint8Array they arrive
as, which is what the snapshot listener already did. Every initial-sync
reader goes through `blobOf`, which copies an inline `Bytes` out once
and shares that copy.

Measured with `benchmarks/initial-sync-bytes.bench.ts`: a fresh client
runs the real `performInitialSync` against aged versicle snapshots, with
the SDK stubbed at its boundary (node, medians of 7 interleaved runs):

| snapshot | before | after | inherent apply |
| --- | ---: | ---: | ---: |
| 1.06 MB (4.3k events) | 247 ms | 120 ms | ~100 ms |
| 3.31 MB (14.4k events) | 843 ms | 385 ms | ~315 ms |

Bytes converted per byte fetched dropped from 3.01 to 0.02, because only
the inline blobs are converted, once each. Node overstates the cost: its
rope is ~31x the blob. In headless Chrome on the same snapshots, the
round trip added 55-75% on top of the inherent apply: +63 ms at 1.06 MB,
+213 ms at 3.31 MB, and ~0.6 s at 3.31 MB under 4x CPU throttling. It
also built a transient rope of ~19x the blob, about 63 MB at 3.31 MB.
The wrap cost steps up sharply above ~1.5 MB. All of it was main-thread
time and heap on every cold start that downloads the snapshot: a fresh
install, cleared storage, a rebuild after a squash, or a device whose
last sync predates a fold of another device's edits. On a real network
the download itself still dominates time-to-synced. Pinned by
`tests/unit/initial-sync-bytes.test.ts`: zero wraps, and at most one
conversion of each inline blob.

## Fix 6: compaction commits no longer re-read what they delete

Every compaction already holds its pending update documents (and, for a
fold, the history segments) from its queries. The commit transactions —
delta, fold, squash and the stale-epoch cleanup — then called
`transaction.get` on every one of those refs AGAIN, only to filter on
`exists()` before deleting it. Each such get is a billed read of the full
payload (segments can approach 1 MB) and its own `BatchGetDocuments` RPC,
and the whole set repeats on every SDK transaction retry: one extra read
per update ever written, about half of every compaction's reads.

The re-reads protected nothing. Update and history documents are
immutable with unique auto IDs, each deleted document's payload is
already in what the transaction commits, deleting a missing document is
a no-op, and every deleter checks `LOCK_COMPACTION` inside its own
transaction. The transactions now read only the lock (plus the main
document's version for a fold or squash) and delete the queried refs
directly. That invariant — only in-transaction lock holders delete
update/history documents — is documented in `src/compaction.ts` and must
hold for any future deleter.

Measured by `tests/integration/compaction_txn_reads.test.ts` (emulator,
SDK call counts, identical in each of 5 interleaved A/B runs):

| scenario | billed reads | RPCs | re-read payload |
| --- | ---: | ---: | ---: |
| delta, 52 updates | 109 → 57 | 61 → 9 | 20.6 KB → 0 |
| delta, 400 updates | 805 → 405 | 409 → 9 | 147.8 KB → 0 |
| fold, 7 segments + 50 updates | 119 → 62 | 67 → 10 | 156.1 KB → 0 |
| squash, 7 segments + 5 updates | 29 → 17 | 22 → 10 | 106.1 KB → 0 |
| delta, 52 updates, one SDK retry | 162 → 58 | 115 → 11 | 34.9 KB → 0 |

A compaction now pays one read per document it merges (the query) instead
of two, and its commit is a fixed handful of RPCs instead of up to ~500
parallel lookups. End to end, a
400-save single-client provider session: median 1222 → 824 billed reads
(3.06 → 2.06 per save, −33%), 441 → 41 RPCs.

## Fix 7: a device that missed a fold downloads the fold's tail

A fold merges base + history + updates into a new snapshot and deletes
its sources, so afterwards the data a lagging client lacks exists only
inside the new snapshot. Both catch-up paths — initial sync (a device
returning warm from y-idb) and the snapshot listener (a device offline
across the fold) — downloaded and applied the WHOLE snapshot whenever
`localCoversSnapshot` failed, although such a device typically holds the
replaced snapshot and lacks only the few sessions the fold added. That is
O(snapshot) Storage egress and main-thread decode, growing with document
age, paid by every returning device on every fold it missed (one fold per
~400 saves, about 6-7 versicle sessions).

Each fold now also publishes its **tail** — `mergeUpdates(history +
updates it folded)`, gc off — as `tail_v{N}_{attempt}.bin`, and writes
three fields in the fold's own transaction: `foldTailStoragePath`,
`foldTailVersion` (binds the tail to that version: fields left behind by a
writer that predates tails are ignored) and `foldTailBaseClocks`, the
replaced snapshot's state vector restricted to the clients the tail
touches. The restriction keeps it O(tail clients) — the full vector would
push many-client main documents past Firestore's 1 MiB limit — and loses
nothing: a reader behind only on clients listed there, at clocks it
covers, holds exactly the replaced snapshot (`foldTailMayCatchUp`). Such a
reader downloads and applies the tail, re-checks `localCoversSnapshot`,
and downloads the snapshot only if that still fails; the delete-set
fingerprint is still applied for deletions. A tail that is gone (the next
fold collected it) or does not apply is a plain fallback, never a
quarantine. The next fold garbage-collects the previous tail and squash
clears it. A tail is published only when it is at most half the snapshot
(early folds over a small base are not) and its fields fit beside the
state vector and inline fingerprint.

Measured in `benchmarks/returning-device.bench.ts` (B hydrated, then away
while A crosses exactly one fold plus 2 sessions; B's catch-up via the
snapshot vs via the published tail; CPU is the median of 5 runs):

| B hydrated at | snapshot download | tail download | structs decoded | CPU to synced |
| ---: | ---: | ---: | ---: | ---: |
| 60 sessions  | 996.9 KB | 152.9 KB | 45,837 → 5,150  | 79 → 18 ms |
| 120 sessions | 1.78 MB  | 152.7 KB | 84,806 → 5,127  | 141 → 18 ms |
| 240 sessions | 3.35 MB  | 139.6 KB | 160,617 → 4,683 | 268 → 23 ms |

The snapshot column grows with document age; the tail tracks what B
actually lacks (1.0-1.1x B's real diff). End to end on the emulator
(`tests/integration/returning_device_fold_tail.test.ts`, 40-session
document), both the warm start and the listener now download the
47,664 B tail instead of the 652,181 B snapshot. On a 120-session
document, a returning device's warm start downloads 52.8 KB instead of
the 1.83 MB snapshot, and its CPU from connect to synced drops from
404 ms to 76 ms (medians of 10 interleaved runs; 144 → 13 ms of it in
`Y.applyUpdate`). The fold pays one extra worker-side merge and upload
of the tail (4-15% of the snapshot) and one Storage delete. A device
several folds behind still needs the snapshot.

## The remaining floor: epoch squash

Everything above bounds the *per-cycle* costs, but the floor itself —
dead structs, delete-set ranges, state-vector entries — still grows
monotonically, and every fresh client load, y-idb hydration, and fold
pays it. It cannot be compacted away within one CRDT history; the only
reset is a new history.

`provider.squash()` rebuilds the document CONTENT into a brand-new Yjs id
space (`buildSquashedDoc`) and publishes it as epoch N+1: one client in
the state vector, zero tombstones, empty delete-set. Epoch fencing makes
the history break explicit and safe:

- the main document carries `epoch`; update/history documents are tagged
  with theirs; clients ignore foreign-epoch data, and compaction deletes
  it without merging (a pre-squash update can never integrate into a
  post-squash document — Yjs would park it in `pendingStructs` forever,
  which also permanently disables GC compaction);
- the squashed document carries its own epoch inside
  (`__ycinder.epoch`), so any local copy — IndexedDB, backups,
  checkpoints — knows which epoch it belongs to;
- a client that discovers a newer server epoch STOPS syncing and emits
  `epoch-changed` with its full local state; the application rebuilds its
  local doc from the new snapshot (versicle: the staged-swap + reload
  machinery) and decides whether any old-epoch local-only changes need
  semantic re-application. The squashing client fences itself the same
  way (`squashed` event, also carrying its local state) — its live doc
  still has the old structure. A squash whose live doc changes after the
  clone commits nothing (`skippedReason: 'local-changed'`), so the new
  snapshot never silently lacks the squasher's own edits.

Squash trades CRDT concurrency across the boundary for the floor reset:
edits made concurrently with the squash surface through events instead of
merging automatically. Use it for single-user / few-device documents
(exactly versicle's shape); do not use it for high-concurrency real-time
collaboration. It is strictly opt-in — nothing squashes automatically.

Measured in the fixed-pipeline benchmark (squash every 96 sessions): at
each squash the state vector resets 96 → 1 clients, dead structs ~22k →
0, delete-set ranges ~3.9k → 0, and fresh-client load drops ~40% — then
the curves restart from the live-content floor instead of compounding.

## y-idb: tiered trim (same document, local persistence)

y-idb re-encoded the WHOLE document (`Y.encodeStateAsUpdate`) onto the
main thread every `PREFERRED_TRIM_SIZE` (500) updates and wrote the
O(document) result as a row — both costs linear in document age, paid
every few minutes of active use. The trim is now tiered: the common trim
folds the fresh tail into ONE delta row via `Y.mergeUpdates` (O(new
updates)); the full re-encode runs only when delta rows exceed a
row-count or byte budget (see y-idb's README). Measured on the identical
workload: write amplification 9.1× (growing) → **2.6× (bounded ≤ ~3×)**,
IndexedDB bytes written 50 MB → 14.7 MB, trim latency flat instead of
growing ~13 ms per 1k events. Hydration remains floor-dominated — that is
what squash resets.

## Cold starts no longer re-upload local persistence

versicle builds a fresh `Y.Doc` and its providers on every page load, and
y-idb hydrates that doc asynchronously, in ONE transaction whose origin is
the persistence instance. With `FireProvider` constructed first (the
standard Yjs idiom), that transaction's `'update'` event is the whole
document, and the provider buffered it like a local edit: the debounced
save wrote it as a new update document although the server already held
every byte and the initial-sync push had just decided what (if anything)
the server lacked. Offline edits from the previous session and edits typed
while initial sync ran were written twice the same way: inside the push,
then again by the save.

The echo cost far more than its own write. Past `MAX_METADATA_CLIENTS`
(50) it carries no clientIDs/clientClocks, so every online peer downloaded
it and ran a full `Y.applyUpdate`. The next delta compaction turned it into
an O(document) segment flagged `hasDeletions`, which fresh clients and
history listeners never skip, or forced a full fold when it did not fit
inline. Above 1 MB it became a Storage blob plus pointer document.

Initial sync is now the single writer for what was buffered before it read
the local doc. `performInitialSync` calls `onLocalStateCaptured`
synchronously right before its push decision; once the attempt succeeds,
the provider drops the entries buffered by then (the server holds all of
them, already or inside the push) and emits `'saved'` for them. A save
that comes due while initial sync runs waits for it, but never past
`maxAggregationTime` from the first buffered update
(`initialSyncSaveHold`), so a stalled sync (offline, its push never
acknowledged) cannot keep edits out of the SDK's write queue. The
clock-skew probe does not hold saves. Entries buffered after the capture
are saved normally, a failed attempt drops nothing, and a save that
started since the capture keeps its batch (a harmless duplicate at worst).
The `destroy()`, unload and `squash()` flushes are unchanged.

Measured in `tests/integration/hydration_echo_upload.test.ts` (emulator,
versicle workload, deterministic op/byte counters, identical across 5
interleaved runs each; hydration lands after construction):

| cold start | before | after |
| --- | ---: | ---: |
| 20 sessions: echo written | 1 doc, 332,535 B | 0 |
| 20 sessions: next compaction | 332,535 B `hasDeletions` segment | nothing to compact |
| 60 sessions: echo, received by an online peer | 931,317 B without metadata | 0 |
| 100 sessions: echo, downloaded by an online peer | 1,527,939 B Storage blob | 0 |
| 100 sessions: next compaction | forced fold, 3.04 MB down / 1.52 MB up | nothing to compact |
| offline edits (server lacked 22,570 B) | 2 docs, 373,117 B | 1 doc, 22,570 B |
| edit typed during initial sync | 2 docs carry it | 1 |

CPU avoided (`benchmarks/hydration-echo.bench.ts`, medians of 5 runs): a
full apply of the metadata-less echo costs 57 / 98 / 187 ms at 60 / 120 /
240 sessions on every online peer, every cold start, and the cold-start
client's save-path metadata walk 22 / 45 / 75 ms.

Two cases still upload the hydrated document: hydration that lands after
initial sync has read the local doc (its update is indistinguishable from
offline edits the server lacks, and filtering by origin would lose those),
and a save that comes due during the clock-skew probe. Constructing the
provider after local persistence has loaded (y-idb `whenSynced`)
avoids both.
