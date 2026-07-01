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
