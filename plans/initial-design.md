Design Document: Tiered Recursive Architecture
==========================================================

**Status:** Final Specification **Objective:** Infinite Nesting & Large Document Support **Target:** `src/provider.ts`

1\. Executive Summary
---------------------

This design transforms `y-fire` from a simple document syncer into a **Recursive CRDT Factory** with **Tiered Storage**.

It addresses two physical constraints of Firestore:

1.  **Concurrency:** Solved via an append-only Delta architecture.

2.  **Size Limits (1MB):** Solved via a "Tiered Compaction" strategy that gracefully degrades from Single-Snapshot mode to Segmented-Log mode when documents grow large.

2\. Core Philosophy: The Recursive Unit
---------------------------------------

The system acts on a single atomic unit: the **Sync Node**. Whether a document is the "Root" of the database or a "Card" nested 10 levels deep, it is treated identically.

**Definition of a Sync Node:**

-   It is bound to exactly one `Y.Doc` (or `Y.Subdoc`).

-   It is bound to exactly one unique Firestore Path.

-   It manages its own persistence lifecycle (Snapshot + History + Deltas).

-   It is unaware of its parent's state, but manages its children's existence.

3\. Data Schema
---------------

We utilize a recursive directory structure with three storage tiers per node.

### 3.1 Path Protocol

-   **Root:** `artifacts/{appId}/public/data/{docId}`

-   **Child:** `.../subdocs/{childGuid}`

-   **Grandchild:** `.../subdocs/{childGuid}/subdocs/{grandChildGuid}`

### 3.2 Tiered Node Storage

At *every* level of the hierarchy, the storage schema is identical:

**Path:** `.../{currentDocPath}`

1.  **Tier 1: Base Snapshot (`content` field)**

    -   **Type:** `Blob` (Uint8Array) inside the main document.

    -   **Purpose:** The baseline state of the document.

    -   **Limit:** Strict < 1MB.

2.  **Tier 2: Segmented History (`history/` subcollection)**

    -   **Path:** `.../history/{segmentId}`

    -   **Fields:** `segment` (Blob), `startTime`, `endTime`.

    -   **Purpose:** When the Snapshot gets full (near 1MB), we stop updating it and instead merge updates into large "History Segments" (Target size: ~500KB - 1MB).

    -   **Structure:** An ordered sequence of large binary blobs.

3.  **Tier 3: Live Deltas (`updates/` subcollection)**

    -   **Path:** `.../updates/{autoId}`

    -   **Fields:** `update` (Blob), `createdAt`, `createdBy`.

    -   **Purpose:** High-frequency, low-latency writes.

    -   **Limit:** Pending queue (usually < 50 items).

4.  **Tier 4: Sub-Directory (`subdocs/` subcollection)**

    -   **Path:** `.../subdocs/{subdocGuid}`

    -   **Purpose:** Directory for child nodes.

4\. Operational Logic
---------------------

### 4.1 The Write Lifecycle (Append Only)

Writes are always lightweight to ensure UI responsiveness.

1.  **Capture:** Listen to `doc.on('update')`.

2.  **Buffer:** Cache updates in memory (debounce ~500ms).

3.  **Write:** Perform `addDoc` to the **Tier 3 (`updates`)** collection.

    -   *Note:* We never write to Tier 1 or 2 during a standard user edit.

### 4.2 The Read Lifecycle (Aggregation)

Loading a node requires aggregating the tiers:

1.  **Fetch Base:** Read `doc.data().content`. Apply to `Y.Doc`.

2.  **Fetch History:** Query `collection('history').orderBy('startTime')`. Apply all segments.

3.  **Subscribe Live:** Listen to `collection('updates').orderBy('createdAt')`. Apply incoming deltas.

### 4.3 Tiered Compaction (The 1MB Solution)

We replace simple compaction with logic that respects the 1MB ceiling.

-   **Trigger:** Any client detecting `updates.length > 50`.

-   **Transaction:**

    1.  **Read:** Load Base Snapshot (`S`), History Segments (`H[]`), and Live Updates (`U[]`).

    2.  **Attempt Level 1 Merge (Target: Snapshot):**

        -   Combine `S + H[] + U[]` -> `CandidateSnapshot`.

        -   **If size < 900KB:**

            -   Write `content = CandidateSnapshot`.

            -   Delete all `H[]` and `U[]`.

            -   *Result:* Clean single file.

    3.  **Fallback Level 2 Merge (Target: History Segment):**

        -   **Condition:** If `CandidateSnapshot` > 900KB (Overflow).

        -   **Action:** We treat `S` as "Frozen". We only compact `U[]` (and potentially small `H[]` chunks).

        -   Combine `U[]` -> `NewSegment`.

        -   Write `NewSegment` to `history/` collection.

        -   Delete `U[]`.

        -   *Result:* Base Snapshot stays frozen. New data is appended as a large chunk in History.

    4.  **Edge Case (Huge Updates):** If a single update in `U[]` is > 1MB, it cannot be saved. (Ideally, the client application should prevent inserting 5MB images into text fields, or use Subdocs for assets).

### 4.4 The Subdoc Lifecycle (Recursive Factory)

How `Y.Subdoc` is actually used:

1.  **Concept:** Yjs creates a "Reference" (GUID) in the parent document. The actual content is not loaded.

2.  **User Trigger:** Application code calls `subdoc.load()`.

3.  **Event Interception:** `FireProvider` listens to `doc.on('subdocs')`.

4.  **Factory Logic:**

    -   **On `added` / `loaded`:**

        -   Provider extracts GUID.

        -   Calculates Path: `currentPath + '/subdocs/' + guid`.

        -   **Instantiates a NEW `FireProvider`** for this path.

        -   Stores instance in `this.subProviders` map.

    -   **On `removed`:**

        -   Provider calls `childProvider.destroy()`.

        -   Removes from map.

5.  **Isolation:** The Child Provider runs its own Read/Write/Compaction cycles completely independently of the Parent.

    -   *Implication:* A Parent can have 0 updates, while a Child has 10,000 updates.

5\. Implementation Specification (`FireProvider`)
-------------------------------------------------

### 5.1 Class State

```
class FireProvider extends ObservableV2 {
  doc: Y.Doc;
  path: string;

  // Recursion
  subProviders: Map<string, FireProvider> = new Map();

  // Compaction State
  isCompacting: boolean = false;

  // Debounce Cache
  updateCache: Uint8Array | null;
}

```

### 5.2 Key Methods

-   **`constructor(opts)`**:

    -   Initialize `this.doc`.

    -   Setup `doc.on('update')` -> `this.handleUpdate`.

    -   Setup `doc.on('subdocs')` -> `this.handleSubdocs`.

    -   Start `this.sync()`.

-   **`sync()`**:

    -   `getDoc(this.path)` (Base Snapshot).

    -   `getDocs(collection(this.path, 'history'))` (Segments).

    -   `onSnapshot(collection(this.path, 'updates'))` (Live).

    -   Apply all to `this.doc`.

-   **`handleUpdate(update)`**:

    -   Merge into `this.updateCache`.

    -   Debounce `saveToFirestore`.

-   **`saveToFirestore()`**:

    -   `addDoc(collection(this.path, 'updates'), { update: this.updateCache })`.

    -   Clear cache.

-   **`compact()` (The Brain):**

    -   Run inside `runTransaction`.

    -   Check sizes.

    -   Decide between **Level 1 Merge** (Snapshot) or **Level 2 Merge** (History Segment).

6\. Security Rules
------------------

The recursive wildcard is mandatory.

```
match /artifacts/{appId}/public/data/{document=**} {
  allow read, write: if request.auth != null;
}

```

7\. Conclusion
--------------

This architecture embraces the physical reality of the platform.

-   **1MB Limit:** We don't fight it; we tier around it.

-   **Latency:** We don't block on big uploads; we stream small deltas.

-   **Scale:** We don't load the world; we recursively load only what is needed via Subdocs.
