import { FirebaseApp } from "@firebase/app";
import {
  getFirestore,
  Firestore,
  Unsubscribe,
  onSnapshot,
  doc,
  collection,
  addDoc,
  Bytes,
  runTransaction,
  query,
  orderBy,
  getDocs,
  getDoc,
  limit,
  serverTimestamp,
  Timestamp,
  writeBatch,
  DocumentReference,
  setDoc,
  QueryDocumentSnapshot
} from "@firebase/firestore";
import * as Y from "yjs";
import { ObservableV2 } from "lib0/observable";
import { toBase64, fromBase64 } from "lib0/buffer";
import * as encoding from "lib0/encoding";

interface UpdateMetadata {
  clientID: number;
  clockStart: number;
  clockEnd: number;
}

export interface FireProviderConfig {
  firebaseApp: FirebaseApp;
  ydoc: Y.Doc;
  path: string;
  maxUpdatesThreshold?: number; // default 50
  maxWaitTime?: number; // default 500ms
  compactionProbability?: number; // default 0.01 (1%)
  depth?: number;
}

export class FireProvider extends ObservableV2<any> {
  doc: Y.Doc;
  path: string;
  db: Firestore;
  firebaseApp: FirebaseApp;
  uid: string; // Random client ID for this session

  // Recursion
  subProviders: Map<string, FireProvider> = new Map();

  // Compaction State
  isCompacting: boolean = false;

  // Debounce Cache
  updateCache: Uint8Array | null = null;

  // Configuration
  maxUpdatesThreshold: number = 50;
  maxWaitTime: number = 500;
  compactionProbability: number = 0.01;
  depth: number;

  private _unsubscribeUpdates: Unsubscribe | null = null;
  private _debouncedSave: () => void;

  private _isDestroyed = false;

  constructor({
    firebaseApp,
    ydoc,
    path,
    maxUpdatesThreshold = 50,
    maxWaitTime = 500,
    compactionProbability = 0.01,
    depth = 0,
  }: FireProviderConfig) {
    super();
    this.firebaseApp = firebaseApp;
    this.db = getFirestore(firebaseApp);
    this.doc = ydoc;
    this.path = path;
    this.maxUpdatesThreshold = maxUpdatesThreshold;
    this.maxWaitTime = maxWaitTime;
    this.compactionProbability = compactionProbability;
    this.depth = depth;

    // Generate a unique ID for this session/provider instance
    this.uid = Math.random().toString(36).substring(2) + Date.now().toString(36);

    // Setup Debounced Save
    this._debouncedSave = this.debounce(this.saveToFirestore.bind(this), this.maxWaitTime);

    // Start Lifecycle
    this.doc.on('update', this.handleUpdate);
    this.doc.on('subdocs', this.handleSubdocs);

    // Start Sync
    this.sync();
  }

  // Simple debounce implementation
  private debounce(func: Function, wait: number) {
    let timeout: any;
    return (...args: any[]) => {
      clearTimeout(timeout);
      timeout = setTimeout(() => func(...args), wait);
    };
  }

  // helper for SV calculation
  private calculateStateVector(update: Uint8Array): string {
    const tempDoc = new Y.Doc();
    Y.applyUpdate(tempDoc, update);
    const sv = Y.encodeStateVector(tempDoc);
    const svBase64 = toBase64(sv);
    tempDoc.destroy();
    return svBase64;
  }

  // Helper: Extract all metadata (multi-client) 
  private extractAllMetadata(update: Uint8Array): UpdateMetadata[] {
    try {
      // @ts-ignore
      const decoded = Y.decodeUpdate(update);
      const results: UpdateMetadata[] = [];
      if (decoded.structs) {
        decoded.structs.forEach((struct: any) => {
          results.push({
            clientID: struct.id.client,
            clockStart: struct.id.clock,
            clockEnd: struct.id.clock + struct.length
          });
        });
      }
      return results;
    } catch (e) {
      return [];
    }
  }

  // helper for delay
  private wait(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // Helper: Write State Vector map to Uint8Array
  private writeStateVector(sv: Map<number, number>): Uint8Array {
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, sv.size);
    for (const [client, clock] of sv) {
      encoding.writeVarUint(encoder, client);
      encoding.writeVarUint(encoder, clock);
    }
    return encoding.toUint8Array(encoder);
  }

  /**
   * Sync Mechanism (Metadata-Only)
   * 1. Fetch Metadata (State Vectors / Clocks)
   * 2. Construct Server SV
   * 3. Calculate Differences
   * 4. Apply only missing updates
   */
  async sync() {
    try {
      const serverSVMap = new Map<number, number>();

      // Store potential updates to apply
      const pendingUpdates: { type: string, data: any, priority: number }[] = [];

      // 1. Fetch Updates (Tier 3)
      const updatesQ = query(collection(this.db, this.path, 'updates'), orderBy('createdAt', 'asc'));
      const updatesSnap = await getDocs(updatesQ);
      if (this._isDestroyed) return;

      updatesSnap.forEach(snap => {
        const data = snap.data();
        if (data) {
          // Metadata extraction
          let addedToPending = false;
          if (typeof data.clientID === 'number' && typeof data.clockEnd === 'number') {
            // We have metadata!
            const current = serverSVMap.get(data.clientID) || 0;
            if (data.clockEnd > current) {
              serverSVMap.set(data.clientID, data.clockEnd);
            }
            addedToPending = true;
          } else if (data.update) {
            // Fallback: Parse metadata from blob to update ServerSV
            try {
              const updateBlob = (data.update as Bytes).toUint8Array();
              const metas = this.extractAllMetadata(updateBlob);
              metas.forEach(meta => {
                const current = serverSVMap.get(meta.clientID) || 0;
                if (meta.clockEnd > current) {
                  serverSVMap.set(meta.clientID, meta.clockEnd);
                }
              });
            } catch (e) {
              console.warn("Failed to parse fallback metadata", e);
            }
            addedToPending = true;
          }

          if (addedToPending) {
            pendingUpdates.push({ type: 'update', data, priority: 3 });
          }
        }
      });

      // 2. Fetch History Segments (Tier 2)
      const historyQ = query(collection(this.db, this.path, 'history'), orderBy('startTime', 'asc'));
      const historySnaps = await getDocs(historyQ);
      if (this._isDestroyed) return;

      historySnaps.forEach(snap => {
        const data = snap.data();
        if (data && data.stateVector) {
          const vector = fromBase64(data.stateVector);
          const map = Y.decodeStateVector(vector);
          for (const [client, clock] of map.entries()) {
            const current = serverSVMap.get(client) || 0;
            if (clock > current) {
              serverSVMap.set(client, clock);
            }
          }
          pendingUpdates.push({ type: 'history', data, priority: 2 });
        } else if (data && data.segment) {
          // Fallback: Parse segment to update ServerSV
          try {
            const segmentBlob = (data.segment as Bytes).toUint8Array();
            const metas = this.extractAllMetadata(segmentBlob);
            metas.forEach(meta => {
              const current = serverSVMap.get(meta.clientID) || 0;
              if (meta.clockEnd > current) {
                serverSVMap.set(meta.clientID, meta.clockEnd);
              }
            });
          } catch (e) {
            console.warn("Failed to parse fallback history segment", e);
          }
          pendingUpdates.push({ type: 'history', data, priority: 2 });
        }
      });

      // 3. Fetch Base Snapshot (Tier 1)
      const mainRef = doc(this.db, this.path);
      const mainSnap = await getDoc(mainRef);
      if (this._isDestroyed) return;

      if (mainSnap.exists()) {
        const data = mainSnap.data();
        if (data && data.stateVector) {
          const vector = fromBase64(data.stateVector);
          const map = Y.decodeStateVector(vector);
          for (const [client, clock] of map.entries()) {
            const current = serverSVMap.get(client) || 0;
            if (clock > current) {
              serverSVMap.set(client, clock);
            }
          }
          pendingUpdates.push({ type: 'snapshot', data, priority: 1 });
        } else if (data && data.content) {
          pendingUpdates.push({ type: 'snapshot', data, priority: 1 });
        }
      }

      // 4. Check what we actually need
      // We compare Local SV with the gathered info.

      const localSV = Y.encodeStateVector(this.doc);
      const localSVMap = Y.decodeStateVector(localSV);

      // Helper to check if we already have this data
      const isRedundant = (metadata: { svMap?: Map<number, number>, client?: number, end?: number }) => {
        if (metadata.svMap) {
          for (const [client, clock] of metadata.svMap) {
            const localClock = localSVMap.get(client) || 0;
            if (clock > localClock) return false; // We miss something
          }
          return true; // We have everything
        }
        if (metadata.client !== undefined && metadata.end !== undefined) {
          const localClock = localSVMap.get(metadata.client) || 0;
          return localClock >= metadata.end;
        }
        return false; // Unknown, assume not redundant
      };

      // Apply things
      // Sort by priority (Snapshot first, then History, then Updates)
      pendingUpdates.sort((a, b) => a.priority - b.priority);

      for (const item of pendingUpdates) {
        let redundant = false;

        // Check checks
        if (item.type === 'snapshot' || item.type === 'history') {
          if (item.data.stateVector) {
            const sv = fromBase64(item.data.stateVector);
            const map = Y.decodeStateVector(sv);
            if (isRedundant({ svMap: map })) redundant = true;
          }
          // For legacy data without SV, we can't be sure, so we apply (idempotent)
        } else if (item.type === 'update') {
          if (item.data.clientID !== undefined && item.data.clockEnd !== undefined) {
            if (isRedundant({ client: item.data.clientID, end: item.data.clockEnd })) redundant = true;
          }
        }

        if (!redundant) {
          try {
            if (item.type === 'snapshot' && item.data.content) {
              Y.applyUpdate(this.doc, (item.data.content as Bytes).toUint8Array(), 'origin:firebase/snapshot');
            } else if (item.type === 'history' && item.data.segment) {
              Y.applyUpdate(this.doc, (item.data.segment as Bytes).toUint8Array(), 'origin:firebase/history');
            } else if (item.type === 'update' && item.data.update) {
              Y.applyUpdate(this.doc, (item.data.update as Bytes).toUint8Array(), 'origin:firebase/update');
            }
          } catch (e) {
            console.error(`Failed to apply ${item.type}`, e);
          }
        }
      }


      // 5. Push Missing Local Updates
      // We calculate diff against the *Server SV* we constructed.
      // This is efficient because we don't assume server has "infinite" state, 
      // we assume it has what we saw.
      const serverSV = this.writeStateVector(serverSVMap);
      const localDiff = Y.encodeStateAsUpdate(this.doc, serverSV);

      if (localDiff.byteLength > 2) {
        console.log("Pushing missing local updates to Firestore.");
        // We apply the same logic for saving metadata
        const metas = this.extractAllMetadata(localDiff);
        const meta = metas.length > 0 ? metas[0] : null;
        const pkg: any = {
          update: Bytes.fromUint8Array(localDiff),
          createdAt: serverTimestamp(),
          createdBy: this.uid
        };
        if (meta) {
          pkg.clientID = meta.clientID;
          pkg.clockStart = meta.clockStart;
          pkg.clockEnd = meta.clockEnd;
        }
        await addDoc(collection(this.db, this.path, 'updates'), pkg);
        if (this._isDestroyed) return;
      }

      if (this._unsubscribeUpdates) this._unsubscribeUpdates();

      const listenerFn = (snapshot: any) => {
        // Check for compaction trigger
        if (snapshot.size > this.maxUpdatesThreshold && !this.isCompacting) {
          if (Math.random() < this.compactionProbability) {
            this.compact();
          }
        }

        snapshot.docChanges().forEach((change: any) => {
          if (change.type === 'added') {
            const data = change.doc.data();
            // Check if this update was created by us
            if (data.createdBy === this.uid) {
              return;
            }

            // FILTER: Check if we already have this update using metadata
            // Only applicable if we have an up-to-date view of our own clocks, which we do.
            if (typeof data.clientID === 'number' && typeof data.clockEnd === 'number') {
              // Get our local clock for this client
              // Note: localSVMap might be stale if we edited locally since sync start.
              // We should get fresh vector.
              const freshSV = Y.encodeStateVector(this.doc);
              // Decoding full SV map every update might be slight overhead but better than parsing update.
              // Actually, Yjs has efficient lookup? No.
              // Optim: Just apply using Yjs idempotency. But wait, "CPU Waste".
              // We can cache our own clocks?
              // Let's decode SV for check.
              const freshMap = Y.decodeStateVector(freshSV);
              const localClock = freshMap.get(data.clientID) || 0;
              if (localClock >= data.clockEnd) {
                return; // Skip
              }
            }

            if (data.update) {
              try {
                const update = (data.update as Bytes).toUint8Array();
                Y.applyUpdate(this.doc, update, 'origin:firebase/update');
              } catch (e) {
                console.error("Failed to apply update", e);
              }
            }
          }
        });
      };

      // Re-query for listener to be safe / clean
      const liveUpdatesQ = query(collection(this.db, this.path, 'updates'), orderBy('createdAt', 'asc'));

      if (this._isDestroyed) return;

      this._unsubscribeUpdates = onSnapshot(liveUpdatesQ, listenerFn, (error) => {
        console.error("onSnapshot listener failed", error);
      });

    } catch (err) {
      console.error("Sync failed", err);
    }
  }

  handleUpdate = (update: Uint8Array, origin: any) => {
    // Prevent loops
    if (origin === 'origin:firebase/snapshot' ||
      origin === 'origin:firebase/history' ||
      origin === 'origin:firebase/update') {
      return;
    }

    // Merge into cache
    this.updateCache = this.updateCache ? Y.mergeUpdates([this.updateCache, update]) : update;

    // Trigger Debounced Write
    this._debouncedSave();
  }



  async saveToFirestore() {
    if (!this.updateCache) return;

    const update = this.updateCache;
    this.updateCache = null;

    const metas = this.extractAllMetadata(update);
    const meta = metas.length > 0 ? metas[0] : null;
    const docData: any = {
      update: Bytes.fromUint8Array(update),
      createdAt: serverTimestamp(),
      createdBy: this.uid
    };

    if (meta) {
      docData.clientID = meta.clientID;
      docData.clockStart = meta.clockStart;
      docData.clockEnd = meta.clockEnd;
    }

    try {
      await addDoc(collection(this.db, this.path, 'updates'), docData);
    } catch (err) {
      console.error("Failed to save update to Firestore", err);
      // Recovery: Put back the updates we failed to save
      // We prepend the failed update to the current cache (if any)
      if (this.updateCache) {
        this.updateCache = Y.mergeUpdates([update, this.updateCache]);
      } else {
        this.updateCache = update;
      }
      // Ensure we retry
      this._debouncedSave();
    }
  }

  /**
   * Compaction Logic (Tiered)
   * Merges updates into History Segments or Base Snapshot
   */
  async compact(attempt = 1) {
    if (this.isCompacting && attempt === 1) return;
    this.isCompacting = true;

    try {
      console.log(`Starting compaction (attempt ${attempt})...`);

      // 1. Get all updates to compact
      // We query them first to get references. 
      // Note: In highly concurrent env, we should verify existence in transaction.
      const updatesQ = query(collection(this.db, this.path, 'updates'), orderBy('createdAt', 'asc'));
      const updatesSnap = await getDocs(updatesQ);

      // We also check for History segments to merge them back if possible
      const historyQ = query(collection(this.db, this.path, 'history'), orderBy('startTime', 'asc'));
      const historySnaps = await getDocs(historyQ);

      if (updatesSnap.empty && historySnaps.empty) {
        this.isCompacting = false;
        return;
      }

      const updateDocs = updatesSnap.docs;
      const historyDocs = historySnaps.docs;

      await runTransaction(this.db, async (transaction) => {
        // 2. Read Base Snapshot (Tier 1) inside transaction
        const mainRef = doc(this.db, this.path);
        const mainSnap = await transaction.get(mainRef);

        let baseSnapshot: Uint8Array | null = null;
        if (mainSnap.exists()) {
          const data = mainSnap.data();
          if (data && data.content) {
            baseSnapshot = (data.content as Bytes).toUint8Array();
          }
        }

        // 3. Read specific updates to ensure they exist
        // (Firestore transactions need read-before-write)
        const updatesToMerge: Uint8Array[] = [];
        for (const uDoc of updateDocs) {
          const freshSnap = await transaction.get(uDoc.ref);
          if (freshSnap.exists()) {
            const data = freshSnap.data();
            if (data && data.update) {
              updatesToMerge.push((data.update as Bytes).toUint8Array());
            }
          }
        }

        // 3b. Read History Segments to attempt full merge
        const historyToMerge: { ref: DocumentReference, val: Uint8Array }[] = [];
        for (const hDoc of historyDocs) {
          const freshSnap = await transaction.get(hDoc.ref);
          if (freshSnap.exists()) {
            const data = freshSnap.data();
            if (data && data.segment) {
              historyToMerge.push({
                ref: hDoc.ref,
                val: (data.segment as Bytes).toUint8Array()
              });
            }
          }
        }

        if (updatesToMerge.length === 0 && historyToMerge.length === 0) return;

        // 4. Merge Logic

        // Strategy: Try Level 1 Merge (Base + History + Updates)
        const allContent: Uint8Array[] = [];
        if (baseSnapshot) allContent.push(baseSnapshot);
        historyToMerge.forEach(h => allContent.push(h.val));
        updatesToMerge.forEach(u => allContent.push(u));

        const candidate = Y.mergeUpdates(allContent);

        const sizeInBytes = candidate.byteLength;
        const TARGET_LIMIT = 900000; // 900KB

        // Decide Level 1 (Snapshot) vs Level 2 (History)
        if (sizeInBytes < TARGET_LIMIT) {
          // Success: Everything fits in Base Snapshot
          console.log(`Compacted to Snapshot (Size: ${sizeInBytes})`);
          transaction.set(mainRef, {
            content: Bytes.fromUint8Array(candidate),
            stateVector: this.calculateStateVector(candidate)
          }, { merge: true });

          // Delete all utilized segments
          for (const uDoc of updateDocs) {
            transaction.delete(uDoc.ref);
          }
          for (const hItem of historyToMerge) {
            transaction.delete(hItem.ref);
          }

        } else {
          // Level 2: Write to History Segment (Fallback)
          // We can't fit everything into Base.
          // Goal: Merge updates into History Segments.

          if (updatesToMerge.length > 0) {
            // FIX: Handle updates that are too large for a single History Segment
            const MAX_SEGMENT_SIZE = 900000; // Safe limit (900KB)

            // 1. Try merging all first (Optimistic)
            let pendingMerge = Y.mergeUpdates(updatesToMerge);

            if (pendingMerge.byteLength < MAX_SEGMENT_SIZE) {
              // Fast path: It fits in one segment
              const segmentId = Math.random().toString(36).substring(2);
              const historyRef = doc(collection(this.db, this.path, 'history'), segmentId);

              transaction.set(historyRef, {
                segment: Bytes.fromUint8Array(pendingMerge),
                startTime: updateDocs[0].data().createdAt,
                endTime: updateDocs[updateDocs.length - 1].data().createdAt
                // Note: We don't save stateVector for segments as they might be deltas (invalid SV)
              });

              // Delete all utilized updates
              for (const uDoc of updateDocs) {
                transaction.delete(uDoc.ref);
              }
            } else {
              // Slow path: The updates are too big. We must split them.
              // We iterate through updates and create multiple History Segments.

              let currentBatch: Uint8Array[] = [];
              let currentBatchSize = 0;
              let batchStartIndex = 0;

              for (let i = 0; i < updatesToMerge.length; i++) {
                const update = updatesToMerge[i];
                const updateSize = update.byteLength;

                // If a SINGLE update is > 1MB, we might still have issues, 
                // but usually Yjs updates are granular.

                // Check if adding this update would likely exceed limit
                if (currentBatchSize + updateSize > MAX_SEGMENT_SIZE && currentBatch.length > 0) {
                  // Flush current batch to a new Segment
                  const mergedBatch = Y.mergeUpdates(currentBatch);
                  const segmentId = Math.random().toString(36).substring(2);
                  const historyRef = doc(collection(this.db, this.path, 'history'), segmentId);

                  transaction.set(historyRef, {
                    segment: Bytes.fromUint8Array(mergedBatch),
                    startTime: updateDocs[batchStartIndex].data().createdAt,
                    endTime: updateDocs[i - 1].data().createdAt
                  });

                  // Delete updates in this batch
                  for (let j = batchStartIndex; j < i; j++) {
                    transaction.delete(updateDocs[j].ref);
                  }

                  // Reset
                  currentBatch = [];
                  currentBatchSize = 0;
                  batchStartIndex = i;
                }

                currentBatch.push(update);
                currentBatchSize += updateSize;
              }

              // Flush remaining batch
              if (currentBatch.length > 0) {
                const mergedBatch = Y.mergeUpdates(currentBatch);
                const segmentId = Math.random().toString(36).substring(2);
                const historyRef = doc(collection(this.db, this.path, 'history'), segmentId);

                transaction.set(historyRef, {
                  segment: Bytes.fromUint8Array(mergedBatch),
                  startTime: updateDocs[batchStartIndex].data().createdAt,
                  endTime: updateDocs[updatesToMerge.length - 1].data().createdAt
                });

                for (let j = batchStartIndex; j < updatesToMerge.length; j++) {
                  transaction.delete(updateDocs[j].ref);
                }
              }
            }
          }
        }
      });

      // On Success:
      this.isCompacting = false;

    } catch (e: any) {
      const MAX_RETRIES = 5;

      // Filter for retryable errors (Contention, Unavailable, Deadline Exceeded)
      // Firestore code 'aborted' is commonly used for transaction contention
      const isRetryable = e.code === 'aborted' || e.code === 'unavailable' || e.code === 'deadline-exceeded';

      if (attempt <= MAX_RETRIES && isRetryable) {
        // Exponential Backoff: 2^attempt * 100ms
        // Jitter: Randomize to prevent "thundering herd" if multiple clients fail simultaneously
        const backoff = (Math.pow(2, attempt) * 100) + (Math.random() * 100);

        console.warn(`Compaction failed (attempt ${attempt}). Retrying in ${Math.floor(backoff)}ms...`, e);

        await this.wait(backoff);
        await this.compact(attempt + 1); // Recursive retry
      } else {
        console.error("Compaction failed permanently or reached max retries.", e);
        this.isCompacting = false; // Give up
      }
    }
  }

  handleSubdocs = ({ added, removed, loaded }: { added: Set<Y.Doc>, removed: Set<Y.Doc>, loaded: Set<Y.Doc> }) => {
    added.forEach(subdoc => {
      this.startSubdocProvider(subdoc);
    });
    loaded.forEach(subdoc => {
      this.startSubdocProvider(subdoc);
    });
    removed.forEach(subdoc => {
      const guid = subdoc.guid;
      const provider = this.subProviders.get(guid);
      if (provider) {
        provider.destroy();
        this.subProviders.delete(guid);
      }
    });
  }

  startSubdocProvider(subdoc: Y.Doc) {
    const guid = subdoc.guid;
    if (this.subProviders.has(guid)) return;

    // Firestore path limit is 100. Safety limit at 50.
    if (this.depth >= 50) {
      console.warn(`Max subdocument depth exceeded at ${this.path}`);
      return;
    }

    const subPath = `${this.path}/subdocs/${guid}`;
    const provider = new FireProvider({
      firebaseApp: this.firebaseApp,
      ydoc: subdoc,
      path: subPath,
      maxUpdatesThreshold: this.maxUpdatesThreshold,
      maxWaitTime: this.maxWaitTime,
      depth: this.depth + 1
    });
    this.subProviders.set(guid, provider);
  }

  destroy() {
    this._isDestroyed = true;
    if (this._unsubscribeUpdates) this._unsubscribeUpdates();

    this.doc.off('update', this.handleUpdate);
    this.doc.off('subdocs', this.handleSubdocs);

    // Destroy children
    this.subProviders.forEach(p => p.destroy());
    this.subProviders.clear();

    // Flush any pending updates
    if (this.updateCache) {
      this.saveToFirestore();
    }

    super.destroy();
  }
}
