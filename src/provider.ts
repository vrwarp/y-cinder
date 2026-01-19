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
  lockTTL?: number; // NEW: default 60000ms (60s)
  compactionLimit?: number; // NEW: default 500
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
  compactionLimit: number = 500;
  depth: number;

  // NEW: Lock Configuration
  lockTTL: number;
  private readonly LOCK_PATH = 'metadata/lock_compaction';

  private _unsubscribeUpdates: Unsubscribe | null = null;
  private _debouncedSave: () => void;
  // Test Hooks
  public _testHooks?: {
    beforeTransaction?: () => Promise<void>;
  };

  private _isDestroyed = false;

  constructor({
    firebaseApp,
    ydoc,
    path,
    maxUpdatesThreshold = 50,
    maxWaitTime = 500,
    compactionProbability = 0.01,
    depth = 0,
    lockTTL = 60000, // Default 60s safety window
    compactionLimit = 500,
  }: FireProviderConfig) {
    super();
    this.firebaseApp = firebaseApp;
    this.db = getFirestore(firebaseApp);
    this.doc = ydoc;
    this.path = path;
    this.maxUpdatesThreshold = maxUpdatesThreshold;
    this.maxWaitTime = maxWaitTime;
    this.compactionProbability = compactionProbability;
    this.compactionLimit = compactionLimit;
    this.depth = depth;
    this.lockTTL = lockTTL;

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
   * Attempts to acquire a distributed lock for compaction.
   * Returns true if lock was successfully acquired.
   */
  private async acquireLock(): Promise<boolean> {
    const lockRef = doc(this.db, this.path, this.LOCK_PATH);
    const clientNow = Date.now();

    try {
      return await runTransaction(this.db, async (transaction) => {
        const lockSnap = await transaction.get(lockRef);

        if (lockSnap.exists()) {
          const data = lockSnap.data();
          const expiresAt = data.expiresAt instanceof Timestamp ? data.expiresAt.toMillis() : 0;

          // Check if valid lock exists and is held by someone else
          if (clientNow < expiresAt && data.owner !== this.uid) {
            return false; // Lock is busy
          }
        }

        // Lock is free, or expired, or owned by us (re-entrant). Claim it.
        const newExpiry = Timestamp.fromMillis(clientNow + this.lockTTL);

        transaction.set(lockRef, {
          owner: this.uid,
          expiresAt: newExpiry
        });

        return true;
      });
    } catch (e) {
      console.warn("Failed to acquire lock (contention):", e);
      return false;
    }
  }

  /**
   * Releases the lock only if we still own it.
   */
  private async releaseLock() {
    const lockRef = doc(this.db, this.path, this.LOCK_PATH);
    try {
      // Optimistic delete: We don't need a transaction here because
      // if we don't own it, deleting it is either impossible (rules)
      // or harmless (TTL will fix it).
      // Ideally, we check owner, but for cleanup, a simple delete is often sufficient.
      // However, to be strictly safe against deleting SOMEONE ELSE'S lock:

      await runTransaction(this.db, async (transaction) => {
        const lockSnap = await transaction.get(lockRef);
        if (lockSnap.exists() && lockSnap.data().owner === this.uid) {
          transaction.delete(lockRef);
        }
      });
    } catch (e) {
      console.warn("Failed to release lock:", e);
    }
  }

  /**
   * Compaction Logic (Tiered)
   * Merges updates into History Segments or Base Snapshot
   */
  async compact(attempt = 1) {
    // 1. Local Gate: If we are already running logic, stop.
    if (this.isCompacting && attempt === 1) return;

    // 2. Distributed Gate: Try to become the Leader.
    const hasLock = await this.acquireLock();
    if (!hasLock) {
      // Another client is handling this. We back off silently.
      return;
    }

    this.isCompacting = true;

    try {
      console.log(`Starting compaction (attempt ${attempt})...`);

      // Query updates and history to identify work
      // FIX: Add limit to prevent unbounded memory usage
      const updatesQ = query(collection(this.db, this.path, 'updates'), orderBy('createdAt', 'asc'), limit(this.compactionLimit));
      const updatesSnap = await getDocs(updatesQ);

      const historyQ = query(collection(this.db, this.path, 'history'), orderBy('startTime', 'asc'));
      const historySnaps = await getDocs(historyQ);

      if (updatesSnap.empty && historySnaps.empty) {
        // Nothing to do
        return;
      }

      const updateDocs = updatesSnap.docs;
      const historyDocs = historySnaps.docs;

      // TEST HOOK: Allow simulating concurrent modifications
      if (this._testHooks && this._testHooks.beforeTransaction) {
        await this._testHooks.beforeTransaction();
      }

      await runTransaction(this.db, async (transaction) => {
        // === STEP A: THE KILL SWITCH ===
        // We re-read the lock inside the transaction to ensure we still own it.
        // If the lock expired during the 'getDocs' above (slow network), 
        // another client might have taken over. We MUST abort.
        const lockRef = doc(this.db, this.path, this.LOCK_PATH);
        const lockSnap = await transaction.get(lockRef);

        if (!lockSnap.exists() || lockSnap.data().owner !== this.uid) {
          throw new Error("Lock lost or expired during compaction phase - Aborting write.");
        }

        // === STEP B: Standard Compaction Logic ===
        // Read Base Snapshot
        const mainRef = doc(this.db, this.path);
        const mainSnap = await transaction.get(mainRef);

        let baseSnapshot: Uint8Array | null = null;
        if (mainSnap.exists()) {
          const data = mainSnap.data();
          if (data && data.content) {
            baseSnapshot = (data.content as Bytes).toUint8Array();
          }
        }

        // Read updates to merge
        const updatesToProcess: { ref: DocumentReference, data: Uint8Array, createdAt: Timestamp }[] = [];
        for (const uDoc of updateDocs) {
          const freshSnap = await transaction.get(uDoc.ref);
          if (freshSnap.exists()) {
            const data = freshSnap.data();
            if (data && data.update) {
              updatesToProcess.push({
                ref: uDoc.ref,
                data: (data.update as Bytes).toUint8Array(),
                createdAt: data.createdAt
              });
            }
          }
        }

        // Read History Segments
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

        if (updatesToProcess.length === 0 && historyToMerge.length === 0) return;

        // Perform Merge
        const allContent: Uint8Array[] = [];
        if (baseSnapshot) allContent.push(baseSnapshot);
        historyToMerge.forEach(h => allContent.push(h.val));
        updatesToProcess.forEach(u => allContent.push(u.data));

        const candidate = Y.mergeUpdates(allContent);
        const sizeInBytes = candidate.byteLength;
        const TARGET_LIMIT = 900000; // 900KB

        if (sizeInBytes < TARGET_LIMIT) {
          // Path 1: Compact to Snapshot
          console.log(`Compacted to Snapshot (Size: ${sizeInBytes})`);
          transaction.set(mainRef, {
            content: Bytes.fromUint8Array(candidate),
            stateVector: this.calculateStateVector(candidate)
          }, { merge: true });

          updatesToProcess.forEach(u => transaction.delete(u.ref));
          historyToMerge.forEach(h => transaction.delete(h.ref));

        } else {
          // Path 2: Write to History Segment (Fallback)
          // We can't fit everything into Base.
          // Goal: Merge updates into History Segments.

          if (updatesToProcess.length > 0) {
            // FIX: Handle updates that are too large for a single History Segment
            const MAX_SEGMENT_SIZE = 900000; // Safe limit (900KB)

            // 1. Try merging all first (Optimistic)
            const allUpdates = updatesToProcess.map(u => u.data);
            let pendingMerge = Y.mergeUpdates(allUpdates);

            if (pendingMerge.byteLength < MAX_SEGMENT_SIZE) {
              // Fast path: It fits in one segment
              const segmentId = Math.random().toString(36).substring(2);
              const historyRef = doc(collection(this.db, this.path, 'history'), segmentId);

              transaction.set(historyRef, {
                segment: Bytes.fromUint8Array(pendingMerge),
                startTime: updatesToProcess[0].createdAt,
                endTime: updatesToProcess[updatesToProcess.length - 1].createdAt
                // Note: We don't save stateVector for segments as they might be deltas (invalid SV)
              });

              // Delete all utilized updates
              for (const item of updatesToProcess) {
                transaction.delete(item.ref);
              }
            } else {
              // Slow path: The updates are too big. We must split them.
              // We iterate through updates and create multiple History Segments.

              let currentBatch: Uint8Array[] = [];
              let currentBatchSize = 0;
              let batchStartIndex = 0;

              for (let i = 0; i < updatesToProcess.length; i++) {
                const item = updatesToProcess[i];
                const update = item.data;
                const updateSize = update.byteLength;

                // Check if adding this update would likely exceed limit
                if (currentBatchSize + updateSize > MAX_SEGMENT_SIZE && currentBatch.length > 0) {
                  // Flush current batch to a new Segment
                  const mergedBatch = Y.mergeUpdates(currentBatch);
                  const segmentId = Math.random().toString(36).substring(2);
                  const historyRef = doc(collection(this.db, this.path, 'history'), segmentId);

                  transaction.set(historyRef, {
                    segment: Bytes.fromUint8Array(mergedBatch),
                    startTime: updatesToProcess[batchStartIndex].createdAt,
                    endTime: updatesToProcess[i - 1].createdAt
                  });

                  // Delete updates in this batch
                  for (let j = batchStartIndex; j < i; j++) {
                    transaction.delete(updatesToProcess[j].ref);
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
                  startTime: updatesToProcess[batchStartIndex].createdAt,
                  endTime: updatesToProcess[updatesToProcess.length - 1].createdAt
                });

                for (let j = batchStartIndex; j < updatesToProcess.length; j++) {
                  transaction.delete(updatesToProcess[j].ref);
                }
              }
            }
          }
        }
      }); // End Transaction

    } catch (e: any) {
      const MAX_RETRIES = 5;
      // Aborted often means contention on the *data* documents, not just the lock.
      const isRetryable = e.code === 'aborted' || e.code === 'unavailable' || e.code === 'deadline-exceeded';

      if (attempt <= MAX_RETRIES && isRetryable) {
        // Backoff and retry
        const backoff = (Math.pow(2, attempt) * 100) + (Math.random() * 100);
        console.warn(`Compaction failed (attempt ${attempt}). Retrying in ${Math.floor(backoff)}ms...`, e);

        await this.wait(backoff);

        // RECURSION SAFEGUARD:
        // We release the lock before retrying to ensure a clean slate.
        // We do NOT recursively call compact() inside the catch block without cleanup.
        // Instead, we let the finally block release the lock, and we should ideally 
        // restructure this to a loop or just let the next trigger handle it.
        // For this implementation, we will exit and rely on the next update trigger 
        // or a dedicated maintenance loop.

      } else {
        console.error("Compaction failed permanently.", e);
      }
    } finally {
      // 3. Always Release Lock
      this.isCompacting = false;
      await this.releaseLock();
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

    const subPath = `${this.path}/subdocs/${guid}`;

    // Firestore path limit is 100. Safety limit at 50.
    if (this.depth >= 50) {
      console.warn(`Max subdocument depth exceeded at ${this.path}`);
      this.emit('connection-error', [{
        code: 'recursion-limit',
        message: 'Max subdocument recursion depth exceeded',
        path: subPath,
        doc: subdoc
      }]);
      return;
    }

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
