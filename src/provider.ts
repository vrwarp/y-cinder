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
  setDoc
} from "@firebase/firestore";
import * as Y from "yjs";
import { ObservableV2 } from "lib0/observable";

export interface FireProviderConfig {
  firebaseApp: FirebaseApp;
  ydoc: Y.Doc;
  path: string;
  maxUpdatesThreshold?: number; // default 50
  maxWaitTime?: number; // default 500ms
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

  private _unsubscribeUpdates: Unsubscribe | null = null;
  private _debouncedSave: () => void;

  constructor({
    firebaseApp,
    ydoc,
    path,
    maxUpdatesThreshold = 50,
    maxWaitTime = 500,
  }: FireProviderConfig) {
    super();
    this.firebaseApp = firebaseApp;
    this.db = getFirestore(firebaseApp);
    this.doc = ydoc;
    this.path = path;
    this.maxUpdatesThreshold = maxUpdatesThreshold;
    this.maxWaitTime = maxWaitTime;

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

  // helper for delay
  private wait(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Sync Mechanism
   * 1. Load Base Snapshot
   * 2. Load History Segments
   * 3. Subscribe to Live Updates
   */
  async sync() {
    try {
      // SHADOW SYNC STRATEGY
      // We reconstruct the server state in a temporary "shadow" doc to determine
      // exactly what local changes are missing from the server.
      const remoteShadow = new Y.Doc();

      try {
        // 1. Fetch Updates (Tier 3)
        // We fetch updates FIRST to avoid the "staggered read" race condition.
        // If compaction happens while reading, we might miss updates if we read them last.
        // By reading them first, we either get them here, OR if they are compacted into the snapshot,
        // we will get them in step 3 when we read the new snapshot.
        const updatesQ = query(collection(this.db, this.path, 'updates'), orderBy('createdAt', 'asc'));
        const updatesSnap = await getDocs(updatesQ);

        updatesSnap.forEach(snap => {
          const data = snap.data();
          if (data && data.update) {
            try {
              const update = (data.update as Bytes).toUint8Array();
              // We DO NOT apply to this.doc here, we let the onSnapshot listener handle it 
              // (or we could, but standardizing on onSnapshot is cleaner for the live path).
              // However, for the shadow doc, we MUST apply it.
              Y.applyUpdate(remoteShadow, update);
            } catch (e) {
              console.error("Failed to apply update to shadow", e);
            }
          }
        });

        // 2. Fetch History Segments (Tier 2)
        const historyQ = query(collection(this.db, this.path, 'history'), orderBy('startTime', 'asc'));
        const historySnaps = await getDocs(historyQ);

        historySnaps.forEach(snap => {
          const data = snap.data();
          if (data && data.segment) {
            try {
              const segment = (data.segment as Bytes).toUint8Array();
              Y.applyUpdate(this.doc, segment, 'origin:firebase/history');
              Y.applyUpdate(remoteShadow, segment);
            } catch (e) {
              console.error("Failed to apply history segment", e);
            }
          }
        });

        // 3. Fetch Base Snapshot (Tier 1)
        const mainRef = doc(this.db, this.path);
        const mainSnap = await getDoc(mainRef);

        if (mainSnap.exists()) {
          const data = mainSnap.data();
          if (data && data.content) {
            try {
              const content = (data.content as Bytes).toUint8Array();
              Y.applyUpdate(this.doc, content, 'origin:firebase/snapshot');
              Y.applyUpdate(remoteShadow, content);
            } catch (e) {
              console.error("Failed to apply snapshot", e);
            }
          }
        }

        // 4. Calculate Missing Local Updates
        const shadowSv = Y.encodeStateVector(remoteShadow);
        const localDiff = Y.encodeStateAsUpdate(this.doc, shadowSv);

        // 5. Push if needed
        // Yjs empty update is 2 bytes.
        if (localDiff.byteLength > 2) {
          console.log("Pushing missing local updates to Firestore.");
          const pkg = {
            update: Bytes.fromUint8Array(localDiff),
            createdAt: serverTimestamp(),
            createdBy: this.uid
          };
          await addDoc(collection(this.db, this.path, 'updates'), pkg);
        }

      } finally {
        remoteShadow.destroy();
      }

      if (this._unsubscribeUpdates) this._unsubscribeUpdates();

      const listenerFn = (snapshot: any) => {
        // Check for compaction trigger
        if (snapshot.size > this.maxUpdatesThreshold && !this.isCompacting) {
          this.compact();
        }

        snapshot.docChanges().forEach((change: any) => {
          if (change.type === 'added') {
            const data = change.doc.data();
            // Check if this update was created by us
            if (data.createdBy === this.uid) {
              return;
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
      this._unsubscribeUpdates = onSnapshot(liveUpdatesQ, listenerFn, (error) => {
        console.error("onSnapshot listener failed", error);
        // If the listener fails (e.g. Grpc error in emulator), we might want to trigger a re-sync
        // but for now we at least log it so it's visible in tests.
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

    try {
      await addDoc(collection(this.db, this.path, 'updates'), {
        update: Bytes.fromUint8Array(update),
        createdAt: serverTimestamp(),
        createdBy: this.uid
      });
    } catch (err) {
      console.error("Failed to save update", err);
      // If failed, we might want to preserve the cache, but strictly we could lose data here on network fail.
      // For this implementation, we log error.
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
      const historySnap = await getDocs(historyQ);

      if (updatesSnap.empty && historySnap.empty) {
        this.isCompacting = false;
        return;
      }

      const updateDocs = updatesSnap.docs;
      const historyDocs = historySnap.docs;

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
          transaction.set(mainRef, { content: Bytes.fromUint8Array(candidate) }, { merge: true });

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
          // Goal: Merge just the UPDATES into a new History Segment.
          // We leave the existing History segments alone (they remain parallel).

          if (updatesToMerge.length > 0) {
            const updatesMerged = Y.mergeUpdates(updatesToMerge);
            const segmentId = Math.random().toString(36).substring(2);
            const historyRef = doc(collection(this.db, this.path, 'history'), segmentId);

            transaction.set(historyRef, {
              segment: Bytes.fromUint8Array(updatesMerged),
              startTime: updateDocs[0].data().createdAt,
              endTime: updateDocs[updateDocs.length - 1].data().createdAt
            });

            // Delete processed updates
            for (const uDoc of updateDocs) {
              transaction.delete(uDoc.ref);
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

    const subPath = `${this.path}/subdocs/${guid}`;
    const provider = new FireProvider({
      firebaseApp: this.firebaseApp,
      ydoc: subdoc,
      path: subPath,
      maxUpdatesThreshold: this.maxUpdatesThreshold,
      maxWaitTime: this.maxWaitTime
    });
    this.subProviders.set(guid, provider);
  }

  destroy() {
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
