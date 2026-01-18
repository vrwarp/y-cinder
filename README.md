# y-cinder

> [!NOTE]
> This is a fork of `y-fire` by [podraven](https://github.com/podraven/y-fire).

A database and connection provider for Yjs based on Firestore.

y-cinder is a Firestore (Firebase) provider, built especially for serverless infrastructure, that offers real-time capabilities to your Yjs-based applications. y-cinder is built with efficiency in mind to reduce the number of calls that the application makes to and from Firestore. With y-cinder, Firestore acts as persistent storage using a granular, tiered architecture (Snapshots, History Segments, and Updates) to optimize write costs and query performance.

# Features

1. **Tiered Storage Architecture**: Uses a smart combination of base snapshots, history segments, and incremental updates to efficiently store and retrieve data.
2. **Automatic Compaction**: Periodically merges incremental updates into history segments or base snapshots to keep read costs low and performance high.
3. **Optimized for Cost**: Debounces writes and compacts data to minimize Firestore writes and reads.
4. **Subdocument Support**: Automatically handles subdocuments recursively within the same provider logic.

# Installation

Install the package directly from GitHub:

```bash
npm install git+https://github.com/vrwarp/y-cinder.git#HEAD
```

# Usage

```typescript
import * as Y from "yjs";
import { FireProvider } from "y-cinder";
import { app } from "path-to-firebase-client";  // ex. app = initializeApp(config)

export const yProvider = (documentPath) => {
  const firebaseApp = app;
  const ydoc = new Y.Doc();
  return new FireProvider({ firebaseApp, ydoc, path: documentPath });
};
```

Tiptap example:

```typescript
const provider = yProvider("path/to/your/firestore/document");

// ...

const editor = new Editor({
  extensions: [
    StarterKit.configure({
      // The Collaboration extension comes with its own history handling
      history: false,
    }),
    // Register the document with Tiptap
    Collaboration.configure({
      document: provider.doc,
    })
    // CollaborationCursor is not directly supported by this provider 
    // as it does not implement the awareness protocol.
  ],
})
```

# Firestore rules

You need to grant **read and write** permissions to the document `/path/to/your/document` and its children `/path/to/your/document/{document=**}` for this module to function properly. y-cinder will write to the `updates`, `history`, and `subdocs` collections within your document path.

# APIs

#### Configuration

- **firebaseApp**: FirebaseApp (required)
- **ydoc**: Y.Doc (required)
- **path**: path to your **document** (required) ex. users/username/tasks/task-1
- **maxUpdatesThreshold**: Number of updates before triggering compaction, defaults to 50
- **maxWaitTime**: Time in milliseconds to debounce writes to Firestore, defaults to 500

Example:

```typescript
new FireProvider({
  firebaseApp,
  ydoc,
  path: "username/tasks/taskuid",
  maxUpdatesThreshold: 10,
  maxWaitTime: 90
});
```

#### Methods

- **destroy**: Destroys the y-cinder instance. You may want to destroy the y-cinder instance when navigating out of the page to avoid the initialization of duplicate instances. Use `provider.destroy();` to destroy the instance.

[1.1]: http://i.imgur.com/wWzX9uB.png "twitter icon without padding"

# Contributors

Made possible by **[Pod Raven](https://podraven.com)**, with special contributions from: **[deathg0d](https://github.com/deathg0d)**, **[dorkysamurai](https://github.com/lachana)**, **[arbitraryvector](https://x.com/arbitraryvector)**, **[Benson Tsai](https://github.com/vrwarp)**

##### Follow Us

- [![alt text][1.1] @pod_raven](https://x.com/pod_raven)
- [![alt text][1.1] @arbitraryvector](https://x.com/arbitraryvector)


# Licensing and Attribution

This module is licensed under the MIT License. You are generally free to reuse or extend upon this code as you see fit. Just include copies of the [y-fire](https://github.com/podraven/y-fire/blob/main/LICENSE) license.
