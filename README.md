# y-cinder

> [!NOTE]
> This project is a fork of [`y-fire`](https://github.com/podraven/y-fire) by [podraven](https://github.com/podraven).

**y-cinder** is a high-performance, serverless-ready Firestore provider for [Yjs](https://github.com/yjs/yjs). It enables real-time collaboration in your applications by synchronizing Yjs documents with Cloud Firestore.

Designed for efficiency and cost-optimization, y-cinder implements a smart tiered storage architecture to minimize Firestore reads and writes, making it ideal for high-traffic serverless deployments.

> [!IMPORTANT]
> **Built with Google Antigravity**
>
> This project is mostly written with **Google Antigravity**, an agentic development platform that brings the IDE into the agent-first era. Antigravity provides a "Mission Control" for managing autonomous agents capable of planning, coding, and verifying complex software tasks.

## Table of Contents

- [Features](#features)
- [Comparison with y-fire](#comparison-with-y-fire)
- [Architecture](#architecture)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Firestore Rules](#firestore-rules)
- [Production Readiness](#production-readiness)
- [Contributors](#contributors)
- [License](#license)

## Features

- 🚀 **Serverless Optimized**: Built to work seamlessly in serverless environments.
- 📉 **Cost Efficient**: Granular, tiered storage (Snapshots, History, Updates) reduces Firestore costs.
- 🧹 **Auto-Compaction**: Automatically merges updates to maintain high read performance.
- 📦 **Subdocument Support**: Recursive handling of subdocuments within the same provider.
- ⚡ **Debounced Writes**: Smart buffering of updates to reduce write frequency.
- 🔒 **Distributed Locking**: Prevents race conditions during compaction.

## Comparison with y-fire

While `y-fire` provides a solid foundation for synchronizing Yjs documents with Firestore, `y-cinder` introduces several architectural enhancements designed for scale and cost control:

| Feature | y-fire | y-cinder |
| :--- | :--- | :--- |
| **Storage Strategy** | Typically stores updates in a linear collection or single document. | **Tiered Storage**: Uses Snapshots, History Segments, and Updates. |
| **Compaction** | Manual or non-existent in base implementation. | **Automatic & Distributed**: Merges updates into history/snapshots automatically using distributed locking. |
| **Cost** | Costs grow linearly with update frequency and document size. | **Optimized**: Reads/writes are minimized through batching and compaction. |
| **Scalability** | Good for small to medium documents. | **High**: Handles large document histories efficiently via segmentation. |

## Architecture

y-cinder uses a unique tiered storage approach to handle Yjs updates:

1.  **Snapshots** (Tier 1): Base documents containing the full state, optimized for fast initial load.
2.  **History Segments** (Tier 2): Merged batches of updates for efficient retrieval and history playback.
3.  **Updates** (Tier 3): Incremental changes from clients, debounced and batched.

This architecture allows y-cinder to provide fast load times and low latency while keeping Firestore billing in check.

## Installation

Since `y-cinder` is a specialized fork, it is not available on the public npm registry. Please install it directly from GitHub:

```bash
npm install git+https://github.com/vrwarp/y-cinder.git#HEAD
```

## Usage

Connect your Yjs document to Firestore using the `FireProvider`.

```typescript
import * as Y from "yjs";
import { FireProvider } from "y-cinder";
import { initializeApp } from "firebase/app";

// Initialize your Firebase app
const firebaseApp = initializeApp({ /* your config */ });

const ydoc = new Y.Doc();
const provider = new FireProvider({
  firebaseApp,
  ydoc,
  path: "documents/my-doc"
});

// Use ydoc as usual
// ...

// When done
// provider.destroy();
```

## Configuration

The `FireProvider` constructor accepts the following configuration options:

| Option | Type | Required | Default | Description |
| :--- | :--- | :---: | :--- | :--- |
| `firebaseApp` | `FirebaseApp` | Yes | - | The initialized Firebase application instance. |
| `ydoc` | `Y.Doc` | Yes | - | The Yjs document to sync. |
| `path` | `string` | Yes | - | Firestore document path (e.g., `users/alice/notes/note-1`). |
| `maxUpdatesThreshold` | `number` | No | `50` | Number of updates before triggering compaction. |
| `maxWaitTime` | `number` | No | `500` | Debounce time (ms) for writing updates to Firestore. |

### API Methods

- **`provider.destroy()`**:
  Stops synchronization and cleans up resources. Call this when the provider is no longer needed (e.g., component unmount) to prevent memory leaks and duplicate connections.
- **`provider.compact()`**:
  Manually triggers the compaction process. Usually handled automatically.

## Firestore Rules

To ensure proper functionality, your Firestore security rules must allow **read and write** access to the document path and its subcollections.

```
match /path/to/your/document/{document=**} {
  allow read, write: if <your-auth-condition>;
}
```

y-cinder writes to the following subcollections:
- `updates`
- `history`
- `subdocs` (if using subdocuments)

## Production Readiness

**Evaluation: High**

`y-cinder` is designed with production constraints in mind. It addresses the common pitfalls of using Firestore with Yjs (read/write limits, cost explosions) through its tiered architecture.

- **Reliability**: Uses Firestore transactions and distributed locking to ensure data integrity during compaction.
- **Performance**: Optimized for fast initial loads by reading snapshots and merged history rather than thousands of individual updates.
- **Stability**: Includes mechanisms like exponential backoff and connection error handling.
- **Testing**: The codebase includes comprehensive tests and handles edge cases like clock skew and concurrent edits.

It is recommended for applications that require robust, scalable real-time collaboration on Firestore.

## Contributors

- **[vrwarp](https://github.com/vrwarp)**

Original work by **[podraven](https://github.com/podraven)**.

## License

This project is licensed under the MIT License. Please include copies of the [y-fire license](https://github.com/podraven/y-fire/blob/main/LICENSE) when reusing or extending this code.
