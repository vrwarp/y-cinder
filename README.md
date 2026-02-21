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

`y-cinder` diverges significantly from `y-fire` in its architectural philosophy. While `y-fire` is a **Hybrid (WebRTC + Firestore)** provider, `y-cinder` is a **Pure Firestore** provider.

| Feature | y-fire | y-cinder |
| :--- | :--- | :--- |
| **Architecture** | **Hybrid P2P**: Uses Firestore for discovery & persistence, WebRTC for real-time updates. | **Serverless**: Uses Firestore for *everything* (discovery, persistence, and real-time sync). |
| **Real-time Latency** | **Low (< 50ms)**: Peers talk directly via WebRTC. | **Medium (500ms - 1s)**: Updates must travel to Firestore and back to listeners. |
| **Network Requirements** | **Complex**: Requires open UDP ports, STUN/TURN servers, and NAT traversal. May be blocked by corporate firewalls. | **Simple**: Only requires standard HTTPS access to Google Cloud. Works behind strict firewalls and proxies. |
| **Statefulness** | **Stateful**: Peers must be online to share latest state efficiently. | **Stateless**: Clients can come and go; state is always durable in the DB. |
| **Storage Strategy** | Single document (content field) with periodic sync. | **Tiered Storage**: Snapshots, History Segments, and Updates. |

**Choose `y-fire` if:**
- You need the lowest possible latency (gaming, collaborative drawing).
- You want to minimize database writes to near-zero during active sessions.
- You can manage the complexity of WebRTC signaling and TURN servers.

**Choose `y-cinder` if:**
- You need a stateless, serverless architecture that "just works" (e.g., enterprise environments).
- You prioritize data durability and history management over sub-100ms latency.
- You want to avoid the complexity and cost of maintaining STUN/TURN infrastructure.

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

### Events

The provider extends `ObservableV2` and emits the following events:

| Event | Payload | Description |
| :--- | :--- | :--- |
| `connection-error` | `{ code: string, message: string, error: Error }` | Emitted when a Firestore listener encounters an error. |
| `sync-failure` | `Error` | Emitted when initial sync fails after all retry attempts. |
| `save-rejected` | See below | Emitted when a local update **cannot** be persisted to Firestore. |

**`save-rejected` payload:**

```typescript
{
  code: 'document-too-large' | 'max-retries-exceeded';
  sizeBytes?: number;    // Present for document-too-large
  limitBytes?: number;   // Present for document-too-large (1MB)
  retries?: number;      // Present for max-retries-exceeded
  error: Error;          // The underlying error
  update: Uint8Array;    // The rejected Yjs update (for recovery)
}
```

**Example — handling oversized documents:**

```typescript
provider.on('save-rejected', (event) => {
  if (event.code === 'document-too-large') {
    console.error(`Document too large (${event.sizeBytes} bytes)`);
    // Recovery: save to IndexedDB, alert user, etc.
  }
});
```

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

**Evaluation: Production Ready (Context Dependent)**

`y-cinder` is designed to solve the specific problem of using Firestore as a Yjs backend. It successfully addresses common pitfalls like read/write costs and initial load performance through its tiered architecture.

However, users should evaluate their specific constraints:

- **Latency**: Firestore snapshot listeners typically have higher latency (500ms - 1s) compared to dedicated WebSocket servers (< 50ms). This makes `y-cinder` excellent for collaborative editing (docs, notes) but unsuitable for high-frequency real-time applications like gaming or cursor tracking.
- **Cost vs. Scale**: While `y-cinder` is highly optimized, every keystroke debounced to a write is still a Firestore operation. Documents with extreme concurrency (50+ active users simultaneously) may still incur significant costs or hit Firestore's write rate limits on specific index ranges.
- **Client-Side Maintenance**: Compaction tasks are distributed among clients. While this keeps the architecture "serverless," it means active clients must burn some CPU and bandwidth to maintain database health.
- **Storage Limits**: Firestore has a strict 1MB limit per document. While `y-cinder` chunks history segments, individual updates and the base snapshot must each fit within 1MB. If an update exceeds this limit, the provider emits a `save-rejected` event with `code: 'document-too-large'` and includes the rejected update for consumer recovery.

## Contributors

- **[vrwarp](https://github.com/vrwarp)**

Original work by **[podraven](https://github.com/podraven)**.

## License

This project is licensed under the MIT License. Please include copies of the [y-fire license](https://github.com/podraven/y-fire/blob/main/LICENSE) when reusing or extending this code.
