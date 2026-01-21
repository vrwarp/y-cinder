# y-cinder

> [!NOTE]
> This project is a fork of [`y-fire`](https://github.com/podraven/y-fire) by [podraven](https://github.com/podraven).

**y-cinder** is a high-performance, serverless-ready Firestore provider for [Yjs](https://github.com/yjs/yjs). It enables real-time collaboration in your applications by synchronizing Yjs documents with Cloud Firestore.

Designed for efficiency and cost-optimization, y-cinder implements a smart tiered storage architecture to minimize Firestore reads and writes, making it ideal for high-traffic serverless deployments.

> [!IMPORTANT]
> **Built with Google Antigravity**
>
> This project leverages **Google Antigravity**, an agentic development platform that brings the IDE into the agent-first era. Antigravity provides a "Mission Control" for managing autonomous agents capable of planning, coding, and verifying complex software tasks.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Usage](#usage)
  - [Basic Setup](#basic-setup)
  - [Tiptap Integration](#tiptap-integration)
- [Configuration](#configuration)
- [Firestore Rules](#firestore-rules)
- [Contributors](#contributors)
- [License](#license)

## Features

- 🚀 **Serverless Optimized**: Built to work seamlessly in serverless environments.
- 📉 **Cost Efficient**: Granular, tiered storage (Snapshots, History, Updates) reduces Firestore costs.
- 🧹 **Auto-Compaction**: Automatically merges updates to maintain high read performance.
- 📦 **Subdocument Support**: Recursive handling of subdocuments within the same provider.
- ⚡ **Debounced Writes**: Smart buffering of updates to reduce write frequency.

## Architecture

y-cinder uses a unique tiered storage approach to handle Yjs updates:

1.  **Snapshots**: Base documents containing the full state.
2.  **History Segments**: Merged batches of updates for efficient retrieval.
3.  **Updates**: Incremental changes from clients.

This architecture allows y-cinder to provide fast load times and low latency while keeping Firestore billing in check.

## Installation

Since `y-cinder` is a specialized fork, it is not available on the public npm registry. Please install it directly from GitHub:

```bash
npm install git+https://github.com/vrwarp/y-cinder.git#HEAD
```

## Usage

### Basic Setup

Connect your Yjs document to Firestore using the `FireProvider`.

```typescript
import * as Y from "yjs";
import { FireProvider } from "y-cinder";
import { initializeApp } from "firebase/app";

// Initialize your Firebase app
const firebaseApp = initializeApp({ /* your config */ });

export const createProvider = (documentPath: string) => {
  const ydoc = new Y.Doc();

  const provider = new FireProvider({
    firebaseApp,
    ydoc,
    path: documentPath
  });

  return provider;
};
```

### Tiptap Integration

Easily integrate with the Tiptap editor using the Collaboration extension.

```typescript
import { Editor } from '@tiptap/core';
import StarterKit from '@tiptap/starter-kit';
import Collaboration from '@tiptap/extension-collaboration';
import { createProvider } from './your-provider-setup';

const provider = createProvider("documents/my-doc");

const editor = new Editor({
  extensions: [
    StarterKit.configure({
      // Disable default history to let Yjs handle it
      history: false,
    }),
    Collaboration.configure({
      document: provider.doc,
    })
  ],
});
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
- `subdocs`

## Contributors

This project is made possible by **[Pod Raven](https://podraven.com)**.

Special thanks to our contributors:
- **[deathg0d](https://github.com/deathg0d)**
- **[dorkysamurai](https://github.com/lachana)**
- **[arbitraryvector](https://x.com/arbitraryvector)**
- **[Benson Tsai](https://github.com/vrwarp)**

### Follow Us
- [![X (Twitter)](http://i.imgur.com/wWzX9uB.png) @pod_raven](https://x.com/pod_raven)
- [![X (Twitter)](http://i.imgur.com/wWzX9uB.png) @arbitraryvector](https://x.com/arbitraryvector)

## License

This project is licensed under the MIT License. Please include copies of the [y-fire license](https://github.com/podraven/y-fire/blob/main/LICENSE) when reusing or extending this code.
