import { initializeApp } from 'firebase/app';
import { getFirestore, doc, setDoc, addDoc, collection, connectFirestoreEmulator, Bytes } from 'firebase/firestore';

const app = initializeApp({ projectId: 'demo-y-cinder' });
const db = getFirestore(app);
connectFirestoreEmulator(db, '127.0.0.1', 8080);

async function populate() {
    const docPath = 'test/doc1';

    // Base doc
    await setDoc(doc(db, docPath), {
        snapshotStoragePath: 'gs://test/snap.bin',
        createdAt: new Date(),
        updatedAt: new Date(),
        size: 1024
    });

    // Updates
    await addDoc(collection(db, `${docPath}/updates`), {
        update: Bytes.fromUint8Array(new Uint8Array([1, 2, 3, 4, 5])),
        createdAt: new Date(),
        createdBy: 'client-A'
    });

    // History
    await addDoc(collection(db, `${docPath}/history`), {
        update: Bytes.fromUint8Array(new Uint8Array([10, 20, 30])),
        createdAt: new Date(),
        createdBy: 'client-B',
        clockStart: 0,
        clockEnd: 10
    });

    console.log('Done populating.');
    process.exit(0);
}

populate().catch(console.error);
