import React, { useState } from 'react';
import { initializeApp } from 'firebase/app';
import { getFirestore, doc, getDoc, collection, getDocs, connectFirestoreEmulator } from 'firebase/firestore';

// Initialize Firebase
const app = initializeApp({ projectId: 'demo-y-cinder' });
const db = getFirestore(app);

// In a real app, you might want to configure this based on environment
try {
  connectFirestoreEmulator(db, '127.0.0.1', 8080);
} catch (e) {
  // Ignore error if already connected
}

function App() {
  const [docPath, setDocPath] = useState('test/doc1');
  const [baseDoc, setBaseDoc] = useState(null);
  const [updates, setUpdates] = useState([]);
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const docRef = doc(db, docPath);
      const docSnap = await getDoc(docRef);
      if (docSnap.exists()) {
        setBaseDoc(docSnap.data());
      } else {
        setBaseDoc(null);
      }

      const updatesRef = collection(db, `${docPath}/updates`);
      const updatesSnap = await getDocs(updatesRef);
      const updatesList = updatesSnap.docs.map(d => ({ id: d.id, ...d.data() }));
      setUpdates(updatesList);

      const historyRef = collection(db, `${docPath}/history`);
      const historySnap = await getDocs(historyRef);
      const historyList = historySnap.docs.map(d => ({ id: d.id, ...d.data() }));
      setHistory(historyList);
    } catch (e) {
      console.error(e);
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const renderData = (data) => {
      return JSON.stringify(data, (key, value) => {
        if (value && value.type === 'Buffer') {
          return `<Buffer ${value.data.length} bytes>`;
        }
        if (value && value instanceof Uint8Array) {
            return `<Uint8Array ${value.length} bytes>`;
        }
        // Handle Firestore Bytes
        if (value && typeof value === 'object' && value.toBase64) {
            return `<Bytes ${value.toUint8Array().length} bytes>`;
        }
        if (value && typeof value === 'object' && value.seconds !== undefined && value.nanoseconds !== undefined) {
             return new Date(value.seconds * 1000).toISOString();
        }
        return value;
      }, 2);
  }

  return (
    <div style={{ padding: '20px', fontFamily: 'sans-serif' }}>
      <h1>y-cinder Debugger</h1>
      <div style={{ marginBottom: '20px' }}>
        <input
          value={docPath}
          onChange={e => setDocPath(e.target.value)}
          placeholder="Firestore Document Path"
          style={{ padding: '5px', width: '300px', marginRight: '10px' }}
        />
        <button onClick={loadData} disabled={loading} style={{ padding: '5px 10px' }}>
          {loading ? 'Loading...' : 'Load'}
        </button>
      </div>

      {error && <div style={{ color: 'red', marginBottom: '20px' }}>Error: {error}</div>}

      <div style={{ display: 'flex', gap: '20px' }}>
        <div style={{ flex: 1 }}>
          <h2>Base Document</h2>
          <pre style={{ background: '#f0f0f0', padding: '10px', overflowX: 'auto', maxHeight: '600px', overflowY: 'auto' }}>
            {baseDoc ? renderData(baseDoc) : 'Not found'}
          </pre>
        </div>

        <div style={{ flex: 1 }}>
          <h2>History ({history.length})</h2>
          <div style={{ maxHeight: '600px', overflowY: 'auto' }}>
            {history.map(h => (
              <div key={h.id} style={{ border: '1px solid #ccc', padding: '10px', marginBottom: '10px' }}>
                <strong>ID: {h.id}</strong>
                <pre style={{ background: '#f0f0f0', padding: '10px', overflowX: 'auto', marginTop: '5px', whiteSpace: 'pre-wrap', wordWrap: 'break-word' }}>
                  {renderData(h)}
                </pre>
              </div>
            ))}
          </div>
        </div>

        <div style={{ flex: 1 }}>
          <h2>Updates ({updates.length})</h2>
          <div style={{ maxHeight: '600px', overflowY: 'auto' }}>
            {updates.map(u => (
              <div key={u.id} style={{ border: '1px solid #ccc', padding: '10px', marginBottom: '10px' }}>
                <strong>ID: {u.id}</strong>
                <pre style={{ background: '#f0f0f0', padding: '10px', overflowX: 'auto', marginTop: '5px', whiteSpace: 'pre-wrap', wordWrap: 'break-word' }}>
                  {renderData(u)}
                </pre>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
