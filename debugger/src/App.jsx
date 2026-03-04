import React, { useState, useEffect } from 'react';
import { initializeApp, getApps, getApp, deleteApp } from 'firebase/app';
import { getFirestore, doc, getDoc, collection, getDocs, connectFirestoreEmulator } from 'firebase/firestore';
import { getAuth, signInWithEmailAndPassword, signOut, onAuthStateChanged } from 'firebase/auth';

function App() {
  const [docPath, setDocPath] = useState('test/doc1');
  const [projectId, setProjectId] = useState('demo-y-cinder');
  const [apiKey, setApiKey] = useState('');
  const [appId, setAppId] = useState('');
  const [authDomain, setAuthDomain] = useState('');
  const [useEmulator, setUseEmulator] = useState(true);
  const [db, setDb] = useState(null);

  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [user, setUser] = useState(null);
  const [authError, setAuthError] = useState(null);
  const [authInstance, setAuthInstance] = useState(null);

  const [baseDoc, setBaseDoc] = useState(null);
  const [updates, setUpdates] = useState([]);
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    let app;
    // Clean up existing apps
    if (getApps().length) {
       app = getApp();
       // Cannot easily reconfigure an app or emulator, so we just use the first initialized one
       // For a robust tool, you might need to handle deleting the app and re-initializing,
       // but for this demo, we'll try to delete and re-create.
       deleteApp(app).catch(console.error);
    }

    try {
        const config = { projectId };
        if (!useEmulator) {
           if (apiKey) config.apiKey = apiKey;
           if (appId) config.appId = appId;
           if (authDomain) config.authDomain = authDomain;
        }

        app = initializeApp(config);
        const firestore = getFirestore(app);

        if (useEmulator) {
           connectFirestoreEmulator(firestore, '127.0.0.1', 8080);
        }

        setDb(firestore);

        if (!useEmulator) {
            const auth = getAuth(app);
            setAuthInstance(auth);

            const unsubscribe = onAuthStateChanged(auth, (u) => {
                setUser(u);
            });

            return () => unsubscribe();
        } else {
            setAuthInstance(null);
            setUser(null);
        }

        setError(null);
    } catch (err) {
        console.error("Firebase init error:", err);
        setError(`Failed to initialize Firebase: ${err.message}`);
        setDb(null);
    }
  }, [projectId, apiKey, appId, authDomain, useEmulator]);

  const handleLogin = async () => {
      if (!authInstance) return;
      setAuthError(null);
      try {
          await signInWithEmailAndPassword(authInstance, email, password);
      } catch (err) {
          setAuthError(err.message);
      }
  };

  const handleLogout = async () => {
      if (!authInstance) return;
      try {
          await signOut(authInstance);
      } catch (err) {
          console.error(err);
      }
  };

  const loadData = async () => {
    if (!db) {
        setError("Database not initialized.");
        return;
    }
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

      <div style={{ marginBottom: '20px', padding: '15px', border: '1px solid #ccc', borderRadius: '5px' }}>
        <h3>Configuration</h3>
        <label>
          <input
            type="checkbox"
            checked={useEmulator}
            onChange={(e) => setUseEmulator(e.target.checked)}
          />
          Use Local Emulator (127.0.0.1:8080)
        </label>

        <div style={{ marginTop: '10px', display: 'grid', gridTemplateColumns: '150px 1fr', gap: '10px', maxWidth: '500px' }}>
            <label>Project ID:</label>
            <input value={projectId} onChange={e => setProjectId(e.target.value)} />

            {!useEmulator && (
                <>
                    <label>API Key:</label>
                    <input value={apiKey} onChange={e => setApiKey(e.target.value)} />

                    <label>App ID:</label>
                    <input value={appId} onChange={e => setAppId(e.target.value)} />

                    <label>Auth Domain:</label>
                    <input value={authDomain} onChange={e => setAuthDomain(e.target.value)} />
                </>
            )}
        </div>
      </div>

      {!useEmulator && (
          <div style={{ marginBottom: '20px', padding: '15px', border: '1px solid #ccc', borderRadius: '5px' }}>
            <h3>Authentication</h3>
            {user ? (
                <div>
                    <p>Logged in as: {user.email}</p>
                    <button onClick={handleLogout}>Sign Out</button>
                </div>
            ) : (
                <div style={{ display: 'grid', gridTemplateColumns: '150px 1fr', gap: '10px', maxWidth: '500px' }}>
                    <label>Email:</label>
                    <input type="email" value={email} onChange={e => setEmail(e.target.value)} />

                    <label>Password:</label>
                    <input type="password" value={password} onChange={e => setPassword(e.target.value)} />

                    <div></div>
                    <button onClick={handleLogin}>Sign In</button>
                </div>
            )}
            {authError && <div style={{ color: 'red', marginTop: '10px' }}>{authError}</div>}
          </div>
      )}

      <div style={{ marginBottom: '20px' }}>
        <input
          value={docPath}
          onChange={e => setDocPath(e.target.value)}
          placeholder="Firestore Document Path"
          style={{ padding: '5px', width: '300px', marginRight: '10px' }}
        />
        <button onClick={loadData} disabled={loading || !db || (!useEmulator && !user)} style={{ padding: '5px 10px' }}>
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
