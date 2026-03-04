import React, { useState, useEffect } from 'react';
import { initializeApp, getApps, getApp, deleteApp } from 'firebase/app';
import { getFirestore, doc, getDoc, collection, getDocs, connectFirestoreEmulator } from 'firebase/firestore';
import { getAuth, GoogleAuthProvider, signInWithPopup, signOut, onAuthStateChanged } from 'firebase/auth';

function App() {
  const [docPath, setDocPath] = useState('test/doc1');
  const [projectId, setProjectId] = useState('demo-y-cinder');
  const [apiKey, setApiKey] = useState('');
  const [appId, setAppId] = useState('');
  const [authDomain, setAuthDomain] = useState('');
  const [pastedConfig, setPastedConfig] = useState('');
  const [useEmulator, setUseEmulator] = useState(true);
  const [db, setDb] = useState(null);

  const [user, setUser] = useState(null);
  const [authError, setAuthError] = useState(null);
  const [authInstance, setAuthInstance] = useState(null);

  const [baseDoc, setBaseDoc] = useState(null);
  const [updates, setUpdates] = useState([]);
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handlePasteConfig = () => {
      try {
          // Try to extract the object part if they pasted the whole code block
          const match = pastedConfig.match(/const\s+\w+\s*=\s*({[\s\S]*?});/);
          let jsonStr = match ? match[1] : pastedConfig;

          // Relaxed JSON parsing to handle unquoted keys and single quotes
          jsonStr = jsonStr
            .replace(/([{,]\s*)([a-zA-Z0-9_]+)\s*:/g, '$1"$2":') // Quote keys
            .replace(/:\s*'([^']*)'/g, ':"$1"') // Replace single quotes with double
            .replace(/,(\s*[}\]])/g, '$1'); // Remove trailing commas

          const config = JSON.parse(jsonStr);

          if (config.projectId) setProjectId(config.projectId);
          if (config.apiKey) setApiKey(config.apiKey);
          if (config.appId) setAppId(config.appId);
          if (config.authDomain) setAuthDomain(config.authDomain);

          setPastedConfig(''); // Clear after successful parse
      } catch (e) {
          setError("Failed to parse configuration. Please ensure it is valid JSON or a valid JavaScript object literal.");
          console.error("Parse error:", e);
      }
  };

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
          const provider = new GoogleAuthProvider();
          await signInWithPopup(authInstance, provider);
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

                    <div style={{ gridColumn: '1 / -1', marginTop: '10px', borderTop: '1px solid #eee', paddingTop: '10px' }}>
                        <label style={{ display: 'block', marginBottom: '5px' }}>Or paste Firebase config object:</label>
                        <textarea
                            value={pastedConfig}
                            onChange={e => setPastedConfig(e.target.value)}
                            placeholder={`const firebaseConfig = {\n  apiKey: "...",\n  authDomain: "...",\n  ...\n};`}
                            style={{ width: '100%', height: '100px', fontFamily: 'monospace', padding: '5px' }}
                        />
                        <button onClick={handlePasteConfig} style={{ marginTop: '5px' }}>Populate Fields</button>
                    </div>
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
                <div>
                    <button onClick={handleLogin}>Sign In with Google</button>
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
