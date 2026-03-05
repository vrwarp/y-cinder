import React, { useState, useEffect } from 'react';
import { initializeApp, getApps, getApp, deleteApp } from 'firebase/app';
import { getFirestore, doc, getDoc, collection, getDocs, connectFirestoreEmulator } from 'firebase/firestore';
import { getAuth, GoogleAuthProvider, signInWithPopup, signOut, onAuthStateChanged } from 'firebase/auth';
import * as Y from 'yjs';

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
  const [combinedDocData, setCombinedDocData] = useState(null);
  const [selectedUpdateIds, setSelectedUpdateIds] = useState(new Set());

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [sortOrder, setSortOrder] = useState('desc');
  const [clientFilter, setClientFilter] = useState('');

  const handlePasteConfig = () => {
      try {
          const match = pastedConfig.match(/const\s+\w+\s*=\s*({[\s\S]*?});/);
          let jsonStr = match ? match[1] : pastedConfig;

          jsonStr = jsonStr
            .replace(/([{,]\s*)([a-zA-Z0-9_]+)\s*:/g, '$1"$2":')
            .replace(/:\s*'([^']*)'/g, ':"$1"')
            .replace(/,(\s*[}\]])/g, '$1');

          const config = JSON.parse(jsonStr);

          if (config.projectId) setProjectId(config.projectId);
          if (config.apiKey) setApiKey(config.apiKey);
          if (config.appId) setAppId(config.appId);
          if (config.authDomain) setAuthDomain(config.authDomain);

          setPastedConfig('');
      } catch (e) {
          setError("Failed to parse configuration. Please ensure it is valid JSON or a valid JavaScript object literal.");
          console.error("Parse error:", e);
      }
  };

  useEffect(() => {
    let app;
    if (getApps().length) {
       app = getApp();
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
      setSelectedUpdateIds(new Set(updatesList.map(u => u.id)));

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

  useEffect(() => {
    if (!baseDoc && history.length === 0 && updates.length === 0) {
      setCombinedDocData(null);
      return;
    }

    const ydoc = new Y.Doc();
    const applyUpdateData = (updateData) => {
      if (!updateData) return;
      let uint8Arr;
      if (updateData.type === 'Buffer') {
        uint8Arr = new Uint8Array(updateData.data);
      } else if (updateData instanceof Uint8Array) {
        uint8Arr = updateData;
      } else if (updateData.toUint8Array) {
        uint8Arr = updateData.toUint8Array();
      }

      if (uint8Arr) {
        try {
          Y.applyUpdate(ydoc, uint8Arr);
        } catch (err) {
          console.error("Failed applying update to Y.Doc", err);
        }
      }
    };

    if (baseDoc && baseDoc.content) applyUpdateData(baseDoc.content);
    history.forEach(h => applyUpdateData(h.segment));
    updates.forEach(u => {
      if (selectedUpdateIds.has(u.id)) {
        applyUpdateData(u.update);
      }
    });

    setCombinedDocData(ydoc.toJSON());
  }, [baseDoc, history, updates, selectedUpdateIds]);

  const toggleUpdateSelection = (id) => {
    const newSet = new Set(selectedUpdateIds);
    if (newSet.has(id)) {
      newSet.delete(id);
    } else {
      newSet.add(id);
    }
    setSelectedUpdateIds(newSet);
  };

  const toggleAllUpdates = () => {
    if (selectedUpdateIds.size === updates.length) {
      setSelectedUpdateIds(new Set());
    } else {
      setSelectedUpdateIds(new Set(updates.map(u => u.id)));
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
  };

  // Modern UI Styles
  const theme = {
    bg: '#f3f4f6',
    panelBg: '#ffffff',
    border: '#e5e7eb',
    text: '#1f2937',
    textMuted: '#6b7280',
    primary: '#3b82f6',
    primaryHover: '#2563eb',
    danger: '#ef4444',
    success: '#10b981',
    headerBg: '#1f2937',
    headerText: '#ffffff',
    radius: '8px',
    shadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)'
  };

  const panelStyle = {
    background: theme.panelBg,
    borderRadius: theme.radius,
    boxShadow: theme.shadow,
    padding: '20px',
    border: `1px solid ${theme.border}`,
    marginBottom: '20px'
  };

  const inputStyle = {
    padding: '8px 12px',
    borderRadius: '4px',
    border: `1px solid ${theme.border}`,
    fontSize: '14px',
    width: '100%',
    boxSizing: 'border-box',
    marginBottom: '10px'
  };

  const btnStyle = {
    background: theme.primary,
    color: '#fff',
    border: 'none',
    padding: '10px 16px',
    borderRadius: '4px',
    cursor: 'pointer',
    fontWeight: 'bold',
    fontSize: '14px',
    transition: 'background 0.2s'
  };

  const preStyle = {
    background: '#f8fafc',
    padding: '12px',
    borderRadius: '4px',
    border: `1px solid ${theme.border}`,
    overflowX: 'auto',
    fontSize: '13px',
    fontFamily: 'monospace',
    color: '#334155'
  };

  return (
    <div style={{ background: theme.bg, minHeight: '100vh', color: theme.text, fontFamily: 'system-ui, -apple-system, sans-serif' }}>
      <header style={{ background: theme.headerBg, color: theme.headerText, padding: '16px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', boxShadow: '0 2px 4px rgba(0,0,0,0.1)' }}>
        <h1 style={{ margin: 0, fontSize: '20px', fontWeight: '600' }}>y-cinder Debugger</h1>
        {useEmulator ? (
          <span style={{ background: theme.success, padding: '4px 8px', borderRadius: '12px', fontSize: '12px', fontWeight: 'bold' }}>Emulator Mode</span>
        ) : (
          <span style={{ background: theme.primary, padding: '4px 8px', borderRadius: '12px', fontSize: '12px', fontWeight: 'bold' }}>Production Mode</span>
        )}
      </header>

      <div style={{ maxWidth: '1600px', margin: '0 auto', padding: '24px', display: 'grid', gridTemplateColumns: '300px 1fr', gap: '24px' }}>

        {/* Left Sidebar: Config & Controls */}
        <aside style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>

          <div style={panelStyle}>
            <h3 style={{ marginTop: 0, marginBottom: '16px', fontSize: '16px', borderBottom: `1px solid ${theme.border}`, paddingBottom: '8px' }}>Environment</h3>
            <label style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '16px', cursor: 'pointer', fontSize: '14px' }}>
              <input type="checkbox" checked={useEmulator} onChange={(e) => setUseEmulator(e.target.checked)} style={{ width: '16px', height: '16px' }} />
              Use Local Emulator
            </label>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
              <label style={{ fontSize: '12px', fontWeight: 'bold', color: theme.textMuted }}>Project ID</label>
              <input style={inputStyle} value={projectId} onChange={e => setProjectId(e.target.value)} />

              {!useEmulator && (
                  <>
                      <label style={{ fontSize: '12px', fontWeight: 'bold', color: theme.textMuted }}>API Key</label>
                      <input style={inputStyle} value={apiKey} onChange={e => setApiKey(e.target.value)} />

                      <label style={{ fontSize: '12px', fontWeight: 'bold', color: theme.textMuted }}>App ID</label>
                      <input style={inputStyle} value={appId} onChange={e => setAppId(e.target.value)} />

                      <label style={{ fontSize: '12px', fontWeight: 'bold', color: theme.textMuted }}>Auth Domain</label>
                      <input style={inputStyle} value={authDomain} onChange={e => setAuthDomain(e.target.value)} />

                      <div style={{ marginTop: '12px', borderTop: `1px solid ${theme.border}`, paddingTop: '12px' }}>
                          <label style={{ fontSize: '12px', fontWeight: 'bold', color: theme.textMuted, display: 'block', marginBottom: '4px' }}>Paste Config Object</label>
                          <textarea
                              value={pastedConfig}
                              onChange={e => setPastedConfig(e.target.value)}
                              placeholder={`const firebaseConfig = {\n  apiKey: "...",\n  ...\n};`}
                              style={{ ...inputStyle, height: '80px', fontFamily: 'monospace', resize: 'vertical' }}
                          />
                          <button onClick={handlePasteConfig} style={{ ...btnStyle, width: '100%', background: theme.textMuted }}>Parse Config</button>
                      </div>
                  </>
              )}
            </div>
          </div>

          {!useEmulator && (
            <div style={panelStyle}>
              <h3 style={{ marginTop: 0, marginBottom: '16px', fontSize: '16px', borderBottom: `1px solid ${theme.border}`, paddingBottom: '8px' }}>Authentication</h3>
              {user ? (
                  <div>
                      <p style={{ fontSize: '14px', marginBottom: '12px', wordBreak: 'break-all' }}>Logged in: <strong>{user.email}</strong></p>
                      <button onClick={handleLogout} style={{ ...btnStyle, background: theme.danger, width: '100%' }}>Sign Out</button>
                  </div>
              ) : (
                  <div>
                      <button onClick={handleLogin} style={{ ...btnStyle, width: '100%' }}>Sign In with Google</button>
                  </div>
              )}
              {authError && <div style={{ color: theme.danger, marginTop: '12px', fontSize: '13px' }}>{authError}</div>}
            </div>
          )}

          <div style={panelStyle}>
            <h3 style={{ marginTop: 0, marginBottom: '16px', fontSize: '16px', borderBottom: `1px solid ${theme.border}`, paddingBottom: '8px' }}>Document Loader</h3>
            <label style={{ fontSize: '12px', fontWeight: 'bold', color: theme.textMuted, display: 'block', marginBottom: '4px' }}>Firestore Document Path</label>
            <input
              style={inputStyle}
              value={docPath}
              onChange={e => setDocPath(e.target.value)}
              placeholder="e.g. test/doc1"
            />
            <button
              onClick={loadData}
              disabled={loading || !db || (!useEmulator && !user)}
              style={{ ...btnStyle, width: '100%', opacity: (loading || !db || (!useEmulator && !user)) ? 0.7 : 1 }}
            >
              {loading ? 'Loading...' : 'Load Document'}
            </button>
            {error && <div style={{ color: theme.danger, marginTop: '12px', fontSize: '13px', padding: '8px', background: '#fef2f2', borderRadius: '4px' }}>{error}</div>}
          </div>
        </aside>

        {/* Right Main Area: Data Viewer */}
        <main style={{ display: 'flex', flexDirection: 'column', gap: '24px', minWidth: 0 }}>

          {/* Top Row: Components View */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '20px' }}>

            {/* Base Document */}
            <div style={{ ...panelStyle, display: 'flex', flexDirection: 'column', marginBottom: 0 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px', borderBottom: `1px solid ${theme.border}`, paddingBottom: '8px' }}>
                <h2 style={{ margin: 0, fontSize: '16px' }}>Base Document</h2>
              </div>
              <div style={{ flex: 1, overflowY: 'auto', maxHeight: '500px' }}>
                {baseDoc ? (
                  <pre style={{ ...preStyle, margin: 0 }}>{renderData(baseDoc)}</pre>
                ) : (
                  <div style={{ padding: '20px', textAlign: 'center', color: theme.textMuted, fontSize: '14px', background: '#f9fafb', borderRadius: '4px' }}>Not loaded or not found</div>
                )}
              </div>
            </div>

            {/* History */}
            <div style={{ ...panelStyle, display: 'flex', flexDirection: 'column', marginBottom: 0 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px', borderBottom: `1px solid ${theme.border}`, paddingBottom: '8px' }}>
                <h2 style={{ margin: 0, fontSize: '16px' }}>History <span style={{ background: '#e5e7eb', padding: '2px 8px', borderRadius: '10px', fontSize: '12px' }}>{history.length}</span></h2>
              </div>
              <div style={{ flex: 1, overflowY: 'auto', maxHeight: '500px' }}>
                {history.length > 0 ? history.map(h => (
                  <div key={h.id} style={{ border: `1px solid ${theme.border}`, borderRadius: '6px', padding: '12px', marginBottom: '12px', background: '#fff' }}>
                    <div style={{ fontSize: '12px', fontWeight: 'bold', color: theme.textMuted, marginBottom: '6px' }}>ID: {h.id}</div>
                    <pre style={{ ...preStyle, margin: 0, padding: '8px', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                      {renderData(h)}
                    </pre>
                  </div>
                )) : (
                  <div style={{ padding: '20px', textAlign: 'center', color: theme.textMuted, fontSize: '14px', background: '#f9fafb', borderRadius: '4px' }}>No history segments</div>
                )}
              </div>
            </div>

            {/* Updates */}
            <div style={{ ...panelStyle, display: 'flex', flexDirection: 'column', marginBottom: 0 }}>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '12px', marginBottom: '16px', borderBottom: `1px solid ${theme.border}`, paddingBottom: '12px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <h2 style={{ margin: 0, fontSize: '16px' }}>Updates <span style={{ background: '#e5e7eb', padding: '2px 8px', borderRadius: '10px', fontSize: '12px' }}>{updates.length}</span></h2>
                  {updates.length > 0 && (
                    <button onClick={toggleAllUpdates} style={{ background: 'transparent', border: `1px solid ${theme.border}`, padding: '4px 8px', borderRadius: '4px', fontSize: '12px', cursor: 'pointer' }}>
                      {selectedUpdateIds.size === updates.length ? 'Deselect All' : 'Select All'}
                    </button>
                  )}
                </div>

                <div style={{ display: 'flex', gap: '8px' }}>
                  <select value={sortOrder} onChange={e => setSortOrder(e.target.value)} style={{ padding: '6px', borderRadius: '4px', border: `1px solid ${theme.border}`, fontSize: '13px', background: '#fff' }}>
                    <option value="desc">Newest First</option>
                    <option value="asc">Oldest First</option>
                  </select>
                  <input
                    value={clientFilter}
                    onChange={e => setClientFilter(e.target.value)}
                    placeholder="Filter Client ID..."
                    style={{ padding: '6px', borderRadius: '4px', border: `1px solid ${theme.border}`, fontSize: '13px', width: '100%', boxSizing: 'border-box' }}
                  />
                </div>
              </div>

              <div style={{ flex: 1, overflowY: 'auto', maxHeight: '450px' }}>
                {updates.length > 0 ? updates
                  .filter(u => {
                    if (!clientFilter) return true;
                    const filterLower = clientFilter.toLowerCase();
                    if (u.createdBy && u.createdBy.toLowerCase().includes(filterLower)) return true;
                    return false;
                  })
                  .sort((a, b) => {
                    const timeA = a.createdAt?.seconds || 0;
                    const timeB = b.createdAt?.seconds || 0;
                    return sortOrder === 'desc' ? timeB - timeA : timeA - timeB;
                  })
                  .map(u => (
                  <div key={u.id} style={{
                      border: `1px solid ${selectedUpdateIds.has(u.id) ? theme.primary : theme.border}`,
                      borderRadius: '6px',
                      padding: '12px',
                      marginBottom: '12px',
                      background: selectedUpdateIds.has(u.id) ? '#eff6ff' : '#fff',
                      transition: 'all 0.2s'
                    }}>
                    <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', marginBottom: '8px' }}>
                      <input
                        type="checkbox"
                        checked={selectedUpdateIds.has(u.id)}
                        onChange={() => toggleUpdateSelection(u.id)}
                        style={{ width: '16px', height: '16px', cursor: 'pointer' }}
                      />
                      <span style={{ fontSize: '13px', fontWeight: 'bold', color: theme.textMuted }}>ID: {u.id}</span>
                    </label>
                    <pre style={{ ...preStyle, margin: 0, padding: '8px', whiteSpace: 'pre-wrap', wordBreak: 'break-all', opacity: selectedUpdateIds.has(u.id) ? 1 : 0.6 }}>
                      {renderData(u)}
                    </pre>
                  </div>
                )) : (
                  <div style={{ padding: '20px', textAlign: 'center', color: theme.textMuted, fontSize: '14px', background: '#f9fafb', borderRadius: '4px' }}>No pending updates</div>
                )}
              </div>
            </div>

          </div>

          {/* Bottom Row: Combined Result */}
          <div style={{ ...panelStyle, background: '#f8fafc', border: `1px solid ${theme.primary}`, boxShadow: `0 4px 6px -1px rgba(59, 130, 246, 0.1)` }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px', borderBottom: `1px solid ${theme.border}`, paddingBottom: '12px' }}>
              <h2 style={{ margin: 0, fontSize: '18px', color: theme.primary }}>Fully Combined Document State</h2>
              <div style={{ fontSize: '13px', color: theme.textMuted }}>
                Applying: Base + History + {selectedUpdateIds.size} Selected Update(s)
              </div>
            </div>
            <pre style={{ ...preStyle, background: '#fff', maxHeight: '400px', border: `1px solid ${theme.border}` }}>
              {combinedDocData ? JSON.stringify(combinedDocData, null, 2) : 'No combined data (load a document first)'}
            </pre>
          </div>

        </main>
      </div>
    </div>
  );
}

export default App;
