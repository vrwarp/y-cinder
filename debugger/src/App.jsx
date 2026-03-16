import React, { useState, useEffect } from 'react';
import { initializeApp, getApps, getApp, deleteApp } from 'firebase/app';
import { getFirestore, doc, getDoc, collection, getDocs, connectFirestoreEmulator } from 'firebase/firestore';
import { getAuth, GoogleAuthProvider, signInWithPopup, signOut, onAuthStateChanged } from 'firebase/auth';
import { getStorage, connectStorageEmulator, ref as storageRef, getBytes } from 'firebase/storage';
import * as Y from 'yjs';

const extractYDocState = (doc) => {
  const res = {};
  for (const [name, type] of doc.share.entries()) {
    if (type.constructor.name !== 'AbstractType') {
      res[name] = type.toJSON ? type.toJSON() : undefined;
      continue;
    }

    let isText = false;
    let isArray = false;
    let isMap = false;
    for (const [client, items] of doc.store.clients) {
      for (const item of items) {
        if (item.parent === type) {
          if (item.parentSub !== null) {
            isMap = true;
          } else if (item.content && (item.content.constructor.name === 'ContentString' || item.content.constructor.name === 'ContentFormat')) {
            isText = true;
          } else {
            isArray = true;
          }
          break;
        }
      }
      if (isMap || isText || isArray) break;
    }

    if (isMap) res[name] = doc.getMap(name).toJSON();
    else if (isText) res[name] = doc.getText(name).toJSON();
    else if (isArray) res[name] = doc.getArray(name).toJSON();
    else {
      res[name] = doc.getMap(name).toJSON(); // default fallback
    }
  }
  return res;
};

const getUint8Array = (value) => {
  if (!value) return null;
  if (value instanceof Uint8Array) return value;
  if (value.type === 'Buffer' && value.data) return new Uint8Array(value.data);
  if (typeof value === 'object' && typeof value.toUint8Array === 'function') return value.toUint8Array();
  if (typeof value === 'object' && value.type === 'firestore/bytes/1.0' && typeof value.bytes === 'string') {
    const binaryString = atob(value.bytes);
    const uint8Arr = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
      uint8Arr[i] = binaryString.charCodeAt(i);
    }
    return uint8Arr;
  }
  return null;
};

const DataCardItem = ({ data, renderData, theme, preStyle }) => {
  const [showRaw, setShowRaw] = useState(false);
  const [structLimit, setStructLimit] = useState(250);

  // Parse fields
  const createdBy = data.createdBy;
  let createdAt = null;
  if (data.createdAt) {
    if (data.createdAt.seconds !== undefined) {
      createdAt = new Date(data.createdAt.seconds * 1000).toLocaleString();
    } else if (typeof data.createdAt === 'string') {
      createdAt = new Date(data.createdAt).toLocaleString();
    }
  }

  const clientIDs = data.clientIDs ? data.clientIDs.join(', ') : '';
  const clientClocks = data.clientClocks ? data.clientClocks.join(', ') : '';

  let yjsBytes = 0;
  let structCount = 0;
  const uint8Arr = getUint8Array(data.update || data.segment || data.content);
  if (uint8Arr) {
    yjsBytes = uint8Arr.length;
    try {
      const decoded = Y.decodeUpdate(uint8Arr);
      structCount = decoded.structs.length;
    } catch (e) { }
  }

  const hasGridData = createdAt || clientIDs || yjsBytes > 0;

  return (
    <div style={{ fontSize: '13px', color: theme.text }}>
      {hasGridData ? (
        <div style={{ display: 'grid', gridTemplateColumns: 'max-content 1fr', gap: '4px 12px', marginBottom: '8px' }}>
          {createdAt && (
            <>
              <strong style={{ color: theme.textMuted }}>Created:</strong>
              <span>{createdAt} {createdBy && <span>by <span style={{ fontFamily: 'monospace', background: theme.bg, padding: '2px 4px', borderRadius: '4px' }}>{createdBy}</span></span>}</span>
            </>
          )}

          {clientIDs && (
            <>
              <strong style={{ color: theme.textMuted }}>Clients:</strong>
              <span style={{ fontFamily: 'monospace' }}>{clientIDs} {clientClocks && `(Clocks: ${clientClocks})`}</span>
            </>
          )}

          {(yjsBytes > 0) && (
            <>
              <strong style={{ color: theme.textMuted }}>Payload:</strong>
              <span>
                Yjs Update ({yjsBytes} bytes) - {structCount} structs
                {(data.updateStoragePath || data.snapshotStoragePath) && (
                  <span style={{ background: theme.primary, color: '#fff', padding: '2px 6px', borderRadius: '10px', fontSize: '10px', marginLeft: '6px', fontWeight: 'bold' }}>OFFLOADED</span>
                )}
              </span>
            </>
          )}
        </div>
      ) : null}

      <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
        <button
          onClick={() => setShowRaw(!showRaw)}
          style={{ background: 'transparent', border: 'none', color: theme.primary, cursor: 'pointer', padding: 0, fontSize: '12px', fontWeight: 'bold' }}
        >
          {showRaw ? '▼ Hide Raw JSON' : '▶ Show Raw JSON'}
        </button>

        <button
          onClick={() => {
            try {
              const jsonStr = JSON.stringify(data, (key, value) => {
                const uintArr = getUint8Array(value);
                if (uintArr) {
                  try {
                    return { decodedFull: Y.decodeUpdate(uintArr) };
                  } catch (e) { return `<Bytes ${uintArr.length} bytes>`; }
                }
                return value;
              }, 2);
              const blob = new Blob([jsonStr], { type: 'application/json' });
              const url = URL.createObjectURL(blob);
              const a = document.createElement('a');
              a.href = url;
              a.download = `update_${data.id || 'data'}.json`;
              a.click();
              URL.revokeObjectURL(url);
            } catch (e) {
              console.error(e);
              alert("Failed to export full JSON.");
            }
          }}
          style={{ background: 'transparent', border: `1px solid ${theme.border}`, borderRadius: '4px', color: theme.textMuted, cursor: 'pointer', padding: '2px 8px', fontSize: '11px' }}
        >
          ⬇ Download JSON
        </button>

        {uint8Arr && (
          <button
            onClick={() => {
              const blob = new Blob([uint8Arr], { type: 'application/octet-stream' });
              const url = URL.createObjectURL(blob);
              const a = document.createElement('a');
              a.href = url;
              a.download = `update_${data.id || 'data'}.bin`;
              a.click();
              URL.revokeObjectURL(url);
            }}
            style={{ background: 'transparent', border: `1px solid ${theme.border}`, borderRadius: '4px', color: theme.textMuted, cursor: 'pointer', padding: '2px 8px', fontSize: '11px' }}
          >
            ⬇ Download Binary
          </button>
        )}
      </div>

      {showRaw && (
        <pre style={{
          ...preStyle,
          marginTop: '8px',
          padding: '8px',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-all',
          margin: 0
        }}>
          {renderData(data, structLimit)}
          {structCount > structLimit && (
            <div style={{ marginTop: '8px', textAlign: 'center' }}>
              <button
                onClick={() => setStructLimit(prev => prev + 250)}
                style={{ background: theme.primary, border: 'none', borderRadius: '4px', color: '#fff', cursor: 'pointer', padding: '6px 12px', fontSize: '13px', fontWeight: 'bold' }}
              >
                Load Next 250 Structs ({structLimit} / {structCount} shown)
              </button>
            </div>
          )}
        </pre>
      )}
    </div>
  );
};

function App() {
  const [docPath, setDocPath] = useState('test/doc1');
  const [projectId, setProjectId] = useState('demo-y-cinder');
  const [apiKey, setApiKey] = useState('');
  const [appId, setAppId] = useState('');
  const [authDomain, setAuthDomain] = useState('');
  const [pastedConfig, setPastedConfig] = useState('');
  const [useEmulator, setUseEmulator] = useState(true);
  const [db, setDb] = useState(null);
  const [storage, setStorage] = useState(null);

  const [user, setUser] = useState(null);
  const [authError, setAuthError] = useState(null);
  const [authInstance, setAuthInstance] = useState(null);

  const [baseDoc, setBaseDoc] = useState(null);
  const [updates, setUpdates] = useState([]);
  const [history, setHistory] = useState([]);
  const [combinedDocData, setCombinedDocData] = useState(null);
  const [selectedUpdateIds, setSelectedUpdateIds] = useState(new Set());
  const [corruptedIds, setCorruptedIds] = useState(new Map()); // id -> error message

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
    // Cleanup any existing apps in the background
    if (getApps().length) {
      getApps().forEach(a => deleteApp(a).catch(console.error));
    }

    try {
      const config = {
        projectId,
        storageBucket: useEmulator ? 'default-bucket' : `${projectId}.firebasestorage.app`
      };
      if (!useEmulator) {
        if (apiKey) config.apiKey = apiKey;
        if (appId) config.appId = appId;
        if (authDomain) config.authDomain = authDomain;
      }

      // Initialize with a unique name to prevent duplicate-app errors
      // during hot-reloads or rapid configuration changes
      app = initializeApp(config, `app-${Date.now()}`);

      const firestore = getFirestore(app);
      const storageInstance = getStorage(app);

      if (useEmulator) {
        connectFirestoreEmulator(firestore, '127.0.0.1', 8080);
        connectStorageEmulator(storageInstance, '127.0.0.1', 9199);
      }

      setDb(firestore);
      setStorage(storageInstance);

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
      setStorage(null);
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

  // Validate a Yjs blob — returns error message or null
  const validateBlob = (blobData) => {
    try {
      const uint8Arr = getUint8Array(blobData);
      if (!uint8Arr || uint8Arr.length === 0) return 'Empty or missing blob';
      Y.decodeUpdate(uint8Arr);
      return null; // valid
    } catch (err) {
      return err.message || 'Invalid Yjs update';
    }
  };

  const loadData = async () => {
    if (!db) {
      setError("Database not initialized.");
      return;
    }
    setLoading(true);
    setError(null);
    const newCorrupted = new Map();
    try {
      const docRef = doc(db, docPath);
      const docSnap = await getDoc(docRef);
      if (docSnap.exists()) {
        const data = docSnap.data();

        // Fetch from Cloud Storage if configured
        if (data.snapshotStoragePath && storage) {
          try {
            const sRef = storageRef(storage, data.snapshotStoragePath);
            const buffer = await getBytes(sRef);
            data.content = new Uint8Array(buffer);
          } catch (e) {
            console.error("Failed to load snapshot from storage", e);
            newCorrupted.set('__base__', "Failed to load snapshot from storage: " + e.message);
          }
        }

        setBaseDoc(data);
        // Validate base snapshot
        if (data.content) {
          const err = validateBlob(data.content);
          if (err) newCorrupted.set('__base__', err);
        }
      } else {
        setBaseDoc(null);
      }

      const updatesRef = collection(db, `${docPath}/updates`);
      const updatesSnap = await getDocs(updatesRef);
      const updatesList = updatesSnap.docs.map(d => ({ id: d.id, ...d.data() }));

      if (storage) {
        await Promise.all(updatesList.map(async (u) => {
          if (u.updateStoragePath && !u.update) {
            try {
              const sRef = storageRef(storage, u.updateStoragePath);
              const buffer = await getBytes(sRef);
              u.update = new Uint8Array(buffer);
            } catch (e) {
              console.error(`Failed to load update ${u.id} from storage`, e);
              newCorrupted.set(u.id, "Failed to load update from storage: " + e.message);
            }
          }
        }));
      }

      setUpdates(updatesList);
      // Validate each update
      updatesList.forEach(u => {
        if (!newCorrupted.has(u.id)) {
          const err = validateBlob(u.update);
          if (err) newCorrupted.set(u.id, err);
        }
      });
      // Select only non-corrupted updates by default
      setSelectedUpdateIds(new Set(updatesList.filter(u => !newCorrupted.has(u.id)).map(u => u.id)));

      const historyRef = collection(db, `${docPath}/history`);
      const historySnap = await getDocs(historyRef);
      const historyList = historySnap.docs.map(d => ({ id: d.id, ...d.data() }));
      setHistory(historyList);
      // Validate each history segment
      historyList.forEach(h => {
        const err = validateBlob(h.segment);
        if (err) newCorrupted.set(h.id, err);
      });

      setCorruptedIds(newCorrupted);
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
      const uint8Arr = getUint8Array(updateData);
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

    setCombinedDocData(extractYDocState(ydoc));
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

  const renderData = (data, structLimit = Infinity) => {
    return JSON.stringify(data, (key, value) => {
      // Handle different Buffer/Uint8Array shapes that represent Yjs updates
      const uint8Arr = getUint8Array(value);

      if (uint8Arr) {
        try {
          // Attempt to decode as a Yjs update to show its inner structure
          const decoded = Y.decodeUpdate(uint8Arr);

          let structsToDisplay = decoded.structs;
          let truncated = false;
          if (decoded.structs.length > structLimit) {
            structsToDisplay = decoded.structs.slice(0, structLimit);
            truncated = true;
          }

          const res = {
            __yjs_update_bytes: uint8Arr.length,
            decoded: {
              ...decoded,
              structs: structsToDisplay
            }
          };

          if (truncated) {
            res.decoded.__warning__ = `Showing ${structLimit} of ${decoded.structs.length} structs. Click 'Load Next 250 Structs' to view more, or download JSON to view all.`;
          }

          return res;
        } catch (e) {
          // If it fails to decode, fallback to just showing the length
          return `<Bytes ${uint8Arr.length} bytes (decode error: ${e.message})>`;
        }
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
          {/* Corruption Summary Banner */}
          {corruptedIds.size > 0 && (
            <div style={{ ...panelStyle, background: '#fef2f2', border: `1px solid ${theme.danger}`, marginBottom: 0 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
                <span style={{ fontSize: '18px' }}>⚠️</span>
                <h3 style={{ margin: 0, fontSize: '16px', color: theme.danger }}>Corrupted Documents Detected ({corruptedIds.size})</h3>
              </div>
              <div style={{ fontSize: '13px', color: '#991b1b' }}>
                {Array.from(corruptedIds.entries()).map(([id, err]) => (
                  <div key={id} style={{ padding: '4px 0', borderBottom: '1px solid #fecaca' }}>
                    <strong>{id === '__base__' ? 'Base Snapshot' : `ID: ${id}`}</strong> — {err}
                  </div>
                ))}
              </div>
              <div style={{ fontSize: '12px', color: '#9ca3af', marginTop: '8px' }}>
                Corrupted items are excluded from the combined state and marked below.
              </div>
            </div>
          )}

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '20px' }}>

            {/* Base Document */}
            <div style={{ ...panelStyle, display: 'flex', flexDirection: 'column', marginBottom: 0 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px', borderBottom: `1px solid ${theme.border}`, paddingBottom: '8px' }}>
                <h2 style={{ margin: 0, fontSize: '16px' }}>
                  Base Document
                  {corruptedIds.has('__base__') && <span style={{ background: theme.danger, color: '#fff', padding: '2px 6px', borderRadius: '10px', fontSize: '10px', marginLeft: '8px', fontWeight: 'bold' }}>CORRUPTED</span>}
                </h2>
              </div>
              <div style={{ flex: 1, overflowY: 'auto', maxHeight: '500px' }}>
                {baseDoc ? (
                  <>
                    {corruptedIds.has('__base__') && (
                      <div style={{ background: '#fef2f2', border: `1px solid #fecaca`, borderRadius: '4px', padding: '8px', marginBottom: '8px', fontSize: '12px', color: '#991b1b' }}>
                        ⚠ {corruptedIds.get('__base__')}
                      </div>
                    )}
                    <DataCardItem data={baseDoc} renderData={renderData} theme={theme} preStyle={preStyle} />
                  </>
                ) : (
                  <div style={{ padding: '20px', textAlign: 'center', color: theme.textMuted, fontSize: '14px', background: '#f9fafb', borderRadius: '4px' }}>Not loaded or not found</div>
                )}
              </div>
            </div>

            {/* History */}
            <div style={{ ...panelStyle, display: 'flex', flexDirection: 'column', marginBottom: 0 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px', borderBottom: `1px solid ${theme.border}`, paddingBottom: '8px' }}>
                <h2 style={{ margin: 0, fontSize: '16px' }}>
                  History <span style={{ background: '#e5e7eb', padding: '2px 8px', borderRadius: '10px', fontSize: '12px' }}>{history.length}</span>
                  {history.some(h => corruptedIds.has(h.id)) && <span style={{ background: theme.danger, color: '#fff', padding: '2px 6px', borderRadius: '10px', fontSize: '10px', marginLeft: '8px', fontWeight: 'bold' }}>{history.filter(h => corruptedIds.has(h.id)).length} CORRUPTED</span>}
                </h2>
              </div>
              <div style={{ flex: 1, overflowY: 'auto', maxHeight: '500px' }}>
                {history.length > 0 ? history.map(h => (
                  <div key={h.id} style={{
                    border: `1px solid ${corruptedIds.has(h.id) ? theme.danger : theme.border}`,
                    borderRadius: '6px',
                    padding: '12px',
                    marginBottom: '12px',
                    background: corruptedIds.has(h.id) ? '#fef2f2' : '#fff'
                  }}>
                    <div style={{ fontSize: '12px', fontWeight: 'bold', color: corruptedIds.has(h.id) ? theme.danger : theme.textMuted, marginBottom: '6px', display: 'flex', alignItems: 'center', gap: '6px' }}>
                      ID: {h.id}
                      {corruptedIds.has(h.id) && <span style={{ background: theme.danger, color: '#fff', padding: '1px 5px', borderRadius: '8px', fontSize: '10px' }}>⚠ CORRUPTED</span>}
                    </div>
                    {corruptedIds.has(h.id) && (
                      <div style={{ background: '#fff5f5', border: '1px solid #fecaca', borderRadius: '4px', padding: '6px', marginBottom: '6px', fontSize: '11px', color: '#991b1b' }}>
                        {corruptedIds.get(h.id)}
                      </div>
                    )}
                    <DataCardItem data={h} renderData={renderData} theme={theme} preStyle={preStyle} />
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
                  <h2 style={{ margin: 0, fontSize: '16px' }}>
                    Updates <span style={{ background: '#e5e7eb', padding: '2px 8px', borderRadius: '10px', fontSize: '12px' }}>{updates.length}</span>
                    {updates.some(u => corruptedIds.has(u.id)) && <span style={{ background: theme.danger, color: '#fff', padding: '2px 6px', borderRadius: '10px', fontSize: '10px', marginLeft: '8px', fontWeight: 'bold' }}>{updates.filter(u => corruptedIds.has(u.id)).length} CORRUPTED</span>}
                  </h2>
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
                      border: `1px solid ${corruptedIds.has(u.id) ? theme.danger : selectedUpdateIds.has(u.id) ? theme.primary : theme.border}`,
                      borderRadius: '6px',
                      padding: '12px',
                      marginBottom: '12px',
                      background: corruptedIds.has(u.id) ? '#fef2f2' : selectedUpdateIds.has(u.id) ? '#eff6ff' : '#fff',
                      transition: 'all 0.2s'
                    }}>
                      <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', marginBottom: '8px' }}>
                        <input
                          type="checkbox"
                          checked={selectedUpdateIds.has(u.id)}
                          onChange={() => toggleUpdateSelection(u.id)}
                          disabled={corruptedIds.has(u.id)}
                          style={{ width: '16px', height: '16px', cursor: corruptedIds.has(u.id) ? 'not-allowed' : 'pointer' }}
                        />
                        <span style={{ fontSize: '13px', fontWeight: 'bold', color: corruptedIds.has(u.id) ? theme.danger : theme.textMuted, display: 'flex', alignItems: 'center', gap: '6px' }}>
                          ID: {u.id}
                          {corruptedIds.has(u.id) && <span style={{ background: theme.danger, color: '#fff', padding: '1px 5px', borderRadius: '8px', fontSize: '10px' }}>⚠ CORRUPTED</span>}
                        </span>
                      </label>
                      {corruptedIds.has(u.id) && (
                        <div style={{ background: '#fff5f5', border: '1px solid #fecaca', borderRadius: '4px', padding: '6px', marginBottom: '6px', fontSize: '11px', color: '#991b1b' }}>
                          {corruptedIds.get(u.id)}
                        </div>
                      )}
                      <div style={{ opacity: selectedUpdateIds.has(u.id) && !corruptedIds.has(u.id) ? 1 : 0.6 }}>
                        <DataCardItem data={u} renderData={renderData} theme={theme} preStyle={preStyle} />
                      </div>
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
