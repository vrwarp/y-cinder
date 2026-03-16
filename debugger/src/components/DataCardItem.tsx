import React, { useState } from 'react';
import * as Y from 'yjs';
import { getUint8Array } from '../utils/yjs-utils';
import { PendingStructsBanner } from './PendingStructsBanner';

export const DataCardItem = ({ data, renderData, theme, preStyle }: any) => {
    const [showRaw, setShowRaw] = useState(false);
    const [structLimit, setStructLimit] = useState(250);

    // Parse fields
    const createdBy = data.createdBy;
    let createdAt: string | null = null;
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
            <PendingStructsBanner pendingStructs={data.__pendingStructs} theme={theme} />
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
