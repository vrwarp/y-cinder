import React, { useState } from 'react';

export const PendingStructsBanner = ({ pendingStructs, theme }: any) => {
    const [showPreview, setShowPreview] = useState(false);

    if (!pendingStructs) return null;

    return (
        <div style={{
            background: '#fff3cd',
            border: '1px solid #ffe69c',
            color: '#664d03',
            padding: '8px 12px',
            borderRadius: '4px',
            marginBottom: '12px',
            fontSize: '12px'
        }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <strong>⚠️ Pending Structs Detected ({pendingStructs.count})</strong>
                <button
                    onClick={() => setShowPreview(!showPreview)}
                    style={{
                        background: 'transparent',
                        border: 'none',
                        color: '#664d03',
                        cursor: 'pointer',
                        fontSize: '11px',
                        textDecoration: 'underline'
                    }}
                >
                    {showPreview ? 'Hide Details' : 'Show Details'}
                </button>
            </div>
            <div style={{ marginTop: '4px' }}>{pendingStructs.note}</div>

            {showPreview && (
                <div style={{
                    marginTop: '8px',
                    maxHeight: '150px',
                    overflowY: 'auto',
                    background: 'rgba(255,255,255,0.7)',
                    padding: '6px',
                    borderRadius: '4px',
                    border: '1px solid rgba(0,0,0,0.05)'
                }}>
                    <div style={{ fontWeight: 'bold', marginBottom: '4px', fontSize: '11px' }}>First {pendingStructs.preview.length} structs:</div>
                    {pendingStructs.preview.map((p: any, idx: number) => (
                        <div key={idx} style={{ fontFamily: 'monospace', fontSize: '10px', borderBottom: '1px solid rgba(0,0,0,0.03)', padding: '2px 0' }}>
                            {p.class} | client: {p.client} | clock: {p.clock} | parentSub: {p.parentSub || 'null'} | deleted: {p.deleted ? 'yes' : 'no'}
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};
