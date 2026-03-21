import * as Y from 'yjs';

export const extractYDocState = (doc: Y.Doc): any => {
    const res: any = {};
    for (const [name, type] of doc.share.entries()) {
        if (type.constructor.name !== 'AbstractType') {
            try {
                res[name] = (type as any).toJSON ? (type as any).toJSON() : undefined;
            } catch (e) {
                res[name] = '<unresolvable: pending structs>';
            }
            continue;
        }

        let isText = false;
        let isArray = false;
        let isMap = false;
        for (const [, items] of doc.store.clients) {
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

        try {
            if (isMap) res[name] = doc.getMap(name).toJSON();
            else if (isText) res[name] = doc.getText(name).toJSON();
            else if (isArray) res[name] = doc.getArray(name).toJSON();
            else {
                res[name] = doc.getMap(name).toJSON(); // default fallback
            }
        } catch (e) {
            res[name] = '<unresolvable: pending structs>';
        }
    }

    // Extract pending structs
    let pendingStructsCount = 0;
    const pendingStructsPreview: any[] = [];
    const missingDeps: Array<{client: number, clock: number}> = [];

    if ((doc.store as any).pendingStructs) {
        const pending = (doc.store as any).pendingStructs;
        
        if (pending.missing && pending.missing instanceof Map) {
            for (const [client, clock] of pending.missing.entries()) {
                missingDeps.push({ client, clock });
            }
        }

        if (pending.update) {
        try {
            // Decode the pending update buffer to get actual structs
            const pendingDecoded = Y.decodeUpdate((doc.store as any).pendingStructs.update);

            pendingStructsCount = pendingDecoded.structs.length;

            for (let i = 0; i < Math.min(50, pendingStructsCount); i++) {
                const struct = pendingDecoded.structs[i];
                pendingStructsPreview.push({
                    client: struct.id.client,
                    clock: struct.id.clock,
                    parentSub: struct.parentSub,
                    class: struct.constructor.name,
                    deleted: struct.deleted || false
                });
            }
        } catch (e) {
            console.error("Failed to decode pending structs:", e);
        }
    }
}

    if (pendingStructsCount > 0 || missingDeps.length > 0) {
        res.__pendingStructs = {
            count: pendingStructsCount,
            preview: pendingStructsPreview,
            missingDependencies: missingDeps,
            note: "These operations are trapped in the pending queue due to missing dependencies from a base document."
        };
    }

    return res;
};

export const getUint8Array = (value: any): Uint8Array | null => {
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

// Validate a Yjs blob — returns error message or null
export const validateBlob = (blobData: any): string | null => {
    try {
        const uint8Arr = getUint8Array(blobData);
        if (!uint8Arr || uint8Arr.length === 0) return 'Empty or missing blob';
        Y.decodeUpdate(uint8Arr);
        return null; // valid
    } catch (err: any) {
        return err.message || 'Invalid Yjs update';
    }
};

export const formatDataForDisplay = (data: any, structLimit = Infinity): string => {
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

                // Calculate external dependencies
                const provided = new Map<number, { min: number, max: number }>();
                for (const s of decoded.structs) {
                    if (s.id && s.id.client !== undefined && s.id.clock !== undefined) {
                        const { client, clock } = s.id;
                        const len = s.length || 1;
                        const range = provided.get(client) || { min: clock, max: clock + len - 1 };
                        range.min = Math.min(range.min, clock);
                        range.max = Math.max(range.max, clock + len - 1);
                        provided.set(client, range);
                    }
                }

                const required = new Set<string>();
                const addReq = (id: any) => {
                    if (id && typeof id === 'object' && id.client !== undefined && id.clock !== undefined) {
                         required.add(`${id.client}:${id.clock}`);
                    }
                };

                for (const s of decoded.structs) {
                     addReq(s.left);
                     addReq(s.right);
                     addReq(s.origin);
                     addReq(s.rightOrigin);
                     if (s.parent && s.parent.constructor && s.parent.constructor.name === 'ID') {
                         addReq(s.parent);
                     }
                }

                const externalReqs: Array<{client: number, clock: number}> = [];
                for (const req of required) {
                    const [cStr, timeStr] = req.split(':');
                    const c = Number(cStr);
                    const t = Number(timeStr);
                    const p = provided.get(c);
                    // If it's not provided by this update at all, or the required clock is outside the provided range
                    if (!p || t < p.min || t > p.max) {
                         externalReqs.push({ client: c, clock: t });
                    }
                }

                const res: any = {
                    __yjs_update_bytes: uint8Arr.length,
                    __external_dependencies: externalReqs.length > 0 ? externalReqs : 'None (Self-contained)',
                    decoded: {
                        ...decoded,
                        structs: structsToDisplay
                    }
                };

                if (truncated) {
                    res.decoded.__warning__ = `Showing ${structLimit} of ${decoded.structs.length} structs. Click 'Load Next 250 Structs' to view more, or download JSON to view all.`;
                }

                return sanitizeYjsTypes(res);
            } catch (e: any) {
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

export const sanitizeYjsTypes = (obj: any, seen = new WeakSet()): any => {
    if (obj === null || typeof obj !== 'object') return obj;
    if (obj instanceof Date) return obj.toISOString();
    if (obj instanceof Uint8Array) return `[Uint8Array ${obj.length}]`;
    if (seen.has(obj)) return '[Circular]';
    seen.add(obj);

    if (Array.isArray(obj)) {
        return obj.map(item => sanitizeYjsTypes(item, seen));
    }

    const cName = obj.constructor ? obj.constructor.name : 'Object';
    const isYjsType = cName === 'AbstractType' || 
                      (cName.startsWith('Y') && typeof obj.toJSON === 'function') ||
                      (obj instanceof Y.AbstractType);

    const res: any = {};
    
    // Add class name for context so the debugger knows what type this is
    if (cName !== 'Object' && cName !== 'Array') {
        res.__class = cName;
    }

    if (isYjsType) {
        res.__debugger_note = "Raw internal state stringified to prevent Yjs warnPrematureAccess out-of-document exceptions.";
    }

    // Explicitly iterate over own properties instead of returning the object directly.
    // This strips out prototype methods like toJSON() so JSON.stringify won't call them,
    // while still giving the user full visibility into the raw internal fields!
    for (const key of Object.keys(obj)) {
        res[key] = sanitizeYjsTypes(obj[key], seen);
    }
    
    return res;
};

