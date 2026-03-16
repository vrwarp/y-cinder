import * as Y from 'yjs';
import { mergeUpdatesAsync } from 'y-cinder';
import { getUint8Array, extractYDocState } from './yjs-utils';

export interface CompactionParams {
    useBaseDoc: boolean;
    baseDoc: any | null;
    history: any[];
    updates: any[];
    selectedUpdateIds: Set<string>;
}

export async function computeCompactedState({
    useBaseDoc,
    baseDoc,
    history,
    updates,
    selectedUpdateIds,
}: CompactionParams): Promise<any | null> {
    const allUpdates: Uint8Array[] = [];

    const addUpdate = (data: any) => {
        const uint8Arr = getUint8Array(data);
        if (uint8Arr) allUpdates.push(uint8Arr);
    };

    if (useBaseDoc && baseDoc && (baseDoc.content || baseDoc.segment || baseDoc.update)) {
        addUpdate(baseDoc.content || baseDoc.segment || baseDoc.update);
    }

    history.forEach(h => addUpdate(h.segment || h.update || h.content));

    updates.forEach(u => {
        if (selectedUpdateIds.has(u.id)) {
            addUpdate(u.update || u.segment || u.content);
        }
    });

    if (allUpdates.length === 0) {
        return null;
    }

    const merged = await mergeUpdatesAsync(allUpdates);
    const newDoc = new Y.Doc();
    Y.applyUpdate(newDoc, merged);
    return extractYDocState(newDoc);
}
