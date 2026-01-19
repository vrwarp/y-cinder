
import * as Y from 'yjs';

const doc1 = new Y.Doc();
doc1.clientID = 12345;
doc1.getText('test').insert(0, 'hello');

const update1 = Y.encodeStateAsUpdate(doc1);

try {
    // Attempt to decode
    console.log("Update length:", update1.byteLength);

    // Check if decodeUpdate is available and what it returns
    // @ts-ignore
    if (typeof Y.decodeUpdate === 'function') {
        // @ts-ignore
        const decoded = Y.decodeUpdate(update1);
        console.log("Decoded keys:", Object.keys(decoded));
        if (decoded.structs) {
            console.log("Structs count:", decoded.structs.length);
            decoded.structs.forEach((s: any) => {
                console.log("Struct:", s.id.client, s.id.clock, s.length);
            });
        }
    } else {
        console.log("Y.decodeUpdate not found");
    }

} catch (e) {
    console.error("Error:", e);
}
