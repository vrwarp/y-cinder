import * as Y from 'yjs';

const doc1 = new Y.Doc();
doc1.getArray('test').insert(0, ['A']);
const update1 = Y.encodeStateAsUpdate(doc1); // clock 0..1 (client X: 0)

doc1.getArray('test').insert(1, ['B']);
const update2 = Y.encodeStateAsUpdate(doc1); // clock 0..2
// wait, we need update2 to be JUST the delta:
// so we need a second doc

const docA = new Y.Doc();
docA.clientID = 123;
docA.getArray('test').insert(0, ['A']);
const u1 = Y.encodeStateAsUpdate(docA);
const sv1 = Y.encodeStateVector(docA);

docA.getArray('test').insert(1, ['B']);
const u2 = Y.encodeStateAsUpdate(docA, sv1);

console.log('u1 length:', u1.length);
console.log('u2 length:', u2.length);

// Now docB only gets u2
const docB = new Y.Doc();
docB.clientID = 456;
Y.applyUpdate(docB, u2);

console.log('docB array length:', docB.getArray('test').length);
console.log('docB pending missing:', (docB.store as any).pendingStructs?.missing);

// Merge u2 with an empty update (simulating compaction merging with empty snapshot)
const merged = Y.mergeUpdates([new Uint8Array([0,0]), u2]);

const docC = new Y.Doc();
Y.applyUpdate(docC, merged);
console.log('docC pending missing after mergeUpdate:', (docC.store as any).pendingStructs?.missing);

// Does encodeStateAsUpdate on docB include the pending structs?
const docB_encoded = Y.encodeStateAsUpdate(docB);
const docD = new Y.Doc();
Y.applyUpdate(docD, docB_encoded);
console.log('docD pending missing after encodeStateAsUpdate:', (docD.store as any).pendingStructs?.missing);

