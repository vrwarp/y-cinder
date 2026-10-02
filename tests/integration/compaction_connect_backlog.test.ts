/**
 * Regression test: update documents already on the server when a client
 * connects must count toward the compaction threshold.
 *
 * Bug: automatic compaction is only triggered from the realtime updates
 * listener, using `snapshot.size` of a query that starts AFTER the last
 * update document read by initial sync. Every update document that existed
 * at connect time is outside that query and never counted, and initial sync
 * itself never triggers compaction. A client therefore only compacts if more
 * than `maxUpdatesThreshold` NEW update documents arrive while it is
 * connected.
 *
 * Documents whose sessions each write fewer than `maxUpdatesThreshold`
 * updates (e.g. short reading sessions) thus never compact: the updates
 * collection grows without bound and every initial sync downloads all of it.
 *
 * Contract (README: "Number of updates before triggering compaction";
 * plans/initial-design.md: "Any client detecting updates.length > 50"):
 * once the updates collection holds more than `maxUpdatesThreshold`
 * documents — including those present at connect time — a connected client
 * compacts it back down, without losing data.
 *
 * @file compaction_connect_backlog.test.ts
 */

import { describe, it, expect, afterEach } from 'vitest';
import { FireProvider } from '../../src/provider';
import * as Y from 'yjs';
import { collection, getDocs } from 'firebase/firestore';
import { setupEmulator } from '../utils/emulator';
import { waitForConditionTruthy } from '../utils/wait';
import { getStableDate } from '../unit/prng';

const THRESHOLD = 5;
const SESSIONS = 3;
const UPDATES_PER_SESSION = THRESHOLD - 1; // each session stays under the threshold

describe('Compaction threshold counts the backlog present at connect time', () => {
    let counter = 0;
    const live: FireProvider[] = [];

    afterEach(async () => {
        while (live.length > 0) {
            await live.pop()!.destroy();
        }
    });

    function open(app: any, path: string, ydoc: Y.Doc): FireProvider {
        const provider = new FireProvider({
            firebaseApp: app,
            ydoc,
            path,
            maxUpdatesThreshold: THRESHOLD,
            maxWaitTime: 50,
        });
        live.push(provider);
        return provider;
    }

    async function close(provider: FireProvider): Promise<void> {
        const idx = live.indexOf(provider);
        if (idx >= 0) live.splice(idx, 1);
        await provider.destroy();
    }

    /** Writes one map key and resolves once that update document is committed. */
    async function writeAndSave(provider: FireProvider, ydoc: Y.Doc, key: string): Promise<void> {
        const saved = new Promise<void>((resolve) => {
            const onSaved = () => {
                (provider as any).off('saved', onSaved);
                resolve();
            };
            (provider as any).on('saved', onSaved);
        });
        ydoc.getMap('m').set(key, key);
        await saved;
    }

    it('compacts an updates backlog accumulated by sessions that each wrote fewer than maxUpdatesThreshold updates', async () => {
        const { app, db } = await setupEmulator();
        const path = `integration-tests/compaction-connect-backlog-${getStableDate()}-${Date.now()}-${counter++}`;
        const updatesCol = collection(db, path, 'updates');
        const expectedKeys: string[] = [];

        // 1. Short sessions, one after another, each writing fewer update
        //    documents than the threshold (e.g. brief reading sessions).
        for (let s = 0; s < SESSIONS; s++) {
            const ydoc = new Y.Doc();
            const provider = open(app, path, ydoc);
            await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: `session ${s} should sync` });

            for (let i = 0; i < UPDATES_PER_SESSION; i++) {
                const key = `s${s}-k${i}`;
                await writeAndSave(provider, ydoc, key);
                expectedKeys.push(key);
            }
            await close(provider);
        }

        // 2. Another client connects to the document (whose updates
        //    collection now holds SESSIONS * UPDATES_PER_SESSION = 12 > 5
        //    documents) and makes a single edit.
        const ydoc = new Y.Doc();
        const provider = open(app, path, ydoc);
        await waitForConditionTruthy(() => provider.synced, { timeout: 30000, message: 'final client should sync' });
        for (const key of expectedKeys) {
            expect(ydoc.getMap('m').get(key)).toBe(key);
        }
        await writeAndSave(provider, ydoc, 'final');
        expectedKeys.push('final');

        // 3. Give the connected client time to compact. Compaction is
        //    fire-and-forget, so poll the server until the backlog is back
        //    under the threshold (or the window elapses).
        const deadline = Date.now() + 15000;
        let updatesCount = (await getDocs(updatesCol)).size;
        while (updatesCount > THRESHOLD && Date.now() < deadline) {
            await new Promise((r) => setTimeout(r, 250));
            updatesCount = (await getDocs(updatesCol)).size;
        }

        // The backlog must have been compacted: the updates collection held
        // 13 documents (> maxUpdatesThreshold) while a client was connected.
        expect(updatesCount).toBeLessThanOrEqual(THRESHOLD);

        // 4. Compaction must not lose data: a fresh client sees every edit.
        await close(provider);
        const checkDoc = new Y.Doc();
        const checker = open(app, path, checkDoc);
        await waitForConditionTruthy(() => checker.synced, { timeout: 30000, message: 'checker should sync' });
        for (const key of expectedKeys) {
            expect(checkDoc.getMap('m').get(key)).toBe(key);
        }
    }, 90000);
});
