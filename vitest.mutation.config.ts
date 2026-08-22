/**
 * Vitest config for mutation testing.
 *
 * Restricted to `tests/unit/**`, which runs WITHOUT the Firestore emulator.
 * The integration suite is deliberately excluded: a mutation run executes the
 * suite thousands of times, and `scripts/test.sh` already has to restart the
 * emulator between three batches to avoid RESOURCE_EXHAUSTED from accumulated
 * in-memory state — a single emulator could not survive a mutation run, and
 * the wall-clock cost would be prohibitive.
 *
 * Consequence: source that is only exercised through the emulator (sync.ts,
 * compaction.ts, provider.ts, merge-worker.ts) reports as "no coverage"
 * rather than "survived". That distinction is the point — it says the fast
 * suite cannot verify that code, which is true and worth seeing.
 */
import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        include: ['tests/unit/**/*.test.ts'],
        fileParallelism: false,
        testTimeout: 30_000,
    },
});
