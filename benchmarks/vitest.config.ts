/**
 * Vitest config for the performance benchmark suite.
 *
 * Kept separate from the default test run: benchmarks are pure-Yjs (no
 * Firebase emulator required) but deliberately heavy, simulating documents
 * with thousands of accumulated edits.
 *
 * Run with: npm run bench
 */
import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        include: ['benchmarks/**/*.bench.ts'],
        testTimeout: 300_000,
        fileParallelism: false,
    },
});
