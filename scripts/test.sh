#!/bin/bash
# scripts/test.sh
set -e

# Capture all arguments passed to this script (e.g. specific test files)
TEST_ARGS="$@"

# If no arguments provided, run all tests in batches
# WHY ARE WE BATCHING?
# Running all 160+ integration tests sequentially against a single emulator
# process accumulates immense internal transaction/snapshot state.
# By the end of the suite, large `runTransaction` compactions trigger
# `RESOURCE_EXHAUSTED` (gRPC message > 4MB) deep within the Firebase SDK
# because the emulator's in-memory representation becomes too bloated.
# Splitting the run into fragments forcibly kills and restarts the emulator
# between batches, completely clearing the JVM memory and state.
if [ -z "$TEST_ARGS" ]; then
  echo "Running all tests in batches to prevent emulator overload..."
  
  echo "--- RUNNING BATCH 1/3 ---"
  npx firebase emulators:exec --project demo-test-project "npx vitest run --environment node --no-file-parallelism --test-timeout=30000 --exclude 'debugger/**/*' --shard=1/3"

  echo "--- RUNNING BATCH 2/3 ---"
  npx firebase emulators:exec --project demo-test-project "npx vitest run --environment node --no-file-parallelism --test-timeout=30000 --exclude 'debugger/**/*' --shard=2/3"

  echo "--- RUNNING BATCH 3/3 ---"
  npx firebase emulators:exec --project demo-test-project "npx vitest run --environment node --no-file-parallelism --test-timeout=30000 --exclude 'debugger/**/*' --shard=3/3"
else
  echo "Running specific tests: $TEST_ARGS"
  npx firebase emulators:exec --project demo-test-project "npx vitest run --environment node --no-file-parallelism --test-timeout=30000 --exclude 'debugger/**/*' $TEST_ARGS"
fi
