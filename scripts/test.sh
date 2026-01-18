#!/bin/bash
# scripts/test.sh

# Capture all arguments passed to this script (e.g. specific test files)
TEST_ARGS="$@"

# If no arguments provided, run all tests
if [ -z "$TEST_ARGS" ]; then
  echo "Running all tests..."
else
  echo "Running specific tests: $TEST_ARGS"
fi

#  Run firebase emulators, passing the arguments INSIDE the quoted command string
#  This ensures vitest receives the file paths/options
firebase emulators:exec --project demo-test-project "npx vitest run --environment node --no-file-parallelism $TEST_ARGS"
