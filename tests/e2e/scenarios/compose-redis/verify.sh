#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

wait_for_file "$E2E_RESULTS_DIR/captured.ndjson" 30
python3 "$REPO_ROOT/tests/e2e/lib/oracle.py" \
    --config "$SCENARIO_DIR/oracle.json" \
    --expected "$E2E_RESULTS_DIR/expected_rows.json" \
    --actual-ndjson "$E2E_RESULTS_DIR/captured.ndjson" \
    --results-dir "$E2E_RESULTS_DIR"
