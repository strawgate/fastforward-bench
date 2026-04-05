#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <scenario-id>" >&2
    exit 2
fi

SCENARIO_ID="$1"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCENARIO_DIR="$REPO_ROOT/tests/e2e/scenarios/$SCENARIO_ID"
E2E_RESULTS_DIR="${E2E_RESULTS_DIR:-$REPO_ROOT/tests/e2e/results/$SCENARIO_ID}"

if [[ ! -d "$SCENARIO_DIR" ]]; then
    echo "unknown e2e scenario: $SCENARIO_ID" >&2
    exit 2
fi

export REPO_ROOT
export SCENARIO_ID
export SCENARIO_DIR
export E2E_RESULTS_DIR

mkdir -p "$E2E_RESULTS_DIR"

cleanup() {
    if [[ -x "$SCENARIO_DIR/collect.sh" ]]; then
        "$SCENARIO_DIR/collect.sh" || true
    fi
    "$SCENARIO_DIR/down.sh" || true
}
trap cleanup EXIT

"$SCENARIO_DIR/up.sh"
"$SCENARIO_DIR/run_workload.sh"
"$SCENARIO_DIR/verify.sh"
