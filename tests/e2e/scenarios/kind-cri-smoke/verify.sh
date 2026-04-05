#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

CLUSTER_NAME="${E2E_CLUSTER_NAME:-$SCENARIO_ID}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="e2e-logfwd"
PF_LOG="$E2E_RESULTS_DIR/port-forward.log"
PF_PID=""

cleanup_pf() {
    if [[ -n "$PF_PID" ]]; then
        kill "$PF_PID" >/dev/null 2>&1 || true
    fi
}
trap cleanup_pf EXIT

kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" port-forward svc/capture-receiver 18080:8080 >"$PF_LOG" 2>&1 &
PF_PID=$!
wait_for_http "http://127.0.0.1:18080/health" 30
curl -fsS "http://127.0.0.1:18080/stats" >"$E2E_RESULTS_DIR/stats.json"
curl -fsS "http://127.0.0.1:18080/dump" >"$E2E_RESULTS_DIR/captured.ndjson"

python3 "$REPO_ROOT/tests/e2e/lib/oracle.py" \
    --config "$SCENARIO_DIR/oracle.json" \
    --expected "$E2E_RESULTS_DIR/expected_rows.json" \
    --actual-ndjson "$E2E_RESULTS_DIR/captured.ndjson" \
    --results-dir "$E2E_RESULTS_DIR"
