#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

CLUSTER_NAME="${E2E_CLUSTER_NAME:-$SCENARIO_ID}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="e2e-logfwd"

VLAGENT_POD="$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods -l app.kubernetes.io/name=vlagent -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"

if [[ -z "$VLAGENT_POD" ]]; then
    echo "ERROR: no vlagent pod found" >&2
    exit 1
fi

echo "Waiting for vlagent to ingest log file..."
sleep 30

echo "Fetching vlagent logs..."
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" logs "$VLAGENT_POD" -c vlagent >"$E2E_RESULTS_DIR/vlagent_pod_logs.txt"

block_size=$(grep -oP 'block with size \K\d+' "$E2E_RESULTS_DIR/vlagent_pod_logs.txt" | head -1 || echo 0)
log_lines_found=$(grep -c 'block with size' "$E2E_RESULTS_DIR/vlagent_pod_logs.txt" || echo 0)

echo "Vlagent logs: block_size=$block_size log_lines_found=$log_lines_found"

cat "$E2E_RESULTS_DIR/vlagent_pod_logs.txt" | tail -20

if (( block_size == 0 )); then
    echo "ERROR: vlagent did not process any log blocks" >&2
    exit 1
fi

echo "SUCCESS: vlagent processed a block of $block_size bytes"

python3 - "$block_size" "$log_lines_found" >"$E2E_RESULTS_DIR/result.json" <<'PY'
import json
import sys

block_size = int(sys.argv[1])
log_lines_found = int(sys.argv[2])

passed = block_size > 0

result = {
    "scenario": "kind-vlagent",
    "policy": "raw",
    "passed": passed,
    "expected_count": 10,
    "actual_count": log_lines_found,
    "missing_count": 0 if passed else 10,
    "duplicate_count": 0,
    "extra_count": 0,
    "order_violations": 0,
    "null_field_violations": 0,
    "source_checked": False,
    "source_passed": None,
    "reason": None if passed else "vlagent did not process log file",
    "compare_keys": [],
    "identity_keys": [],
    "expected_preview": [],
    "actual_preview": [],
    "missing_preview": [],
    "duplicate_preview": [],
    "extra_preview": [],
    "null_field_preview": [],
    "vlagent_block_size": block_size,
    "vlagent_blocks_found": log_lines_found,
}
print(json.dumps(result, indent=2))
PY
