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

metrics_output=$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" exec "$VLAGENT_POD" -- \
    wget -q -O - 'http://localhost:8429/metrics' 2>/dev/null || echo '')

echo "$metrics_output" >"$E2E_RESULTS_DIR/vlagent_metrics.txt"

lines_ingested=$(echo "$metrics_output" | grep -E '^vl_lines_ingested_total' | awk '{print $2}' || echo 0)
blocks_sent=$(echo "$metrics_output" | grep -E '^vm_remote_write_blocks_pushed_total' | awk '{print $2}' || echo 0)
blocks_failed=$(echo "$metrics_output" | grep -E '^vm_remote_write_blocks_failed_total' | awk '{print $2}' || echo 0)
dropped_total=$(echo "$metrics_output" | grep -E '^vl_lines_dropped_total' | awk '{print $2}' || echo 0)

echo "Vlagent metrics: ingested=$lines_ingested sent=$blocks_sent failed=$blocks_failed dropped=$dropped_total"

if (( lines_ingested == 0 )); then
    echo "ERROR: vlagent reports 0 ingested lines" >&2
    kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" logs "$VLAGENT_POD" --tail=20 >&2 || true
    exit 1
fi

echo "Vlagent successfully ingested $lines_ingested lines"

python3 - "$lines_ingested" "$blocks_sent" "$blocks_failed" >"$E2E_RESULTS_DIR/result.json" <<'PY'
import json
import sys

lines_ingested = int(sys.argv[1])
blocks_sent = int(sys.argv[2])
blocks_failed = int(sys.argv[3])

passed = lines_ingested >= 10

result = {
    "scenario": "kind-vlagent",
    "policy": "raw",
    "passed": passed,
    "expected_count": 10,
    "actual_count": lines_ingested,
    "missing_count": max(0, 10 - lines_ingested),
    "duplicate_count": 0,
    "extra_count": 0,
    "order_violations": 0,
    "null_field_violations": 0,
    "source_checked": False,
    "source_passed": None,
    "reason": None if passed else f"vlagent ingested {lines_ingested} lines, expected >= 10",
    "compare_keys": [],
    "identity_keys": [],
    "expected_preview": [],
    "actual_preview": [],
    "missing_preview": [],
    "duplicate_preview": [],
    "extra_preview": [],
    "null_field_preview": [],
    "vlagent_lines_ingested": lines_ingested,
    "vlagent_blocks_sent": blocks_sent,
    "vlagent_blocks_failed": blocks_failed,
}
print(json.dumps(result, indent=2))
PY
