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

tsbs_output=$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" exec "$VLAGENT_POD" -- \
    wget -q -O - 'http://localhost:8429/api/v1/status/tsbs?pick=5m' 2>/dev/null || echo '{"total_series":0}')

total_series=$(echo "$tsbs_output" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('total_series', 0))" 2>/dev/null || echo 0)

if (( total_series == 0 )); then
    echo "ERROR: vlagent reports 0 total series (no data ingested)" >&2
    echo "Vlagent debug output:" >&2
    kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" logs "$VLAGENT_POD" --tail=20 >&2 || true
    exit 1
fi

echo "Vlagent successfully ingested $total_series series in the last 5 minutes"

kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" exec "$VLAGENT_POD" -- \
    wget -q -O - 'http://localhost:8429/api/v1/series/count' >"$E2E_RESULTS_DIR/vlagent_series.json" 2>/dev/null || true

echo "$total_series" >"$E2E_RESULTS_DIR/vlagent_total_series.txt"

python3 - "$total_series" >"$E2E_RESULTS_DIR/result.json" <<'PY'
import json
import sys

total_series = int(sys.argv[1])

result = {
    "scenario": "kind-vlagent",
    "policy": "raw",
    "passed": total_series > 0,
    "expected_count": 0,
    "actual_count": total_series,
    "missing_count": 0,
    "duplicate_count": 0,
    "extra_count": 0,
    "order_violations": 0,
    "null_field_violations": 0,
    "source_checked": False,
    "source_passed": None,
    "source_missing_count": 0,
    "source_duplicate_count": 0,
    "source_extra_count": 0,
    "source_null_field_violations": 0,
    "reason": None if total_series > 0 else "vlagent reported 0 series",
    "compare_keys": [],
    "identity_keys": [],
    "expected_preview": [],
    "actual_preview": [],
    "missing_preview": [],
    "duplicate_preview": [],
    "extra_preview": [],
    "null_field_preview": [],
    "vlagent_series": total_series,
}
print(json.dumps(result, indent=2))
PY
