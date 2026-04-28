#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

CLUSTER_NAME="${E2E_CLUSTER_NAME:-$SCENARIO_ID}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="e2e-logfwd"

python3 - <<'PY' >"$E2E_RESULTS_DIR/expected_rows.json"
import json
rows = [
    {
        "scenario": "kind-vlagent",
        "source_id": "log-generator",
        "event_id": f"kind-vlagent:{i:04d}",
        "seq": i,
        "level": level,
        "message": f"KIND_E2E_VLAGENT_MARKER_{i:03d}",
    }
    for i, level in enumerate(["INFO", "INFO", "WARN", "INFO", "ERROR", "INFO", "DEBUG", "INFO", "WARN", "INFO"], start=1)
]
print(json.dumps(rows, indent=2))
PY

VLAGENT_POD="$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods -l app.kubernetes.io/name=vlagent -o jsonpath='{.items[0].metadata.name}')"
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" wait pod/"$VLAGENT_POD" --for=condition=Ready --timeout=60s
sleep 10
