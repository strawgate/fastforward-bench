#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

python3 - <<'PY' >"$E2E_RESULTS_DIR/expected_rows.json"
import json
rows = [
    {
        "scenario": "compose-nginx",
        "source_id": "nginx-access",
        "event_id": f"compose-nginx:{i:04d}",
        "seq": i,
        "method": "GET",
        "path": f"/e2e/compose-nginx/compose-nginx:{i:04d}",
        "status": 204,
    }
    for i in range(1, 6)
]
print(json.dumps(rows, indent=2))
PY

docker compose -p "memagent-${SCENARIO_ID}" -f "$SCENARIO_DIR/compose.yaml" exec -T workload \
    sh -lc 'for i in 1 2 3 4 5; do curl -fsS "http://nginx:8080/e2e/compose-nginx/compose-nginx:$(printf "%04d" "$i")" >/dev/null; done'

sleep 3

python3 "$REPO_ROOT/tests/e2e/lib/source_evidence.py" \
    --mode nginx-access \
    --input "$E2E_LOG_DIR/nginx.access.log" \
    --output "$E2E_RESULTS_DIR/source_rows.json" \
    --scenario "$SCENARIO_ID" \
    --source-id "nginx-access"
