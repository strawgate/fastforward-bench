#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

python3 - <<'PY' >"$E2E_RESULTS_DIR/expected_rows.json"
import json
rows = [
    {
        "scenario": "compose-redis",
        "source_id": "redis-monitor",
        "event_id": f"compose-redis:{i:04d}",
        "command": "SET",
        "key": f"compose-redis:{i:04d}",
    }
    for i in range(1, 4)
]
print(json.dumps(rows, indent=2))
PY

wait_for_file "$E2E_LOG_DIR/redis-monitor.log" 30 || true
sleep 2

docker compose -p "memagent-${SCENARIO_ID}" -f "$SCENARIO_DIR/compose.yaml" exec -T workload \
    sh -lc 'for i in 1 2 3; do redis-cli -h redis SET "compose-redis:$(printf "%04d" "$i")" "value-$(printf "%04d" "$i")" >/dev/null; done'

sleep 3
