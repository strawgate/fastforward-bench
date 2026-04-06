#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

python3 - <<'PY' >"$E2E_RESULTS_DIR/expected_rows.json"
import json
rows = [
    {
        "scenario": "compose-memcached",
        "source_id": "memcached-verbose",
        "event_id": f"compose-memcached:{i:04d}",
        "seq": i,
        "command": "set",
        "key": f"compose-memcached:{i:04d}",
    }
    for i in range(1, 4)
]
print(json.dumps(rows, indent=2))
PY

docker compose -p "memagent-${SCENARIO_ID}" -f "$SCENARIO_DIR/compose.yaml" exec -T workload sh -lc '
for i in 1 2 3; do
  key="compose-memcached:$(printf "%04d" "$i")"
  payload="value-$(printf "%04d" "$i")"
  len=${#payload}
  printf "set ${key} 0 0 ${len}\r\n${payload}\r\n" | nc memcached 11211 >/dev/null
done
'

sleep 3

python3 "$REPO_ROOT/tests/e2e/lib/source_evidence.py" \
    --mode memcached-verbose \
    --input "$E2E_LOG_DIR/memcached.log" \
    --output "$E2E_RESULTS_DIR/source_rows.json" \
    --scenario "$SCENARIO_ID" \
    --source-id "memcached-verbose"
