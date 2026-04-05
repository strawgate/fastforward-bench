#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

python3 - <<'PY' >"$E2E_RESULTS_DIR/expected_rows.json"
import json
rows = [
    {"scenario": "compose-synthetic-rotation", "seq": i, "level": "INFO", "message": f"rotation-{i:04d}"}
    for i in range(1, 41)
]
print(json.dumps(rows, indent=2))
PY

docker compose -p "memagent-${SCENARIO_ID}" -f "$SCENARIO_DIR/compose.yaml" exec -T generator sh -lc '
rm -f /logs/app.log /logs/app.log.1
for i in $(seq 1 20); do
  printf "{\"scenario\":\"compose-synthetic-rotation\",\"seq\":%s,\"level\":\"INFO\",\"message\":\"rotation-%04d\"}\n" "$i" "$i" >> /logs/app.log
done
sleep 2
mv /logs/app.log /logs/app.log.1
for i in $(seq 21 40); do
  printf "{\"scenario\":\"compose-synthetic-rotation\",\"seq\":%s,\"level\":\"INFO\",\"message\":\"rotation-%04d\"}\n" "$i" "$i" >> /logs/app.log
done
'

sleep 5
