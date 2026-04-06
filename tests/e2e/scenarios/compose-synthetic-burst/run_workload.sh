#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

python3 - <<'PY' >"$E2E_RESULTS_DIR/expected_rows.json"
import json
rows = [
    {
        "scenario": "compose-synthetic-burst",
        "source_id": "burst-generator",
        "event_id": f"compose-synthetic-burst:{i:04d}",
        "seq": i,
        "level": "INFO",
        "message": f"burst-{i:04d}",
    }
    for i in range(1, 201)
]
print(json.dumps(rows, indent=2))
PY

docker compose -p "memagent-${SCENARIO_ID}" -f "$SCENARIO_DIR/compose.yaml" exec -T generator sh -lc '
rm -f /logs/burst.log
for i in $(seq 1 200); do
  printf "{\"scenario\":\"compose-synthetic-burst\",\"source_id\":\"burst-generator\",\"event_id\":\"compose-synthetic-burst:%04d\",\"seq\":%s,\"level\":\"INFO\",\"message\":\"burst-%04d\"}\n" "$i" "$i" "$i" >> /logs/burst.log
done
'

sleep 3
