#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

python3 - <<'PY' >"$E2E_RESULTS_DIR/expected_rows.json"
import json

rows = [
    {
        "scenario": "compose-esql-input-oracle",
        "source_id": "elasticsearch-esql",
        "event_id": f"compose-esql-input-oracle:{i:04d}",
        "seq": i,
        "message": f"oracle event {i:04d}",
        "status": 200 + i,
    }
    for i in range(1, 6)
]
print(json.dumps(rows, indent=2))
PY

wait_for_file "$E2E_RESULTS_DIR/captured.ndjson" 30

docker compose -p "memagent-${SCENARIO_ID}" -f "$SCENARIO_DIR/compose.yaml" exec -T workload \
    sh -lc "curl -fsS 'http://elasticsearch:9200/e2e_logs/_search?size=10&sort=seq:asc&_source=scenario,source_id,event_id,seq,message,status'" \
    >"$E2E_RESULTS_DIR/source-search.json"

python3 - <<'PY' "$E2E_RESULTS_DIR/source-search.json" "$E2E_RESULTS_DIR/source_rows.json"
import json
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
output = pathlib.Path(sys.argv[2])
payload = json.loads(source.read_text(encoding="utf-8"))
rows = [hit["_source"] for hit in payload["hits"]["hits"]]
output.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
