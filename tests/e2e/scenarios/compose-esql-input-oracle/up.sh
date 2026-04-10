#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

compose up -d --wait --remove-orphans capture elasticsearch workload

docker compose -p "memagent-${SCENARIO_ID}" -f "$SCENARIO_DIR/compose.yaml" exec -T workload \
    sh -lc 'until curl -fsS "http://elasticsearch:9200/_cluster/health?wait_for_status=yellow&timeout=1s" >/dev/null; do sleep 1; done'

payload="$(python3 - <<'PY'
import json

rows = []
for i in range(1, 6):
    rows.append({"index": {"_index": "e2e_logs"}})
    rows.append(
        {
            "scenario": "compose-esql-input-oracle",
            "source_id": "elasticsearch-esql",
            "event_id": f"compose-esql-input-oracle:{i:04d}",
            "seq": i,
            "message": f"oracle event {i:04d}",
            "status": 200 + i,
        }
    )
print("\n".join(json.dumps(row) for row in rows) + "\n")
PY
)"

docker compose -p "memagent-${SCENARIO_ID}" -f "$SCENARIO_DIR/compose.yaml" exec -T workload \
    sh -lc "cat >/tmp/bulk.ndjson && curl -fsS -X POST 'http://elasticsearch:9200/_bulk?refresh=wait_for' -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/bulk.ndjson >/tmp/bulk-response.json" \
    <<EOF
${payload}
EOF

compose up -d memagent
