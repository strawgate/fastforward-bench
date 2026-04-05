#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

python3 - <<'PY' >"$E2E_RESULTS_DIR/expected_rows.json"
import json
rows = [
    {"scenario": "compose-synthetic-multiline", "seq": 1, "level": "ERROR", "message": "alpha omega"},
    {"scenario": "compose-synthetic-multiline", "seq": 2, "level": "WARN", "message": "beta gamma delta"},
    {"scenario": "compose-synthetic-multiline", "seq": 3, "level": "INFO", "message": "single frame"},
]
print(json.dumps(rows, indent=2))
PY

docker compose -p "memagent-${SCENARIO_ID}" -f "$SCENARIO_DIR/compose.yaml" exec -T generator sh -lc '
cat > /logs/app.cri <<'"'"'EOF'"'"'
2024-01-15T10:30:00Z stdout P {"scenario":"compose-synthetic-multiline","seq":1,"level":"ERROR","message":"alpha 
2024-01-15T10:30:00Z stdout F omega"}
2024-01-15T10:30:01Z stdout P {"scenario":"compose-synthetic-multiline","seq":2,"level":"WARN","message":"beta 
2024-01-15T10:30:01Z stdout P gamma 
2024-01-15T10:30:01Z stdout F delta"}
2024-01-15T10:30:02Z stdout F {"scenario":"compose-synthetic-multiline","seq":3,"level":"INFO","message":"single frame"}
EOF
'

sleep 3
