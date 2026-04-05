#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

status="$(cat "$E2E_RESULTS_DIR/test.exitcode")"
printf '[]\n' >"$E2E_RESULTS_DIR/actual_rows.json"
python3 - <<'PY' "$E2E_RESULTS_DIR" "$status"
import json
import pathlib
import sys

results = pathlib.Path(sys.argv[1])
status = int(sys.argv[2])
result = {
    "scenario": "otlp-input-oracle",
    "policy": "external-oracle",
    "passed": status == 0,
    "expected_count": 1,
    "actual_count": 1 if status == 0 else 0,
}
(results / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
summary = [
    "## otlp-input-oracle",
    "",
    f"- Status: `{'PASS' if status == 0 else 'FAIL'}`",
    "- Mode: `cargo test ignored external oracle`",
    f"- Log: `{results / 'test.log'}`",
]
(results / "summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")
sys.exit(0 if status == 0 else 1)
PY
