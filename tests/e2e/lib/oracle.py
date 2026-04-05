#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare expected and actual e2e rows.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--expected", required=True)
    parser.add_argument("--actual-ndjson", required=True)
    parser.add_argument("--results-dir", required=True)
    return parser.parse_args()


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_ndjson(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not Path(path).exists():
        return rows
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def project_row(row: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    return {key: row.get(key) for key in keys}


def list_difference(expected: list[dict[str, Any]], actual: list[dict[str, Any]]) -> list[dict[str, Any]]:
    missing: list[dict[str, Any]] = []
    actual_counter = Counter(json.dumps(row, sort_keys=True) for row in actual)
    for row in expected:
        encoded = json.dumps(row, sort_keys=True)
        if actual_counter[encoded] > 0:
            actual_counter[encoded] -= 1
        else:
            missing.append(row)
    return missing


def main() -> None:
    args = parse_args()
    config = load_json(args.config)
    expected_rows = load_json(args.expected)
    actual_rows = load_ndjson(args.actual_ndjson)

    selector = config.get("selector") or {}
    selector_field = selector.get("field")
    selector_value = selector.get("value")
    if selector_field:
        actual_rows = [row for row in actual_rows if row.get(selector_field) == selector_value]

    compare_keys = config.get("compare_keys")
    if not compare_keys:
        all_keys = set()
        for row in expected_rows:
            all_keys.update(row.keys())
        compare_keys = sorted(all_keys)

    projected_expected = [project_row(row, compare_keys) for row in expected_rows]
    projected_actual = [project_row(row, compare_keys) for row in actual_rows]

    policy = config["policy"]
    order_key = config.get("order_key", "seq")
    order_violations = 0
    last_seen = None
    for row in projected_actual:
        current = row.get(order_key)
        if current is None:
            continue
        if last_seen is not None and current < last_seen:
            order_violations += 1
        last_seen = current

    missing_rows = list_difference(projected_expected, projected_actual)
    duplicate_count = max(0, len(projected_actual) - len({json.dumps(row, sort_keys=True) for row in projected_actual}))
    extra_count = max(0, len(projected_actual) - len(projected_expected))

    passed = False
    if policy == "exact_once_ordered":
        passed = projected_actual == projected_expected
    elif policy == "exact_once_unordered":
        passed = Counter(json.dumps(row, sort_keys=True) for row in projected_actual) == Counter(
            json.dumps(row, sort_keys=True) for row in projected_expected
        )
    elif policy == "at_least_once_bounded_duplicates":
        max_extra_rows = int(config.get("max_extra_rows", len(projected_expected)))
        expected_index = 0
        for row in projected_actual:
            if expected_index < len(projected_expected) and row == projected_expected[expected_index]:
                expected_index += 1
        passed = expected_index == len(projected_expected) and extra_count <= max_extra_rows
    else:
        raise SystemExit(f"unsupported oracle policy: {policy}")

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    actual_rows_path = results_dir / "actual_rows.json"
    result_path = results_dir / "result.json"
    summary_path = results_dir / "summary.md"

    actual_rows_path.write_text(json.dumps(projected_actual, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    result = {
        "scenario": config.get("scenario"),
        "policy": policy,
        "passed": passed,
        "expected_count": len(projected_expected),
        "actual_count": len(projected_actual),
        "missing_count": len(missing_rows),
        "duplicate_count": duplicate_count,
        "extra_count": extra_count,
        "order_violations": order_violations,
        "missing_preview": missing_rows[:10],
    }
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    status = "PASS" if passed else "FAIL"
    summary_lines = [
        f"## {config.get('scenario', 'e2e-scenario')}",
        "",
        f"- Status: `{status}`",
        f"- Policy: `{policy}`",
        f"- Expected rows: `{len(projected_expected)}`",
        f"- Actual rows: `{len(projected_actual)}`",
        f"- Missing rows: `{len(missing_rows)}`",
        f"- Duplicate rows: `{duplicate_count}`",
        f"- Order violations: `{order_violations}`",
    ]
    if missing_rows:
        summary_lines.extend(["", "### Missing Preview", "", "```json", json.dumps(missing_rows[:5], indent=2, sort_keys=True), "```"])
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
