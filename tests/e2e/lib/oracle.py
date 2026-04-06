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


def encode_row(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True)


def multiset_difference(left: list[dict[str, Any]], right: list[dict[str, Any]]) -> list[dict[str, Any]]:
    difference: list[dict[str, Any]] = []
    right_counter = Counter(encode_row(row) for row in right)
    for row in left:
        encoded = encode_row(row)
        if right_counter[encoded] > 0:
            right_counter[encoded] -= 1
        else:
            difference.append(row)
    return difference


def count_duplicates(rows: list[dict[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    counter = Counter(encode_row(row) for row in rows)
    duplicates: list[dict[str, Any]] = []
    duplicate_count = 0
    for row in rows:
        encoded = encode_row(row)
        if counter[encoded] > 1:
            duplicate_count += 1
            counter[encoded] = 0
            duplicates.append(row)
    return duplicate_count, duplicates


def count_null_violations(rows: list[dict[str, Any]], required_fields: list[str]) -> tuple[int, list[dict[str, Any]]]:
    violations: list[dict[str, Any]] = []
    for row in rows:
        if any(row.get(field) is None for field in required_fields):
            violations.append(row)
    return len(violations), violations


def count_order_violations(rows: list[dict[str, Any]], order_key: str, source_key: str | None) -> int:
    order_violations = 0
    last_seen: dict[str, Any] = {}
    for row in rows:
        current = row.get(order_key)
        if current is None:
            continue
        source = str(row.get(source_key)) if source_key else "__global__"
        if source in last_seen and current < last_seen[source]:
            order_violations += 1
        last_seen[source] = current
    return order_violations


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

    identity_keys = config.get("identity_keys") or compare_keys
    required_fields = config.get("required_fields") or identity_keys
    source_key = config.get("source_key")

    projected_expected = [project_row(row, compare_keys) for row in expected_rows]
    projected_actual = [project_row(row, compare_keys) for row in actual_rows]
    identity_expected = [project_row(row, identity_keys) for row in expected_rows]
    identity_actual = [project_row(row, identity_keys) for row in actual_rows]

    policy = config["policy"]
    order_key = config.get("order_key", "seq")
    order_violations = count_order_violations(projected_actual, order_key, source_key)

    missing_rows = multiset_difference(projected_expected, projected_actual)
    extra_rows = multiset_difference(projected_actual, projected_expected)
    duplicate_count, duplicate_preview = count_duplicates(identity_actual)
    null_field_violations, null_field_preview = count_null_violations(projected_actual, required_fields)
    extra_count = len(extra_rows)

    passed = False
    if policy == "exact_once_ordered":
        passed = (
            projected_actual == projected_expected
            and duplicate_count == 0
            and null_field_violations == 0
            and order_violations == 0
        )
    elif policy == "exact_once_unordered":
        passed = Counter(encode_row(row) for row in projected_actual) == Counter(
            encode_row(row) for row in projected_expected
        )
        passed = passed and duplicate_count == 0 and null_field_violations == 0
    elif policy == "at_least_once_bounded_duplicates":
        max_extra_rows = int(config.get("max_extra_rows", len(projected_expected)))
        expected_index = 0
        for row in projected_actual:
            if expected_index < len(projected_expected) and row == projected_expected[expected_index]:
                expected_index += 1
        passed = (
            expected_index == len(projected_expected)
            and extra_count <= max_extra_rows
            and null_field_violations == 0
            and order_violations == 0
        )
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
        "null_field_violations": null_field_violations,
        "identity_keys": identity_keys,
        "compare_keys": compare_keys,
        "missing_preview": missing_rows[:10],
        "duplicate_preview": duplicate_preview[:10],
        "extra_preview": extra_rows[:10],
        "null_field_preview": null_field_preview[:10],
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
        f"- Extra rows: `{extra_count}`",
        f"- Order violations: `{order_violations}`",
        f"- Null field violations: `{null_field_violations}`",
        f"- Identity keys: `{', '.join(identity_keys)}`",
        f"- Compare keys: `{', '.join(compare_keys)}`",
    ]
    if missing_rows:
        summary_lines.extend(["", "### Missing Preview", "", "```json", json.dumps(missing_rows[:5], indent=2, sort_keys=True), "```"])
    if duplicate_preview:
        summary_lines.extend(["", "### Duplicate Preview", "", "```json", json.dumps(duplicate_preview[:5], indent=2, sort_keys=True), "```"])
    if extra_rows:
        summary_lines.extend(["", "### Extra Preview", "", "```json", json.dumps(extra_rows[:5], indent=2, sort_keys=True), "```"])
    if null_field_preview:
        summary_lines.extend(["", "### Null Field Preview", "", "```json", json.dumps(null_field_preview[:5], indent=2, sort_keys=True), "```"])
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
