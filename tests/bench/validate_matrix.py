"""Validate benchmark matrix truthfulness.

Checks that all expected benchmark combinations ran, flags gating vs
non-gating failures, detects data integrity violations, and enforces
single-CPU envelope constraints.

Environment variables (all required unless noted):
  SUMMARY_JSON           - path to bench-summary.json
  EXPECTED_COLLECTORS    - JSON array of collector names
  EXPECTED_CPU_PROFILES  - JSON array of cpu profile names
  EXPECTED_INGEST_MODES  - JSON array of ingest mode names
  EXPECTED_WORKLOADS     - JSON array of workload objects with "label" key
  OTLP_EXCLUDED_COLLECTORS  - JSON array of collectors excluded from OTLP ingest
  TMAX_ONLY_COLLECTORS      - JSON array of collectors that only run tmax workload
  NON_GATING_COLLECTORS     - JSON array of collectors whose failures are tolerated (optional, default [])
  NON_GATING_EPS_MIN        - EPS threshold above which failures are tolerated (optional, default 10000)
  SINGLE_CPU_AVG_MAX        - max avg CPU cores for single-cpu profile (optional, default 1.05)
"""

import json
import os
import sys


def main():
    summary_path = os.environ.get("SUMMARY_JSON", "bench-summary.json")
    with open(summary_path, "r", encoding="utf-8") as handle:
        summary = json.load(handle)

    benchmark_count = int(summary.get("benchmark_count", 0))
    failed_count = int(summary.get("failed_count", 0))
    non_gating_eps_min = int(os.environ.get("NON_GATING_EPS_MIN", "10000"))
    single_cpu_avg_max = float(os.environ.get("SINGLE_CPU_AVG_MAX", "1.05"))
    non_gating_collectors = set(
        json.loads(os.environ.get("NON_GATING_COLLECTORS", "[]"))
    )
    otlp_excluded = set(
        json.loads(os.environ.get("OTLP_EXCLUDED_COLLECTORS", "[]"))
    )
    tmax_only = set(
        json.loads(os.environ.get("TMAX_ONLY_COLLECTORS", "[]"))
    )

    collectors = json.loads(os.environ["EXPECTED_COLLECTORS"])
    cpu_profiles = json.loads(os.environ["EXPECTED_CPU_PROFILES"])
    ingest_modes = json.loads(os.environ["EXPECTED_INGEST_MODES"])
    workloads = json.loads(os.environ["EXPECTED_WORKLOADS"])

    def is_excluded(c, _cp, i, w):
        if i == "otlp" and c in otlp_excluded:
            return True
        if c in tmax_only and w["label"] != "tmax":
            return True
        return False

    expected = sum(
        1
        for c in collectors
        for cp in cpu_profiles
        for i in ingest_modes
        for w in workloads
        if not is_excluded(c, cp, i, w)
    )

    print(
        f"expected={expected} benchmark_count={benchmark_count} "
        f"failed_count={failed_count}"
    )

    if benchmark_count < expected:
        print(
            f"missing benchmark artifacts: expected {expected}, "
            f"got {benchmark_count}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Classify failures as gating vs tolerated
    gating_failures = []
    tolerated_failures = []
    for row in summary.get("results", []):
        status = str(row.get("status") or "fail").lower()
        if status == "pass":
            continue
        collector = str(row.get("collector") or "")
        target_eps = int(row.get("total_target_eps", 0) or 0)
        non_gating = (
            collector in non_gating_collectors
            or target_eps == 0
            or target_eps >= non_gating_eps_min
        )
        entry = {
            "artifact": row.get("artifact_name"),
            "collector": collector,
            "ingest_mode": row.get("ingest_mode"),
            "cpu_profile": row.get("cpu_profile"),
            "target_eps": target_eps,
            "status": status,
        }
        if non_gating:
            tolerated_failures.append(entry)
        else:
            gating_failures.append(entry)

    if tolerated_failures:
        print(f"tolerating {len(tolerated_failures)} non-gating failures")
        for row in tolerated_failures[:20]:
            print(row)
    if gating_failures:
        print("gating benchmark failures detected:", file=sys.stderr)
        for row in gating_failures[:20]:
            print(row, file=sys.stderr)
        sys.exit(1)

    # Data integrity checks (non-gating, diagnostic only)
    integrity_violations = []
    for row in summary.get("results", []):
        collector = str(row.get("collector") or "")
        target_eps = int(row.get("total_target_eps", 0) or 0)
        if (
            collector in non_gating_collectors
            or target_eps == 0
            or target_eps >= non_gating_eps_min
        ):
            continue
        missing = int(row.get("missing_event_count", 0) or 0)
        unexpected = int(row.get("unexpected_event_count", 0) or 0)
        dup = int(row.get("dup_estimate", 0) or 0)
        drop = int(row.get("drop_estimate", 0) or 0)
        if missing > 0 or unexpected > 0 or dup > 0 or drop > 0:
            integrity_violations.append(
                {
                    "artifact": row.get("artifact_name"),
                    "missing": missing,
                    "unexpected": unexpected,
                    "dup": dup,
                    "drop": drop,
                }
            )

    if integrity_violations:
        print("diagnostic integrity deltas detected (non-gating):")
        for violation in integrity_violations[:20]:
            print(violation)

    # Single-CPU envelope enforcement
    single_cpu_violations = []
    for row in summary.get("results", []):
        if str(row.get("cpu_profile") or "").lower() != "single":
            continue
        cpu_avg = row.get("collector_cpu_cores_avg")
        if cpu_avg is None:
            continue
        if float(cpu_avg) > single_cpu_avg_max:
            single_cpu_violations.append(
                {
                    "artifact": row.get("artifact_name"),
                    "collector": row.get("collector"),
                    "ingest_mode": row.get("ingest_mode"),
                    "target_eps": row.get("total_target_eps"),
                    "collector_cpu_cores_avg": cpu_avg,
                }
            )

    if single_cpu_violations:
        print(
            f"single CPU envelope violations detected "
            f"(> {single_cpu_avg_max} cores avg):",
            file=sys.stderr,
        )
        for violation in single_cpu_violations[:20]:
            print(violation, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
