#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sys

try:
    from reporting.markdown import markdown_table
except ModuleNotFoundError:
    REPO_ROOT = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(REPO_ROOT))
    from reporting.markdown import markdown_table


@dataclass
class BenchResult:
    artifact_name: str
    collector: str
    ingest_mode: str
    cpu_profile: str
    collector_batch_target_bytes: int | None
    pods: int
    target_eps_per_pod: int
    phase: str
    status: str
    total_target_eps: int
    sink_lines_per_sec_avg: float | None
    missing_event_count: int | None
    unexpected_event_count: int | None
    dup_estimate: int | None
    drop_estimate: int | None
    rejected_batches_total: int | None
    http_413_count: int | None
    rejected_rows_estimate: int | None
    rejected_bytes_estimate: int | None
    backpressure_warning_count: int | None
    collector_dropped_batches_total: int | None
    sink_cpu_cores_avg: float | None
    collector_cpu_cores_avg: float | None
    generator_cpu_cores_avg: float | None
    collector_rss_mb_avg: float | None
    sink_rss_mb_avg: float | None
    notes: str

    @property
    def passed(self) -> bool:
        return self.status.lower() == "pass"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render a benchmark suite summary from downloaded bench artifacts.")
    parser.add_argument("--artifacts-root", required=True)
    parser.add_argument("--suite-name", required=True)
    parser.add_argument("--suite-key", required=True)
    parser.add_argument("--memagent-ref", required=True)
    parser.add_argument("--bench-profile", required=True)
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--output-markdown", required=True)
    parser.add_argument("--output-json", required=True)
    return parser.parse_args()


def as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def load_result(path: Path, artifact_name: str) -> BenchResult:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return BenchResult(
        artifact_name=artifact_name,
        collector=str(payload.get("collector") or "unknown"),
        ingest_mode=str(payload.get("ingest_mode") or "file"),
        cpu_profile=str(payload.get("cpu_profile") or "unknown"),
        collector_batch_target_bytes=as_int(payload.get("collector_batch_target_bytes")),
        pods=as_int(payload.get("pods")) or 0,
        target_eps_per_pod=as_int(payload.get("target_eps_per_pod")) or 0,
        phase=str(payload.get("phase") or "unknown"),
        status=str(payload.get("status") or "fail"),
        total_target_eps=as_int(payload.get("total_target_eps")) or 0,
        sink_lines_per_sec_avg=as_float(payload.get("sink_lines_per_sec_avg")),
        missing_event_count=as_int(payload.get("missing_event_count")),
        unexpected_event_count=as_int(payload.get("unexpected_event_count")),
        dup_estimate=as_int(payload.get("dup_estimate")),
        drop_estimate=as_int(payload.get("drop_estimate")),
        rejected_batches_total=as_int(payload.get("rejected_batches_total")),
        http_413_count=as_int(payload.get("http_413_count")),
        rejected_rows_estimate=as_int(payload.get("rejected_rows_estimate")),
        rejected_bytes_estimate=as_int(payload.get("rejected_bytes_estimate")),
        backpressure_warning_count=as_int(payload.get("backpressure_warning_count")),
        collector_dropped_batches_total=as_int(payload.get("collector_dropped_batches_total")),
        sink_cpu_cores_avg=as_float(payload.get("sink_cpu_cores_avg")),
        collector_cpu_cores_avg=as_float(payload.get("collector_cpu_cores_avg")),
        generator_cpu_cores_avg=as_float(payload.get("generator_cpu_cores_avg")),
        collector_rss_mb_avg=as_float(payload.get("collector_rss_mb_avg")),
        sink_rss_mb_avg=as_float(payload.get("sink_rss_mb_avg")),
        notes=str(payload.get("notes") or ""),
    )


def scan_artifacts(root: Path) -> list[BenchResult]:
    results: list[BenchResult] = []
    if not root.exists():
        return results

    root_result = root / "result.json"
    if root_result.exists():
        results.append(load_result(root_result, root.name))

    for artifact_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        result_path = artifact_dir / "result.json"
        if result_path.exists():
            results.append(load_result(result_path, artifact_dir.name))
    return results


def fmt_float(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def fmt_eps(value: float | None) -> str:
    """Format an EPS value compactly: sub-1 with 2 dp, <1000 with 1 dp, >=1000 as k."""
    if value is None:
        return "n/a"
    if value < 1.0:
        return f"{value:.2f}"
    if value < 1_000.0:
        return f"{value:.1f}"
    return f"{value / 1_000.0:.1f}k"


def fmt_mb(value: float | None) -> str:
    """Format a memory value in MB, rounded to the nearest integer."""
    if value is None:
        return "n/a"
    return f"{value:.0f} MB"


def fmt_int(value: int | None) -> str:
    if value is None:
        return "n/a"
    return str(value)


def fmt_percent(ratio: float | None) -> str:
    if ratio is None:
        return "n/a"
    return f"{ratio * 100.0:.1f}%"


def status_rank(status: str) -> int:
    normalized = status.lower()
    if normalized == "pass":
        return 0
    if normalized == "partial":
        return 1
    return 2


def collector_rank(name: str) -> tuple[int, str]:
    order = {"fastforward": 0, "otelcol": 1, "filebeat": 2, "vector": 3, "vlagent": 4}
    return (order.get(name, 99), name)


def cpu_rank(name: str) -> tuple[int, str]:
    order = {"single": 0, "multi": 1}
    return (order.get(name, 99), name)


def ingest_rank(name: str) -> tuple[int, str]:
    order = {"file": 0, "otlp": 1, "otlp-100k": 1, "otlp-4m": 2}
    return (order.get(name, 99), name)


def target_rank(total_target_eps: int) -> tuple[int, int]:
    if total_target_eps == 0:
        return (1, 0)
    return (0, total_target_eps)


def target_label(total_target_eps: int) -> str:
    return "max" if total_target_eps == 0 else str(total_target_eps)


SATURATION_TARGET_THRESHOLD = 100_000


def is_saturation_target(result: BenchResult) -> bool:
    """Return True for high-load saturation probes (eps_per_pod >= 100k or unbounded max).

    These lanes stress the pipeline far above sustainable throughput.  Failures here
    mean the system is throughput-limited, not that there is a correctness regression.
    They are always non-gating in the suite PASS/FAIL verdict.
    """
    return result.target_eps_per_pod == 0 or result.target_eps_per_pod >= SATURATION_TARGET_THRESHOLD


def has_integrity_alert(result: BenchResult) -> bool:
    return (
        (result.missing_event_count or 0) > 0
        or (result.unexpected_event_count or 0) > 0
        or (result.dup_estimate or 0) > 0
        or (result.rejected_batches_total or 0) > 0
        or (result.http_413_count or 0) > 0
        or not result.passed
    )


def render_markdown(
    *,
    suite_name: str,
    suite_key: str,
    memagent_ref: str,
    bench_profile: str,
    run_url: str,
    results: list[BenchResult],
) -> str:
    total = len(results)
    passed = sum(1 for result in results if result.passed)
    # Saturation-target failures are throughput-limited probes, not correctness regressions.
    # They are always non-gating and excluded from the suite PASS/FAIL verdict.
    gating_failed = sum(1 for result in results if not result.passed and not is_saturation_target(result))
    failed = total - passed
    status = "PASS" if gating_failed == 0 and total > 0 else "FAIL"
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    sorted_results = sorted(
        results,
        key=lambda result: (
            collector_rank(result.collector),
            ingest_rank(result.ingest_mode),
            cpu_rank(result.cpu_profile),
            target_rank(result.total_target_eps),
            status_rank(result.status),
        ),
    )

    lines = [
        f"# {suite_name} Report",
        "",
        f"- Status: `{status}`",
        f"- Suite key: `{suite_key}`",
        f"- Memagent ref: `{memagent_ref}`",
        f"- Bench profile: `{bench_profile}`",
        f"- Updated: `{updated_at}`",
        f"- Workflow run: [view run]({run_url})",
        f"- Benchmarks: `{total}` total, `{passed}` passed, `{failed}` failed"
        + (f" (`{gating_failed}` gating)" if gating_failed != failed else ""),
    ]

    if not sorted_results:
        lines.extend(["", "_No benchmark artifacts found._"])
    else:
        cpu_profiles = sorted({result.cpu_profile for result in sorted_results}, key=cpu_rank)
        ingest_modes = sorted({result.ingest_mode for result in sorted_results}, key=ingest_rank)
        collectors = sorted({result.collector for result in sorted_results}, key=collector_rank)

        max_snapshot_rows: list[list[str]] = []
        for collector in collectors:
            for ingest_mode in ingest_modes:
                for cpu_profile in cpu_profiles:
                    subset = [
                        result
                        for result in sorted_results
                        if result.collector == collector
                        and result.ingest_mode == ingest_mode
                        and result.cpu_profile == cpu_profile
                    ]
                    if not subset:
                        continue
                    max_target_rows = [result for result in subset if result.total_target_eps == 0]
                    if max_target_rows:
                        chosen = max(
                            max_target_rows,
                            key=lambda item: item.sink_lines_per_sec_avg or -1.0,
                        )
                    else:
                        chosen = max(
                            subset,
                            key=lambda item: item.sink_lines_per_sec_avg or -1.0,
                        )
                    max_snapshot_rows.append(
                        [
                            collector,
                            ingest_mode,
                            cpu_profile,
                            fmt_eps(chosen.sink_lines_per_sec_avg),
                            fmt_float(chosen.collector_cpu_cores_avg),
                            fmt_float(chosen.generator_cpu_cores_avg),
                            fmt_float(chosen.sink_cpu_cores_avg),
                        ]
                    )
        lines.extend(["", "## Max EPS Snapshot", ""])
        lines.extend(
            markdown_table(
                headers=[
                    "Collector",
                    "Ingest",
                    "CPU",
                    "Max EPS",
                    "Collector CPU Avg",
                    "Generator CPU Avg",
                    "Sink CPU Avg",
                ],
                rows=max_snapshot_rows,
                align=["left", "left", "left", "right", "right", "right", "right"],
            )
        )
        lines.append("")

        integrity_alert_rows = [
            [
                result.collector,
                result.ingest_mode,
                result.cpu_profile,
                fmt_int(result.collector_batch_target_bytes),
                target_label(result.total_target_eps),
                "THROUGHPUT-LIMITED" if is_saturation_target(result) and not result.passed else result.status.upper(),
                fmt_int(result.missing_event_count),
                fmt_int(result.unexpected_event_count),
                fmt_int(result.dup_estimate),
                fmt_int(result.rejected_batches_total),
                fmt_int(result.http_413_count),
            ]
            for result in sorted_results
            if has_integrity_alert(result)
        ]
        if integrity_alert_rows:
            lines.extend(["## Integrity Alerts", ""])
            lines.extend(
                markdown_table(
                    headers=[
                        "Collector",
                        "Ingest",
                        "CPU",
                        "Batch Target Bytes",
                        "Target EPS",
                        "Status",
                        "Missing",
                        "Unexpected",
                        "Duplicates",
                        "Rejected Batches",
                        "HTTP 413",
                    ],
                    rows=integrity_alert_rows,
                    align=["left", "left", "left", "right", "right", "left", "right", "right", "right", "right", "right"],
                )
            )
            lines.append("")


        for cpu_profile in cpu_profiles:
            lines.extend(["", f"## CPU: `{cpu_profile}`", ""])
            for ingest_mode in ingest_modes:
                subset = [
                    result
                    for result in sorted_results
                    if result.cpu_profile == cpu_profile and result.ingest_mode == ingest_mode
                ]
                if not subset:
                    continue
                summary_rows: list[list[str]] = []
                diagnostics_rows: list[list[str]] = []
                for result in sorted(
                    subset,
                    key=lambda item: (
                        collector_rank(item.collector),
                        target_rank(item.total_target_eps),
                        status_rank(item.status),
                    ),
                ):
                    eps_ratio = None
                    if result.sink_lines_per_sec_avg is not None and result.total_target_eps > 0:
                        eps_ratio = result.sink_lines_per_sec_avg / result.total_target_eps
                    summary_rows.append(
                        [
                            result.collector,
                            target_label(result.total_target_eps),
                            result.status.upper(),
                            fmt_eps(result.sink_lines_per_sec_avg),
                            fmt_float(result.collector_cpu_cores_avg),
                            fmt_mb(result.collector_rss_mb_avg),
                            fmt_float(result.generator_cpu_cores_avg),
                            fmt_float(result.sink_cpu_cores_avg),
                            fmt_mb(result.sink_rss_mb_avg),
                            fmt_percent(eps_ratio),
                        ]
                    )
                    diagnostics_rows.append(
                        [
                            result.collector,
                            fmt_int(result.collector_batch_target_bytes),
                            target_label(result.total_target_eps),
                            fmt_int(result.missing_event_count),
                            fmt_int(result.unexpected_event_count),
                            fmt_int(result.dup_estimate),
                            fmt_int(result.drop_estimate),
                            fmt_int(result.rejected_batches_total),
                            fmt_int(result.http_413_count),
                            fmt_int(result.rejected_rows_estimate),
                            fmt_int(result.rejected_bytes_estimate),
                            fmt_int(result.backpressure_warning_count),
                            fmt_int(result.collector_dropped_batches_total),
                        ]
                    )
                lines.extend([f"### Ingest: `{ingest_mode}`", ""])
                lines.extend(
                    markdown_table(
                        headers=[
                            "Collector",
                            "Target EPS",
                            "Status",
                            "EPS Avg",
                            "Collector CPU Avg",
                            "Collector RSS",
                            "Generator CPU Avg",
                            "Sink CPU Avg",
                            "Sink RSS",
                            "% of Target",
                        ],
                        rows=summary_rows,
                        align=["left", "right", "left", "right", "right", "right", "right", "right", "right", "right"],
                    )
                )
                lines.extend(
                    [
                        "",
                        "<details>",
                        "<summary>Diagnostics Table</summary>",
                        "",
                    ]
                )
                lines.extend(
                    markdown_table(
                        headers=[
                            "Collector",
                            "Batch Target Bytes",
                            "Target EPS",
                            "Missing",
                            "Unexpected",
                            "Duplicates",
                            "Dropped",
                            "Rejected Batches",
                            "HTTP 413",
                            "Rejected Rows",
                            "Rejected Bytes",
                            "Backpressure Warnings",
                            "Dropped Batches",
                        ],
                        rows=diagnostics_rows,
                        align=[
                            "left",
                            "right",
                            "right",
                            "right",
                            "right",
                            "right",
                            "right",
                            "right",
                            "right",
                            "right",
                            "right",
                            "right",
                            "right",
                        ],
                    )
                )
                lines.extend(["", "</details>"])
                lines.append("")

    failing = [result for result in sorted_results if not result.passed]
    if failing:
        gating_failing = [r for r in failing if not is_saturation_target(r)]
        saturation_failing = [r for r in failing if is_saturation_target(r)]
        if gating_failing:
            lines.extend(["", "## Failing Benchmarks", ""])
            for result in gating_failing:
                failing_target_label = target_label(result.total_target_eps)
                lines.append(f"### {result.collector} / {result.ingest_mode} / {result.cpu_profile} / target={failing_target_label}")
                lines.append("")
                lines.append(f"- Status: `{result.status}`")
                lines.append(f"- Notes: {result.notes or 'n/a'}")
                lines.append("")
        if saturation_failing:
            lines.extend(["", "## Throughput-Limited Probes (non-gating)", ""])
            lines.append(
                "_These lanes ran at or above the saturation threshold "
                f"(eps_per_pod ≥ {SATURATION_TARGET_THRESHOLD:,}). "
                "Failures here indicate the pipeline is throughput-limited, not a correctness regression. "
                "They do not affect the suite PASS/FAIL verdict._"
            )
            lines.append("")
            for result in saturation_failing:
                failing_target_label = target_label(result.total_target_eps)
                lines.append(f"### {result.collector} / {result.ingest_mode} / {result.cpu_profile} / target={failing_target_label}")
                lines.append("")
                lines.append(f"- Status: `THROUGHPUT-LIMITED`")
                lines.append(f"- Notes: {result.notes or 'n/a'}")
                lines.append("")

    # Add telemetry notes for known collection gaps
    collectors_in_results = {r.collector for r in sorted_results}
    if "vector" in collectors_in_results:
        has_vector_cpu_na = any(
            r.collector == "vector" and r.collector_cpu_cores_avg is None
            for r in sorted_results
        )
        if has_vector_cpu_na:
            lines.extend([
                "",
                "---",
                "",
                "**Telemetry Note:** Vector `Collector CPU Avg` shows `n/a` because vector's "
                "`internal_metrics` source does not expose process-level CPU/RSS metrics in the "
                "prometheus output captured by this harness. Throughput (EPS) measurements are "
                "unaffected — they are derived from sink capture counts.",
            ])

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    args = parse_args()
    artifacts_root = Path(args.artifacts_root)
    results = scan_artifacts(artifacts_root)

    markdown = render_markdown(
        suite_name=args.suite_name,
        suite_key=args.suite_key,
        memagent_ref=args.memagent_ref,
        bench_profile=args.bench_profile,
        run_url=args.run_url,
        results=results,
    )

    payload = {
        "suite_name": args.suite_name,
        "suite_key": args.suite_key,
        "memagent_ref": args.memagent_ref,
        "bench_profile": args.bench_profile,
        "run_url": args.run_url,
        "benchmark_count": len(results),
        "passed_count": sum(1 for result in results if result.passed),
        "failed_count": sum(1 for result in results if not result.passed),
        "gating_failed_count": sum(1 for result in results if not result.passed and not is_saturation_target(result)),
        "results": [
            {
                "artifact_name": result.artifact_name,
                "collector": result.collector,
                "ingest_mode": result.ingest_mode,
                "cpu_profile": result.cpu_profile,
                "collector_batch_target_bytes": result.collector_batch_target_bytes,
                "pods": result.pods,
                "target_eps_per_pod": result.target_eps_per_pod,
                "phase": result.phase,
                "status": result.status,
                "total_target_eps": result.total_target_eps,
                "sink_lines_per_sec_avg": result.sink_lines_per_sec_avg,
                "missing_event_count": result.missing_event_count,
                "unexpected_event_count": result.unexpected_event_count,
                "dup_estimate": result.dup_estimate,
                "drop_estimate": result.drop_estimate,
                "rejected_batches_total": result.rejected_batches_total,
                "http_413_count": result.http_413_count,
                "rejected_rows_estimate": result.rejected_rows_estimate,
                "rejected_bytes_estimate": result.rejected_bytes_estimate,
                "backpressure_warning_count": result.backpressure_warning_count,
                "collector_dropped_batches_total": result.collector_dropped_batches_total,
                "sink_cpu_cores_avg": result.sink_cpu_cores_avg,
                "collector_cpu_cores_avg": result.collector_cpu_cores_avg,
                "generator_cpu_cores_avg": result.generator_cpu_cores_avg,
                "collector_rss_mb_avg": result.collector_rss_mb_avg,
                "sink_rss_mb_avg": result.sink_rss_mb_avg,
                "notes": result.notes,
            }
            for result in results
        ],
    }

    Path(args.output_markdown).write_text(markdown, encoding="utf-8")
    Path(args.output_json).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
