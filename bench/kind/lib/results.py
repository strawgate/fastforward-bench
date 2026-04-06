from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class BenchmarkResult:
    benchmark_id: str
    timestamp_utc: str
    phase: str
    benchmark_mode: str
    cluster: str
    cluster_name: str
    namespace: str
    collector: str
    protocol: str
    pods: int
    target_eps_per_pod: int
    total_target_eps: int
    warmup_sec: int
    measure_sec: int
    cooldown_sec: int
    sink_lines_total: int | None
    captured_rows_total: int | None
    source_rows_total: int | None
    missing_source_count: int | None
    missing_event_count: int | None
    unexpected_event_count: int | None
    sink_lines_per_sec_avg: float | None
    sink_lines_per_sec_p50: float | None
    sink_lines_per_sec_p95: float | None
    sink_lines_per_sec_p99: float | None
    drop_estimate: int | None
    dup_estimate: int | None
    latency_ms_p50: float | None
    latency_ms_p95: float | None
    latency_ms_p99: float | None
    collector_cpu_cores_avg: float | None
    collector_cpu_cores_p95: float | None
    collector_rss_mb_avg: float | None
    collector_rss_mb_p95: float | None
    cluster_ready: bool
    sink_ready: bool
    status: str
    notes: str


def write_result_files(results_dir: Path, result: BenchmarkResult) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    payload = asdict(result)
    (results_dir / "result.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (results_dir / "results.jsonl").write_text(
        json.dumps(payload, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (results_dir / "summary.md").write_text(render_summary(result), encoding="utf-8")


def render_summary(result: BenchmarkResult) -> str:
    status = result.status.upper()

    def show(value: object) -> str:
        return "n/a" if value is None else str(value)

    lines = [
        f"# KIND Benchmark / {result.phase}",
        "",
        f"- Status: `{status}`",
        f"- Benchmark ID: `{result.benchmark_id}`",
        f"- Benchmark mode: `{result.benchmark_mode}`",
        f"- Collector: `{result.collector}`",
        f"- Protocol: `{result.protocol}`",
        f"- Cluster: `{result.cluster_name}`",
        f"- Namespace: `{result.namespace}`",
        f"- Profile target: `{result.pods}` pods x `{result.target_eps_per_pod}` eps",
        f"- Cluster ready: `{'yes' if result.cluster_ready else 'no'}`",
        f"- Sink ready: `{'yes' if result.sink_ready else 'no'}`",
        f"- Notes: {result.notes}",
        "",
        "## Metrics",
        "",
        f"- sink_lines_total: `{show(result.sink_lines_total)}`",
        f"- captured_rows_total: `{show(result.captured_rows_total)}`",
        f"- source_rows_total: `{show(result.source_rows_total)}`",
        f"- missing_source_count: `{show(result.missing_source_count)}`",
        f"- missing_event_count: `{show(result.missing_event_count)}`",
        f"- unexpected_event_count: `{show(result.unexpected_event_count)}`",
        f"- sink_lines_per_sec_avg: `{show(result.sink_lines_per_sec_avg)}`",
        f"- drop_estimate: `{show(result.drop_estimate)}`",
        f"- dup_estimate: `{show(result.dup_estimate)}`",
        f"- collector_cpu_cores_avg: `{show(result.collector_cpu_cores_avg)}`",
        f"- collector_rss_mb_avg: `{show(result.collector_rss_mb_avg)}`",
    ]
    return "\n".join(lines).rstrip() + "\n"
