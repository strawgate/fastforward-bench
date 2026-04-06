from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


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


def _otlp_attr(key: str, value: str) -> dict[str, Any]:
    return {"key": key, "value": {"stringValue": value}}


def _otlp_metric(
    *,
    name: str,
    value: float | int,
    unit: str,
    timestamp_unix_nano: str,
    scenario: str,
    series: str,
    direction: str,
    role: str,
    tags: dict[str, str],
) -> dict[str, Any]:
    attributes = [
        _otlp_attr("benchkit.scenario", scenario),
        _otlp_attr("benchkit.series", series),
        _otlp_attr("benchkit.metric.direction", direction),
        _otlp_attr("benchkit.metric.role", role),
    ]
    for key, tag_value in sorted(tags.items()):
        attributes.append(_otlp_attr(key, tag_value))

    data_point: dict[str, Any] = {
        "timeUnixNano": timestamp_unix_nano,
        "attributes": attributes,
    }
    if isinstance(value, int):
        data_point["asInt"] = str(value)
    else:
        data_point["asDouble"] = value

    return {
        "name": name,
        "unit": unit,
        "gauge": {
            "dataPoints": [data_point],
        },
    }


def _metric_specs(result: BenchmarkResult) -> list[tuple[str, float | int | None, str, str, str]]:
    return [
        ("sink_lines_total", result.sink_lines_total, "events", "bigger_is_better", "outcome"),
        ("captured_rows_total", result.captured_rows_total, "events", "bigger_is_better", "outcome"),
        ("source_rows_total", result.source_rows_total, "events", "bigger_is_better", "diagnostic"),
        ("missing_source_count", result.missing_source_count, "sources", "smaller_is_better", "outcome"),
        ("missing_event_count", result.missing_event_count, "events", "smaller_is_better", "outcome"),
        ("unexpected_event_count", result.unexpected_event_count, "events", "smaller_is_better", "outcome"),
        ("sink_lines_per_sec_avg", result.sink_lines_per_sec_avg, "events/sec", "bigger_is_better", "outcome"),
        ("sink_lines_per_sec_p50", result.sink_lines_per_sec_p50, "events/sec", "bigger_is_better", "outcome"),
        ("sink_lines_per_sec_p95", result.sink_lines_per_sec_p95, "events/sec", "bigger_is_better", "outcome"),
        ("sink_lines_per_sec_p99", result.sink_lines_per_sec_p99, "events/sec", "bigger_is_better", "outcome"),
        ("drop_estimate", result.drop_estimate, "events", "smaller_is_better", "outcome"),
        ("dup_estimate", result.dup_estimate, "events", "smaller_is_better", "outcome"),
        ("latency_ms_p50", result.latency_ms_p50, "ms", "smaller_is_better", "outcome"),
        ("latency_ms_p95", result.latency_ms_p95, "ms", "smaller_is_better", "outcome"),
        ("latency_ms_p99", result.latency_ms_p99, "ms", "smaller_is_better", "outcome"),
        ("collector_cpu_cores_avg", result.collector_cpu_cores_avg, "cores", "smaller_is_better", "diagnostic"),
        ("collector_cpu_cores_p95", result.collector_cpu_cores_p95, "cores", "smaller_is_better", "diagnostic"),
        ("collector_rss_mb_avg", result.collector_rss_mb_avg, "MB", "smaller_is_better", "diagnostic"),
        ("collector_rss_mb_p95", result.collector_rss_mb_p95, "MB", "smaller_is_better", "diagnostic"),
    ]


def build_otlp_result_payload(
    *,
    result: BenchmarkResult,
    run_id: str,
    kind: str,
    service_name: str,
    profile: str,
    ref: str | None = None,
    commit: str | None = None,
    workflow: str | None = None,
    job: str | None = None,
    run_attempt: str | None = None,
    runner: str | None = None,
) -> dict[str, Any]:
    from datetime import datetime, timezone  # local import keeps module surface small

    parsed = datetime.strptime(result.timestamp_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    timestamp_unix_nano = str(int(parsed.timestamp() * 1_000_000_000))

    resource_attrs = [
        _otlp_attr("benchkit.run_id", run_id),
        _otlp_attr("benchkit.kind", kind),
        _otlp_attr("benchkit.source_format", "otlp"),
        _otlp_attr("service.name", service_name),
    ]
    optional_resource_attrs = {
        "benchkit.ref": ref,
        "benchkit.commit": commit,
        "benchkit.workflow": workflow,
        "benchkit.job": job,
        "benchkit.run_attempt": run_attempt,
        "benchkit.runner": runner,
    }
    for key, value in optional_resource_attrs.items():
        if value:
            resource_attrs.append(_otlp_attr(key, value))

    scenario = f"kind/{result.phase}/{result.benchmark_mode}"
    tags = {
        "benchkit.impl": result.collector,
        "benchkit.protocol": result.protocol,
        "benchkit.profile": profile,
        "benchkit.cluster": result.cluster,
        "benchkit.namespace": result.namespace,
        "benchkit.pods": str(result.pods),
        "benchkit.target_eps_per_pod": str(result.target_eps_per_pod),
        "benchkit.total_target_eps": str(result.total_target_eps),
    }

    metrics = [
        _otlp_metric(
            name=name,
            value=value,
            unit=unit,
            timestamp_unix_nano=timestamp_unix_nano,
            scenario=scenario,
            series=result.collector,
            direction=direction,
            role=role,
            tags=tags,
        )
        for name, value, unit, direction, role in _metric_specs(result)
        if value is not None
    ]

    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": resource_attrs,
                },
                "scopeMetrics": [
                    {
                        "scope": {
                            "name": "memagent-e2e.kind-bench",
                        },
                        "metrics": metrics,
                    }
                ],
            }
        ]
    }


def build_otlp_phase_signal_payload(
    *,
    result: BenchmarkResult,
    run_id: str,
    kind: str,
    service_name: str,
    profile: str,
    phase_name: str,
    event: str,
    status: str | None = None,
    ref: str | None = None,
    commit: str | None = None,
    workflow: str | None = None,
    job: str | None = None,
    run_attempt: str | None = None,
    runner: str | None = None,
) -> dict[str, Any]:
    from datetime import datetime, timezone  # local import keeps module surface small

    now = datetime.now(timezone.utc)
    timestamp_unix_nano = str(int(now.timestamp() * 1_000_000_000))

    resource_attrs = [
        _otlp_attr("benchkit.run_id", run_id),
        _otlp_attr("benchkit.kind", kind),
        _otlp_attr("benchkit.source_format", "otlp"),
        _otlp_attr("service.name", service_name),
    ]
    optional_resource_attrs = {
        "benchkit.ref": ref,
        "benchkit.commit": commit,
        "benchkit.workflow": workflow,
        "benchkit.job": job,
        "benchkit.run_attempt": run_attempt,
        "benchkit.runner": runner,
    }
    for key, value in optional_resource_attrs.items():
        if value:
            resource_attrs.append(_otlp_attr(key, value))

    scenario = f"kind/{result.phase}/{result.benchmark_mode}"
    tags = {
        "benchkit.impl": result.collector,
        "benchkit.protocol": result.protocol,
        "benchkit.profile": profile,
        "benchkit.cluster": result.cluster,
        "benchkit.namespace": result.namespace,
        "phase": phase_name,
        "event": event,
        "benchmark.id": result.benchmark_id,
    }
    if status:
        tags["result.status"] = status

    metric = _otlp_metric(
        name="_monitor.phase_signal",
        value=1,
        unit="events",
        timestamp_unix_nano=timestamp_unix_nano,
        scenario=scenario,
        series=result.collector,
        direction="bigger_is_better",
        role="diagnostic",
        tags=tags,
    )

    return {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": resource_attrs,
                },
                "scopeMetrics": [
                    {
                        "scope": {
                            "name": "memagent-e2e.kind-bench",
                        },
                        "metrics": [metric],
                    }
                ],
            }
        ]
    }


def write_otlp_result_file(
    *,
    results_dir: Path,
    result: BenchmarkResult,
    run_id: str,
    kind: str,
    service_name: str,
    profile: str,
    ref: str | None = None,
    commit: str | None = None,
    workflow: str | None = None,
    job: str | None = None,
    run_attempt: str | None = None,
    runner: str | None = None,
) -> Path:
    payload = build_otlp_result_payload(
        result=result,
        run_id=run_id,
        kind=kind,
        service_name=service_name,
        profile=profile,
        ref=ref,
        commit=commit,
        workflow=workflow,
        job=job,
        run_attempt=run_attempt,
        runner=runner,
    )
    path = results_dir / "benchkit-run.otlp.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def write_result_files(
    results_dir: Path,
    result: BenchmarkResult,
    *,
    benchkit_run_id: str | None = None,
    benchkit_kind: str | None = None,
    benchkit_service_name: str | None = None,
    benchkit_profile: str | None = None,
    benchkit_ref: str | None = None,
    benchkit_commit: str | None = None,
    benchkit_workflow: str | None = None,
    benchkit_job: str | None = None,
    benchkit_run_attempt: str | None = None,
    benchkit_runner: str | None = None,
) -> None:
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
    if benchkit_run_id and benchkit_kind and benchkit_service_name and benchkit_profile:
        write_otlp_result_file(
            results_dir=results_dir,
            result=result,
            run_id=benchkit_run_id,
            kind=benchkit_kind,
            service_name=benchkit_service_name,
            profile=benchkit_profile,
            ref=benchkit_ref,
            commit=benchkit_commit,
            workflow=benchkit_workflow,
            job=benchkit_job,
            run_attempt=benchkit_run_attempt,
            runner=benchkit_runner,
        )


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
