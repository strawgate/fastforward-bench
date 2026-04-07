#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import subprocess
import string
import time
import traceback
import uuid
import urllib.request
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

from lib.analyze import (
    benchmark_rows,
    compare_source_and_sink,
    expected_emitter_pods,
    load_json_lines,
)
from lib.cluster import (
    CommandError,
    create_kind_cluster,
    delete_kind_cluster,
    load_image_into_kind,
    require_tool,
    set_kind_control_plane_cpu_limit,
)
from lib.collectors import CollectorAdapter, get_collector_adapter
from lib.kube import (
    apply_manifest,
    collect_debug_artifacts,
    get_first_pod_name,
    get_pod_names,
    rollout_status,
    wait_for_deployment,
    wait_for_namespace,
)
from lib.measure import (
    StatsSample,
    avg,
    collect_bench_samples,
    collect_emitter_reported_total,
    collect_sink_reported_stats,
    cpu_cores_series,
    diff_output_lines,
    lines_per_sec_series,
    percentile,
    rss_mb_series,
)
from lib.profiles import PROFILES, Profile
from lib.results import (
    BenchmarkResult,
    build_otlp_phase_signal_payload,
    build_otlp_result_payload,
    write_result_files,
)


BENCH_ROOT = Path(__file__).resolve().parent
COMMON_MANIFESTS_ROOT = BENCH_ROOT / "manifests" / "common"
COLLECTOR_MANIFESTS_ROOT = BENCH_ROOT / "manifests" / "collectors"
WORKLOAD_MANIFESTS_ROOT = BENCH_ROOT / "manifests" / "workload"


@dataclass(frozen=True)
class CpuProfile:
    name: str
    cluster_cpu_cores: float
    collector_cpu_mcpu_min: int
    collector_cpu_mcpu_target: int
    emitter_cpu_mcpu_per_pod: int
    sink_cpu_mcpu: int
    capture_reader_cpu_mcpu: int
    collector_memory_limit: str
    emitter_memory_limit: str
    sink_memory_limit: str
    capture_reader_memory_limit: str


@dataclass(frozen=True)
class ResourcePlan:
    cpu_profile: CpuProfile
    collector_cpu: str
    emitter_cpu: str
    sink_cpu: str
    capture_reader_cpu: str
    collector_memory: str
    emitter_memory: str
    sink_memory: str
    capture_reader_memory: str


CPU_PROFILES: dict[str, CpuProfile] = {
    "single": CpuProfile(
        name="single",
        cluster_cpu_cores=1.0,
        collector_cpu_mcpu_min=500,
        collector_cpu_mcpu_target=900,
        emitter_cpu_mcpu_per_pod=60,
        sink_cpu_mcpu=100,
        capture_reader_cpu_mcpu=20,
        collector_memory_limit="512Mi",
        emitter_memory_limit="96Mi",
        sink_memory_limit="256Mi",
        capture_reader_memory_limit="128Mi",
    ),
    "multi": CpuProfile(
        name="multi",
        cluster_cpu_cores=2.0,
        collector_cpu_mcpu_min=1200,
        collector_cpu_mcpu_target=1800,
        emitter_cpu_mcpu_per_pod=60,
        sink_cpu_mcpu=120,
        capture_reader_cpu_mcpu=20,
        collector_memory_limit="1Gi",
        emitter_memory_limit="96Mi",
        sink_memory_limit="256Mi",
        capture_reader_memory_limit="128Mi",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the KIND competitive benchmark harness.")
    parser.add_argument("--phase", choices=["infra", "smoke"], default="smoke")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="smoke")
    parser.add_argument("--collector", default="logfwd")
    parser.add_argument("--ingest-mode", choices=["file", "otlp"], default="file")
    parser.add_argument("--protocol", default="otlp_http")
    parser.add_argument("--cluster-name", default="memagent-bench")
    parser.add_argument("--namespace", default="memagent-bench")
    parser.add_argument("--memagent-image", default="logfwd:e2e")
    parser.add_argument("--collector-image", default=None)
    parser.add_argument("--results-dir", default=None)
    parser.add_argument("--benchkit-run-id", default=None)
    parser.add_argument("--benchkit-kind", choices=["workflow", "hybrid"], default="workflow")
    parser.add_argument("--benchkit-service-name", default="memagent-e2e.kind-bench")
    parser.add_argument("--benchkit-otlp-http-endpoint", default=None)
    parser.add_argument("--cpu-profile", choices=sorted(CPU_PROFILES), default="single")
    parser.add_argument("--pods", type=int, default=None)
    parser.add_argument("--eps-per-pod", type=int, default=None)
    parser.add_argument("--keep-cluster", action="store_true")
    return parser.parse_args()


def resolve_results_dir(raw: str | None) -> Path:
    if raw:
        return Path(raw).resolve()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (BENCH_ROOT / "results" / timestamp).resolve()


def render_template(template_name: str, destination: Path, substitutions: dict[str, str]) -> None:
    if template_name.startswith("collectors/"):
        template_path = COLLECTOR_MANIFESTS_ROOT / template_name.removeprefix("collectors/")
    elif template_name.startswith("workload/"):
        template_path = WORKLOAD_MANIFESTS_ROOT / template_name.removeprefix("workload/")
    else:
        template_path = COMMON_MANIFESTS_ROOT / template_name
    content = string.Template(template_path.read_text(encoding="utf-8")).safe_substitute(substitutions)
    destination.write_text(content, encoding="utf-8")


def copy_static_manifest(filename: str, destination: Path, substitutions: dict[str, str]) -> None:
    source = COMMON_MANIFESTS_ROOT / filename
    content = string.Template(source.read_text(encoding="utf-8")).safe_substitute(substitutions)
    destination.write_text(content, encoding="utf-8")


def ensure_tools() -> None:
    for tool in ("curl", "docker", "kind", "kubectl", "python3"):
        require_tool(tool)


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_samples(path: Path, samples: list[StatsSample]) -> None:
    write_json(
        path,
        [
            {
                "timestamp": sample.timestamp,
                "output_lines": sample.output_lines,
                "rss_bytes": sample.rss_bytes,
                "cpu_total_ms": sample.cpu_total_ms,
            }
            for sample in samples
        ],
    )


def normalize_otlp_metrics_url(endpoint: str) -> str:
    trimmed = endpoint.strip().rstrip("/")
    if not trimmed:
        raise ValueError("OTLP HTTP endpoint must not be blank")
    if trimmed.endswith("/v1/metrics"):
        return trimmed
    return f"{trimmed}/v1/metrics"


def format_cpu_quantity(mcpu: int) -> str:
    if mcpu < 0:
        raise ValueError("cpu quantity cannot be negative")
    if mcpu % 1000 == 0:
        return str(mcpu // 1000)
    return f"{mcpu}m"


def build_resource_plan(
    *,
    cpu_profile: CpuProfile,
    emitter_pods: int,
    eps_per_pod: int,
    unbounded_generator: bool = False,
) -> ResourcePlan:
    if emitter_pods <= 0:
        raise ValueError("emitter pod count must be > 0")

    node_budget_mcpu = int(cpu_profile.cluster_cpu_cores * 1000)
    reserved_mcpu = cpu_profile.sink_cpu_mcpu + cpu_profile.capture_reader_cpu_mcpu
    emitter_mcpu = cpu_profile.emitter_cpu_mcpu_per_pod
    if eps_per_pod >= 100_000:
        emitter_mcpu = max(emitter_mcpu, 200)
    if unbounded_generator:
        emitter_mcpu = max(emitter_mcpu, 200)
    collector_mcpu = node_budget_mcpu - reserved_mcpu - (emitter_mcpu * emitter_pods)

    if collector_mcpu < cpu_profile.collector_cpu_mcpu_min:
        emitter_budget = max(1, (node_budget_mcpu - reserved_mcpu - cpu_profile.collector_cpu_mcpu_min) // emitter_pods)
        emitter_mcpu = min(emitter_mcpu, emitter_budget)
        collector_mcpu = node_budget_mcpu - reserved_mcpu - (emitter_mcpu * emitter_pods)

    collector_mcpu = min(collector_mcpu, cpu_profile.collector_cpu_mcpu_target)
    if collector_mcpu < 100:
        raise ValueError(
            f"cpu profile '{cpu_profile.name}' leaves only {collector_mcpu}m for collector with {emitter_pods} emitter pods"
        )

    emitter_memory_limit = cpu_profile.emitter_memory_limit
    if eps_per_pod >= 10_000:
        emitter_memory_limit = "256Mi"
    if eps_per_pod >= 100_000:
        emitter_memory_limit = "512Mi"
    if unbounded_generator:
        emitter_memory_limit = "512Mi"

    return ResourcePlan(
        cpu_profile=cpu_profile,
        collector_cpu=format_cpu_quantity(collector_mcpu),
        emitter_cpu=format_cpu_quantity(emitter_mcpu),
        sink_cpu=format_cpu_quantity(cpu_profile.sink_cpu_mcpu),
        capture_reader_cpu=format_cpu_quantity(cpu_profile.capture_reader_cpu_mcpu),
        collector_memory=cpu_profile.collector_memory_limit,
        emitter_memory=emitter_memory_limit,
        sink_memory=cpu_profile.sink_memory_limit,
        capture_reader_memory=cpu_profile.capture_reader_memory_limit,
    )


def resolve_profile(
    *,
    profile_name: str,
    pods_override: int | None,
    eps_per_pod_override: int | None,
) -> Profile:
    profile = PROFILES[profile_name]
    pods = pods_override if pods_override is not None else profile.pods
    eps_per_pod = eps_per_pod_override if eps_per_pod_override is not None else profile.eps_per_pod
    if pods <= 0:
        raise ValueError("pods must be > 0")
    if eps_per_pod < 0:
        raise ValueError("eps_per_pod must be >= 0")
    return replace(profile, pods=pods, eps_per_pod=eps_per_pod)


def emit_otlp_metrics(*, endpoint: str, payload: dict[str, object], timeout_sec: int = 10) -> None:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        normalize_otlp_metrics_url(endpoint),
        data=body,
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        status = getattr(response, "status", 200)
        if status >= 400:
            raise RuntimeError(f"collector rejected OTLP metrics with HTTP {status}")


def append_artifact_note(results_dir: Path, filename: str, message: str) -> None:
    artifacts_dir = results_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    path = artifacts_dir / filename
    with path.open("a", encoding="utf-8") as handle:
        handle.write(message.rstrip() + "\n")


def emit_phase_signal(
    *,
    args: argparse.Namespace,
    results_dir: Path,
    result: BenchmarkResult,
    profile_name: str,
    phase_name: str,
    event: str,
    status: str | None = None,
) -> None:
    if not args.benchkit_otlp_http_endpoint:
        return
    try:
        payload = build_otlp_phase_signal_payload(
            result=result,
            run_id=args.benchkit_run_id or result.benchmark_id,
            kind=args.benchkit_kind,
            service_name=args.benchkit_service_name,
            profile=profile_name,
            phase_name=phase_name,
            event=event,
            status=status,
            ref=os.environ.get("GITHUB_REF_NAME") or os.environ.get("GITHUB_REF"),
            commit=os.environ.get("GITHUB_SHA"),
            workflow=os.environ.get("GITHUB_WORKFLOW"),
            job=os.environ.get("GITHUB_JOB"),
            run_attempt=os.environ.get("GITHUB_RUN_ATTEMPT"),
            runner=os.environ.get("RUNNER_NAME") or os.environ.get("ImageOS"),
        )
        emit_otlp_metrics(
            endpoint=args.benchkit_otlp_http_endpoint,
            payload=payload,
        )
    except Exception as exc:  # noqa: BLE001
        append_artifact_note(results_dir, "otlp-emit-error.txt", f"phase signal {phase_name}/{event}: {exc}")


def collect_pod_logs(
    *,
    namespace: str,
    pod_names: list[str],
    destination: Path,
    tail: int,
) -> None:
    chunks: list[str] = []
    for pod_name in pod_names:
        chunks.append(f"==== {pod_name}")
        proc = subprocess.run(
            ["kubectl", "-n", namespace, "logs", pod_name, f"--tail={tail}"],
            text=True,
            capture_output=True,
            check=False,
        )
        chunks.append(proc.stdout if proc.stdout else proc.stderr)
    destination.write_text("\n".join(chunks), encoding="utf-8")


def collect_sink_capture(namespace: str, sink_pod: str, destination: Path) -> None:
    sink_capture = subprocess.run(
        [
            "kubectl",
            "-n",
            namespace,
            "exec",
            "-c",
            "capture-reader",
            sink_pod,
            "--",
            "cat",
            "/var/lib/logfwd/capture.ndjson",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    destination.write_text(
        sink_capture.stdout if sink_capture.stdout else sink_capture.stderr,
        encoding="utf-8",
    )


def filter_rows_to_emitter_snapshot(
    rows: list[dict[str, object]],
    emitter_reported_stats: list[dict[str, object]],
) -> list[dict[str, object]]:
    cutoff_by_pod: dict[str, int] = {}
    for stat in emitter_reported_stats:
        pod_name = stat.get("pod_name")
        output_lines = stat.get("output_lines")
        if isinstance(pod_name, str) and isinstance(output_lines, int):
            cutoff_by_pod[pod_name] = output_lines

    filtered: list[dict[str, object]] = []
    for row in rows:
        pod_name = row.get("pod_name")
        seq = row.get("seq")
        if not isinstance(pod_name, str) or not isinstance(seq, int):
            continue
        cutoff = cutoff_by_pod.get(pod_name)
        if cutoff is None or seq <= cutoff:
            filtered.append(row)
    return filtered


def sink_reported_events_total(
    sink_reported_stats: dict[str, object],
    *,
    sink_stats_kind: str,
) -> int:
    if sink_stats_kind == "capture_reader":
        return int(sink_reported_stats.get("benchmark_rows_total", 0) or 0)
    return int(sink_reported_stats.get("output_lines", 0) or 0)


def wait_for_sink_catch_up(
    *,
    namespace: str,
    sink_pod: str,
    sink_stats_kind: str,
    sink_stats_port: int,
    target_events_total: int | None,
    timeout_sec: int,
    poll_sec: float = 1.0,
) -> dict[str, object]:
    deadline = time.time() + timeout_sec
    last_stats: dict[str, object] = {}
    while True:
        try:
            last_stats = collect_sink_reported_stats(
                namespace,
                sink_pod,
                sink_stats_kind=sink_stats_kind,
                sink_stats_port=sink_stats_port,
            )
        except Exception:  # noqa: BLE001
            if time.time() >= deadline:
                return last_stats
            time.sleep(poll_sec)
            continue

        if target_events_total is None:
            return last_stats

        if sink_reported_events_total(last_stats, sink_stats_kind=sink_stats_kind) >= target_events_total:
            return last_stats

        if time.time() >= deadline:
            return last_stats
        time.sleep(poll_sec)


def render_manifests(
    *,
    args: argparse.Namespace,
    profile: Profile,
    adapter: CollectorAdapter,
    resource_plan: ResourcePlan,
    benchmark_id: str,
    rendered_dir: Path,
) -> dict[str, Path]:
    generator_batch_size = 64 if profile.eps_per_pod == 0 else 1024
    substitutions = {
        "NAMESPACE": args.namespace,
        "MEMAGENT_IMAGE": args.memagent_image,
        "COLLECTOR_IMAGE": args.collector_image or adapter.collector_image or args.memagent_image,
        "BENCHMARK_ID": benchmark_id,
        "EPS_PER_POD": str(profile.eps_per_pod),
        "GENERATOR_BATCH_SIZE": str(generator_batch_size),
        "POD_REPLICAS": str(profile.pods),
        "COLLECTOR_CPU_REQUEST": resource_plan.collector_cpu,
        "COLLECTOR_CPU_LIMIT": resource_plan.collector_cpu,
        "COLLECTOR_MEMORY_REQUEST": resource_plan.collector_memory,
        "COLLECTOR_MEMORY_LIMIT": resource_plan.collector_memory,
        "EMITTER_CPU_REQUEST": resource_plan.emitter_cpu,
        "EMITTER_CPU_LIMIT": resource_plan.emitter_cpu,
        "EMITTER_MEMORY_REQUEST": resource_plan.emitter_memory,
        "EMITTER_MEMORY_LIMIT": resource_plan.emitter_memory,
        "SINK_CPU_REQUEST": resource_plan.sink_cpu,
        "SINK_CPU_LIMIT": resource_plan.sink_cpu,
        "SINK_MEMORY_REQUEST": resource_plan.sink_memory,
        "SINK_MEMORY_LIMIT": resource_plan.sink_memory,
        "CAPTURE_READER_CPU_REQUEST": resource_plan.capture_reader_cpu,
        "CAPTURE_READER_CPU_LIMIT": resource_plan.capture_reader_cpu,
        "CAPTURE_READER_MEMORY_REQUEST": resource_plan.capture_reader_memory,
        "CAPTURE_READER_MEMORY_LIMIT": resource_plan.capture_reader_memory,
    }
    manifests = {
        "namespace": rendered_dir / "namespace.yaml",
        "sink_configmap": rendered_dir / "sink-configmap.yaml",
        "sink_deployment": rendered_dir / "sink-deployment.yaml",
        "collector_configmap": rendered_dir / "collector-configmap.yaml",
        "collector_workload": rendered_dir / "collector-workload.yaml",
        "emitter_configmap": rendered_dir / "emitter-configmap.yaml",
        "emitter_statefulset": rendered_dir / "emitter-statefulset.yaml",
    }
    copy_static_manifest("namespace.yaml", manifests["namespace"], substitutions)
    render_template("sink-configmap.yaml.tmpl", manifests["sink_configmap"], substitutions)
    render_template("sink-deployment.yaml.tmpl", manifests["sink_deployment"], substitutions)
    if args.phase == "smoke":
        collector_config_template, collector_workload_template = adapter.templates_for_ingest_mode(args.ingest_mode)
        emitter_config_template = (
            "workload/log-emitter-configmap-otlp.yaml.tmpl"
            if args.ingest_mode == "otlp"
            else "workload/log-emitter-configmap.yaml.tmpl"
        )
        render_template(collector_config_template, manifests["collector_configmap"], substitutions)
        render_template(collector_workload_template, manifests["collector_workload"], substitutions)
        render_template(emitter_config_template, manifests["emitter_configmap"], substitutions)
        render_template("workload/log-emitter-statefulset.yaml.tmpl", manifests["emitter_statefulset"], substitutions)
    return manifests


def run_smoke_phase(
    *,
    args: argparse.Namespace,
    profile: Profile,
    adapter: CollectorAdapter,
    manifests: dict[str, Path],
    results_dir: Path,
    result: BenchmarkResult,
) -> int:
    max_throughput_mode = profile.eps_per_pod == 0

    apply_manifest(manifests["collector_configmap"])
    apply_manifest(manifests["collector_workload"])
    rollout_status(args.namespace, adapter.rollout_kind, adapter.rollout_name, timeout_sec=120)

    collector_pod = get_first_pod_name(args.namespace, adapter.pod_selector)
    if not collector_pod:
        raise CommandError("collector pod not found after rollout")

    apply_manifest(manifests["emitter_configmap"])
    apply_manifest(manifests["emitter_statefulset"])
    emitter_rollout_timeout = max(120, profile.pods * 12)
    rollout_status(args.namespace, "statefulset", "log-emitter", timeout_sec=emitter_rollout_timeout)

    emit_phase_signal(
        args=args,
        results_dir=results_dir,
        result=result,
        profile_name=args.profile,
        phase_name="warmup",
        event="start",
    )
    sink_samples, collector_samples = collect_bench_samples(
        args.namespace,
        "deployment/logfwd-capture",
        adapter.diagnostics_target_format.format(pod_name=collector_pod),
        sink_stats_kind="capture_reader" if adapter.sink_transport == "http_ndjson" else "logfwd",
        sink_stats_port=8081 if adapter.sink_transport == "http_ndjson" else 9090,
        collector_stats_kind=adapter.collector_stats_kind,
        collector_stats_port=adapter.collector_stats_port,
        warmup_sec=profile.warmup_sec,
        measure_sec=profile.measure_sec,
        on_measure_start=lambda: emit_phase_signal(
            args=args,
            results_dir=results_dir,
            result=result,
            profile_name=args.profile,
            phase_name="measure",
            event="start",
        ),
        on_measure_complete=lambda: emit_phase_signal(
            args=args,
            results_dir=results_dir,
            result=result,
            profile_name=args.profile,
            phase_name="measure",
            event="complete",
        ),
    )
    write_samples(results_dir / "sink-samples.json", sink_samples)
    write_samples(results_dir / "collector-samples.json", collector_samples)

    if profile.cooldown_sec > 0:
        emit_phase_signal(
            args=args,
            results_dir=results_dir,
            result=result,
            profile_name=args.profile,
            phase_name="cooldown",
            event="start",
        )
        time.sleep(profile.cooldown_sec)
        emit_phase_signal(
            args=args,
            results_dir=results_dir,
            result=result,
            profile_name=args.profile,
            phase_name="cooldown",
            event="complete",
        )

    result.sink_lines_total = diff_output_lines(sink_samples)
    sink_series = lines_per_sec_series(sink_samples)
    result.sink_lines_per_sec_avg = avg(sink_series)
    result.sink_lines_per_sec_p50 = percentile(sink_series, 0.50)
    result.sink_lines_per_sec_p95 = percentile(sink_series, 0.95)
    result.sink_lines_per_sec_p99 = percentile(sink_series, 0.99)

    cpu_series = cpu_cores_series(collector_samples)
    rss_series = rss_mb_series(collector_samples)
    result.collector_cpu_cores_avg = avg(cpu_series)
    result.collector_cpu_cores_p95 = percentile(cpu_series, 0.95)
    result.collector_rss_mb_avg = avg(rss_series)
    result.collector_rss_mb_p95 = percentile(rss_series, 0.95)

    artifacts_dir = results_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    emitter_pods = get_pod_names(args.namespace, "app.kubernetes.io/name=log-emitter")
    emitter_reported_total, emitter_reported_stats = collect_emitter_reported_total(args.namespace, emitter_pods)
    result.emitter_reported_events_total = emitter_reported_total

    sink_pod = get_first_pod_name(args.namespace, "app.kubernetes.io/name=logfwd-capture")
    if not sink_pod:
        raise CommandError("sink pod not found after rollout")

    sink_stats_kind = "capture_reader" if adapter.sink_transport == "http_ndjson" else "logfwd"
    sink_stats_port = 8081 if adapter.sink_transport == "http_ndjson" else 9090
    drain_timeout_sec = 45 if adapter.sink_transport == "http_ndjson" else 10
    sink_reported_stats = wait_for_sink_catch_up(
        namespace=args.namespace,
        sink_pod=sink_pod,
        sink_stats_kind=sink_stats_kind,
        sink_stats_port=sink_stats_port,
        target_events_total=emitter_reported_total,
        timeout_sec=drain_timeout_sec,
    )
    result.sink_reported_events_total = sink_reported_events_total(
        sink_reported_stats,
        sink_stats_kind=sink_stats_kind,
    )
    collect_pod_logs(
        namespace=args.namespace,
        pod_names=[sink_pod],
        destination=artifacts_dir / "sink-logs.txt",
        tail=-1,
    )
    collect_sink_capture(args.namespace, sink_pod, artifacts_dir / "sink-capture.ndjson")

    sink_rows = filter_rows_to_emitter_snapshot(
        benchmark_rows(load_json_lines(artifacts_dir / "sink-capture.ndjson"), result.benchmark_id),
        emitter_reported_stats,
    )

    if max_throughput_mode:
        result.captured_rows_total = len(sink_rows)
        result.source_rows_total = None
        result.missing_source_count = None
        result.missing_event_count = None
        result.unexpected_event_count = None
        result.dup_estimate = None
        if result.emitter_reported_events_total is not None and result.sink_reported_events_total is not None:
            result.drop_estimate = max(0, result.emitter_reported_events_total - result.sink_reported_events_total)
        else:
            result.drop_estimate = None

        write_json(results_dir / "actual_rows.json", sink_rows)
        write_json(results_dir / "source_rows.json", [])
        write_json(
            results_dir / "stream-summary.json",
            {
                "mode": "max-throughput",
                "source_oracle": "skipped",
                "emitter_reported_events_total": result.emitter_reported_events_total,
                "sink_reported_events_total": result.sink_reported_events_total,
                "sink_row_count": len(sink_rows),
                "sink_lines_total": result.sink_lines_total,
                "sink_lines_per_sec_avg": result.sink_lines_per_sec_avg,
                "drop_estimate": result.drop_estimate,
            },
        )
        write_json(results_dir / "emitter-stats.json", emitter_reported_stats)
        write_json(results_dir / "sink-stats.json", sink_reported_stats)

        observed_sink_output = bool(
            (result.sink_lines_total is not None and result.sink_lines_total > 0)
            or (result.sink_reported_events_total is not None and result.sink_reported_events_total > 0)
            or len(sink_rows) > 0
        )
        if observed_sink_output:
            result.status = "pass"
            result.notes = (
                "max-throughput benchmark completed with unbounded generator; strict source-vs-sink oracle "
                "is intentionally skipped in this mode. "
                f"sink_lines_total={result.sink_lines_total}, sink_lines_per_sec_avg={result.sink_lines_per_sec_avg}, "
                f"emitter_reported_events_total={result.emitter_reported_events_total}, "
                f"sink_reported_events_total={result.sink_reported_events_total}, "
                f"drop_estimate={result.drop_estimate}."
            )
            return 0

        result.status = "fail"
        result.notes = (
            "max-throughput benchmark did not observe sink output in unbounded mode; "
            f"sink_lines_total={result.sink_lines_total}, sink_reported_events_total={result.sink_reported_events_total}."
        )
        return 1

    if args.ingest_mode == "otlp":
        result.captured_rows_total = len(sink_rows)
        result.source_rows_total = None
        result.missing_source_count = None
        result.missing_event_count = None
        result.unexpected_event_count = None
        result.dup_estimate = None
        if result.emitter_reported_events_total is not None and result.sink_reported_events_total is not None:
            result.drop_estimate = max(0, result.emitter_reported_events_total - result.sink_reported_events_total)
        else:
            result.drop_estimate = None

        write_json(results_dir / "actual_rows.json", sink_rows)
        write_json(results_dir / "source_rows.json", [])
        write_json(
            results_dir / "stream-summary.json",
            {
                "mode": "ingest-otlp",
                "source_oracle": "skipped",
                "emitter_reported_events_total": result.emitter_reported_events_total,
                "sink_reported_events_total": result.sink_reported_events_total,
                "sink_row_count": len(sink_rows),
                "sink_lines_total": result.sink_lines_total,
                "sink_lines_per_sec_avg": result.sink_lines_per_sec_avg,
                "drop_estimate": result.drop_estimate,
            },
        )
        write_json(results_dir / "emitter-stats.json", emitter_reported_stats)
        write_json(results_dir / "sink-stats.json", sink_reported_stats)

        observed_sink_output = bool(
            (result.sink_lines_total is not None and result.sink_lines_total > 0)
            or (result.sink_reported_events_total is not None and result.sink_reported_events_total > 0)
            or len(sink_rows) > 0
        )
        if observed_sink_output:
            result.status = "pass"
            result.notes = (
                f"smoke benchmark completed in {adapter.benchmark_mode} with ingest_mode={args.ingest_mode}; "
                "source-vs-sink oracle is intentionally skipped for direct-OTLP input. "
                f"sink_lines_total={result.sink_lines_total}, sink_lines_per_sec_avg={result.sink_lines_per_sec_avg}, "
                f"emitter_reported_events_total={result.emitter_reported_events_total}, "
                f"sink_reported_events_total={result.sink_reported_events_total}, "
                f"drop_estimate={result.drop_estimate}."
            )
            return 0

        result.status = "fail"
        result.notes = (
            f"smoke benchmark did not observe sink output with ingest_mode={args.ingest_mode}; "
            f"sink_lines_total={result.sink_lines_total}, sink_reported_events_total={result.sink_reported_events_total}."
        )
        return 1

    collect_pod_logs(
        namespace=args.namespace,
        pod_names=emitter_pods,
        destination=artifacts_dir / "emitter-logs.txt",
        tail=-1,
    )

    collector_pods = get_pod_names(args.namespace, adapter.pod_selector)
    if collector_pods:
        collect_pod_logs(
            namespace=args.namespace,
            pod_names=collector_pods,
            destination=artifacts_dir / "collector-logs.txt",
            tail=200,
        )

    source_rows = filter_rows_to_emitter_snapshot(
        benchmark_rows(load_json_lines(artifacts_dir / "emitter-logs.txt"), result.benchmark_id),
        emitter_reported_stats,
    )
    expected_sources = expected_emitter_pods("log-emitter", profile.pods)
    comparison = compare_source_and_sink(
        source_rows=source_rows,
        sink_rows=sink_rows,
        expected_sources=expected_sources,
    )

    result.captured_rows_total = comparison.sink_row_count
    result.source_rows_total = comparison.source_row_count
    result.missing_source_count = comparison.missing_source_count
    result.missing_event_count = comparison.missing_event_count
    result.unexpected_event_count = comparison.unexpected_event_count
    result.dup_estimate = comparison.duplicate_event_count
    result.drop_estimate = comparison.missing_event_count

    write_json(results_dir / "actual_rows.json", sink_rows)
    write_json(results_dir / "source_rows.json", source_rows)
    write_json(
        results_dir / "stream-summary.json",
        {
            "expected_sources": comparison.expected_sources,
            "observed_sources": comparison.observed_sources,
            "missing_sources": comparison.missing_sources,
            "emitter_reported_events_total": result.emitter_reported_events_total,
            "sink_reported_events_total": result.sink_reported_events_total,
            "source_row_count": comparison.source_row_count,
            "sink_row_count": comparison.sink_row_count,
            "missing_event_count": comparison.missing_event_count,
            "unexpected_event_count": comparison.unexpected_event_count,
            "duplicate_event_count": comparison.duplicate_event_count,
            "gap_count": comparison.gap_count,
        },
    )
    write_json(results_dir / "emitter-stats.json", emitter_reported_stats)
    write_json(results_dir / "sink-stats.json", sink_reported_stats)

    source_oracle_incomplete = (
        result.emitter_reported_events_total is not None
        and comparison.source_row_count < result.emitter_reported_events_total
    )
    diagnostics_available = result.emitter_reported_events_total is not None and result.sink_reported_events_total is not None
    diagnostics_oracle_clean = (
        diagnostics_available
        and result.sink_reported_events_total >= result.emitter_reported_events_total
        and comparison.missing_source_count == 0
    )
    observed_any_sink_output = bool((result.sink_lines_total is not None and result.sink_lines_total > 0) or comparison.sink_row_count > 0)

    if source_oracle_incomplete:
        result.missing_event_count = None
        result.unexpected_event_count = None
        result.dup_estimate = None
        if diagnostics_available:
            result.drop_estimate = max(0, result.emitter_reported_events_total - result.sink_reported_events_total)
        else:
            result.drop_estimate = None

        if diagnostics_oracle_clean and observed_any_sink_output:
            result.status = "pass"
            result.notes = (
                f"smoke benchmark passed in {adapter.benchmark_mode} with degraded source oracle; "
                f"source rows captured ({comparison.source_row_count}) were lower than emitter diagnostics total "
                f"({result.emitter_reported_events_total}), so pass/fail used emitter/sink diagnostics totals instead. "
                f"captured_rows_total={comparison.sink_row_count}, sink_lines_total={result.sink_lines_total}, "
                f"sink_reported_events_total={result.sink_reported_events_total}, drop_estimate={result.drop_estimate}."
            )
            return 0

        if diagnostics_available:
            result.status = "fail"
            result.notes = (
                f"smoke benchmark failed in {adapter.benchmark_mode} with degraded source oracle; "
                f"source rows captured ({comparison.source_row_count}) were lower than emitter diagnostics total "
                f"({result.emitter_reported_events_total}), and sink diagnostics remained behind "
                f"({result.sink_reported_events_total}). captured_rows_total={comparison.sink_row_count}, "
                f"sink_lines_total={result.sink_lines_total}, drop_estimate={result.drop_estimate}."
            )
        else:
            result.status = "fail"
            result.notes = (
                f"smoke benchmark failed in {adapter.benchmark_mode} with degraded source oracle and missing diagnostics; "
                f"source_rows_total={comparison.source_row_count}, captured_rows_total={comparison.sink_row_count}, "
                f"sink_lines_total={result.sink_lines_total}."
            )
        return 1

    strict_oracle_clean = (
        comparison.missing_source_count == 0
        and comparison.missing_event_count == 0
        and comparison.unexpected_event_count == 0
        and comparison.duplicate_event_count == 0
        and comparison.gap_count == 0
    )
    if strict_oracle_clean and observed_any_sink_output:
        result.status = "pass"
        result.notes = (
            f"smoke benchmark succeeded in {adapter.benchmark_mode}; source_rows_total={comparison.source_row_count}, "
            f"captured_rows_total={comparison.sink_row_count}, sink_lines_total={result.sink_lines_total}, "
            f"missing_events={comparison.missing_event_count}, unexpected_events={comparison.unexpected_event_count}, "
            f"duplicates={comparison.duplicate_event_count}, gaps={comparison.gap_count}."
        )
        return 0

    result.status = "fail"
    result.notes = (
        f"smoke benchmark failed in {adapter.benchmark_mode}; source_rows_total={comparison.source_row_count}, "
        f"captured_rows_total={comparison.sink_row_count}, sink_lines_total={result.sink_lines_total}, "
        f"missing_sources={comparison.missing_sources}, missing_events={comparison.missing_event_count}, "
        f"unexpected_events={comparison.unexpected_event_count}, duplicates={comparison.duplicate_event_count}, "
        f"gaps={comparison.gap_count}."
    )
    return 1


def main() -> int:
    args = parse_args()
    profile = resolve_profile(
        profile_name=args.profile,
        pods_override=args.pods,
        eps_per_pod_override=args.eps_per_pod,
    )
    adapter = get_collector_adapter(args.collector)
    if not adapter.supports_ingest_mode(args.ingest_mode):
        raise NotImplementedError(f"collector '{args.collector}' does not support ingest mode '{args.ingest_mode}'")
    cpu_profile = CPU_PROFILES[args.cpu_profile]
    resource_plan = build_resource_plan(
        cpu_profile=cpu_profile,
        emitter_pods=profile.pods,
        eps_per_pod=profile.eps_per_pod,
        unbounded_generator=profile.eps_per_pod == 0,
    )
    results_dir = resolve_results_dir(args.results_dir)
    rendered_dir = results_dir / "rendered-manifests"
    rendered_dir.mkdir(parents=True, exist_ok=True)

    benchmark_id = str(uuid.uuid4())
    timestamp_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = BenchmarkResult(
        benchmark_id=benchmark_id,
        timestamp_utc=timestamp_utc,
        phase=args.phase,
        benchmark_mode="infra-bootstrap" if args.phase == "infra" else adapter.benchmark_mode,
        cluster="kind-single-node",
        cluster_name=args.cluster_name,
        namespace=args.namespace,
        collector=args.collector,
        protocol=adapter.sink_transport if args.protocol == "otlp_http" else args.protocol,
        ingest_mode=args.ingest_mode,
        cpu_profile=args.cpu_profile,
        cluster_cpu_limit_cores=cpu_profile.cluster_cpu_cores,
        pods=profile.pods,
        target_eps_per_pod=profile.eps_per_pod,
        total_target_eps=profile.total_target_eps,
        warmup_sec=profile.warmup_sec,
        measure_sec=profile.measure_sec,
        cooldown_sec=profile.cooldown_sec,
        sink_lines_total=None,
        emitter_reported_events_total=None,
        sink_reported_events_total=None,
        captured_rows_total=None,
        source_rows_total=None,
        missing_source_count=None,
        missing_event_count=None,
        unexpected_event_count=None,
        sink_lines_per_sec_avg=None,
        sink_lines_per_sec_p50=None,
        sink_lines_per_sec_p95=None,
        sink_lines_per_sec_p99=None,
        drop_estimate=None,
        dup_estimate=None,
        latency_ms_p50=None,
        latency_ms_p95=None,
        latency_ms_p99=None,
        collector_cpu_cores_avg=None,
        collector_cpu_cores_p95=None,
        collector_rss_mb_avg=None,
        collector_rss_mb_p95=None,
        cluster_ready=False,
        sink_ready=False,
        status="partial",
        notes="run started",
    )

    return_code = 1
    manifests = render_manifests(
        args=args,
        profile=profile,
        adapter=adapter,
        resource_plan=resource_plan,
        benchmark_id=benchmark_id,
        rendered_dir=rendered_dir,
    )

    try:
        ensure_tools()
        emit_phase_signal(
            args=args,
            results_dir=results_dir,
            result=result,
            profile_name=args.profile,
            phase_name="setup",
            event="start",
        )
        create_kind_cluster(args.cluster_name)
        set_kind_control_plane_cpu_limit(args.cluster_name, cpu_profile.cluster_cpu_cores)
        result.cluster_ready = True
        load_image_into_kind(args.cluster_name, args.memagent_image)

        apply_manifest(manifests["namespace"])
        wait_for_namespace(args.namespace)
        apply_manifest(manifests["sink_configmap"])
        apply_manifest(manifests["sink_deployment"])
        wait_for_deployment(args.namespace, "logfwd-capture", timeout_sec=90)
        result.sink_ready = True

        if args.phase == "infra":
            result.status = "pass"
            result.notes = (
                "infra-only benchmark run succeeded; cluster lifecycle and logfwd sink deployment are healthy. "
                "Run --phase smoke to exercise the collector, emitters, and source-vs-sink oracle."
            )
            return_code = 0
        else:
            return_code = run_smoke_phase(
                args=args,
                profile=profile,
                adapter=adapter,
                manifests=manifests,
                results_dir=results_dir,
                result=result,
            )
    except NotImplementedError as exc:
        result.status = "partial"
        result.notes = str(exc)
        return_code = 1
    except Exception as exc:  # noqa: BLE001
        result.status = "fail"
        result.notes = f"{type(exc).__name__}: {exc}"
        (results_dir / "artifacts").mkdir(parents=True, exist_ok=True)
        (results_dir / "artifacts" / "traceback.txt").write_text(traceback.format_exc(), encoding="utf-8")
        return_code = 1
    finally:
        try:
            collect_debug_artifacts(
                results_dir,
                namespace=args.namespace,
                deployment="logfwd-capture",
                selector="app.kubernetes.io/name=logfwd-capture",
                collector_selector=adapter.pod_selector,
                emitter_selector="app.kubernetes.io/name=log-emitter",
            )
        except Exception as exc:  # noqa: BLE001
            artifacts_dir = results_dir / "artifacts"
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            (artifacts_dir / "artifact-collection-error.txt").write_text(str(exc), encoding="utf-8")
        finally:
            write_result_files(
                results_dir,
                result,
                benchkit_run_id=args.benchkit_run_id or benchmark_id,
                benchkit_kind=args.benchkit_kind,
                benchkit_service_name=args.benchkit_service_name,
                benchkit_profile=args.profile,
                benchkit_ref=os.environ.get("GITHUB_REF_NAME") or os.environ.get("GITHUB_REF"),
                benchkit_commit=os.environ.get("GITHUB_SHA"),
                benchkit_workflow=os.environ.get("GITHUB_WORKFLOW"),
                benchkit_job=os.environ.get("GITHUB_JOB"),
                benchkit_run_attempt=os.environ.get("GITHUB_RUN_ATTEMPT"),
                benchkit_runner=os.environ.get("RUNNER_NAME") or os.environ.get("ImageOS"),
            )
            emit_phase_signal(
                args=args,
                results_dir=results_dir,
                result=result,
                profile_name=args.profile,
                phase_name="run",
                event="complete",
                status=result.status,
            )
            if args.benchkit_otlp_http_endpoint:
                try:
                    otlp_payload = build_otlp_result_payload(
                        result=result,
                        run_id=args.benchkit_run_id or benchmark_id,
                        kind=args.benchkit_kind,
                        service_name=args.benchkit_service_name,
                        profile=args.profile,
                        ref=os.environ.get("GITHUB_REF_NAME") or os.environ.get("GITHUB_REF"),
                        commit=os.environ.get("GITHUB_SHA"),
                        workflow=os.environ.get("GITHUB_WORKFLOW"),
                        job=os.environ.get("GITHUB_JOB"),
                        run_attempt=os.environ.get("GITHUB_RUN_ATTEMPT"),
                        runner=os.environ.get("RUNNER_NAME") or os.environ.get("ImageOS"),
                    )
                    emit_otlp_metrics(
                        endpoint=args.benchkit_otlp_http_endpoint,
                        payload=otlp_payload,
                    )
                except Exception as exc:  # noqa: BLE001
                    append_artifact_note(results_dir, "otlp-emit-error.txt", f"result payload: {exc}")
            if not args.keep_cluster:
                try:
                    delete_kind_cluster(args.cluster_name)
                except Exception as exc:  # noqa: BLE001
                    artifacts_dir = results_dir / "artifacts"
                    artifacts_dir.mkdir(parents=True, exist_ok=True)
                    (artifacts_dir / "cluster-delete-error.txt").write_text(str(exc), encoding="utf-8")
            print(results_dir)
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
