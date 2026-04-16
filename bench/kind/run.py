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
from lib.diagnostics import analyze_delivery_diagnostics
from lib.kube import (
    apply_manifest,
    collect_debug_artifacts,
    collect_pprof_from_pod,
    get_first_pod_name,
    get_pod_names,
    rollout_status,
    send_signal_to_pod,
    wait_for_deployment,
    wait_for_namespace,
)
from lib.measure import (
    PortForward,
    StatsSample,
    avg,
    collect_bench_samples,
    collect_emitter_reported_total,
    collect_sink_reported_stats,
    cpu_cores_series,
    diff_output_lines,
    fetch_text,
    lines_per_sec_series,
    percentile,
    reserve_local_port,
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
    cluster_cpu_cores: float
    collector_cpu: str
    emitter_cpu_request: str
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
        # 2.0 cores gives the collector exactly 1 core at all EPS targets:
        # 2000m - 100m(sink) - 20m(capture) - 5×60m(emitters) = 1580m → capped at 1000m.
        cluster_cpu_cores=2.0,
        collector_cpu_mcpu_min=500,
        collector_cpu_mcpu_target=1000,
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
        # 3.0 cores gives the collector exactly 2 cores at all EPS targets:
        # 3000m - 120m(sink) - 20m(capture) - 5×60m(emitters) = 2560m → capped at 2000m.
        cluster_cpu_cores=3.0,
        collector_cpu_mcpu_min=1200,
        collector_cpu_mcpu_target=2000,
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
    parser.add_argument("--ingest-label", type=lambda v: v or None, default=None, help="Label stored in result.json for ingest config (defaults to --ingest-mode)")
    parser.add_argument(
        "--emitter-batch-target-bytes",
        type=lambda v: int(v) if v else None,
        default=None,
        help="Target batch size in bytes for OTLP emitter output. Sets EMITTER_BATCH_TARGET_BYTES_YAML in manifests.",
    )
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
    parser.add_argument(
        "--collector-batch-target-bytes",
        type=int,
        default=None,
        help=(
            "Optional logfwd collector batch_target_bytes override for benchmark experiments. "
            "Can also be set with BENCH_COLLECTOR_BATCH_TARGET_BYTES."
        ),
    )
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


def resolve_optional_positive_int(cli_value: int | None, env_name: str) -> int | None:
    if cli_value is not None:
        if cli_value <= 0:
            raise ValueError(f"{env_name.lower()} must be > 0")
        return cli_value
    raw = os.environ.get(env_name)
    if raw is None or raw.strip() == "":
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{env_name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{env_name} must be > 0")
    return value


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
    sink_mcpu = cpu_profile.sink_cpu_mcpu
    capture_reader_mcpu = cpu_profile.capture_reader_cpu_mcpu
    collector_mcpu_min = cpu_profile.collector_cpu_mcpu_min
    collector_mcpu_target = cpu_profile.collector_cpu_mcpu_target

    capacity_probe = eps_per_pod >= 10_000 or unbounded_generator
    if capacity_probe:
        # For ladder/max capacity probes, give the generator and collector more
        # CPU headroom.  Both single and multi use a 3-core cluster budget so
        # that all jobs fit on ubuntu-latest (2 physical CPUs + Docker quota).
        if cpu_profile.name == "single":
            node_budget_mcpu = 3000
            sink_mcpu = 900
            capture_reader_mcpu = 100
            collector_mcpu_min = 1000
            collector_mcpu_target = 1000
        else:
            # Multi capacity probes: keep the 3-core budget so the emitter pod
            # can schedule on a 2-core GH runner.  Sink/capture stay lean;
            # collector gets the remaining ~1960 m (≈ 2 cores).
            node_budget_mcpu = 3000
            sink_mcpu = 120
            capture_reader_mcpu = 20
            collector_mcpu_min = 1400
            collector_mcpu_target = 2000

    reserved_mcpu = sink_mcpu + capture_reader_mcpu
    if capacity_probe:
        emitter_total_budget_mcpu = 1000 if cpu_profile.name == "single" else 900
        emitter_mcpu = max(cpu_profile.emitter_cpu_mcpu_per_pod, emitter_total_budget_mcpu // emitter_pods)
    else:
        emitter_mcpu = cpu_profile.emitter_cpu_mcpu_per_pod
        if eps_per_pod >= 100_000:
            emitter_mcpu = max(emitter_mcpu, 200)
        if unbounded_generator:
            emitter_mcpu = max(emitter_mcpu, 200)
    collector_mcpu = node_budget_mcpu - reserved_mcpu - (emitter_mcpu * emitter_pods)

    if collector_mcpu < collector_mcpu_min:
        emitter_budget = max(1, (node_budget_mcpu - reserved_mcpu - collector_mcpu_min) // emitter_pods)
        emitter_mcpu = min(emitter_mcpu, emitter_budget)
        collector_mcpu = node_budget_mcpu - reserved_mcpu - (emitter_mcpu * emitter_pods)

    collector_mcpu = min(collector_mcpu, collector_mcpu_target)
    if collector_mcpu < 100:
        raise ValueError(
            f"cpu profile '{cpu_profile.name}' leaves only {collector_mcpu}m for collector with {emitter_pods} emitter pods"
        )

    emitter_request_mcpu = emitter_mcpu
    if capacity_probe and cpu_profile.name == "multi":
        # Cap request to keep the emitter schedulable; the limit still allows
        # burst headroom under the configured CPU limit.
        emitter_request_mcpu = min(emitter_mcpu, 300)

    # Keep generator and sink memory fixed at 1Gi for benchmark stability.
    # This avoids memory-limit artifacts while we tune throughput behavior.
    emitter_memory_limit = "1Gi"
    sink_memory_limit = "1Gi"

    return ResourcePlan(
        cpu_profile=cpu_profile,
        cluster_cpu_cores=node_budget_mcpu / 1000.0,
        collector_cpu=format_cpu_quantity(collector_mcpu),
        emitter_cpu_request=format_cpu_quantity(emitter_request_mcpu),
        emitter_cpu=format_cpu_quantity(emitter_mcpu),
        sink_cpu=format_cpu_quantity(sink_mcpu),
        capture_reader_cpu=format_cpu_quantity(capture_reader_mcpu),
        collector_memory=cpu_profile.collector_memory_limit,
        emitter_memory=emitter_memory_limit,
        sink_memory=sink_memory_limit,
        capture_reader_memory=cpu_profile.capture_reader_memory_limit,
    )


def adjust_resource_plan_for_adapter(
    *,
    resource_plan: ResourcePlan,
    adapter: CollectorAdapter,
    ingest_mode: str,
    profile: Profile,
) -> ResourcePlan:
    # Keep lane resources identical across collectors. For max-throughput OTLP
    # runs, use a common collector memory envelope to reduce restart noise.
    if ingest_mode == "otlp" and profile.eps_per_pod == 0:
        return replace(resource_plan, collector_memory="8Gi")
    return resource_plan


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


def collector_runtime_snapshot(namespace: str, pod_name: str) -> tuple[int, set[str]]:
    completed = subprocess.run(
        ["kubectl", "-n", namespace, "get", "pod", pod_name, "-o", "json"],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        return 0, set()
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return 0, set()

    container_statuses = payload.get("status", {}).get("containerStatuses", [])
    if not isinstance(container_statuses, list) or not container_statuses:
        return 0, set()
    status = container_statuses[0]
    if not isinstance(status, dict):
        return 0, set()

    restart_count = int(status.get("restartCount", 0) or 0)
    reasons: set[str] = set()
    for state_key in ("state", "lastState"):
        state = status.get(state_key)
        if not isinstance(state, dict):
            continue
        terminated = state.get("terminated")
        if not isinstance(terminated, dict):
            continue
        reason = terminated.get("reason")
        if isinstance(reason, str) and reason:
            reasons.add(reason)
    return restart_count, reasons


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


def collect_collector_status_artifact(
    *,
    namespace: str,
    pod_name: str,
    adapter: CollectorAdapter,
    destination: Path,
) -> None:
    if adapter.collector_stats_kind != "logfwd":
        return
    local_port = reserve_local_port()
    with PortForward(namespace, f"pod/{pod_name}", local_port, adapter.collector_stats_port):
        body = fetch_text(local_port, "/admin/v1/status")
    destination.write_text(body, encoding="utf-8")


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
    seq_bounds_by_pod: dict[str, tuple[int, int]] = {}
    for row in rows:
        pod_name = row.get("pod_name")
        seq_raw = row.get("seq")
        if not isinstance(pod_name, str) or not isinstance(seq_raw, (int, float)):
            continue
        seq = int(seq_raw)
        bounds = seq_bounds_by_pod.get(pod_name)
        if bounds is None:
            seq_bounds_by_pod[pod_name] = (seq, seq)
            continue
        min_seq, max_seq = bounds
        seq_bounds_by_pod[pod_name] = (min(min_seq, seq), max(max_seq, seq))

    cutoff_by_pod: dict[str, int] = {}
    for stat in emitter_reported_stats:
        pod_name = stat.get("pod_name")
        output_lines = stat.get("output_lines")
        if isinstance(pod_name, str) and isinstance(output_lines, (int, float)):
            bounds = seq_bounds_by_pod.get(pod_name)
            if bounds is None:
                continue
            min_seq, _max_seq = bounds
            # `output_lines` is a count, not an absolute sequence number.
            # Sequence values can begin well above 1, so anchor the cutoff to
            # the first observed sequence for this benchmark snapshot.
            cutoff_by_pod[pod_name] = min_seq + max(0, int(output_lines) - 1)

    filtered: list[dict[str, object]] = []
    for row in rows:
        pod_name = row.get("pod_name")
        seq_raw = row.get("seq")
        if not isinstance(pod_name, str) or not isinstance(seq_raw, (int, float)):
            continue
        seq = int(seq_raw)
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


def sink_reached_emitter_snapshot(
    sink_reported_stats: dict[str, object],
    *,
    emitter_reported_stats: list[dict[str, object]],
) -> bool:
    sink_max_by_pod = sink_reported_stats.get("benchmark_max_seq_by_pod")
    if not isinstance(sink_max_by_pod, dict):
        return False

    for stat in emitter_reported_stats:
        pod_name = stat.get("pod_name")
        output_lines = stat.get("output_lines")
        if not isinstance(pod_name, str) or not isinstance(output_lines, int):
            continue
        sink_max = sink_max_by_pod.get(pod_name)
        if not isinstance(sink_max, int) or sink_max < output_lines:
            return False

    return True


def wait_for_sink_catch_up(
    *,
    namespace: str,
    sink_pod: str,
    sink_stats_kind: str,
    sink_stats_port: int,
    target_events_total: int | None,
    emitter_reported_stats: list[dict[str, object]] | None = None,
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

        if sink_stats_kind == "capture_reader" and emitter_reported_stats:
            # For NDJSON capture, global totals can mask per-pod lag
            # (one stream advances while another is behind). Require
            # per-pod sequence coverage against the emitter snapshot.
            if sink_reached_emitter_snapshot(
                last_stats,
                emitter_reported_stats=emitter_reported_stats,
            ):
                return last_stats
        elif sink_reported_events_total(last_stats, sink_stats_kind=sink_stats_kind) >= target_events_total:
            return last_stats

        if time.time() >= deadline:
            return last_stats
        time.sleep(poll_sec)


def wait_for_capture_reader_stability(
    *,
    namespace: str,
    sink_pod: str,
    sink_stats_port: int,
    initial_stats: dict[str, object],
    timeout_sec: float = 3.0,
    poll_sec: float = 0.25,
    required_stable_polls: int = 2,
) -> dict[str, object]:
    deadline = time.time() + timeout_sec
    last_stats = initial_stats
    last_total = int(initial_stats.get("benchmark_rows_total", 0) or 0)
    last_max_by_pod = initial_stats.get("benchmark_max_seq_by_pod")
    if not isinstance(last_max_by_pod, dict):
        last_max_by_pod = {}
    stable_polls = 0

    while time.time() < deadline:
        try:
            current = collect_sink_reported_stats(
                namespace,
                sink_pod,
                sink_stats_kind="capture_reader",
                sink_stats_port=sink_stats_port,
            )
        except Exception:  # noqa: BLE001
            time.sleep(poll_sec)
            continue

        current_total = int(current.get("benchmark_rows_total", 0) or 0)
        current_max_by_pod = current.get("benchmark_max_seq_by_pod")
        if not isinstance(current_max_by_pod, dict):
            current_max_by_pod = {}

        if current_total == last_total and current_max_by_pod == last_max_by_pod:
            stable_polls += 1
            if stable_polls >= required_stable_polls:
                return current
        else:
            stable_polls = 0
            last_total = current_total
            last_max_by_pod = current_max_by_pod

        last_stats = current
        time.sleep(poll_sec)

    return last_stats


def render_manifests(
    *,
    args: argparse.Namespace,
    profile: Profile,
    adapter: CollectorAdapter,
    resource_plan: ResourcePlan,
    benchmark_id: str,
    rendered_dir: Path,
    collector_batch_target_bytes: int | None,
) -> dict[str, Path]:
    if profile.eps_per_pod == 0:
        generator_batch_size = 64
    else:
        # Keep low-rate targets smooth so per-second throughput sampling reflects
        # the requested EPS instead of front-loading a large first burst.
        generator_batch_size = max(1, min(1024, profile.eps_per_pod))
    collector_batch_target_yaml = ""
    if collector_batch_target_bytes is not None:
        collector_batch_target_yaml = f"        batch_target_bytes: {collector_batch_target_bytes}"

    emitter_batch_target_yaml = ""
    if args.emitter_batch_target_bytes is not None:
        emitter_batch_target_yaml = f"batch_target_bytes: {args.emitter_batch_target_bytes}"

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
        "EMITTER_CPU_REQUEST": resource_plan.emitter_cpu_request,
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
        "COLLECTOR_BATCH_TARGET_BYTES_YAML": collector_batch_target_yaml,
        "EMITTER_BATCH_TARGET_BYTES_YAML": emitter_batch_target_yaml,
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
    # At very high bounded targets, source/sink row capture is too expensive and
    # can dominate the lane runtime. Treat these as saturation probes regardless
    # of ingest mode and rely on diagnostics totals + sink throughput signals.
    saturation_target_mode = profile.eps_per_pod >= 100_000

    apply_manifest(manifests["collector_configmap"])
    apply_manifest(manifests["collector_workload"])
    rollout_status(args.namespace, adapter.rollout_kind, adapter.rollout_name, timeout_sec=300)

    collector_pod = get_first_pod_name(args.namespace, adapter.pod_selector)
    if not collector_pod:
        raise CommandError("collector pod not found after rollout")

    apply_manifest(manifests["emitter_configmap"])
    apply_manifest(manifests["emitter_statefulset"])
    emitter_rollout_timeout = max(300, profile.pods * 12)
    rollout_status(args.namespace, "statefulset", "log-emitter", timeout_sec=emitter_rollout_timeout)
    emitter_pods = get_pod_names(args.namespace, "app.kubernetes.io/name=log-emitter")
    collector_restart_before, collector_reasons_before = collector_runtime_snapshot(args.namespace, collector_pod)

    emit_phase_signal(
        args=args,
        results_dir=results_dir,
        result=result,
        profile_name=args.profile,
        phase_name="warmup",
        event="start",
    )
    sink_samples, collector_samples, emitter_samples = collect_bench_samples(
        args.namespace,
        "deployment/logfwd-capture",
        adapter.diagnostics_target_format.format(pod_name=collector_pod),
        sink_stats_kind="capture_reader" if adapter.sink_transport == "http_ndjson" else "logfwd",
        sink_stats_port=8081 if adapter.sink_transport == "http_ndjson" else 9090,
        collector_stats_kind=adapter.collector_stats_kind,
        collector_stats_port=adapter.collector_stats_port,
        warmup_sec=profile.warmup_sec,
        measure_sec=profile.measure_sec,
        emitter_targets=[f"pod/{pod_name}" for pod_name in emitter_pods],
        emitter_stats_port=9090,
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
    write_samples(results_dir / "emitter-samples.json", emitter_samples)
    collector_pod_after = get_first_pod_name(args.namespace, adapter.pod_selector)
    if collector_pod_after is None:
        collector_pod_after = collector_pod
    collector_restart_after, collector_reasons_after = collector_runtime_snapshot(args.namespace, collector_pod_after)

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

    sink_cpu_series = cpu_cores_series(sink_samples)
    sink_rss_series = rss_mb_series(sink_samples)
    result.sink_cpu_cores_avg = avg(sink_cpu_series)
    result.sink_cpu_cores_p95 = percentile(sink_cpu_series, 0.95)
    result.sink_rss_mb_avg = avg(sink_rss_series)
    result.sink_rss_mb_p95 = percentile(sink_rss_series, 0.95)

    cpu_series = cpu_cores_series(collector_samples)
    rss_series = rss_mb_series(collector_samples)
    result.collector_cpu_cores_avg = avg(cpu_series)
    result.collector_cpu_cores_p95 = percentile(cpu_series, 0.95)
    result.collector_rss_mb_avg = avg(rss_series)
    result.collector_rss_mb_p95 = percentile(rss_series, 0.95)

    emitter_cpu_series = cpu_cores_series(emitter_samples)
    result.generator_cpu_cores_avg = avg(emitter_cpu_series)
    result.generator_cpu_cores_p95 = percentile(emitter_cpu_series, 0.95)

    artifacts_dir = results_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    collector_restarts_during_measure = max(0, collector_restart_after - collector_restart_before)
    collector_pod_replaced = collector_pod_after != collector_pod
    collector_new_termination_reasons = sorted(collector_reasons_after - collector_reasons_before)
    write_json(
        artifacts_dir / "collector-runtime.json",
        {
            "collector_pod_before": collector_pod,
            "collector_pod_after": collector_pod_after,
            "restart_count_before": collector_restart_before,
            "restart_count_after": collector_restart_after,
            "restart_count_delta": collector_restarts_during_measure,
            "termination_reasons_before": sorted(collector_reasons_before),
            "termination_reasons_after": sorted(collector_reasons_after),
            "termination_reasons_new": collector_new_termination_reasons,
            "pod_replaced": collector_pod_replaced,
        },
    )
    collector_instability_reasons: list[str] = []
    if collector_pod_replaced:
        collector_instability_reasons.append(f"collector pod changed from {collector_pod} to {collector_pod_after}")
    if collector_restarts_during_measure > 0:
        collector_instability_reasons.append(
            f"collector restart_count increased by {collector_restarts_during_measure} "
            f"(from {collector_restart_before} to {collector_restart_after})"
        )
    if collector_restarts_during_measure > 0 and "OOMKilled" in collector_reasons_after:
        collector_instability_reasons.append("collector was OOMKilled during warmup/measure")
    collector_instability_note = "; ".join(collector_instability_reasons) if collector_instability_reasons else None

    def finalize(exit_code: int) -> int:
        if collector_instability_note is None:
            return exit_code
        result.status = "fail"
        prior = result.notes.strip() if isinstance(result.notes, str) else ""
        prefix = f"collector instability detected: {collector_instability_note}."
        result.notes = f"{prefix} {prior}".strip() if prior else prefix
        return 1

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
        emitter_reported_stats=emitter_reported_stats,
        timeout_sec=drain_timeout_sec,
    )
    if sink_stats_kind == "capture_reader":
        # capture-reader stats can lead file writes by a short interval.
        # Wait for a couple of stable polls before reading capture.ndjson so
        # strict source-vs-sink checks do not fail on last-line races.
        sink_reported_stats = wait_for_capture_reader_stability(
            namespace=args.namespace,
            sink_pod=sink_pod,
            sink_stats_port=sink_stats_port,
            initial_stats=sink_reported_stats,
        )
    result.sink_reported_events_total = sink_reported_events_total(
        sink_reported_stats,
        sink_stats_kind=sink_stats_kind,
    )
    try:
        collect_collector_status_artifact(
            namespace=args.namespace,
            pod_name=collector_pod_after,
            adapter=adapter,
            destination=artifacts_dir / "collector-status.json",
        )
    except Exception as exc:  # noqa: BLE001
        append_artifact_note(results_dir, "collector-status-error.txt", str(exc))
    # Collect a pprof profile from the collector pod.
    # Sending SIGUSR1 triggers logfwd to write profile.pb.gz then begin graceful shutdown.
    # We copy the file in the short window between write and process exit.
    try:
        send_signal_to_pod(args.namespace, collector_pod_after, "USR1")
        time.sleep(2)
        collect_pprof_from_pod(
            args.namespace,
            collector_pod_after,
            artifacts_dir / "collector-pprof.pb.gz",
        )
    except Exception as exc:  # noqa: BLE001
        append_artifact_note(results_dir, "collector-pprof-error.txt", str(exc))
    collect_pod_logs(
        namespace=args.namespace,
        pod_names=[sink_pod],
        destination=artifacts_dir / "sink-logs.txt",
        tail=-1,
    )
    sink_rows: list[dict[str, object]] = []
    if not max_throughput_mode and not saturation_target_mode:
        collect_sink_capture(args.namespace, sink_pod, artifacts_dir / "sink-capture.ndjson")
        sink_rows = filter_rows_to_emitter_snapshot(
            benchmark_rows(load_json_lines(artifacts_dir / "sink-capture.ndjson"), result.benchmark_id),
            emitter_reported_stats,
        )

    if max_throughput_mode:
        result.captured_rows_total = None
        result.source_rows_total = None
        result.missing_source_count = None
        result.missing_event_count = None
        result.unexpected_event_count = None
        result.dup_estimate = None
        if result.emitter_reported_events_total is not None and result.sink_reported_events_total is not None:
            result.drop_estimate = max(0, result.emitter_reported_events_total - result.sink_reported_events_total)
        else:
            result.drop_estimate = None

        write_json(results_dir / "actual_rows.json", [])
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
            return finalize(0)

        result.status = "fail"
        result.notes = (
            "max-throughput benchmark did not observe sink output in unbounded mode; "
            f"sink_lines_total={result.sink_lines_total}, sink_reported_events_total={result.sink_reported_events_total}."
        )
        return finalize(1)

    if saturation_target_mode:
        result.captured_rows_total = None
        result.source_rows_total = None
        result.missing_source_count = None
        result.missing_event_count = None
        result.unexpected_event_count = None
        result.dup_estimate = None
        if result.emitter_reported_events_total is not None and result.sink_reported_events_total is not None:
            result.drop_estimate = max(0, result.emitter_reported_events_total - result.sink_reported_events_total)
        else:
            result.drop_estimate = None

        write_json(results_dir / "actual_rows.json", [])
        write_json(results_dir / "source_rows.json", [])
        write_json(
            results_dir / "stream-summary.json",
            {
                "mode": "saturation-target",
                "source_oracle": "relaxed",
                "target_eps_per_pod": profile.eps_per_pod,
                "emitter_reported_events_total": result.emitter_reported_events_total,
                "sink_reported_events_total": result.sink_reported_events_total,
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
        )
        if observed_sink_output:
            result.status = "pass"
            result.notes = (
                f"smoke benchmark completed in {adapter.benchmark_mode} at saturation target "
                f"(target_eps_per_pod={profile.eps_per_pod}); strict source-vs-sink oracle is relaxed in this mode. "
                f"sink_lines_total={result.sink_lines_total}, sink_lines_per_sec_avg={result.sink_lines_per_sec_avg}, "
                f"emitter_reported_events_total={result.emitter_reported_events_total}, "
                f"sink_reported_events_total={result.sink_reported_events_total}, "
                f"drop_estimate={result.drop_estimate}."
            )
            return finalize(0)

        result.status = "fail"
        result.notes = (
            f"smoke benchmark did not observe sink output at saturation target (target_eps_per_pod={profile.eps_per_pod}); "
            f"sink_lines_total={result.sink_lines_total}, sink_reported_events_total={result.sink_reported_events_total}."
        )
        return finalize(1)

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
            return finalize(0)

        result.status = "fail"
        result.notes = (
            f"smoke benchmark did not observe sink output with ingest_mode={args.ingest_mode}; "
            f"sink_lines_total={result.sink_lines_total}, sink_reported_events_total={result.sink_reported_events_total}."
        )
        return finalize(1)

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
    observed_any_sink_output = bool((result.sink_lines_total is not None and result.sink_lines_total > 0) or comparison.sink_row_count > 0)

    if source_oracle_incomplete:
        result.missing_event_count = None
        result.unexpected_event_count = None
        result.dup_estimate = None
        if diagnostics_available:
            result.drop_estimate = max(0, result.emitter_reported_events_total - result.sink_reported_events_total)
        else:
            result.drop_estimate = None

        if observed_any_sink_output:
            result.status = "pass"
            result.notes = (
                f"smoke benchmark produced sink output in {adapter.benchmark_mode} with a degraded source oracle; "
                f"source rows captured ({comparison.source_row_count}) were lower than emitter diagnostics total "
                f"({result.emitter_reported_events_total}). Integrity counters remain diagnostic-only for "
                "competitive benchmark scoring. "
                f"captured_rows_total={comparison.sink_row_count}, sink_lines_total={result.sink_lines_total}, "
                f"sink_reported_events_total={result.sink_reported_events_total}, drop_estimate={result.drop_estimate}."
            )
            return finalize(0)

        result.status = "fail"
        if diagnostics_available:
            result.notes = (
                f"smoke benchmark did not observe sink output in {adapter.benchmark_mode} with degraded source oracle; "
                f"source_rows_total={comparison.source_row_count}, captured_rows_total={comparison.sink_row_count}, "
                f"sink_lines_total={result.sink_lines_total}, sink_reported_events_total={result.sink_reported_events_total}."
            )
        else:
            result.notes = (
                f"smoke benchmark failed in {adapter.benchmark_mode} with degraded source oracle and missing diagnostics; "
                f"source_rows_total={comparison.source_row_count}, captured_rows_total={comparison.sink_row_count}, "
                f"sink_lines_total={result.sink_lines_total}."
            )
        return finalize(1)

    strict_oracle_clean = (
        comparison.missing_source_count == 0
        and comparison.missing_event_count == 0
        and comparison.unexpected_event_count == 0
        and comparison.duplicate_event_count == 0
        and comparison.gap_count == 0
    )
    if observed_any_sink_output:
        result.status = "pass"
        if strict_oracle_clean:
            result.notes = (
                f"smoke benchmark succeeded in {adapter.benchmark_mode}; source_rows_total={comparison.source_row_count}, "
                f"captured_rows_total={comparison.sink_row_count}, sink_lines_total={result.sink_lines_total}, "
                f"missing_events={comparison.missing_event_count}, unexpected_events={comparison.unexpected_event_count}, "
                f"duplicates={comparison.duplicate_event_count}, gaps={comparison.gap_count}."
            )
        else:
            result.notes = (
                f"smoke benchmark produced sink output in {adapter.benchmark_mode}; integrity deltas are recorded as "
                "diagnostics and do not gate competitive scoring. "
                f"source_rows_total={comparison.source_row_count}, captured_rows_total={comparison.sink_row_count}, "
                f"missing_sources={comparison.missing_sources}, missing_events={comparison.missing_event_count}, "
                f"unexpected_events={comparison.unexpected_event_count}, duplicates={comparison.duplicate_event_count}, "
                f"gaps={comparison.gap_count}."
            )
        return finalize(0)

    result.status = "fail"
    result.notes = (
        f"smoke benchmark failed in {adapter.benchmark_mode}; source_rows_total={comparison.source_row_count}, "
        f"captured_rows_total={comparison.sink_row_count}, sink_lines_total={result.sink_lines_total}, "
        f"missing_sources={comparison.missing_sources}, missing_events={comparison.missing_event_count}, "
        f"unexpected_events={comparison.unexpected_event_count}, duplicates={comparison.duplicate_event_count}, "
        f"gaps={comparison.gap_count}."
    )
    return finalize(1)


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
    collector_batch_target_bytes = resolve_optional_positive_int(
        args.collector_batch_target_bytes,
        "BENCH_COLLECTOR_BATCH_TARGET_BYTES",
    )
    if collector_batch_target_bytes is not None and adapter.name != "logfwd":
        raise ValueError("--collector-batch-target-bytes currently applies only to the logfwd collector adapter")
    cpu_profile = CPU_PROFILES[args.cpu_profile]
    resource_plan = build_resource_plan(
        cpu_profile=cpu_profile,
        emitter_pods=profile.pods,
        eps_per_pod=profile.eps_per_pod,
        unbounded_generator=profile.eps_per_pod == 0,
    )
    resource_plan = adjust_resource_plan_for_adapter(
        resource_plan=resource_plan,
        adapter=adapter,
        ingest_mode=args.ingest_mode,
        profile=profile,
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
        ingest_mode=args.ingest_label or args.ingest_mode,
        cpu_profile=args.cpu_profile,
        cluster_cpu_limit_cores=resource_plan.cluster_cpu_cores,
        collector_batch_target_bytes=collector_batch_target_bytes,
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
        rejected_batches_total=None,
        http_413_count=None,
        rejected_rows_estimate=None,
        rejected_bytes_estimate=None,
        backpressure_warning_count=None,
        collector_dropped_batches_total=None,
        latency_ms_p50=None,
        latency_ms_p95=None,
        latency_ms_p99=None,
        sink_cpu_cores_avg=None,
        sink_cpu_cores_p95=None,
        sink_rss_mb_avg=None,
        sink_rss_mb_p95=None,
        collector_cpu_cores_avg=None,
        collector_cpu_cores_p95=None,
        generator_cpu_cores_avg=None,
        generator_cpu_cores_p95=None,
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
        collector_batch_target_bytes=collector_batch_target_bytes,
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
        set_kind_control_plane_cpu_limit(args.cluster_name, resource_plan.cluster_cpu_cores)
        result.cluster_ready = True
        load_image_into_kind(args.cluster_name, args.memagent_image)

        apply_manifest(manifests["namespace"])
        wait_for_namespace(args.namespace)
        apply_manifest(manifests["sink_configmap"])
        apply_manifest(manifests["sink_deployment"])
        # Cold-start image pulls and CNI bring-up on shared runners can make
        # sink rollout occasionally exceed 90s; use a wider timeout to reduce
        # false negatives on low-EPS gating lanes.
        wait_for_deployment(args.namespace, "logfwd-capture", timeout_sec=180)
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
            artifacts_dir = results_dir / "artifacts"
            delivery_diagnostics = analyze_delivery_diagnostics(artifacts_dir)
            result.rejected_batches_total = delivery_diagnostics.rejected_batches_total
            result.http_413_count = delivery_diagnostics.http_413_count
            result.rejected_rows_estimate = delivery_diagnostics.rejected_rows_estimate
            result.rejected_bytes_estimate = delivery_diagnostics.rejected_bytes_estimate
            result.backpressure_warning_count = delivery_diagnostics.backpressure_warning_count
            result.collector_dropped_batches_total = delivery_diagnostics.collector_dropped_batches_total
            write_json(artifacts_dir / "delivery-diagnostics.json", delivery_diagnostics.to_dict())
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
