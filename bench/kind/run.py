#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import subprocess
import string
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from lib.analyze import (
    benchmark_rows,
    compare_source_and_sink,
    expected_emitter_pods,
    load_json_lines,
)
from lib.cluster import CommandError, create_kind_cluster, delete_kind_cluster, load_image_into_kind, require_tool
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
    avg,
    collect_logfwd_samples,
    cpu_cores_series,
    diff_output_lines,
    lines_per_sec_series,
    percentile,
    rss_mb_series,
)
from lib.profiles import PROFILES, Profile
from lib.results import BenchmarkResult, write_result_files


BENCH_ROOT = Path(__file__).resolve().parent
COMMON_MANIFESTS_ROOT = BENCH_ROOT / "manifests" / "common"
COLLECTOR_MANIFESTS_ROOT = BENCH_ROOT / "manifests" / "collectors"
WORKLOAD_MANIFESTS_ROOT = BENCH_ROOT / "manifests" / "workload"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the KIND competitive benchmark harness.")
    parser.add_argument("--phase", choices=["infra", "smoke"], default="smoke")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="smoke")
    parser.add_argument("--collector", default="logfwd")
    parser.add_argument("--protocol", default="otlp_http")
    parser.add_argument("--cluster-name", default="memagent-bench")
    parser.add_argument("--namespace", default="memagent-bench")
    parser.add_argument("--memagent-image", default="logfwd:e2e")
    parser.add_argument("--results-dir", default=None)
    parser.add_argument("--benchkit-run-id", default=None)
    parser.add_argument("--benchkit-kind", choices=["workflow", "hybrid"], default="workflow")
    parser.add_argument("--benchkit-service-name", default="memagent-e2e.kind-bench")
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


def render_manifests(
    *,
    args: argparse.Namespace,
    profile: Profile,
    adapter: CollectorAdapter,
    benchmark_id: str,
    rendered_dir: Path,
) -> dict[str, Path]:
    substitutions = {
        "NAMESPACE": args.namespace,
        "MEMAGENT_IMAGE": args.memagent_image,
        "BENCHMARK_ID": benchmark_id,
        "EPS_PER_POD": str(profile.eps_per_pod),
        "POD_REPLICAS": str(profile.pods),
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
        render_template(adapter.config_template, manifests["collector_configmap"], substitutions)
        render_template(adapter.workload_template, manifests["collector_workload"], substitutions)
        render_template("workload/log-emitter-configmap.yaml.tmpl", manifests["emitter_configmap"], substitutions)
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
    apply_manifest(manifests["emitter_configmap"])
    apply_manifest(manifests["emitter_statefulset"])
    rollout_status(args.namespace, "statefulset", "log-emitter", timeout_sec=120)

    apply_manifest(manifests["collector_configmap"])
    apply_manifest(manifests["collector_workload"])
    rollout_status(args.namespace, adapter.rollout_kind, adapter.rollout_name, timeout_sec=120)

    collector_pod = get_first_pod_name(args.namespace, adapter.pod_selector)
    if not collector_pod:
        raise CommandError("collector pod not found after rollout")

    sink_samples, collector_samples = collect_logfwd_samples(
        args.namespace,
        "deployment/logfwd-capture",
        adapter.diagnostics_target_format.format(pod_name=collector_pod),
        warmup_sec=profile.warmup_sec,
        measure_sec=profile.measure_sec,
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

    sink_pod = get_first_pod_name(args.namespace, "app.kubernetes.io/name=logfwd-capture")
    if not sink_pod:
        raise CommandError("sink pod not found after rollout")
    collect_pod_logs(
        namespace=args.namespace,
        pod_names=[sink_pod],
        destination=artifacts_dir / "sink-logs.txt",
        tail=-1,
    )
    collect_sink_capture(args.namespace, sink_pod, artifacts_dir / "sink-capture.ndjson")

    emitter_pods = get_pod_names(args.namespace, "app.kubernetes.io/name=log-emitter")
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

    sink_rows = benchmark_rows(load_json_lines(artifacts_dir / "sink-capture.ndjson"), result.benchmark_id)
    source_rows = benchmark_rows(load_json_lines(artifacts_dir / "emitter-logs.txt"), result.benchmark_id)
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
            "source_row_count": comparison.source_row_count,
            "sink_row_count": comparison.sink_row_count,
            "missing_event_count": comparison.missing_event_count,
            "unexpected_event_count": comparison.unexpected_event_count,
            "duplicate_event_count": comparison.duplicate_event_count,
            "gap_count": comparison.gap_count,
        },
    )

    if (
        result.sink_lines_total
        and comparison.missing_source_count == 0
        and comparison.missing_event_count == 0
        and comparison.unexpected_event_count == 0
        and comparison.duplicate_event_count == 0
        and comparison.gap_count == 0
    ):
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
    profile = PROFILES[args.profile]
    adapter = get_collector_adapter(args.collector)
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
        protocol=args.protocol,
        pods=profile.pods,
        target_eps_per_pod=profile.eps_per_pod,
        total_target_eps=profile.total_target_eps,
        warmup_sec=profile.warmup_sec,
        measure_sec=profile.measure_sec,
        cooldown_sec=profile.cooldown_sec,
        sink_lines_total=None,
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
        benchmark_id=benchmark_id,
        rendered_dir=rendered_dir,
    )

    try:
        ensure_tools()
        create_kind_cluster(args.cluster_name)
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
