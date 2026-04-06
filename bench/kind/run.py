#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import shutil
import string
import sys
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from lib.analyze import benchmark_rows, expected_emitter_pods, load_json_lines, summarize_rows
from lib.cluster import CommandError, create_kind_cluster, delete_kind_cluster, load_image_into_kind, require_tool
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
from lib.results import BenchmarkResult, write_result_files


BENCH_ROOT = Path(__file__).resolve().parent
COMMON_MANIFESTS_ROOT = BENCH_ROOT / "manifests" / "common"
COLLECTOR_MANIFESTS_ROOT = BENCH_ROOT / "manifests" / "collectors"
WORKLOAD_MANIFESTS_ROOT = BENCH_ROOT / "manifests" / "workload"


@dataclass(frozen=True)
class Profile:
    name: str
    pods: int
    eps_per_pod: int
    warmup_sec: int
    measure_sec: int
    cooldown_sec: int


PROFILES = {
    "smoke": Profile("smoke", pods=5, eps_per_pod=100, warmup_sec=5, measure_sec=15, cooldown_sec=5),
    "default": Profile("default", pods=30, eps_per_pod=300, warmup_sec=30, measure_sec=120, cooldown_sec=10),
}


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


def main() -> int:
    args = parse_args()
    profile = PROFILES[args.profile]
    results_dir = resolve_results_dir(args.results_dir)
    rendered_dir = results_dir / "rendered-manifests"
    rendered_dir.mkdir(parents=True, exist_ok=True)

    benchmark_id = str(uuid.uuid4())
    timestamp_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    total_target_eps = profile.pods * profile.eps_per_pod

    result = BenchmarkResult(
        benchmark_id=benchmark_id,
        timestamp_utc=timestamp_utc,
        phase=args.phase,
        cluster="kind-single-node",
        cluster_name=args.cluster_name,
        namespace=args.namespace,
        collector=args.collector,
        protocol=args.protocol,
        pods=profile.pods,
        target_eps_per_pod=profile.eps_per_pod,
        total_target_eps=total_target_eps,
        warmup_sec=profile.warmup_sec,
        measure_sec=profile.measure_sec,
        cooldown_sec=profile.cooldown_sec,
        sink_lines_total=None,
        captured_rows_total=None,
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

    namespace_manifest = rendered_dir / "namespace.yaml"
    sink_configmap_manifest = rendered_dir / "sink-configmap.yaml"
    sink_deployment_manifest = rendered_dir / "sink-deployment.yaml"
    collector_configmap_manifest = rendered_dir / "collector-configmap.yaml"
    collector_daemonset_manifest = rendered_dir / "collector-daemonset.yaml"
    emitter_configmap_manifest = rendered_dir / "emitter-configmap.yaml"
    emitter_statefulset_manifest = rendered_dir / "emitter-statefulset.yaml"
    substitutions = {
        "NAMESPACE": args.namespace,
        "MEMAGENT_IMAGE": args.memagent_image,
        "BENCHMARK_ID": benchmark_id,
        "EPS_PER_POD": str(profile.eps_per_pod),
        "POD_REPLICAS": str(profile.pods),
    }

    try:
        ensure_tools()
        copy_static_manifest("namespace.yaml", namespace_manifest, substitutions)
        render_template("sink-configmap.yaml.tmpl", sink_configmap_manifest, substitutions)
        render_template("sink-deployment.yaml.tmpl", sink_deployment_manifest, substitutions)
        if args.phase == "smoke":
            if args.collector != "logfwd":
                raise CommandError(f"collector not implemented yet in benchmark harness: {args.collector}")
            render_template("collectors/logfwd-configmap.yaml.tmpl", collector_configmap_manifest, substitutions)
            render_template("collectors/logfwd-daemonset.yaml.tmpl", collector_daemonset_manifest, substitutions)
            render_template("workload/log-emitter-configmap.yaml.tmpl", emitter_configmap_manifest, substitutions)
            render_template("workload/log-emitter-statefulset.yaml.tmpl", emitter_statefulset_manifest, substitutions)

        create_kind_cluster(args.cluster_name)
        result.cluster_ready = True
        load_image_into_kind(args.cluster_name, args.memagent_image)

        apply_manifest(namespace_manifest)
        wait_for_namespace(args.namespace)
        apply_manifest(sink_configmap_manifest)
        apply_manifest(sink_deployment_manifest)
        wait_for_deployment(args.namespace, "logfwd-capture", timeout_sec=90)

        result.sink_ready = True

        if args.phase == "infra":
            result.status = "pass"
            result.notes = (
                "infra-only benchmark run succeeded; cluster lifecycle and logfwd sink deployment are healthy. "
                "Workload generation and collector-under-test installation land in later phases."
            )
        else:
            apply_manifest(emitter_configmap_manifest)
            apply_manifest(emitter_statefulset_manifest)
            rollout_status(args.namespace, "statefulset", "log-emitter", timeout_sec=120)

            apply_manifest(collector_configmap_manifest)
            apply_manifest(collector_daemonset_manifest)
            rollout_status(args.namespace, "daemonset", "logfwd-bench-collector", timeout_sec=120)

            collector_pod = get_first_pod_name(args.namespace, "app.kubernetes.io/name=logfwd-bench-collector")
            if not collector_pod:
                raise CommandError("collector pod not found after rollout")

            sink_samples, collector_samples = collect_logfwd_samples(
                args.namespace,
                "deployment/logfwd-capture",
                f"pod/{collector_pod}",
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

            sink_pod = get_first_pod_name(args.namespace, "app.kubernetes.io/name=logfwd-capture")
            if not sink_pod:
                raise CommandError("sink pod not found after rollout")

            artifacts_dir = results_dir / "artifacts"
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            sink_logs_path = artifacts_dir / "sink-logs.txt"
            sink_logs = subprocess.run(
                ["kubectl", "-n", args.namespace, "logs", sink_pod, "--tail=-1"],
                text=True,
                capture_output=True,
                check=False,
            )
            sink_logs_path.write_text(
                sink_logs.stdout if sink_logs.stdout else sink_logs.stderr,
                encoding="utf-8",
            )

            sink_capture_path = artifacts_dir / "sink-capture.ndjson"
            sink_capture = subprocess.run(
                [
                    "kubectl",
                    "-n",
                    args.namespace,
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
            sink_capture_path.write_text(
                sink_capture.stdout if sink_capture.stdout else sink_capture.stderr,
                encoding="utf-8",
            )

            rows = benchmark_rows(load_json_lines(sink_capture_path), benchmark_id)
            summary = summarize_rows(rows)
            expected_sources = expected_emitter_pods("log-emitter", profile.pods)
            missing_sources = sorted(set(expected_sources) - set(summary["sources"]))
            result.captured_rows_total = int(summary["row_count"])

            write_json(results_dir / "actual_rows.json", rows)
            write_json(
                results_dir / "stream-summary.json",
                {
                    "expected_sources": expected_sources,
                    "observed_sources": summary["sources"],
                    "missing_sources": missing_sources,
                    "duplicate_event_count": summary["duplicate_event_count"],
                    "gap_count": summary["gap_count"],
                    "row_count": summary["row_count"],
                },
            )

            result.dup_estimate = int(summary["duplicate_event_count"])
            result.drop_estimate = int(summary["gap_count"])

            if (
                result.sink_lines_total
                and not missing_sources
                and summary["gap_count"] == 0
                and summary["duplicate_event_count"] == 0
            ):
                result.status = "pass"
                result.notes = (
                    f"smoke benchmark succeeded; captured_rows_total={summary['row_count']}, "
                    f"sink_lines_total={result.sink_lines_total}; observed "
                    f"{len(summary['sources'])}/{profile.pods} emitter pods with "
                    f"{summary['gap_count']} gaps and {summary['duplicate_event_count']} duplicate events."
                )
                return_code = 0
            else:
                result.status = "fail"
                result.notes = (
                    f"smoke benchmark failed; sink_lines_total={result.sink_lines_total}, "
                    f"captured_rows_total={summary['row_count']}, "
                    f"missing_sources={missing_sources}, gaps={summary['gap_count']}, "
                    f"duplicates={summary['duplicate_event_count']}"
                )
                return_code = 1
        if args.phase == "infra":
            return_code = 0
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
            emitter_logs_path = results_dir / "artifacts" / "emitter-logs.txt"
            emitter_pods = get_pod_names(args.namespace, "app.kubernetes.io/name=log-emitter")
            if emitter_pods:
                chunks: list[str] = []
                for pod_name in emitter_pods:
                    chunks.append(f"==== {pod_name}")
                    proc = subprocess.run(
                        ["kubectl", "-n", args.namespace, "logs", pod_name, "--tail=100"],
                        text=True,
                        capture_output=True,
                        check=False,
                    )
                    chunks.append(proc.stdout if proc.stdout else proc.stderr)
                emitter_logs_path.write_text("\n".join(chunks), encoding="utf-8")
            collector_pods = get_pod_names(args.namespace, "app.kubernetes.io/name=logfwd-bench-collector")
            if collector_pods:
                chunks = []
                for pod_name in collector_pods:
                    chunks.append(f"==== {pod_name}")
                    proc = subprocess.run(
                        ["kubectl", "-n", args.namespace, "logs", pod_name, "--tail=200"],
                        text=True,
                        capture_output=True,
                        check=False,
                    )
                    chunks.append(proc.stdout if proc.stdout else proc.stderr)
                (results_dir / "artifacts" / "collector-logs.txt").write_text("\n".join(chunks), encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            artifacts_dir = results_dir / "artifacts"
            artifacts_dir.mkdir(parents=True, exist_ok=True)
            (artifacts_dir / "artifact-collection-error.txt").write_text(str(exc), encoding="utf-8")

        if not args.keep_cluster:
            try:
                delete_kind_cluster(args.cluster_name)
            except Exception as exc:  # noqa: BLE001
                artifacts_dir = results_dir / "artifacts"
                artifacts_dir.mkdir(parents=True, exist_ok=True)
                (artifacts_dir / "cluster-delete-error.txt").write_text(str(exc), encoding="utf-8")

        write_result_files(results_dir, result)

    print(results_dir)
    return return_code


if __name__ == "__main__":
    sys.exit(main())
