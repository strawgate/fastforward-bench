#!/usr/bin/env python3

from __future__ import annotations

import argparse
from dataclasses import fields
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
KIND_LIB = REPO_ROOT / "bench" / "kind" / "lib"
if str(KIND_LIB) not in sys.path:
    sys.path.insert(0, str(KIND_LIB))

from results import BenchmarkResult, write_result_files  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write a canonical fallback benchmark result row.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--cluster-name", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--collector", required=True)
    parser.add_argument("--ingest-mode", required=True)
    parser.add_argument("--ingest-label", default=None, help="Label stored in result.json for ingest config (defaults to --ingest-mode)")
    parser.add_argument("--cpu-profile", required=True)
    parser.add_argument("--pods", type=int, required=True)
    parser.add_argument("--target-eps-per-pod", type=int, required=True)
    parser.add_argument("--collector-batch-target-bytes", type=int)
    parser.add_argument("--phase", default="smoke")
    parser.add_argument("--benchmark-mode", default="baseline-pass-through")
    parser.add_argument("--status", default="fail")
    parser.add_argument("--notes", required=True)
    return parser.parse_args()


def infer_protocol(*, cluster: str, collector: str) -> str:
    if cluster == "docker-compose":
        if collector == "filebeat":
            return "file_ndjson"
        if collector == "vector":
            return "http_ndjson"
        return "otlp_http"
    if collector == "vector":
        return "http_ndjson"
    return "otlp_http"


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = {field.name: None for field in fields(BenchmarkResult)}
    payload.update(
        {
            "benchmark_id": "missing-result",
            "timestamp_utc": "",
            "phase": args.phase,
            "benchmark_mode": args.benchmark_mode,
            "cluster": args.cluster,
            "cluster_name": args.cluster_name,
            "namespace": args.namespace,
            "collector": args.collector,
            "protocol": infer_protocol(cluster=args.cluster, collector=args.collector),
            "ingest_mode": args.ingest_label or args.ingest_mode,
            "cpu_profile": args.cpu_profile,
            "cluster_cpu_limit_cores": None,
            "collector_batch_target_bytes": args.collector_batch_target_bytes,
            "pods": args.pods,
            "target_eps_per_pod": args.target_eps_per_pod,
            "total_target_eps": args.pods * args.target_eps_per_pod,
            "sink_lines_total": 0,
            "sink_reported_events_total": 0,
            "sink_lines_per_sec_avg": 0.0,
            "cluster_ready": False,
            "sink_ready": False,
            "status": args.status,
            "notes": args.notes,
        }
    )

    result = BenchmarkResult(**payload)
    write_result_files(output_dir, result)


if __name__ == "__main__":
    main()
