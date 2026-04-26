#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parent
REPO_ROOT = BENCH_ROOT.parent.parent
KIND_LIB = REPO_ROOT / "bench" / "kind" / "lib"

if str(KIND_LIB) not in sys.path:
    sys.path.insert(0, str(KIND_LIB))

from measure import (  # noqa: E402
    StatsSample,
    avg,
    cpu_cores_series,
    diff_output_lines,
    fetch_capture_stats,
    fetch_stats,
    fetch_text,
    lines_per_sec_series,
    percentile,
    rss_mb_series,
)
from profiles import PROFILES, Profile  # noqa: E402
from results import BenchmarkResult, write_result_files  # noqa: E402


@dataclass(frozen=True)
class CollectorAdapter:
    name: str
    service_name: str
    image: str | None
    diagnostics_kind: str
    sink_stats_kind: str


@dataclass(frozen=True)
class CpuProfile:
    name: str
    collector_cpu: str
    generator_cpu: str
    sink_cpu: str
    collector_memory: str
    generator_memory: str
    sink_memory: str
    capture_reader_cpu: str
    capture_reader_memory: str

    @property
    def cluster_cpu_cores(self) -> float:
        # Logical cap used for normalized reporting in summary tables.
        if self.name == "multi":
            return 2.0
        return 1.0


COLLECTORS: dict[str, CollectorAdapter] = {
    "fastforward": CollectorAdapter(
        name="fastforward",
        service_name="collector-fastforward",
        image=None,
        diagnostics_kind="fastforward",
        sink_stats_kind="fastforward",
    ),
    "otelcol": CollectorAdapter(
        name="otelcol",
        service_name="collector-otelcol",
        image="otel/opentelemetry-collector-contrib:0.148.0",
        diagnostics_kind="prometheus",
        sink_stats_kind="fastforward",
    ),
    "vector": CollectorAdapter(
        name="vector",
        service_name="collector-vector",
        image="timberio/vector:0.54.0-debian",
        diagnostics_kind="prometheus",
        sink_stats_kind="capture_reader",
    ),
    "filebeat": CollectorAdapter(
        name="filebeat",
        service_name="collector-filebeat",
        image="docker.elastic.co/beats/filebeat:8.17.3",
        diagnostics_kind="http_json",
        sink_stats_kind="capture_file",
    ),
}


CPU_PROFILES: dict[str, CpuProfile] = {
    "single": CpuProfile(
        name="single",
        # Allocator: collector=1.0, generator=1.2, sink=1.2 (3.4 cores total).
        collector_cpu="1.0",
        generator_cpu="1.2",
        sink_cpu="1.2",
        collector_memory="1g",
        generator_memory="1g",
        sink_memory="1g",
        capture_reader_cpu="0.10",
        capture_reader_memory="256m",
    ),
    "multi": CpuProfile(
        name="multi",
        # Same allocation as single (multi-core tests are disabled).
        # Kept for CLI compatibility but uses identical CPU values.
        collector_cpu="1.0",
        generator_cpu="1.2",
        sink_cpu="1.2",
        collector_memory="1g",
        generator_memory="1g",
        sink_memory="1g",
        capture_reader_cpu="0.10",
        capture_reader_memory="256m",
    ),
}


CAPTURE_STATS_SCRIPT = """\
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock

benchmark_id = os.environ.get("BENCHMARK_ID")
capture_path = Path("/runtime/capture.ndjson")
capture_lock = Lock()
metrics = {
  "capture_rows_total": 0,
  "benchmark_rows_total": 0,
  "capture_size_bytes": 0,
  "benchmark_id": benchmark_id,
}

class Handler(BaseHTTPRequestHandler):
  def do_POST(self):
    if self.path != "/ingest":
      self.send_response(404)
      self.end_headers()
      return
    length = int(self.headers.get("Content-Length", "0") or 0)
    body = self.rfile.read(length)
    if body:
      if not body.endswith(b"\\n"):
        body += b"\\n"
      rows = [line for line in body.splitlines() if line.strip()]
      benchmark_rows = 0
      if benchmark_id is None:
        benchmark_rows = len(rows)
      else:
        for line in rows:
          try:
            row = json.loads(line.decode("utf-8"))
          except Exception:
            continue
          if isinstance(row, dict) and row.get("benchmark_id") == benchmark_id:
            benchmark_rows += 1
      with capture_lock:
        with capture_path.open("ab") as handle:
          handle.write(body)
        metrics["capture_size_bytes"] += len(body)
        metrics["capture_rows_total"] += len(rows)
        metrics["benchmark_rows_total"] += benchmark_rows
    self.send_response(200)
    self.end_headers()

  def do_GET(self):
    if self.path != "/stats":
      self.send_response(404)
      self.end_headers()
      return
    with capture_lock:
      payload = dict(metrics)
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    self.send_response(200)
    self.send_header("Content-Type", "application/json")
    self.send_header("Content-Length", str(len(body)))
    self.end_headers()
    self.wfile.write(body)

  def log_message(self, format, *args):
    return

server = ThreadingHTTPServer(("0.0.0.0", 8081), Handler)
server.daemon_threads = True
server.serve_forever()
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run non-KIND compose benchmark harness.")
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--collector", choices=sorted(COLLECTORS), default="fastforward")
    parser.add_argument("--ingest-mode", choices=["file", "otlp"], default="file")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="smoke")
    parser.add_argument("--cpu-profile", choices=sorted(CPU_PROFILES), default="single")
    parser.add_argument("--eps-per-sec", type=int, default=None)
    parser.add_argument("--memagent-image", default="fastforward:e2e")
    parser.add_argument("--benchkit-run-id", default=None)
    parser.add_argument("--benchkit-kind", choices=["workflow", "hybrid"], default="workflow")
    parser.add_argument("--benchkit-service-name", default="memagent-e2e.compose-bench")
    parser.add_argument("--benchkit-ref", default=os.environ.get("MEMAGENT_REF"))
    parser.add_argument("--benchkit-commit", default=os.environ.get("GITHUB_SHA"))
    parser.add_argument("--benchkit-workflow", default=os.environ.get("GITHUB_WORKFLOW"))
    parser.add_argument("--benchkit-job", default=os.environ.get("GITHUB_JOB"))
    parser.add_argument("--benchkit-run-attempt", default=os.environ.get("GITHUB_RUN_ATTEMPT"))
    parser.add_argument("--benchkit-runner", default=os.environ.get("RUNNER_NAME"))
    return parser.parse_args()


def run(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    completed = subprocess.run(cmd, cwd=cwd, env=env)
    if completed.returncode != 0:
        raise RuntimeError(f"command failed ({completed.returncode}): {' '.join(cmd)}")


def run_capture(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> str:
    completed = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        raise RuntimeError(f"command failed ({completed.returncode}): {' '.join(cmd)}: {stderr}")
    return completed.stdout


SEQ_FIELD_RE = re.compile(rb'"seq"\s*:\s*([0-9]+)')


def reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, payload: object) -> None:
    write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def ensure_world_writable_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    # Docker bind mounts require world-writable dirs when host and container
    # users differ. This is a local-only benchmark harness, not production.
    os.chmod(path, 0o777)


def build_generator_config(
    benchmark_id: str,
    eps_per_sec: int,
    *,
    ingest_mode: str,
    collector_service_name: str,
) -> str:
    if eps_per_sec == 0:
        batch_size = 1024
    else:
        # Keep low-rate targets smooth so per-second throughput sampling is meaningful.
        batch_size = max(1, min(1024, eps_per_sec))
    base = f"""\
server:
  diagnostics: 0.0.0.0:9090
  log_level: error

input:
  type: generator
  generator:
    events_per_sec: {eps_per_sec}
    batch_size: {batch_size}
    profile: record
    attributes:
      benchmark_id: "{benchmark_id}"
      pod_name: compose-generator
      stream_id: compose-generator
      service: bench-emitter
    sequence:
      field: seq
    event_created_unix_nano_field: event_created_unix_nano

transform: |
  SELECT
    benchmark_id,
    pod_name,
    stream_id,
    concat(stream_id, ':', CAST(seq AS VARCHAR)) AS event_id,
    CAST(seq AS BIGINT) AS seq,
    CAST(event_created_unix_nano AS BIGINT) AS event_created_unix_nano,
    CASE CAST(seq % 4 AS BIGINT)
      WHEN 0 THEN 'ERROR'
      WHEN 1 THEN 'INFO'
      WHEN 2 THEN 'DEBUG'
      ELSE 'WARN'
    END AS level,
    concat('bench event ', CAST(seq AS VARCHAR)) AS message,
    CASE
      WHEN CAST(seq % 4 AS BIGINT) = 0 THEN 500
      ELSE 200
    END AS status,
    CAST((seq % 250) + 1 AS BIGINT) AS duration_ms,
    service
  FROM logs

"""
    if ingest_mode == "file":
        return (
            base
            + """
output:
  type: file
  path: /runtime/events.ndjson
  format: json
"""
        )
    return (
        base
        + f"""
output:
  type: otlp
  endpoint: http://{collector_service_name}:4318/v1/logs
"""
    )


def build_sink_config() -> str:
    return """\
server:
  diagnostics: 0.0.0.0:9090
  log_level: error

pipelines:
  capture:
    inputs:
      - type: otlp
        listen: 0.0.0.0:4318
    outputs:
      - type: file
        path: /runtime/capture.ndjson
        format: json
"""


def build_fastforward_collector_config(benchmark_id: str, *, ingest_mode: str) -> str:
    input_block = (
        """\
input:
  type: file
  path: /runtime/events.ndjson
  format: json
"""
        if ingest_mode == "file"
        else """\
input:
  type: otlp
  listen: 0.0.0.0:4318
"""
    )
    message_projection = "message" if ingest_mode == "file" else "body AS message"
    return f"""\
server:
  diagnostics: 0.0.0.0:9090
  log_level: error

storage:
  data_dir: /runtime/collector

{input_block}

transform: |
  SELECT
    benchmark_id,
    pod_name,
    stream_id,
    event_id,
    seq,
    event_created_unix_nano,
    level,
    {message_projection},
    status,
    duration_ms,
    service
  FROM logs
  WHERE benchmark_id = '{benchmark_id}'

output:
  type: otlp
  endpoint: http://sink:4318/v1/logs
"""


def build_otel_collector_config(*, ingest_mode: str) -> str:
    if ingest_mode == "file":
        receiver_block = """\
receivers:
  filelog:
    include:
      - /runtime/events.ndjson
    start_at: beginning
"""
        receiver_name = "filelog"
        processors_block = """\
processors:
  transform/bench:
    log_statements:
      - context: log
        statements:
          - merge_maps(attributes, ParseJSON(body), "upsert")
  batch:
    send_batch_size: 1024
    timeout: 1s
"""
        processors_pipeline = "[transform/bench, batch]"
    else:
        receiver_block = """\
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
"""
        receiver_name = "otlp"
        processors_block = """\
processors:
  batch:
    send_batch_size: 1024
    timeout: 1s
"""
        processors_pipeline = "[batch]"

    return f"""\
{receiver_block}

{processors_block}

exporters:
  otlphttp:
    endpoint: http://sink:4318
    compression: none

extensions:
  health_check:
    endpoint: 0.0.0.0:13133

service:
  telemetry:
    logs:
      level: error
    metrics:
      level: detailed
      readers:
        - pull:
            exporter:
              prometheus:
                host: 0.0.0.0
                port: 8888
  extensions: [health_check]
  pipelines:
    logs:
      receivers: [{receiver_name}]
      processors: {processors_pipeline}
      exporters: [otlphttp]
"""


def build_vector_collector_config() -> str:
    return """\
data_dir: /runtime/vector
api:
  enabled: true
  address: 0.0.0.0:8686

sources:
  bench_file:
    type: file
    include:
      - /runtime/events.ndjson
    read_from: beginning
    ignore_checkpoints: true
  internal_metrics:
    type: internal_metrics
    scrape_interval_secs: 1

transforms:
  bench_parse:
    type: remap
    inputs: ["bench_file"]
    source: |
      if !exists(.message) {
        abort
      }

      if !starts_with(string!(.message), "{") {
        abort
      }

      parsed = object!(parse_json!(string!(.message)))
      . = parsed

sinks:
  bench_out:
    type: http
    inputs: ["bench_parse"]
    uri: http://capture-reader:8081/ingest
    method: post
    encoding:
      codec: json
    framing:
      method: newline_delimited
    batch:
      max_bytes: 1048576
      timeout_secs: 1
    healthcheck:
      enabled: false

  prom_exporter:
    type: prometheus_exporter
    inputs: ["internal_metrics"]
    address: 0.0.0.0:9090
"""


def build_filebeat_collector_config(benchmark_id: str) -> str:
    return f"""\
filebeat.inputs:
  - type: filestream
    id: bench-file
    enabled: true
    paths:
      - /runtime/events.ndjson
    parsers:
      - ndjson:
          target: ""
          overwrite_keys: true

processors:
  - drop_event:
      when:
        not:
          equals:
            benchmark_id: "{benchmark_id}"

output.file:
  path: /runtime
  filename: capture.ndjson
  rotate_every_kb: 1048576
  number_of_files: 3
  permissions: 0644
  codec.json:
    pretty: false

http.enabled: true
http.host: 0.0.0.0
http.port: 5066

logging.level: error
"""


def build_compose_yaml() -> str:
    return """\
services:
  sink:
    image: ${MEMAGENT_IMAGE}
    command: ["run", "--config", "/config/sink.yaml"]
    volumes:
      - ${BENCH_RESULTS_RUNTIME_DIR}:/runtime
      - ${BENCH_RESULTS_RENDERED_DIR}:/config:ro
    ports:
      - "${SINK_DIAG_PORT}:9090"
      - "${SINK_OTLP_PORT}:4318"
    cpus: "${SINK_CPUS}"
    mem_limit: "${SINK_MEMORY}"

  capture-reader:
    image: python:3.12-alpine
    command: ["python", "/config/capture-stats.py"]
    environment:
      BENCHMARK_ID: "${BENCHMARK_ID}"
    volumes:
      - ${BENCH_RESULTS_RUNTIME_DIR}:/runtime
      - ${BENCH_RESULTS_RENDERED_DIR}:/config:ro
    ports:
      - "${CAPTURE_STATS_PORT}:8081"
    cpus: "${CAPTURE_READER_CPUS}"
    mem_limit: "${CAPTURE_READER_MEMORY}"
    depends_on:
      sink:
        condition: service_started

  generator:
    image: ${MEMAGENT_IMAGE}
    command: ["run", "--config", "/config/generator.yaml"]
    volumes:
      - ${BENCH_RESULTS_RUNTIME_DIR}:/runtime
      - ${BENCH_RESULTS_RENDERED_DIR}:/config:ro
    ports:
      - "${GENERATOR_DIAG_PORT}:9090"
    cpus: "${GENERATOR_CPUS}"
    mem_limit: "${GENERATOR_MEMORY}"

  collector-fastforward:
    profiles: ["fastforward"]
    image: ${FASTFORWARD_COLLECTOR_IMAGE}
    command: ["run", "--config", "/config/collector-fastforward.yaml"]
    volumes:
      - ${BENCH_RESULTS_RUNTIME_DIR}:/runtime
      - ${BENCH_RESULTS_RENDERED_DIR}:/config:ro
    ports:
      - "${COLLECTOR_STATS_PORT}:9090"
    cpus: "${COLLECTOR_CPUS}"
    mem_limit: "${COLLECTOR_MEMORY}"
    depends_on:
      sink:
        condition: service_started

  collector-otelcol:
    profiles: ["otelcol"]
    image: ${OTELCOL_COLLECTOR_IMAGE}
    command: ["--config=/config/collector-otelcol.yaml"]
    volumes:
      - ${BENCH_RESULTS_RUNTIME_DIR}:/runtime
      - ${BENCH_RESULTS_RENDERED_DIR}:/config:ro
    ports:
      - "${COLLECTOR_STATS_PORT}:8888"
    cpus: "${COLLECTOR_CPUS}"
    mem_limit: "${COLLECTOR_MEMORY}"
    depends_on:
      sink:
        condition: service_started

  collector-vector:
    profiles: ["vector"]
    image: ${VECTOR_COLLECTOR_IMAGE}
    command: ["--config", "/config/collector-vector.yaml"]
    volumes:
      - ${BENCH_RESULTS_RUNTIME_DIR}:/runtime
      - ${BENCH_RESULTS_RENDERED_DIR}:/config:ro
    ports:
      - "${COLLECTOR_STATS_PORT}:9090"
    cpus: "${COLLECTOR_CPUS}"
    mem_limit: "${COLLECTOR_MEMORY}"
    depends_on:
      capture-reader:
        condition: service_started

  collector-filebeat:
    profiles: ["filebeat"]
    image: ${FILEBEAT_COLLECTOR_IMAGE}
    command: ["-e", "--strict.perms=false", "-c", "/config/collector-filebeat.yaml"]
    user: "0:0"
    volumes:
      - ${BENCH_RESULTS_RUNTIME_DIR}:/runtime
      - ${BENCH_RESULTS_RENDERED_DIR}:/config:ro
    ports:
      - "${COLLECTOR_STATS_PORT}:5066"
    cpus: "${COLLECTOR_CPUS}"
    mem_limit: "${COLLECTOR_MEMORY}"
    depends_on:
      sink:
        condition: service_started
"""


def compose_cmd(project: str, compose_file: Path) -> list[str]:
    return ["docker", "compose", "-p", project, "-f", str(compose_file)]


def parse_byte_size(value: str) -> int:
    raw = value.strip()
    if not raw:
        return 0
    units = {
        "b": 1,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "tb": 1000**4,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
    }
    index = 0
    while index < len(raw) and (raw[index].isdigit() or raw[index] in ".-"):
        index += 1
    number = float(raw[:index] or "0")
    if number < 0:
        raise ValueError(f"byte size cannot be negative: {value}")
    unit = raw[index:].strip().lower()
    if not unit:
        return int(number)
    multiplier = units.get(unit, 1)
    return int(number * multiplier)


def resolve_container_id(compose: list[str], service: str, env: dict[str, str]) -> str | None:
    try:
        output = run_capture(compose + ["ps", "-q", service], env=env).strip()
    except Exception:
        return None
    return output or None


def read_container_resource_sample(container_id: str) -> tuple[float, float] | None:
    try:
        output = run_capture(["docker", "stats", "--no-stream", "--format", "{{json .}}", container_id]).strip()
        if not output:
            return None
        row = json.loads(output.splitlines()[-1])
        cpu_percent_raw = str(row.get("CPUPerc", "0")).strip().rstrip("%")
        cpu_cores = float(cpu_percent_raw or "0") / 100.0
        mem_usage_raw = str(row.get("MemUsage", "0B / 0B")).split("/", 1)[0].strip()
        rss_mb = parse_byte_size(mem_usage_raw) / (1024.0 * 1024.0)
        return cpu_cores, rss_mb
    except Exception:
        return None


def stats_sample_from_fastforward(port: int) -> StatsSample:
    payload = fetch_stats(port)
    return StatsSample(
        timestamp=time.time(),
        output_lines=int(payload.get("output_lines", 0) or 0),
        rss_bytes=int(payload.get("rss_bytes", 0) or 0),
        cpu_total_ms=int(payload.get("cpu_user_ms", 0) or 0) + int(payload.get("cpu_sys_ms", 0) or 0),
    )


def stats_sample_from_capture_reader(port: int) -> StatsSample:
    payload = fetch_capture_stats(port)
    return StatsSample(
        timestamp=time.time(),
        output_lines=int(payload.get("benchmark_rows_total", 0) or 0),
        rss_bytes=0,
        cpu_total_ms=0,
    )


def wait_until_ready(fetch_fn, timeout_sec: int = 60) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            fetch_fn()
            return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("timed out waiting for service readiness")


def wait_for_collector_ready(adapter: CollectorAdapter, port: int, timeout_sec: int = 90) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            if adapter.diagnostics_kind == "fastforward":
                fetch_stats(port)
            elif adapter.diagnostics_kind == "prometheus":
                fetch_text(port, "/metrics")
            elif adapter.diagnostics_kind == "http_json":
                fetch_text(port, "/stats")
            else:
                raise ValueError(f"unknown collector diagnostics kind: {adapter.diagnostics_kind}")
            return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("timed out waiting for collector readiness")


def stats_sample_from_capture_file(capture_file_path: Path) -> StatsSample:
    return StatsSample(
        timestamp=time.time(),
        output_lines=count_rows_with_rollovers(capture_file_path),
        rss_bytes=0,
        cpu_total_ms=0,
    )


def stats_sample_empty() -> StatsSample:
    return StatsSample(timestamp=time.time(), output_lines=0, rss_bytes=0, cpu_total_ms=0)


def sample_sink(
    adapter: CollectorAdapter,
    sink_diag_port: int,
    capture_stats_port: int,
    capture_file_path: Path,
    benchmark_id: str,
) -> StatsSample:
    if adapter.sink_stats_kind == "fastforward":
        return stats_sample_from_fastforward(sink_diag_port)
    if adapter.sink_stats_kind == "capture_reader":
        return stats_sample_from_capture_reader(capture_stats_port)
    if adapter.sink_stats_kind == "capture_file":
        # Avoid scanning large output files on every sample tick.
        return stats_sample_empty()
    raise ValueError(f"unknown sink stats kind: {adapter.sink_stats_kind}")


def sink_reported_events(
    adapter: CollectorAdapter,
    sink_diag_port: int,
    capture_stats_port: int,
    capture_file_path: Path,
    benchmark_id: str,
) -> int:
    if adapter.sink_stats_kind == "capture_reader":
        payload = fetch_capture_stats(capture_stats_port)
        return int(payload.get("benchmark_rows_total", 0) or 0)
    if adapter.sink_stats_kind == "capture_file":
        return int(stats_sample_from_capture_file(capture_file_path).output_lines)
    payload = fetch_stats(sink_diag_port)
    return int(payload.get("output_lines", 0) or 0)


def emitter_reported_events(generator_diag_port: int) -> int:
    payload = fetch_stats(generator_diag_port)
    return int(payload.get("output_lines", 0) or 0)


def write_samples(path: Path, samples: list[StatsSample]) -> None:
    serialized = [asdict(sample) for sample in samples]
    write_json(path, serialized)


def copy_with_cap(src: Path, dst: Path, *, max_bytes: int) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    with src.open("rb") as src_handle, dst.open("wb") as dst_handle:
        remaining = max_bytes
        while remaining > 0:
            chunk = src_handle.read(min(1024 * 1024, remaining))
            if not chunk:
                break
            dst_handle.write(chunk)
            remaining -= len(chunk)
        truncated = False
        if max_bytes > 0:
            truncated = src_handle.read(1)
    if truncated:
        with dst.open("ab") as dst_handle:
            dst_handle.write(b"\n")
            dst_handle.write(f'{{"_note":"truncated_at_bytes","max_bytes":{max_bytes}}}\n'.encode("utf-8"))


def count_ndjson_rows(path: Path) -> int:
    if not path.exists():
        return 0
    rows = 0
    ends_with_newline = True
    try:
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                rows += chunk.count(b"\n")
                ends_with_newline = chunk.endswith(b"\n")
    except OSError:
        return 0
    if rows == 0:
        return 0
    if not ends_with_newline:
        # Account for a trailing partial line.
        rows += 1
    return rows


def _rollover_files(base_file_path: Path) -> list[Path]:
    """Return sorted list of rollover files matching base_file_path name."""
    parent = base_file_path.parent
    base_name = base_file_path.name
    return sorted(path for path in parent.glob(f"{base_name}*") if path.is_file())


def count_rows_with_rollovers(base_file_path: Path) -> int:
    files = _rollover_files(base_file_path)
    if not files:
        return 0
    return sum(count_ndjson_rows(path) for path in files)


def remove_rollover_files(base_file_path: Path) -> None:
    for path in _rollover_files(base_file_path):
        try:
            path.unlink()
        except OSError:
            continue


def scan_ndjson_seq(base_file_path: Path) -> tuple[int, int, int]:
    """Scan NDJSON files and return (row_count, max_seq, duplicate_count)."""
    files = _rollover_files(base_file_path)
    row_count = 0
    max_seq = 0
    seq_counts: dict[int, int] = {}
    for path in files:
        try:
            with path.open("rb") as handle:
                for raw_line in handle:
                    row_count += 1
                    match = SEQ_FIELD_RE.search(raw_line)
                    if match is None:
                        continue
                    seq = int(match.group(1))
                    if seq > max_seq:
                        max_seq = seq
                    seq_counts[seq] = seq_counts.get(seq, 0) + 1
        except OSError:
            continue
    duplicate_count = sum(count - 1 for count in seq_counts.values() if count > 1)
    return row_count, max_seq, duplicate_count


def max_seq_with_rollovers(base_file_path: Path) -> int:
    return scan_ndjson_seq(base_file_path)[1]


def find_duplicate_seqs(base_file_path: Path) -> dict[str, object]:
    """Return diagnostic info about duplicate seq values in an NDJSON file."""
    files = _rollover_files(base_file_path)
    seq_counts: dict[int, int] = {}
    first_seq: int | None = None
    last_seq: int | None = None
    total_rows = 0
    for path in files:
        try:
            with path.open("rb") as handle:
                for raw_line in handle:
                    total_rows += 1
                    match = SEQ_FIELD_RE.search(raw_line)
                    if match is None:
                        continue
                    seq = int(match.group(1))
                    seq_counts[seq] = seq_counts.get(seq, 0) + 1
                    if first_seq is None or seq < first_seq:
                        first_seq = seq
                    if last_seq is None or seq > last_seq:
                        last_seq = seq
        except OSError:
            continue
    duplicates = {seq: count for seq, count in seq_counts.items() if count > 1}
    return {
        "file": str(base_file_path),
        "total_rows": total_rows,
        "unique_seqs": len(seq_counts),
        "first_seq": first_seq,
        "last_seq": last_seq,
        "duplicate_seqs": duplicates,
        "duplicate_count": sum(count - 1 for count in duplicates.values()),
    }


def wait_for_sink_catch_up(
    *,
    adapter: CollectorAdapter,
    sink_diag_port: int,
    capture_stats_port: int,
    capture_file_path: Path,
    benchmark_id: str,
    target_events_total: int,
    timeout_sec: int,
) -> int:
    deadline = time.time() + timeout_sec
    last_seen = -1

    while time.time() < deadline:
        current = sink_reported_events(
            adapter,
            sink_diag_port,
            capture_stats_port,
            capture_file_path,
            benchmark_id,
        )
        if current >= target_events_total:
            return current
        if current != last_seen:
            last_seen = current
        time.sleep(0.5)

    return max(0, last_seen)


def main() -> int:
    args = parse_args()
    adapter = COLLECTORS[args.collector]
    if args.ingest_mode == "otlp" and adapter.name in {"vector", "filebeat"}:
        raise NotImplementedError(f"collector '{adapter.name}' does not support ingest mode 'otlp' in compose harness")
    cpu_profile = CPU_PROFILES[args.cpu_profile]
    base_profile: Profile = PROFILES[args.profile]
    eps_per_sec = base_profile.eps_per_pod if args.eps_per_sec is None else args.eps_per_sec
    if eps_per_sec < 0:
        raise ValueError("eps-per-sec must be >= 0")

    # File output has no natural backpressure (unlike OTLP HTTP), so unlimited
    # mode (eps_per_sec=0) causes the generator to spin at 100% CPU writing
    # faster than any collector reads.  Cap at 400k EPS — well above what any
    # single-core collector can consume, but low enough that the generator's
    # built-in rate limiter keeps CPU usage reasonable.
    FILE_INGEST_MAX_EPS = 400_000
    # Time to wait after stopping collector before counting source file events.
    COLLECTOR_FLUSH_SEC = 1.0
    if args.ingest_mode == "file" and eps_per_sec == 0:
        eps_per_sec = FILE_INGEST_MAX_EPS

    benchmark_id = str(uuid.uuid4())
    timestamp_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    results_dir = args.results_dir.resolve()
    rendered_dir = results_dir / "rendered"
    runtime_dir = results_dir / "runtime"
    artifacts_dir = results_dir / "artifacts"
    rendered_dir.mkdir(parents=True, exist_ok=True)
    ensure_world_writable_dir(runtime_dir)
    ensure_world_writable_dir(runtime_dir / "vector")
    ensure_world_writable_dir(runtime_dir / "collector")
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    # Remove stale runtime files (including rollover segments) from prior local reruns.
    for stale_base_file in (runtime_dir / "events.ndjson", runtime_dir / "capture.ndjson"):
        remove_rollover_files(stale_base_file)

    compose_file = rendered_dir / "compose.yaml"
    write_text(compose_file, build_compose_yaml())
    write_text(
        rendered_dir / "generator.yaml",
        build_generator_config(
            benchmark_id,
            eps_per_sec,
            ingest_mode=args.ingest_mode,
            collector_service_name=adapter.service_name,
        ),
    )
    write_text(rendered_dir / "sink.yaml", build_sink_config())
    write_text(
        rendered_dir / "collector-fastforward.yaml",
        build_fastforward_collector_config(benchmark_id, ingest_mode=args.ingest_mode),
    )
    write_text(rendered_dir / "collector-otelcol.yaml", build_otel_collector_config(ingest_mode=args.ingest_mode))
    write_text(rendered_dir / "collector-vector.yaml", build_vector_collector_config())
    write_text(rendered_dir / "collector-filebeat.yaml", build_filebeat_collector_config(benchmark_id))
    write_text(rendered_dir / "capture-stats.py", CAPTURE_STATS_SCRIPT)

    sink_diag_port = reserve_local_port()
    sink_otlp_port = reserve_local_port()
    generator_diag_port = reserve_local_port()
    collector_stats_port = reserve_local_port()
    capture_stats_port = reserve_local_port()

    project = f"memagent-bench-compose-{benchmark_id[:8]}"
    compose = compose_cmd(project, compose_file)

    env = dict(os.environ)
    env.update(
        {
            "MEMAGENT_IMAGE": args.memagent_image,
            "FASTFORWARD_COLLECTOR_IMAGE": args.memagent_image,
            "OTELCOL_COLLECTOR_IMAGE": COLLECTORS["otelcol"].image or "",
            "VECTOR_COLLECTOR_IMAGE": COLLECTORS["vector"].image or "",
            "FILEBEAT_COLLECTOR_IMAGE": COLLECTORS["filebeat"].image or "",
            "BENCH_RESULTS_RUNTIME_DIR": str(runtime_dir),
            "BENCH_RESULTS_RENDERED_DIR": str(rendered_dir),
            "BENCHMARK_ID": benchmark_id,
            "SINK_DIAG_PORT": str(sink_diag_port),
            "SINK_OTLP_PORT": str(sink_otlp_port),
            "GENERATOR_DIAG_PORT": str(generator_diag_port),
            "COLLECTOR_STATS_PORT": str(collector_stats_port),
            "CAPTURE_STATS_PORT": str(capture_stats_port),
            "COLLECTOR_CPUS": cpu_profile.collector_cpu,
            "GENERATOR_CPUS": cpu_profile.generator_cpu,
            "SINK_CPUS": cpu_profile.sink_cpu,
            "COLLECTOR_MEMORY": cpu_profile.collector_memory,
            "GENERATOR_MEMORY": cpu_profile.generator_memory,
            "SINK_MEMORY": cpu_profile.sink_memory,
            "CAPTURE_READER_CPUS": cpu_profile.capture_reader_cpu,
            "CAPTURE_READER_MEMORY": cpu_profile.capture_reader_memory,
        }
    )

    result = BenchmarkResult(
        benchmark_id=benchmark_id,
        timestamp_utc=timestamp_utc,
        phase="smoke",
        benchmark_mode="baseline-pass-through",
        cluster="docker-compose",
        cluster_name=project,
        namespace="compose",
        collector=adapter.name,
        protocol="file_ndjson" if adapter.name == "filebeat" else ("otlp_http" if adapter.name != "vector" else "http_ndjson"),
        ingest_mode=args.ingest_mode,
        cpu_profile=cpu_profile.name,
        cluster_cpu_limit_cores=cpu_profile.cluster_cpu_cores,
        collector_batch_target_bytes=None,
        pods=1,
        target_eps_per_pod=eps_per_sec,
        total_target_eps=eps_per_sec,
        warmup_sec=base_profile.warmup_sec,
        measure_sec=base_profile.measure_sec,
        cooldown_sec=base_profile.cooldown_sec,
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
        status="fail",
        notes="compose benchmark did not finish",
    )

    sink_samples: list[StatsSample] = []
    sink_cpu_samples: list[float] = []
    sink_rss_samples: list[float] = []
    collector_cpu_samples: list[float] = []
    collector_rss_samples: list[float] = []
    collector_resource_samples: list[dict[str, float]] = []
    generator_cpu_samples: list[float] = []
    measure_sink_events_start: int | None = None
    measure_sink_events_end: int | None = None
    measure_started_at: float | None = None
    measure_completed_at: float | None = None
    capture_file_path = runtime_dir / "capture.ndjson"
    events_file_path = runtime_dir / "events.ndjson"
    max_throughput_mode = args.ingest_mode == "file" and base_profile.eps_per_pod == 0

    try:
        run(compose + ["--profile", adapter.name, "up", "-d", "sink", "capture-reader", adapter.service_name], env=env)
        wait_until_ready(lambda: fetch_stats(sink_diag_port), timeout_sec=90)
        result.cluster_ready = True
        result.sink_ready = True
        sink_container_id = resolve_container_id(compose, "sink", env)

        wait_for_collector_ready(adapter, collector_stats_port, timeout_sec=90)
        collector_container_id = resolve_container_id(compose, adapter.service_name, env)
        run(compose + ["--profile", adapter.name, "up", "-d", "generator"], env=env)
        wait_until_ready(lambda: fetch_stats(generator_diag_port), timeout_sec=60)
        generator_container_id = resolve_container_id(compose, "generator", env)

        if base_profile.warmup_sec > 0:
            time.sleep(base_profile.warmup_sec)

        try:
            measure_sink_events_start = sink_reported_events(
                adapter,
                sink_diag_port,
                capture_stats_port,
                capture_file_path,
                benchmark_id,
            )
        except Exception:
            measure_sink_events_start = None

        measure_started_at = time.time()
        deadline = measure_started_at + base_profile.measure_sec
        while True:
            sink_samples.append(
                sample_sink(
                    adapter,
                    sink_diag_port,
                    capture_stats_port,
                    capture_file_path,
                    benchmark_id,
                )
            )
            if sink_container_id:
                resource_sample = read_container_resource_sample(sink_container_id)
                if resource_sample is not None:
                    cpu_cores, rss_mb = resource_sample
                    sink_cpu_samples.append(cpu_cores)
                    sink_rss_samples.append(rss_mb)
            if collector_container_id:
                resource_sample = read_container_resource_sample(collector_container_id)
                if resource_sample is not None:
                    cpu_cores, rss_mb = resource_sample
                    collector_cpu_samples.append(cpu_cores)
                    collector_rss_samples.append(rss_mb)
                    collector_resource_samples.append(
                        {
                            "timestamp": time.time(),
                            "cpu_cores": cpu_cores,
                            "rss_mb": rss_mb,
                        }
                    )
            if generator_container_id:
                resource_sample = read_container_resource_sample(generator_container_id)
                if resource_sample is not None:
                    cpu_cores, _rss_mb = resource_sample
                    generator_cpu_samples.append(cpu_cores)
            if time.time() >= deadline:
                break
            time.sleep(1)

        measure_completed_at = time.time()
        try:
            measure_sink_events_end = sink_reported_events(
                adapter,
                sink_diag_port,
                capture_stats_port,
                capture_file_path,
                benchmark_id,
            )
        except Exception:
            measure_sink_events_end = None

        if base_profile.cooldown_sec > 0:
            time.sleep(base_profile.cooldown_sec)

        emitter_before_stop: int | None = None
        try:
            emitter_before_stop = emitter_reported_events(generator_diag_port)
        except Exception:
            emitter_before_stop = None

        subprocess.run(compose + ["stop", "generator"], env=env, check=False)

        if args.ingest_mode == "file":
            # Stop the collector so it finishes consuming events.ndjson before we
            # count the source.  Otherwise the collector keeps reading and sending
            # events to the sink after we count, causing sink > source (the 4-dup
            # issue at 1 EPS in file mode).
            subprocess.run(compose + ["stop", adapter.service_name], env=env, check=False)
            time.sleep(COLLECTOR_FLUSH_SEC)
            source_rows_total = count_rows_with_rollovers(events_file_path)
            if source_rows_total > 0:
                result.source_rows_total = source_rows_total
                result.emitter_reported_events_total = source_rows_total
            else:
                result.emitter_reported_events_total = emitter_before_stop
        else:
            result.emitter_reported_events_total = emitter_before_stop

        if result.emitter_reported_events_total is not None and result.emitter_reported_events_total > 0:
            result.sink_reported_events_total = wait_for_sink_catch_up(
                adapter=adapter,
                sink_diag_port=sink_diag_port,
                capture_stats_port=capture_stats_port,
                capture_file_path=capture_file_path,
                benchmark_id=benchmark_id,
                target_events_total=result.emitter_reported_events_total,
                timeout_sec=10 if max_throughput_mode else 30,
            )
        else:
            result.sink_reported_events_total = sink_reported_events(
                adapter,
                sink_diag_port,
                capture_stats_port,
                capture_file_path,
                benchmark_id,
            )

        # Final integrity counts should come from the sink capture file, not per-process
        # diagnostics counters, which can lag under sustained load.
        captured_rows, captured_max_seq, _captured_dups = scan_ndjson_seq(capture_file_path)
        if captured_rows > 0:
            result.sink_reported_events_total = captured_rows
        if captured_max_seq > 0:
            result.source_rows_total = max(result.source_rows_total or 0, captured_max_seq)
            result.emitter_reported_events_total = max(result.emitter_reported_events_total or 0, captured_max_seq)

        result.sink_lines_total = diff_output_lines(sink_samples)
        result.captured_rows_total = result.sink_reported_events_total

        sink_series = lines_per_sec_series(sink_samples)
        result.sink_lines_per_sec_avg = avg(sink_series)
        result.sink_lines_per_sec_p50 = percentile(sink_series, 0.50)
        result.sink_lines_per_sec_p95 = percentile(sink_series, 0.95)
        result.sink_lines_per_sec_p99 = percentile(sink_series, 0.99)
        sink_cpu_series = cpu_cores_series(sink_samples)
        sink_rss_series = rss_mb_series(sink_samples)
        if sink_cpu_series and any(sample.cpu_total_ms > 0 for sample in sink_samples):
            result.sink_cpu_cores_avg = avg(sink_cpu_series)
            result.sink_cpu_cores_p95 = percentile(sink_cpu_series, 0.95)
        if sink_rss_series and any(sample.rss_bytes > 0 for sample in sink_samples):
            result.sink_rss_mb_avg = avg(sink_rss_series)
            result.sink_rss_mb_p95 = percentile(sink_rss_series, 0.95)
        if result.sink_cpu_cores_avg is None and sink_cpu_samples:
            result.sink_cpu_cores_avg = avg(sink_cpu_samples)
            result.sink_cpu_cores_p95 = percentile(sink_cpu_samples, 0.95)
        if result.sink_rss_mb_avg is None and sink_rss_samples:
            result.sink_rss_mb_avg = avg(sink_rss_samples)
            result.sink_rss_mb_p95 = percentile(sink_rss_samples, 0.95)

        measured_sink_events: int | None = None
        measured_window_sec: float | None = None
        if (
            measure_sink_events_start is not None
            and measure_sink_events_end is not None
            and measure_sink_events_end >= measure_sink_events_start
        ):
            measured_sink_events = max(0, measure_sink_events_end - measure_sink_events_start)
        if measure_started_at is not None and measure_completed_at is not None and measure_completed_at > measure_started_at:
            measured_window_sec = measure_completed_at - measure_started_at

        if result.sink_lines_total is None and measured_sink_events is not None:
            result.sink_lines_total = measured_sink_events

        estimated_throughput = False
        if (
            (result.sink_lines_per_sec_avg is None or result.sink_lines_per_sec_avg <= 0.0)
            and measured_sink_events is not None
            and measured_window_sec is not None
            and measured_window_sec > 0.0
        ):
            estimated = float(measured_sink_events) / measured_window_sec
            result.sink_lines_per_sec_avg = estimated
            result.sink_lines_per_sec_p50 = estimated
            result.sink_lines_per_sec_p95 = estimated
            result.sink_lines_per_sec_p99 = estimated
            if (result.sink_lines_total or 0) == 0:
                result.sink_lines_total = int(measured_sink_events)
            estimated_throughput = True

        if collector_cpu_samples and collector_rss_samples:
            result.collector_cpu_cores_avg = avg(collector_cpu_samples)
            result.collector_cpu_cores_p95 = percentile(collector_cpu_samples, 0.95)
            result.collector_rss_mb_avg = avg(collector_rss_samples)
            result.collector_rss_mb_p95 = percentile(collector_rss_samples, 0.95)
        if generator_cpu_samples:
            result.generator_cpu_cores_avg = avg(generator_cpu_samples)
            result.generator_cpu_cores_p95 = percentile(generator_cpu_samples, 0.95)

        if max_throughput_mode:
            # In unbounded mode we focus on peak throughput; strict source-vs-sink oracle is not applied.
            result.drop_estimate = None
            result.dup_estimate = None
        elif result.emitter_reported_events_total is not None and result.sink_reported_events_total is not None:
            result.drop_estimate = max(0, result.emitter_reported_events_total - result.sink_reported_events_total)
            result.dup_estimate = max(0, result.sink_reported_events_total - result.emitter_reported_events_total)
            result.missing_event_count = result.drop_estimate
            result.unexpected_event_count = result.dup_estimate

        saturation_target_mode = not max_throughput_mode and (result.total_target_eps or 0) >= 100_000
        integrity_clean = (
            max_throughput_mode
            or saturation_target_mode
            or (
                (result.drop_estimate or 0) == 0
                and (result.dup_estimate or 0) == 0
                and (result.missing_event_count or 0) == 0
                and (result.unexpected_event_count or 0) == 0
            )
        )
        if args.ingest_mode == "file" and not integrity_clean:
            source_dup_info = find_duplicate_seqs(events_file_path)
            sink_dup_info = find_duplicate_seqs(capture_file_path)
            write_json(
                artifacts_dir / "seq-integrity-diagnostic.json",
                {
                    "source": source_dup_info,
                    "sink": sink_dup_info,
                    "emitter_reported_total": result.emitter_reported_events_total,
                    "sink_reported_total": result.sink_reported_events_total,
                },
            )
        if (result.sink_reported_events_total or 0) > 0:
            result.status = "pass"
            if estimated_throughput:
                result.notes = (
                    f"compose benchmark succeeded for collector={adapter.name}; "
                    f"sink_lines_per_sec_avg estimated from measure-window sink delta: "
                    f"{result.sink_lines_per_sec_avg}"
                )
            elif not integrity_clean:
                result.notes = (
                    f"compose benchmark produced sink output for collector={adapter.name}; "
                    "integrity deltas are recorded as diagnostics and do not gate competitive scoring. "
                    f"missing_event_count={result.missing_event_count}, unexpected_event_count={result.unexpected_event_count}, "
                    f"drop_estimate={result.drop_estimate}, dup_estimate={result.dup_estimate}, "
                    f"sink_lines_per_sec_avg={result.sink_lines_per_sec_avg}"
                )
            elif saturation_target_mode and (
                (result.drop_estimate or 0) > 0 or (result.dup_estimate or 0) > 0
            ):
                result.notes = (
                    f"compose benchmark saturation lane for collector={adapter.name}; "
                    f"integrity deltas recorded but not treated as hard failure at target_eps={result.total_target_eps}. "
                    f"missing_event_count={result.missing_event_count}, unexpected_event_count={result.unexpected_event_count}, "
                    f"drop_estimate={result.drop_estimate}, dup_estimate={result.dup_estimate}, "
                    f"sink_lines_per_sec_avg={result.sink_lines_per_sec_avg}"
                )
            else:
                result.notes = (
                    f"compose benchmark succeeded for collector={adapter.name}; "
                    f"sink_lines_per_sec_avg={result.sink_lines_per_sec_avg}"
                )
        else:
            result.status = "fail"
            result.notes = (
                f"compose benchmark did not observe sink throughput for collector={adapter.name}; "
                f"sink_reported_events_total={result.sink_reported_events_total}, "
                f"sink_lines_per_sec_avg={result.sink_lines_per_sec_avg}"
            )

    except Exception as exc:  # noqa: BLE001
        result.status = "fail"
        result.notes = f"compose benchmark failed before completion: {exc}"
    finally:
        write_samples(results_dir / "sink-samples.json", sink_samples)
        write_json(results_dir / "collector-samples.json", collector_resource_samples)
        write_json(
            results_dir / "ports.json",
            {
                "sink_diag_port": sink_diag_port,
                "sink_otlp_port": sink_otlp_port,
                "generator_diag_port": generator_diag_port,
                "collector_stats_port": collector_stats_port,
                "capture_stats_port": capture_stats_port,
            },
        )

        for log_service in ("sink", "capture-reader", "generator", adapter.service_name):
            log_path = artifacts_dir / f"{log_service}.log"
            with log_path.open("w", encoding="utf-8") as handle:
                subprocess.run(
                    compose + ["logs", "--no-color", log_service],
                    env=env,
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                )

        capture_files = sorted(path for path in runtime_dir.glob("capture.ndjson*") if path.is_file())
        for capture_file in capture_files:
            try:
                copy_with_cap(
                    capture_file,
                    artifacts_dir / f"sink-{capture_file.name}",
                    max_bytes=10 * 1024 * 1024,
                )
            except OSError:
                continue
        if (runtime_dir / "events.ndjson").exists():
            copy_with_cap(
                runtime_dir / "events.ndjson",
                artifacts_dir / "generator-events.ndjson",
                max_bytes=10 * 1024 * 1024,
            )

        subprocess.run(compose + ["--profile", adapter.name, "down", "-v", "--remove-orphans"], env=env, check=False)
        shutil.rmtree(runtime_dir, ignore_errors=True)

    write_result_files(
        results_dir,
        result,
        benchkit_run_id=args.benchkit_run_id,
        benchkit_kind=args.benchkit_kind if args.benchkit_run_id else None,
        benchkit_service_name=args.benchkit_service_name if args.benchkit_run_id else None,
        benchkit_profile=args.profile if args.benchkit_run_id else None,
        benchkit_ref=args.benchkit_ref if args.benchkit_run_id else None,
        benchkit_commit=args.benchkit_commit if args.benchkit_run_id else None,
        benchkit_workflow=args.benchkit_workflow if args.benchkit_run_id else None,
        benchkit_job=args.benchkit_job if args.benchkit_run_id else None,
        benchkit_run_attempt=args.benchkit_run_attempt if args.benchkit_run_id else None,
        benchkit_runner=args.benchkit_runner if args.benchkit_run_id else None,
    )

    # Persist lightweight run diagnostics for easier debugging when status is fail.
    write_json(
        results_dir / "run-meta.json",
        {
            "benchmark_id": benchmark_id,
            "collector": adapter.name,
            "ingest_mode": args.ingest_mode,
            "profile": args.profile,
            "cpu_profile": args.cpu_profile,
            "eps_per_sec": eps_per_sec,
            "result_status": result.status,
            "result_notes": result.notes,
        },
    )

    return 0 if result.status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
