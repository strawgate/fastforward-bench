from __future__ import annotations

import json
import re
import socket
import statistics
import subprocess
import time
import urllib.error
import urllib.request
from http.client import RemoteDisconnected
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Callable


@dataclass
class StatsSample:
    timestamp: float
    output_lines: int
    rss_bytes: int
    cpu_total_ms: int


PROM_SAMPLE_RE = re.compile(
    r"^(?P<name>[A-Za-z_:][A-Za-z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>[-+0-9.eE]+)$"
)


def reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class PortForward:
    def __init__(
        self,
        namespace: str,
        target: str,
        local_port: int,
        remote_port: int,
        *,
        ready_check: Callable[[int], object] | None = None,
    ) -> None:
        self.namespace = namespace
        self.target = target
        self.local_port = local_port
        self.remote_port = remote_port
        self.ready_check = ready_check or fetch_stats
        self.process: subprocess.Popen[str] | None = None

    def _stop_process(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)

    def __enter__(self) -> "PortForward":
        self.process = subprocess.Popen(
            [
                "kubectl",
                "-n",
                self.namespace,
                "port-forward",
                self.target,
                f"{self.local_port}:{self.remote_port}",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.time() + 30
        try:
            while time.time() < deadline:
                if self.process.poll() is not None:
                    stderr = ""
                    if self.process.stderr is not None:
                        stderr = self.process.stderr.read().strip()
                    raise RuntimeError(
                        f"port-forward exited early for {self.target} "
                        f"(code={self.process.returncode}): {stderr or 'no stderr output'}"
                    )
                try:
                    self.ready_check(self.local_port)
                    return self
                except Exception:  # noqa: BLE001
                    time.sleep(0.25)
            raise RuntimeError(f"port-forward did not become ready: {self.target}")
        except Exception:
            self._stop_process()
            raise

    def __exit__(self, exc_type, exc, tb) -> None:
        self._stop_process()


def _fetch_json_with_retries(
    local_port: int,
    path: str,
    *,
    timeout_sec: int = 5,
    attempts: int = 4,
) -> dict[str, object]:
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{local_port}{path}", timeout=timeout_sec) as response:
                return json.loads(response.read().decode("utf-8"))
        except (
            urllib.error.URLError,
            TimeoutError,
            ConnectionResetError,
            ConnectionAbortedError,
            ConnectionRefusedError,
            OSError,
            RemoteDisconnected,
            json.JSONDecodeError,
        ) as exc:
            last_exc = exc
            if attempt + 1 < attempts:
                time.sleep(0.1 * (attempt + 1))
    if last_exc is None:
        raise RuntimeError(f"failed to fetch JSON from {path}")
    raise RuntimeError(f"failed to fetch JSON from {path}: {last_exc}") from last_exc


def _fetch_json_from_paths(
    local_port: int,
    paths: list[str],
    *,
    timeout_sec: int = 5,
    attempts: int = 4,
) -> tuple[dict[str, object], str]:
    last_exc: Exception | None = None
    for path in paths:
        try:
            payload = _fetch_json_with_retries(
                local_port,
                path,
                timeout_sec=timeout_sec,
                attempts=attempts,
            )
            return payload, path
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
    if last_exc is None:
        raise RuntimeError("failed to fetch JSON from any candidate path")
    raise RuntimeError(f"failed to fetch JSON from candidate paths {paths}: {last_exc}") from last_exc


def _as_int(value: object, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            if "." in text:
                return int(float(text))
            return int(text)
        except ValueError:
            return default
    return default


def _normalize_status_payload(payload: dict[str, object]) -> dict[str, object]:
    pipelines = payload.get("pipelines", [])
    input_lines = 0
    output_lines = 0
    if isinstance(pipelines, list):
        for pipeline in pipelines:
            if not isinstance(pipeline, dict):
                continue
            transform = pipeline.get("transform")
            if isinstance(transform, dict):
                input_lines += _as_int(transform.get("lines_in"), 0)
                output_lines += _as_int(transform.get("lines_out"), 0)

    system = payload.get("system")
    memory = system.get("memory") if isinstance(system, dict) else None
    rss_bytes = _as_int(memory.get("resident"), 0) if isinstance(memory, dict) else 0

    return {
        "input_lines": input_lines,
        "output_lines": output_lines,
        "rss_bytes": rss_bytes,
        "cpu_user_ms": 0,
        "cpu_sys_ms": 0,
    }


def _normalize_stats_payload(payload: dict[str, object]) -> dict[str, object]:
    if "input_lines" in payload or "output_lines" in payload:
        return {
            "input_lines": _as_int(payload.get("input_lines"), 0),
            "output_lines": _as_int(payload.get("output_lines"), 0),
            "rss_bytes": _as_int(payload.get("rss_bytes"), 0),
            "cpu_user_ms": _as_int(payload.get("cpu_user_ms"), 0),
            "cpu_sys_ms": _as_int(payload.get("cpu_sys_ms"), 0),
        }
    if "pipelines" in payload:
        return _normalize_status_payload(payload)
    return {
        "input_lines": 0,
        "output_lines": 0,
        "rss_bytes": 0,
        "cpu_user_ms": 0,
        "cpu_sys_ms": 0,
    }


def fetch_stats(local_port: int) -> dict[str, object]:
    payload, _ = _fetch_json_from_paths(
        local_port,
        ["/admin/v1/stats", "/api/stats", "/admin/v1/status"],
    )
    return _normalize_stats_payload(payload)


def fetch_capture_stats(local_port: int) -> dict[str, object]:
    return _fetch_json_with_retries(local_port, "/stats")


def fetch_text(local_port: int, path: str) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{local_port}{path}", timeout=5) as response:
        return response.read().decode("utf-8")


def _sample_from_payload(payload: dict[str, object]) -> StatsSample:
    cpu_user_ms = int(payload.get("cpu_user_ms", 0) or 0)
    cpu_sys_ms = int(payload.get("cpu_sys_ms", 0) or 0)
    return StatsSample(
        timestamp=time.time(),
        output_lines=int(payload.get("output_lines", 0) or 0),
        rss_bytes=int(payload.get("rss_bytes", 0) or 0),
        cpu_total_ms=cpu_user_ms + cpu_sys_ms,
    )


def _sample_from_capture_payload(payload: dict[str, object]) -> StatsSample:
    return StatsSample(
        timestamp=time.time(),
        output_lines=int(payload.get("benchmark_rows_total", 0) or 0),
        rss_bytes=0,
        cpu_total_ms=0,
    )


def _parse_prom_labels(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    labels: dict[str, str] = {}
    for match in re.finditer(r'([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\\\]|\\\\.)*)"', raw):
        labels[match.group(1)] = bytes(match.group(2), "utf-8").decode("unicode_escape")
    return labels


def _prometheus_metric_total(
    body: str,
    metric_name: str,
    *,
    labels: dict[str, str] | None = None,
) -> float:
    total = 0.0
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        match = PROM_SAMPLE_RE.match(line.strip())
        if not match:
            continue
        if match.group("name") != metric_name:
            continue
        sample_labels = _parse_prom_labels(match.group("labels"))
        if labels and any(sample_labels.get(key) != value for key, value in labels.items()):
            continue
        total += float(match.group("value"))
    return total


def _prometheus_metric_first(body: str, metric_names: list[str], *, labels: dict[str, str] | None = None) -> float:
    for metric_name in metric_names:
        total = _prometheus_metric_total(body, metric_name, labels=labels)
        if total:
            return total
    return 0.0


def _sample_from_otelcol_prometheus(body: str) -> StatsSample:
    process_cpu_seconds = _prometheus_metric_total(body, "otelcol_process_cpu_seconds_total")
    sent_logs = _prometheus_metric_total(body, "otelcol_exporter_sent_log_records_total")
    accepted_logs = _prometheus_metric_total(body, "otelcol_receiver_accepted_log_records_total")
    rss_bytes = int(_prometheus_metric_total(body, "otelcol_process_memory_rss_bytes"))
    return StatsSample(
        timestamp=time.time(),
        output_lines=int(max(sent_logs, accepted_logs)),
        rss_bytes=rss_bytes,
        cpu_total_ms=int(process_cpu_seconds * 1000.0),
    )


def fetch_otelcol_prometheus_sample(local_port: int) -> StatsSample:
    return _sample_from_otelcol_prometheus(fetch_text(local_port, "/metrics"))


def _sample_from_vector_prometheus(body: str) -> StatsSample:
    process_cpu_seconds = _prometheus_metric_first(
        body,
        ["process_cpu_seconds_total", "vector_process_cpu_seconds_total"],
    )
    sent_logs = 0.0
    for component_id in ["bench_out", "otel_sink"]:
        sent_logs = _prometheus_metric_first(
            body,
            ["component_sent_events_total", "vector_component_sent_events_total"],
            labels={"component_id": component_id},
        )
        if sent_logs:
            break
    rss_bytes = int(
        _prometheus_metric_first(
            body,
            ["process_resident_memory_bytes", "vector_process_resident_memory_bytes"],
        )
    )
    # If both process-level metrics are absent the endpoint is returning only
    # component-level metrics (no process CPU/RSS). Raise so the caller treats
    # this sample as unavailable rather than recording misleading zeros.
    if process_cpu_seconds == 0.0 and rss_bytes == 0:
        raise ValueError("vector process metrics (CPU/RSS) not present in prometheus output")
    return StatsSample(
        timestamp=time.time(),
        output_lines=int(sent_logs),
        rss_bytes=rss_bytes,
        cpu_total_ms=int(process_cpu_seconds * 1000.0),
    )


def fetch_vector_prometheus_sample(local_port: int) -> StatsSample:
    return _sample_from_vector_prometheus(fetch_text(local_port, "/metrics"))


def _fetch_json(local_port: int, path: str) -> dict[str, object]:
    with urllib.request.urlopen(f"http://127.0.0.1:{local_port}{path}", timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_vlagent_sample(local_port: int) -> StatsSample:
    payload = _fetch_json(local_port, "/stats")
    return StatsSample(
        timestamp=time.time(),
        output_lines=int(payload.get("output_lines", 0) or 0),
        rss_bytes=int(payload.get("rss_bytes", 0) or 0),
        cpu_total_ms=int(payload.get("cpu_total_ms", 0) or 0),
    )


def collect_bench_samples(
    namespace: str,
    sink_target: str,
    collector_target: str,
    *,
    sink_stats_kind: str = "fastforward",
    sink_stats_port: int = 9090,
    collector_stats_kind: str,
    collector_stats_port: int,
    warmup_sec: int,
    measure_sec: int,
    emitter_targets: list[str] | None = None,
    emitter_stats_port: int = 9090,
    on_measure_start: Callable[[], None] | None = None,
    on_measure_complete: Callable[[], None] | None = None,
) -> tuple[list[StatsSample], list[StatsSample], list[StatsSample]]:
    if sink_stats_kind == "fastforward":
        sink_ready_check = fetch_stats
        def sink_fetch_sample(port: int) -> StatsSample:
            return _sample_from_payload(fetch_stats(port))
    elif sink_stats_kind == "capture_reader":
        sink_ready_check = fetch_capture_stats
        def sink_fetch_sample(port: int) -> StatsSample:
            return _sample_from_capture_payload(fetch_capture_stats(port))
    else:
        raise ValueError(f"unknown sink_stats_kind: {sink_stats_kind}")

    if collector_stats_kind == "fastforward":
        collector_ready_check = fetch_stats
        def collector_fetch_sample(port: int) -> StatsSample:
            return _sample_from_payload(fetch_stats(port))
    elif collector_stats_kind == "otelcol_prometheus":
        def collector_ready_check(port: int) -> str:
            return fetch_text(port, "/metrics")
        collector_fetch_sample = fetch_otelcol_prometheus_sample
    elif collector_stats_kind == "vector_prometheus":
        def collector_ready_check(port: int) -> str:
            return fetch_text(port, "/metrics")
        collector_fetch_sample = fetch_vector_prometheus_sample
    elif collector_stats_kind == "vlagent_json":
        def collector_ready_check(port: int) -> str:
            return fetch_text(port, "/health")
        collector_fetch_sample = fetch_vlagent_sample
    else:
        raise ValueError(f"unknown collector_stats_kind: {collector_stats_kind}")

    sink_local_port = reserve_local_port()
    collector_local_port = reserve_local_port()
    emitter_local_ports: list[int] = []
    with ExitStack() as stack:
        stack.enter_context(
            PortForward(
                namespace,
                sink_target,
                sink_local_port,
                sink_stats_port,
                ready_check=sink_ready_check,
            )
        )
        stack.enter_context(
            PortForward(
                namespace,
                collector_target,
                collector_local_port,
                collector_stats_port,
                ready_check=collector_ready_check,
            )
        )
        for target in emitter_targets or []:
            local_port = reserve_local_port()
            stack.enter_context(
                PortForward(
                    namespace,
                    target,
                    local_port,
                    emitter_stats_port,
                    ready_check=fetch_stats,
                )
            )
            emitter_local_ports.append(local_port)

        if warmup_sec > 0:
            time.sleep(warmup_sec)
        if on_measure_start is not None:
            on_measure_start()

        sink_samples: list[StatsSample] = []
        collector_samples: list[StatsSample] = []
        emitter_samples: list[StatsSample] = []
        deadline = time.time() + measure_sec

        while True:
            try:
                sink_sample = sink_fetch_sample(sink_local_port)
            except Exception:
                sink_sample = None
            if sink_sample is not None:
                sink_samples.append(sink_sample)

            try:
                collector_sample = collector_fetch_sample(collector_local_port)
            except Exception:
                collector_sample = None
            if collector_sample is not None:
                collector_samples.append(collector_sample)

            if emitter_local_ports:
                emitter_cpu_total_ms = 0
                emitter_output_lines = 0
                emitter_rss_bytes = 0
                emitter_successes = 0
                for local_port in emitter_local_ports:
                    try:
                        payload = fetch_stats(local_port)
                    except Exception:
                        continue
                    sample = _sample_from_payload(payload)
                    emitter_cpu_total_ms += sample.cpu_total_ms
                    emitter_output_lines += sample.output_lines
                    emitter_rss_bytes += sample.rss_bytes
                    emitter_successes += 1
                if emitter_successes > 0:
                    emitter_samples.append(
                        StatsSample(
                            timestamp=time.time(),
                            output_lines=emitter_output_lines,
                            rss_bytes=emitter_rss_bytes,
                            cpu_total_ms=emitter_cpu_total_ms,
                        )
                    )
            if time.time() >= deadline:
                break
            time.sleep(1)
        if on_measure_complete is not None:
            on_measure_complete()

    return sink_samples, collector_samples, emitter_samples


def diff_output_lines(samples: list[StatsSample]) -> int | None:
    if len(samples) < 2:
        return None
    return max(0, samples[-1].output_lines - samples[0].output_lines)


def lines_per_sec_series(samples: list[StatsSample]) -> list[float]:
    series: list[float] = []
    for prev, cur in zip(samples, samples[1:]):
        elapsed = max(cur.timestamp - prev.timestamp, 1e-6)
        delta = max(0, cur.output_lines - prev.output_lines)
        series.append(delta / elapsed)
    return series


def cpu_cores_series(samples: list[StatsSample]) -> list[float]:
    series: list[float] = []
    for prev, cur in zip(samples, samples[1:]):
        elapsed = max(cur.timestamp - prev.timestamp, 1e-6)
        delta_ms = max(0, cur.cpu_total_ms - prev.cpu_total_ms)
        series.append((delta_ms / 1000.0) / elapsed)
    return series


def rss_mb_series(samples: list[StatsSample]) -> list[float]:
    return [sample.rss_bytes / (1024.0 * 1024.0) for sample in samples]


def percentile(series: list[float], pct: float) -> float | None:
    if not series:
        return None
    if len(series) == 1:
        return series[0]
    ordered = sorted(series)
    rank = (len(ordered) - 1) * pct
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    fraction = rank - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def avg(series: list[float]) -> float | None:
    if not series:
        return None
    return statistics.fmean(series)


def collect_emitter_reported_total(namespace: str, pod_names: list[str]) -> tuple[int | None, list[dict[str, object]]]:
    per_pod: list[dict[str, object]] = []
    total = 0
    for pod_name in pod_names:
        local_port = reserve_local_port()
        with PortForward(namespace, f"pod/{pod_name}", local_port, 9090):
            stats = fetch_stats(local_port)
        per_pod.append(
            {
                "pod_name": pod_name,
                "output_lines": int(stats.get("output_lines", 0) or 0),
                "input_lines": int(stats.get("input_lines", 0) or 0),
                "rss_bytes": int(stats.get("rss_bytes", 0) or 0),
                "cpu_total_ms": int(stats.get("cpu_user_ms", 0) or 0)
                + int(stats.get("cpu_sys_ms", 0) or 0),
            }
        )
        total += int(stats.get("output_lines", 0) or 0)
    return (total if per_pod else None, per_pod)


def collect_sink_reported_stats(
    namespace: str,
    sink_pod: str,
    *,
    sink_stats_kind: str = "fastforward",
    sink_stats_port: int = 9090,
) -> dict[str, object]:
    local_port = reserve_local_port()
    if sink_stats_kind == "fastforward":
        with PortForward(namespace, f"pod/{sink_pod}", local_port, sink_stats_port):
            return fetch_stats(local_port)
    if sink_stats_kind == "capture_reader":
        with PortForward(
            namespace,
            f"pod/{sink_pod}",
            local_port,
            sink_stats_port,
            ready_check=fetch_capture_stats,
        ):
            return fetch_capture_stats(local_port)
    raise ValueError(f"unknown sink_stats_kind: {sink_stats_kind}")
