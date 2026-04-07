from __future__ import annotations

import json
import re
import socket
import statistics
import subprocess
import time
import urllib.request
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Callable


@dataclass
class StatsSample:
    timestamp: float
    output_lines: int
    rss_bytes: int
    cpu_total_ms: int


PROM_SAMPLE_RE = re.compile(r"^(?P<name>[A-Za-z_:][A-Za-z0-9_:]*)(?:\{[^}]*\})?\s+(?P<value>[-+0-9.eE]+)$")


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
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                self.ready_check(self.local_port)
                return self
            except Exception:  # noqa: BLE001
                time.sleep(0.25)
        raise RuntimeError(f"port-forward did not become ready: {self.target}")

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)


def fetch_stats(local_port: int) -> dict[str, object]:
    with urllib.request.urlopen(f"http://127.0.0.1:{local_port}/api/stats", timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


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


def _prometheus_metric_total(body: str, metric_name: str) -> float:
    total = 0.0
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        match = PROM_SAMPLE_RE.match(line.strip())
        if not match:
            continue
        if match.group("name") != metric_name:
            continue
        total += float(match.group("value"))
    return total


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


def collect_bench_samples(
    namespace: str,
    sink_target: str,
    collector_target: str,
    *,
    collector_stats_kind: str,
    collector_stats_port: int,
    warmup_sec: int,
    measure_sec: int,
    on_measure_start: Callable[[], None] | None = None,
    on_measure_complete: Callable[[], None] | None = None,
) -> tuple[list[StatsSample], list[StatsSample]]:
    if collector_stats_kind == "logfwd":
        collector_ready_check = fetch_stats
        collector_fetch_sample = lambda port: _sample_from_payload(fetch_stats(port))
    elif collector_stats_kind == "otelcol_prometheus":
        collector_ready_check = lambda port: fetch_text(port, "/metrics")
        collector_fetch_sample = fetch_otelcol_prometheus_sample
    else:
        raise ValueError(f"unknown collector_stats_kind: {collector_stats_kind}")

    with ExitStack() as stack:
        stack.enter_context(PortForward(namespace, sink_target, 19090, 9090))
        stack.enter_context(
            PortForward(
                namespace,
                collector_target,
                19091,
                collector_stats_port,
                ready_check=collector_ready_check,
            )
        )

        if warmup_sec > 0:
            time.sleep(warmup_sec)
        if on_measure_start is not None:
            on_measure_start()

        sink_samples: list[StatsSample] = []
        collector_samples: list[StatsSample] = []
        deadline = time.time() + measure_sec

        while True:
            sink_samples.append(_sample_from_payload(fetch_stats(19090)))
            collector_samples.append(collector_fetch_sample(19091))
            if time.time() >= deadline:
                break
            time.sleep(1)
        if on_measure_complete is not None:
            on_measure_complete()

    return sink_samples, collector_samples


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


def collect_sink_reported_stats(namespace: str, sink_pod: str) -> dict[str, object]:
    local_port = reserve_local_port()
    with PortForward(namespace, f"pod/{sink_pod}", local_port, 9090):
        return fetch_stats(local_port)
