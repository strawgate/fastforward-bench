from __future__ import annotations

import json
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


class PortForward:
    def __init__(self, namespace: str, target: str, local_port: int, remote_port: int) -> None:
        self.namespace = namespace
        self.target = target
        self.local_port = local_port
        self.remote_port = remote_port
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
                fetch_stats(self.local_port)
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


def _sample_from_payload(payload: dict[str, object]) -> StatsSample:
    cpu_user_ms = int(payload.get("cpu_user_ms", 0) or 0)
    cpu_sys_ms = int(payload.get("cpu_sys_ms", 0) or 0)
    return StatsSample(
        timestamp=time.time(),
        output_lines=int(payload.get("output_lines", 0) or 0),
        rss_bytes=int(payload.get("rss_bytes", 0) or 0),
        cpu_total_ms=cpu_user_ms + cpu_sys_ms,
    )


def collect_logfwd_samples(
    namespace: str,
    sink_target: str,
    collector_target: str,
    *,
    warmup_sec: int,
    measure_sec: int,
    on_measure_start: Callable[[], None] | None = None,
    on_measure_complete: Callable[[], None] | None = None,
) -> tuple[list[StatsSample], list[StatsSample]]:
    with ExitStack() as stack:
        stack.enter_context(PortForward(namespace, sink_target, 19090, 9090))
        stack.enter_context(PortForward(namespace, collector_target, 19091, 9090))

        if warmup_sec > 0:
            time.sleep(warmup_sec)
        if on_measure_start is not None:
            on_measure_start()

        sink_samples: list[StatsSample] = []
        collector_samples: list[StatsSample] = []
        deadline = time.time() + measure_sec

        while True:
            sink_samples.append(_sample_from_payload(fetch_stats(19090)))
            collector_samples.append(_sample_from_payload(fetch_stats(19091)))
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
