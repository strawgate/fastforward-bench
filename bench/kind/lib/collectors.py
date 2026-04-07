from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CollectorAdapter:
    name: str
    benchmark_mode: str
    config_template: str
    workload_template: str
    rollout_kind: str
    rollout_name: str
    pod_selector: str
    diagnostics_target_format: str
    collector_image: str | None = None
    collector_stats_kind: str = "logfwd"
    collector_stats_port: int = 9090
    sink_transport: str = "otlp_http"


LOGFWD_COLLECTOR = CollectorAdapter(
    name="logfwd",
    benchmark_mode="baseline-pass-through",
    config_template="collectors/logfwd-configmap.yaml.tmpl",
    workload_template="collectors/logfwd-daemonset.yaml.tmpl",
    rollout_kind="daemonset",
    rollout_name="logfwd-bench-collector",
    pod_selector="app.kubernetes.io/name=logfwd-bench-collector",
    diagnostics_target_format="pod/{pod_name}",
)

OTELCOL_COLLECTOR = CollectorAdapter(
    name="otelcol",
    benchmark_mode="baseline-pass-through",
    config_template="collectors/otelcol-configmap.yaml.tmpl",
    workload_template="collectors/otelcol-daemonset.yaml.tmpl",
    rollout_kind="daemonset",
    rollout_name="otelcol-bench-collector",
    pod_selector="app.kubernetes.io/name=otelcol-bench-collector",
    diagnostics_target_format="pod/{pod_name}",
    collector_image="otel/opentelemetry-collector-contrib:0.148.0",
    collector_stats_kind="otelcol_prometheus",
    collector_stats_port=8888,
)


def get_collector_adapter(name: str) -> CollectorAdapter:
    if name == LOGFWD_COLLECTOR.name:
        return LOGFWD_COLLECTOR
    if name == OTELCOL_COLLECTOR.name:
        return OTELCOL_COLLECTOR
    raise NotImplementedError(f"collector not implemented yet in benchmark harness: {name}")
