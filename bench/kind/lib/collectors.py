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


def get_collector_adapter(name: str) -> CollectorAdapter:
    if name == LOGFWD_COLLECTOR.name:
        return LOGFWD_COLLECTOR
    raise NotImplementedError(f"collector not implemented yet in benchmark harness: {name}")
