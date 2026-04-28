from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CollectorAdapter:
    name: str
    benchmark_mode: str
    file_config_template: str
    file_workload_template: str
    rollout_kind: str
    rollout_name: str
    pod_selector: str
    diagnostics_target_format: str
    otlp_config_template: str | None = None
    otlp_workload_template: str | None = None
    collector_image: str | None = None
    collector_stats_kind: str = "fastforward"
    collector_stats_port: int = 9090
    sink_transport: str = "otlp_http"

    def supports_ingest_mode(self, ingest_mode: str) -> bool:
        if ingest_mode == "file":
            return True
        if ingest_mode == "otlp":
            return self.otlp_config_template is not None and self.otlp_workload_template is not None
        return False

    def templates_for_ingest_mode(self, ingest_mode: str) -> tuple[str, str]:
        if ingest_mode == "file":
            return self.file_config_template, self.file_workload_template
        if ingest_mode == "otlp":
            if self.otlp_config_template is None or self.otlp_workload_template is None:
                raise NotImplementedError(f"collector '{self.name}' does not support ingest mode '{ingest_mode}'")
            return self.otlp_config_template, self.otlp_workload_template
        raise ValueError(f"unsupported ingest mode: {ingest_mode}")


FASTFORWARD_COLLECTOR = CollectorAdapter(
    name="fastforward",
    benchmark_mode="baseline-pass-through",
    file_config_template="collectors/fastforward-configmap.yaml.tmpl",
    file_workload_template="collectors/fastforward-daemonset.yaml.tmpl",
    otlp_config_template="collectors/fastforward-otlp-configmap.yaml.tmpl",
    otlp_workload_template="collectors/fastforward-otlp-daemonset.yaml.tmpl",
    rollout_kind="daemonset",
    rollout_name="fastforward-bench-collector",
    pod_selector="app.kubernetes.io/name=fastforward-bench-collector",
    diagnostics_target_format="pod/{pod_name}",
)

OTELCOL_COLLECTOR = CollectorAdapter(
    name="otelcol",
    benchmark_mode="baseline-pass-through",
    file_config_template="collectors/otelcol-configmap.yaml.tmpl",
    file_workload_template="collectors/otelcol-daemonset.yaml.tmpl",
    otlp_config_template="collectors/otelcol-otlp-configmap.yaml.tmpl",
    otlp_workload_template="collectors/otelcol-otlp-daemonset.yaml.tmpl",
    rollout_kind="daemonset",
    rollout_name="otelcol-bench-collector",
    pod_selector="app.kubernetes.io/name=otelcol-bench-collector",
    diagnostics_target_format="pod/{pod_name}",
    collector_image="otel/opentelemetry-collector-contrib:0.148.0",
    collector_stats_kind="otelcol_prometheus",
    collector_stats_port=8888,
)

VECTOR_COLLECTOR = CollectorAdapter(
    name="vector",
    benchmark_mode="baseline-pass-through",
    file_config_template="collectors/vector-configmap.yaml.tmpl",
    file_workload_template="collectors/vector-daemonset.yaml.tmpl",
    rollout_kind="daemonset",
    rollout_name="vector-bench-collector",
    pod_selector="app.kubernetes.io/name=vector-bench-collector",
    diagnostics_target_format="pod/{pod_name}",
    collector_image="timberio/vector:0.54.0-debian",
    collector_stats_kind="vector_prometheus",
    collector_stats_port=9090,
    sink_transport="http_ndjson",
)

FILEBEAT_COLLECTOR = CollectorAdapter(
    name="filebeat",
    benchmark_mode="baseline-pass-through",
    file_config_template="collectors/filebeat-configmap.yaml.tmpl",
    file_workload_template="collectors/filebeat-daemonset.yaml.tmpl",
    rollout_kind="daemonset",
    rollout_name="filebeat-bench-collector",
    pod_selector="app.kubernetes.io/name=filebeat-bench-collector",
    diagnostics_target_format="pod/{pod_name}",
    collector_image="docker.elastic.co/beats/filebeat:8.17.3",
    collector_stats_kind="fastforward",
    sink_transport="http_ndjson",
)

VLAGENT_COLLECTOR = CollectorAdapter(
    name="vlagent",
    benchmark_mode="baseline-pass-through",
    file_config_template="collectors/vlagent-configmap.yaml.tmpl",
    file_workload_template="collectors/vlagent-daemonset.yaml.tmpl",
    rollout_kind="daemonset",
    rollout_name="vlagent-bench-collector",
    pod_selector="app.kubernetes.io/name=vlagent-bench-collector",
    diagnostics_target_format="pod/{pod_name}",
    collector_image="victoriametrics/vlagent:v1.50.0",
    collector_stats_kind="vlagent_json",
    collector_stats_port=9429,
    sink_transport="http_ndjson",
)


def get_collector_adapter(name: str) -> CollectorAdapter:
    if name == FASTFORWARD_COLLECTOR.name:
        return FASTFORWARD_COLLECTOR
    if name == OTELCOL_COLLECTOR.name:
        return OTELCOL_COLLECTOR
    if name == VECTOR_COLLECTOR.name:
        return VECTOR_COLLECTOR
    if name == FILEBEAT_COLLECTOR.name:
        return FILEBEAT_COLLECTOR
    if name == VLAGENT_COLLECTOR.name:
        return VLAGENT_COLLECTOR
    raise NotImplementedError(f"collector not implemented yet in benchmark harness: {name}")
