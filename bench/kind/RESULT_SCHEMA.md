# KIND Benchmark Result Schema

Each benchmark run emits a canonical row in `results.jsonl` and a matching
single-object `result.json`.

The contract is designed to be stable across implementation phases. Fields that
are not collected yet are emitted as `null`.

## Required Fields

| Field | Type | Notes |
| --- | --- | --- |
| `benchmark_id` | string | UUID for the run |
| `timestamp_utc` | string | ISO-8601 UTC timestamp |
| `phase` | string | `infra` today; later `full` |
| `cluster` | string | `kind-single-node` |
| `cluster_name` | string | Actual KIND cluster name |
| `namespace` | string | Kubernetes namespace used by the run |
| `collector` | string | Target collector name for the cell |
| `protocol` | string | Sink protocol, currently `otlp_http` by default |
| `pods` | integer | Target emitter pod count for the profile |
| `target_eps_per_pod` | integer | Target rate for the profile |
| `total_target_eps` | integer | `pods * target_eps_per_pod` |
| `warmup_sec` | integer | Profile knob |
| `measure_sec` | integer | Profile knob |
| `cooldown_sec` | integer | Profile knob |
| `sink_lines_total` | integer or null | Steady-state delta from sink diagnostics during the measured window |
| `captured_rows_total` | integer or null | Full count of captured benchmark rows observed in sink artifacts |
| `sink_lines_per_sec_avg` | number or null | Not yet collected in `infra` |
| `sink_lines_per_sec_p50` | number or null | Not yet collected in `infra` |
| `sink_lines_per_sec_p95` | number or null | Not yet collected in `infra` |
| `sink_lines_per_sec_p99` | number or null | Not yet collected in `infra` |
| `drop_estimate` | integer or null | Not yet collected in `infra` |
| `dup_estimate` | integer or null | Not yet collected in `infra` |
| `latency_ms_p50` | number or null | Not yet collected in `infra` |
| `latency_ms_p95` | number or null | Not yet collected in `infra` |
| `latency_ms_p99` | number or null | Not yet collected in `infra` |
| `collector_cpu_cores_avg` | number or null | Not yet collected in `infra` |
| `collector_cpu_cores_p95` | number or null | Not yet collected in `infra` |
| `collector_rss_mb_avg` | number or null | Not yet collected in `infra` |
| `collector_rss_mb_p95` | number or null | Not yet collected in `infra` |
| `cluster_ready` | boolean | Whether KIND was created and reachable |
| `sink_ready` | boolean | Whether the sink deployment became ready |
| `status` | string | `pass`, `fail`, or `partial` |
| `notes` | string | Human-readable status note |

## Phase 0 / 1 Behavior

The current implementation always emits the full schema, but only the
infrastructure status fields are populated. This allows downstream summary code
to remain stable when throughput and loss metrics are added later.

## Artifact Expectations

Alongside the JSON row, each run should preserve:

- rendered manifests applied to the cluster;
- `kubectl get all` output;
- deployment and pod descriptions;
- sink pod logs when available.
