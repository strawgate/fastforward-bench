# KIND Benchmark Result Schema

Each benchmark run emits a canonical row in `results.jsonl` and a matching
single-object `result.json`.

The contract is designed to stay stable as new collectors and benchmark modes
land. Fields that are not collected in a phase are emitted as `null`.

## Required Fields

| Field | Type | Notes |
| --- | --- | --- |
| `benchmark_id` | string | UUID for the run |
| `timestamp_utc` | string | ISO-8601 UTC timestamp |
| `phase` | string | `infra` or `smoke` |
| `benchmark_mode` | string | `infra-bootstrap` or a benchmark mode such as `baseline-pass-through` |
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
| `source_rows_total` | integer or null | Full count of benchmark rows observed in emitter artifacts |
| `missing_source_count` | integer or null | Expected emitter pods absent from sink artifacts |
| `missing_event_count` | integer or null | Benchmark events seen at the source but absent from sink artifacts |
| `unexpected_event_count` | integer or null | Benchmark events seen at the sink but not at the source |
| `sink_lines_per_sec_avg` | number or null | Average steady-state sink throughput |
| `sink_lines_per_sec_p50` | number or null | Median steady-state sink throughput |
| `sink_lines_per_sec_p95` | number or null | P95 steady-state sink throughput |
| `sink_lines_per_sec_p99` | number or null | P99 steady-state sink throughput |
| `drop_estimate` | integer or null | Current drop estimate; exact source-vs-sink count in `smoke` |
| `dup_estimate` | integer or null | Duplicate benchmark event estimate |
| `latency_ms_p50` | number or null | Reserved for later latency capture |
| `latency_ms_p95` | number or null | Reserved for later latency capture |
| `latency_ms_p99` | number or null | Reserved for later latency capture |
| `collector_cpu_cores_avg` | number or null | Average collector CPU usage during the measured window |
| `collector_cpu_cores_p95` | number or null | P95 collector CPU usage during the measured window |
| `collector_rss_mb_avg` | number or null | Average collector RSS during the measured window |
| `collector_rss_mb_p95` | number or null | P95 collector RSS during the measured window |
| `cluster_ready` | boolean | Whether KIND was created and reachable |
| `sink_ready` | boolean | Whether the sink deployment became ready |
| `status` | string | `pass`, `fail`, or `partial` |
| `notes` | string | Human-readable status note |

## Phase Semantics

### `infra`

The infrastructure phase validates cluster lifecycle and sink deployment only.
All source-vs-sink and steady-state throughput fields remain `null`.

### `smoke`

The smoke phase currently runs one narrow benchmark mode:

- collector: `logfwd`
- mode: `baseline-pass-through`
- oracle: compare benchmark-tagged sink rows against the emitter logs captured
  from the source pods

This is intended to be a useful and honest baseline:

- it does verify exact event preservation for the benchmark envelope
- it does not claim parse-and-enrich coverage or score those costs yet

## Artifact Expectations

Alongside the JSON row, each run should preserve:

- rendered manifests applied to the cluster
- `kubectl get all` output
- deployment and pod descriptions
- sink pod logs
- `actual_rows.json` in `smoke`
- `source_rows.json` in `smoke`
- `stream-summary.json` in `smoke`
