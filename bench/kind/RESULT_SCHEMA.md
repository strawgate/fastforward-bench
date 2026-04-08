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
| `ingest_mode` | string | Collector input mode: `file` or `otlp` |
| `cpu_profile` | string | CPU shaping profile, currently `single` or `multi` |
| `cluster_cpu_limit_cores` | number | CPU cap applied to the KIND control-plane container |
| `pods` | integer | Target emitter pod count for the profile |
| `target_eps_per_pod` | integer | Target rate for the profile |
| `total_target_eps` | integer | `pods * target_eps_per_pod` |
| `warmup_sec` | integer | Profile knob |
| `measure_sec` | integer | Profile knob |
| `cooldown_sec` | integer | Profile knob |
| `sink_lines_total` | integer or null | Steady-state delta from sink diagnostics during the measured window |
| `emitter_reported_events_total` | integer or null | Sum of emitter-side reported totals collected from each emitter pod's `logfwd` `/api/stats` diagnostics |
| `sink_reported_events_total` | integer or null | Sink-side reported total collected from the sink `logfwd` `/api/stats` diagnostics |
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
| `sink_cpu_cores_avg` | number or null | Average sink CPU usage during the measured window (`null` when sink diagnostics come from `capture-reader`) |
| `sink_cpu_cores_p95` | number or null | P95 sink CPU usage during the measured window (`null` when sink diagnostics come from `capture-reader`) |
| `generator_cpu_cores_avg` | number or null | Average aggregate generator (emitter pods) CPU during the measured window |
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

- collectors: `logfwd`, `otelcol`, `vector`, `fluent-bit`, `vlagent`
- mode: `baseline-pass-through`
- oracle: compare benchmark-tagged sink rows against the emitter logs captured
  from the source pods

This is intended to be a useful and honest baseline:

- it does verify exact event preservation for the benchmark envelope
- it does not claim parse-and-enrich coverage or score those costs yet

For `ingest_mode=otlp`, the smoke run intentionally skips strict source-vs-sink
comparison because the emitter is not writing source logs to stdout in that
mode. In OTLP ingest mode, pass/fail is based on positive sink observation and
diagnostic counters.

Some file-ingest collectors can also run in diagnostics-only oracle mode when
their output schema is not source-row comparable yet. In that mode, pass/fail
is based on emitter/sink diagnostics totals plus positive sink observation.

## Artifact Expectations

Alongside the JSON row, each run should preserve:

- rendered manifests applied to the cluster
- `kubectl get all` output
- deployment and pod descriptions
- `benchkit-run.otlp.json` for Octo11y/Benchkit stash
- sink pod logs
- `emitter-stats.json`
- `sink-stats.json`
- `actual_rows.json` in `smoke`
- `source_rows.json` in `smoke`
- `stream-summary.json` in `smoke`

## Benchkit OTLP Projection

For reporting/history, the harness also writes `benchkit-run.otlp.json`.

Current projection rules:

- resource attributes:
  - `benchkit.run_id`
  - `benchkit.kind`
  - `benchkit.source_format=otlp`
  - `service.name`
- datapoint identity:
  - `benchkit.scenario=kind/{phase}/{benchmark_mode}/{ingest_mode}`
  - `benchkit.series={collector}`
- datapoint tags:
  - implementation, protocol, profile, cpu profile, cluster CPU cap, cluster, namespace, and profile knobs

The OTLP document is intentionally a projection of the canonical `result.json`,
not a second source of truth. If the two ever disagree, `result.json` is the
debugging source and the OTLP file should be regenerated/fixed.

When the harness is given a monitor OTLP HTTP endpoint, this same OTLP payload
is also posted directly to the monitor collector as a best-effort telemetry
signal. Persisted history still comes from the stashed run file.

In the same mode, the harness emits lightweight `_monitor.phase_signal` metrics
for benchmark lifecycle transitions such as `setup`, `warmup`, `measure`,
`cooldown`, and final run completion. These are breadcrumbs for Octo11y and do
not replace the canonical benchmark result row.
