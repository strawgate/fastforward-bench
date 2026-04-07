# KIND Competitive Benchmark

This directory contains the single-node KIND benchmark harness for `memagent-e2e`.

The benchmark is intentionally separate from the correctness scenarios under
`tests/e2e/`. It measures throughput-oriented Kubernetes collection behavior,
while the scenario suite proves semantic correctness.

The harness now also emits a Benchkit/Octo11y-compatible OTLP run document so
nightly or manual runs can be stashed to `bench-data` and aggregated without a
repo-local reporting format.

## Current Scope

The harness currently supports two phases:

- `infra`
  - deterministic KIND cluster lifecycle
  - `logfwd` capture sink deployment
  - canonical run metadata, summary markdown, and artifact collection
- `smoke`
  - all `infra` behavior
  - a replicated stdout emitter workload
  - one collector-under-test DaemonSet
  - sink-side diagnostics sampling
  - source-vs-sink artifact comparison for exact event preservation

The current smoke implementation is intentionally narrow:

- collector support: `logfwd`, `otelcol`, `vector`
- benchmark mode: `baseline-pass-through`
- sink transport:
  - `logfwd`/`otelcol`: OTLP/HTTP into a `logfwd` capture sink
  - `vector`: HTTP NDJSON into the same capture sink

That means the current scores are useful for benchmarking discovery, framing,
shipping, and loss/dup behavior under load. They are **not** parse-and-enrich
scores yet.

## Design Choices

- Harness runtime: Python 3, standard library only.
- Generator: `logfwd` itself running the `generator.profile=record` source and
  shaping the benchmark envelope in SQL before writing to stdout.
- Sink: `logfwd` configured as a dumb capture sink writing JSON lines to a file.
- Producer counters:
  - emitters expose `logfwd` diagnostics via `/api/stats`
  - the sink exposes `logfwd` diagnostics via `/api/stats`
- Benchmark artifacts: JSON row, JSONL stream, summary markdown, rendered
  manifests, and `kubectl` debug output.
- Reporting integration: `benchkit-run.otlp.json` for Octo11y
  `stash`/`aggregate` workflows.
- Extension seam: add collector adapters without changing the result contract.

## Profiles

The harness ships with two named profiles:

- `smoke`
  - `pods=5`
  - `eps_per_pod=100`
  - `warmup=5s`
  - `measure=15s`
  - `cooldown=5s`
- `default`
  - `pods=30`
  - `eps_per_pod=300`
  - `warmup=30s`
  - `measure=120s`
  - `cooldown=10s`

CPU behavior is controlled separately via `--cpu-profile`:

- `single`
  - caps the KIND control-plane container to `1` core
  - computes stable per-pod resource requests/limits from that budget
- `multi`
  - caps the KIND control-plane container to `2` cores
  - computes stable per-pod resource requests/limits from that budget

Local runs default to `single`. CI runs can matrix both `single` and `multi`
for apples-to-apples comparisons.

These timing windows now have real runtime meaning in the harness:

- `warmup`
  collector and sink are live, but samples do not count toward the score yet
- `measure`
  sink and collector diagnostics are sampled for the benchmark result
- `cooldown`
  the workload keeps running briefly so the collector can drain before teardown

## Prerequisites

- Docker
- `kind`
- `kubectl`
- Python 3
- a local `logfwd:e2e` image, or a workflow step that builds/tags it first

## Quickstart

Run the real smoke benchmark:

```bash
python3 bench/kind/run.py \
  --phase smoke \
  --profile smoke \
  --collector logfwd \
  --cpu-profile single \
  --cluster-name memagent-bench-smoke \
  --memagent-image logfwd:e2e \
  --results-dir bench/kind/results/local-smoke
```

Run only the infrastructure bootstrap path:

```bash
python3 bench/kind/run.py \
  --phase infra \
  --profile smoke \
  --collector logfwd \
  --cpu-profile single \
  --cluster-name memagent-bench-infra \
  --memagent-image logfwd:e2e \
  --results-dir bench/kind/results/local-infra
```

Useful flags:

- `--keep-cluster`
  Leaves the KIND cluster running for inspection.
- `--namespace`
  Overrides the benchmark namespace.
- `--pods`
  Overrides emitter pod count from the selected profile.
- `--eps-per-pod`
  Overrides generator `events_per_sec` per emitter pod.
  Use `0` for unbounded generation ("as fast as possible").
- `--collector-image`
  Overrides the collector image for adapters that do not use `--memagent-image`.
- `--protocol`
  Records the target sink protocol in metadata. Defaults to `otlp_http`.

## Outputs

Each run writes a directory under `bench/kind/results/` containing:

- `result.json`
- `results.jsonl`
- `benchkit-run.otlp.json`
- `summary.md`
- `rendered-manifests/`
- `artifacts/`
- `emitter-stats.json`
- `sink-stats.json`
- `stream-summary.json` for `smoke`
- `actual_rows.json` for `smoke`
- `source_rows.json` for `smoke`

## Reading The Results

Current smoke runs should be interpreted as:

- benchmark mode: `baseline-pass-through`
- pass means the sink observed the same benchmark-tagged events the emitters
  produced, with no duplicates or unexpected rows
- the result row also records producer-reported totals from the emitter
  and sink `logfwd` diagnostics as extra diagnostics
- scores do not yet include parse-and-enrich overhead

See [RESULT_SCHEMA.md](./RESULT_SCHEMA.md)
for the row contract.

## Octo11y Integration

The GitHub workflow at
[bench-kind-smoke.yml](../../.github/workflows/bench-kind-smoke.yml)
uses this OTLP run file in a Benchkit/Octo11y pipeline:

- `actions/monitor` captures runner telemetry sidecars for persisted runs
- the harness can post the benchmark outcome OTLP payload directly to the
  monitor collector when an OTLP endpoint is provided
- the harness also emits lightweight lifecycle signals for `setup`, `warmup`,
  `measure`, `cooldown`, and final run completion
- the harness writes `benchkit-run.otlp.json`
- `actions/stash` stores the benchmark run on `bench-data`
- [bench-kind-aggregate.yml](../../.github/workflows/bench-kind-aggregate.yml)
  rebuilds the derived views

Pull requests still use the same smoke harness, but only scheduled or manual
runs persist benchmark history to `bench-data`.

Nightly scheduled runs publish a benchmark suite summary (`bench-summary.md`)
with EPS-oriented tables. The nightly workflow upserts that summary into the
`Bench Nightly EPS Report` issue so trend checks stay visible without opening
run artifacts.

The benchmark workflow also supports target EPS sweeps:

- `ladder`: `1, 10, 100, 500, 1k, 10k, 100k, 1m`
- `max`: unbounded generator mode (`eps_per_pod=0`) for
  "fast as the current resource allocation allows"
- high tiers (`10k+`) are treated as capacity probes in CI:
  results are still collected and summarized, but they are non-gating
  because emitter/source capture can saturate before the collector does
