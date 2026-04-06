# KIND Competitive Benchmark

This directory contains the single-node KIND benchmark harness for `memagent-e2e`.

The benchmark is intentionally separate from the correctness scenarios under
`tests/e2e/`. It measures throughput-oriented Kubernetes collection behavior,
while the scenario suite proves semantic correctness.

## Current Scope

The current implementation covers Phase 0 and Phase 1:

- runtime/tooling decision and default profile selection;
- deterministic KIND cluster lifecycle;
- `logfwd` capture sink deployment;
- canonical run metadata, summary markdown, and artifact collection;
- smoke CI workflow that exercises the infrastructure path.

It does **not** yet deploy the stdout emitter workload or collector-under-test
DaemonSets. Those land in later phases on top of this harness.

## Design Choices

- Harness runtime: Python 3, standard library only.
- Generator and sink direction: `logfwd` is the preferred generator and sink.
- v1 sink transport: OTLP/HTTP into a `logfwd` sink deployment.
- Sink persistence: the sink writes JSON lines to stdout and the harness collects
  pod logs as the benchmark artifact.
- Benchmark artifacts: JSON row, JSONL stream, summary markdown, rendered
  manifests, and kubectl debug output.

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

For the current `infra` phase, these values are recorded in metadata but not yet
driven by a workload deployment.

## Prerequisites

- Docker
- `kind`
- `kubectl`
- Python 3
- a local `logfwd:e2e` image, or a workflow step that builds/tags it first

## Quickstart

```bash
python3 bench/kind/run.py \
  --phase infra \
  --profile smoke \
  --collector logfwd \
  --cluster-name memagent-bench-smoke \
  --memagent-image logfwd:e2e \
  --results-dir bench/kind/results/local-smoke
```

Useful flags:

- `--keep-cluster`
  Leaves the KIND cluster running for inspection.
- `--namespace`
  Overrides the benchmark namespace.
- `--protocol`
  Records the target sink protocol in metadata. Defaults to `otlp_http`.

## Outputs

Each run writes a directory under `bench/kind/results/` containing:

- `result.json`
- `results.jsonl`
- `summary.md`
- `rendered-manifests/`
- `artifacts/`

See [RESULT_SCHEMA.md](/Users/billeaston/Documents/repos/memagent-e2e/bench/kind/RESULT_SCHEMA.md)
for the current row contract.
