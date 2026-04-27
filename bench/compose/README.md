# Compose Competitive Benchmark

This directory contains the non-KIND benchmark harness for `memagent-e2e`.

## Purpose

The KIND harness is our primary Kubernetes competitive benchmark lane. This
compose harness gives us a lower-overhead non-KIND lane that still exercises
the same collector adapters and output contracts for throughput checks.

## Design

- Reuses the same `fastforward:e2e` image for:
  - benchmark generator
  - benchmark sink
  - `fastforward` collector adapter
- Uses collector-specific images for competitor adapters:
  - `otel/opentelemetry-collector-contrib`
  - `docker.elastic.co/beats/filebeat`
  - `timberio/vector`
- Writes benchmark artifacts into the same result contract used by the KIND
  harness (`result.json`, `results.jsonl`, `summary.md`, `benchkit-run.otlp.json`).

## Current Scope

- phase: `smoke`
- ingest modes:
  - `file` for `fastforward`, `otelcol`, `filebeat`, `vector`
  - `otlp` for `fastforward`, `otelcol`
- collectors: `fastforward`, `otelcol`, `filebeat`, `vector`
- profiles: `quick`, `smoke`, `default`
- CPU profiles: `single`, `multi`

## Usage

```bash
python3 bench/compose/run.py \
  --collector fastforward \
  --ingest-mode file \
  --profile smoke \
  --cpu-profile single \
  --eps-per-sec 0 \
  --results-dir bench/compose/results/local/fastforward-max
```

- Set `--eps-per-sec 0` for max-throughput mode.
- Set `--ingest-mode otlp` for OTLP ingest parity (`fastforward` and `otelcol`).
- In CI, `benchkit-run.otlp.json` is stashed by Octo11y for aggregate reporting.
