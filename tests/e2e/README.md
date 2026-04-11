# E2E Platform

This directory contains the explicit-job e2e platform for `memagent`.

## Layout

- `lib/` shared plumbing only
  - TCP capture server
  - row oracle
  - shell helpers
- `scenarios/<scenario-id>/` self-contained scenarios
  - required: `run_workload.sh`
  - optional: `up.sh`, `verify.sh`, `down.sh`, `collect.sh`
  - scenario-local manifests, compose files, and config

Phases missing from a scenario can be satisfied by family defaults in `lib/common.sh`.
Today:
- compose scenarios default `up`, `down`, `collect`, and `verify`
- otlp scenarios default `up`, `down`, and `collect`

## Design rules

- Every GitHub Actions job maps to one named scenario.
- Scenarios are explicit and can differ substantially from one another.
- Shared code is limited to setup, capture, artifact handling, and row comparison.
- The larger long-running scenario catalog is expected to grow in `memagent-e2e`.

## Standard outputs

Each scenario writes its artifacts under `tests/e2e/results/<scenario-id>/`:

- `expected_rows.json`
- `source_rows.json` when source-side evidence is available
- `actual_rows.json`
- `result.json`
- `summary.md`

`expected_rows.json` is the contract the scenario is trying to prove.

`actual_rows.json` is what the capture sink saw after the forwarder processed the log stream.

`source_rows.json` is optional, but for real infrastructure scenarios it should come from the actual service log or pod log so we can prove both:

- the source emitted the marker events
- the forwarder delivered the expected events
