# E2E Platform

This directory contains the explicit-job e2e platform for `memagent`.

## Layout

- `lib/` shared plumbing only
  - HTTP capture server
  - row oracle
  - shell helpers
- `scenarios/<scenario-id>/` self-contained scenarios
  - `up.sh`
  - `run_workload.sh`
  - `verify.sh`
  - `down.sh`
  - scenario-local manifests, compose files, and config

## Design rules

- Every GitHub Actions job maps to one named scenario.
- Scenarios are explicit and can differ substantially from one another.
- Shared code is limited to setup, capture, artifact handling, and row comparison.
- The larger long-running scenario catalog is expected to grow in `memagent-e2e`.

## Standard outputs

Each scenario writes its artifacts under `tests/e2e/results/<scenario-id>/`:

- `expected_rows.json`
- `actual_rows.json`
- `result.json`
- `summary.md`
