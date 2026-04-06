# memagent-e2e

Explicit-job end-to-end scenario coverage for `memagent`.

This repo owns:

- one workflow per named e2e scenario
- suite workflows that call named scenario workflows
- self-contained scenario folders
- shared artifact/oracle plumbing

This repo does **not** own the `memagent` product code. Workflows check out
`strawgate/memagent` as the system under test and build `logfwd:e2e` from that
checkout.

When `memagent_ref` points at `master` or a full 40-character commit SHA, the
shared setup action first tries to pull a prebuilt image from
`ghcr.io/strawgate/memagent`. It falls back to a local build when no matching
image is available.

## Layout

- `.github/actions/` shared setup and artifact actions
- `.github/workflows/` one workflow per scenario plus suite callers
- `tests/e2e/lib/` shared shell and oracle helpers
- `tests/e2e/scenarios/` one directory per scenario job

## Workflow inputs

Every scenario workflow can be run directly with `workflow_dispatch`.

The suite workflows call those scenario workflows with `workflow_call`, then:

- download each scenario artifact bundle
- render a single markdown and JSON suite summary from `result.json`
- upsert a tracking issue in this repo with the latest suite status

Manual runs accept a `memagent_ref` input. Use a branch, tag, or commit SHA
from `strawgate/memagent`.

## Local usage

1. Clone this repo.
2. Clone `strawgate/memagent` nearby or set `MEMAGENT_REPO_ROOT`.
3. Build the image from `memagent`.
4. Run a named scenario with `tests/e2e/run-scenario.sh <scenario-id>`.
