# memagent-e2e

Explicit-job end-to-end scenario coverage for `memagent`.

This repo owns:

- one workflow per named e2e scenario
- suite workflows that call named scenario workflows
- self-contained scenario folders
- shared artifact/oracle plumbing
- family-level workflow and phase defaults to keep common cases cheap
- the KIND competitive benchmark harness under `bench/kind/`

This repo does **not** own the `memagent` product code. Workflows check out
`strawgate/memagent` as the system under test and build `fastforward:e2e` from that
checkout.

When `memagent_ref` points at `main` or a full
40-character commit SHA, the shared setup action first tries to pull a prebuilt image from
`ghcr.io/strawgate/memagent`. It falls back to a local build when no matching
image is available.

## Layout

- `.github/actions/` shared setup and artifact actions
- `.github/workflows/` one workflow per scenario plus suite callers and reusable family workflows
- `bench/kind/` benchmark harness, manifests, schema notes, and smoke workflow inputs
- `tests/e2e/lib/` shared shell and oracle helpers
- `tests/e2e/scenarios/` one directory per scenario job
- `docs/SCENARIO_PLATFORM.md` platform rules and scenario contract

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

## Maintainability Rules

- Keep named workflows explicit and easy to find in Actions.
- Push reuse below that layer into family workflows and family defaults.
- Prefer deleting copied boilerplate when a family default already covers it.
- Use `python3 tests/e2e/lib/check_scenarios.py --repo-root .` before wiring a new scenario.
