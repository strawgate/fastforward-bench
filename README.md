# memagent-e2e

Explicit-job end-to-end scenario coverage for `memagent`.

This repo owns:

- GitHub Actions workflows for named e2e jobs
- self-contained scenario folders
- shared artifact/oracle plumbing

This repo does **not** own the `memagent` product code. Workflows check out
`strawgate/memagent` as the system under test and build `logfwd:e2e` from that
checkout.

## Layout

- `.github/actions/` shared setup and artifact actions
- `.github/workflows/` explicit PR/nightly workflows
- `tests/e2e/lib/` shared shell and oracle helpers
- `tests/e2e/scenarios/` one directory per scenario job

## Workflow inputs

Manual runs accept a `memagent_ref` input. Use a branch, tag, or commit SHA
from `strawgate/memagent`.

## Local usage

1. Clone this repo.
2. Clone `strawgate/memagent` nearby or set `MEMAGENT_REPO_ROOT`.
3. Build the image from `memagent`.
4. Run a named scenario with `tests/e2e/run-scenario.sh <scenario-id>`.
