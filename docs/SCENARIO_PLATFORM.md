# Scenario Platform

`memagent-e2e` is intentionally split into two layers:

- explicit named scenario workflows that are easy to find in GitHub Actions
- shared family defaults underneath them so common cases stay cheap to maintain

## Design Rules

- Every scenario keeps its own directory under `tests/e2e/scenarios/<scenario-id>/`.
- Every scenario keeps its own named workflow under `.github/workflows/e2e-<scenario-id>.yml`.
- Suite workflows call named scenario workflows. They do not expand a matrix.
- Family-level reuse is allowed only below the named workflow layer.
- Scenario-specific behavior always wins over shared defaults.

## Scenario Contract

All scenarios run through `tests/e2e/run-scenario.sh` and expose the same thin phases:

- `up`
- `run_workload`
- `verify`
- `down`

`collect` is also supported as an optional artifact-gathering phase run from the cleanup trap.

Each phase can be implemented in one of two ways:

- an explicit `<phase>.sh` file inside the scenario directory
- a documented family default supplied by `tests/e2e/lib/common.sh`

Today only the `compose` family has defaults for:

- `up`
- `down`
- `collect`
- `verify`

That means a normal compose scenario only needs:

- `compose.yaml`
- `memagent.yaml`
- `oracle.json`
- `run_workload.sh`

Everything else is optional unless the scenario needs custom behavior.

## Workflow Families

Named workflows stay explicit, but they delegate to a reusable workflow for their family:

- `_scenario-compose.yml`
- `_scenario-kind.yml`
- `_scenario-otlp.yml`

This keeps the Actions UI easy to scan while removing copy-paste from checkout, artifact upload, and setup logic.

## Oracle Contract

Scenarios that use the default capture-oracle path emit:

- `expected_rows.json`
- `actual_rows.json`
- `result.json`
- `summary.md`

`oracle.json` defines:

- the comparison policy
- the selector used to isolate scenario rows
- the ordered comparison keys
- the stable identity keys for duplicate and missing-event reporting
- the required non-null fields that must survive the forwarder

## V2 Event Contract

For forwarder correctness scenarios, expected and actual rows should converge on a shared shape:

- `scenario`
- `source_id`
- `event_id`
- `seq`

Then add scenario-specific semantic fields such as:

- `message`
- `level`
- `command`
- `key`
- `value`
- `path`
- `status`

`event_id` is the canonical per-event identity used to detect:

- missing events
- duplicate events
- out-of-order delivery within a source stream

`source_id` identifies the emitting stream, so order checks can be evaluated per source instead of globally when needed.

## Validation

Run the platform validator before adding or wiring scenarios:

```bash
python3 tests/e2e/lib/check_scenarios.py --repo-root .
```

It checks:

- required files per family
- whether a missing phase is covered by a family default
- whether each scenario has a matching named workflow

## Adding A Compose Scenario

1. Copy a nearby `compose-*` scenario directory.
2. Keep only files that are truly scenario-specific.
3. Prefer family defaults instead of re-copying identical `up.sh`, `down.sh`, `collect.sh`, or `verify.sh`.
4. Add the named workflow wrapper `e2e-<scenario-id>.yml`.
5. Wire that named workflow into `e2e-smoke.yml` or `e2e-nightly.yml` when appropriate.
6. Run the validator and then run the scenario directly.
