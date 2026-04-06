#!/usr/bin/env bash

set -euo pipefail

if [[ -z "${REPO_ROOT:-}" ]]; then
    REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
fi

if [[ -z "${MEMAGENT_REPO_ROOT:-}" ]]; then
    MEMAGENT_REPO_ROOT="$REPO_ROOT"
fi

if [[ -z "${SCENARIO_DIR:-}" && -n "${SCENARIO_ID:-}" ]]; then
    SCENARIO_DIR="$REPO_ROOT/tests/e2e/scenarios/$SCENARIO_ID"
fi

if [[ -z "${SCENARIO_ID:-}" ]]; then
    SCENARIO_ID="$(basename "${SCENARIO_DIR:-$(pwd)}")"
fi

if [[ -z "${SCENARIO_DIR:-}" ]]; then
    SCENARIO_DIR="$REPO_ROOT/tests/e2e/scenarios/$SCENARIO_ID"
fi

if [[ -z "${E2E_RESULTS_DIR:-}" ]]; then
    E2E_RESULTS_DIR="$REPO_ROOT/tests/e2e/results/$SCENARIO_ID"
fi

export REPO_ROOT
export MEMAGENT_REPO_ROOT
export SCENARIO_ID
export SCENARIO_DIR
export E2E_RESULTS_DIR
export E2E_LOG_DIR="${E2E_RESULTS_DIR}/logs"

mkdir -p "$E2E_RESULTS_DIR" "$E2E_LOG_DIR"

detect_scenario_family() {
    case "$SCENARIO_ID" in
        compose-*) echo "compose" ;;
        kind-*) echo "kind" ;;
        otlp-*) echo "otlp" ;;
        *) echo "custom" ;;
    esac
}

export SCENARIO_FAMILY="${SCENARIO_FAMILY:-$(detect_scenario_family)}"

compose() {
    docker compose -p "memagent-${SCENARIO_ID}" -f "$SCENARIO_DIR/compose.yaml" "$@"
}

wait_for_http() {
    local url="$1"
    local timeout="${2:-30}"
    local deadline=$((SECONDS + timeout))
    while (( SECONDS < deadline )); do
        if curl --connect-timeout 1 --max-time 2 -fsS "$url" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "Timed out waiting for HTTP endpoint: $url" >&2
    return 1
}

wait_for_file() {
    local path="$1"
    local timeout="${2:-30}"
    local deadline=$((SECONDS + timeout))
    while (( SECONDS < deadline )); do
        if [[ -s "$path" ]]; then
            return 0
        fi
        sleep 1
    done
    echo "Timed out waiting for file: $path" >&2
    return 1
}

append_job_summary() {
    local summary_file="${1:-$E2E_RESULTS_DIR/summary.md}"
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" && -f "$summary_file" ]]; then
        cat "$summary_file" >>"$GITHUB_STEP_SUMMARY"
    fi
}

run_oracle_verify() {
    local actual_ndjson="${1:-$E2E_RESULTS_DIR/captured.ndjson}"
    wait_for_file "$actual_ndjson" 30
    python3 "$REPO_ROOT/tests/e2e/lib/oracle.py" \
        --config "$SCENARIO_DIR/oracle.json" \
        --expected "$E2E_RESULTS_DIR/expected_rows.json" \
        --actual-ndjson "$actual_ndjson" \
        --results-dir "$E2E_RESULTS_DIR"
}

run_default_phase() {
    local phase="$1"

    case "${SCENARIO_FAMILY}:${phase}" in
        compose:up)
            compose up -d --wait --remove-orphans
            ;;
        compose:down)
            compose down -v --remove-orphans >/dev/null 2>&1 || true
            ;;
        compose:collect)
            compose logs --no-color >"$E2E_RESULTS_DIR/compose.log" 2>&1 || true
            ;;
        compose:verify)
            run_oracle_verify
            ;;
        *)
            echo "No default implementation for phase '${phase}' in family '${SCENARIO_FAMILY}'" >&2
            return 1
            ;;
    esac
}
