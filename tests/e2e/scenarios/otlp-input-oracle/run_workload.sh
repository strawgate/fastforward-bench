#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

set +e
cargo test -p logfwd-io --test it otlp_receiver_matches_official_otelcol_sender_oracle --manifest-path "$MEMAGENT_REPO_ROOT/Cargo.toml" -- --ignored --exact --nocapture \
    >"$E2E_RESULTS_DIR/test.log" 2>&1
status=$?
set -e
printf '%s\n' "$status" >"$E2E_RESULTS_DIR/test.exitcode"
printf '[]\n' >"$E2E_RESULTS_DIR/expected_rows.json"
