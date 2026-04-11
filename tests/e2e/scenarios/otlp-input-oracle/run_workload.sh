#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"
source "$REPO_ROOT/tests/e2e/lib/otlp_oracle.sh"

run_otlp_oracle_test otlp_receiver_matches_official_otelcol_sender_oracle
