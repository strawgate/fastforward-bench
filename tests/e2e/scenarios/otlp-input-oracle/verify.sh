#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"
source "$REPO_ROOT/tests/e2e/lib/otlp_oracle.sh"

verify_otlp_oracle_result otlp-input-oracle
