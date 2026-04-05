#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

compose logs --no-color >"$E2E_RESULTS_DIR/compose.log" 2>&1 || true
