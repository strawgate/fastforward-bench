#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

compose down -v --remove-orphans >/dev/null 2>&1 || true
