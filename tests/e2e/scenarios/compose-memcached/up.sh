#!/usr/bin/env bash

source "$(cd "$(dirname "$0")/../.." && pwd)/lib/common.sh"

compose up -d --wait --remove-orphans
sleep 2
