#!/usr/bin/env bash

set -euo pipefail

if [[ -z "${GH_TOKEN:-}" ]]; then
    echo "GH_TOKEN must be set" >&2
    exit 2
fi

if [[ -z "${ISSUE_TITLE:-}" ]]; then
    echo "ISSUE_TITLE must be set" >&2
    exit 2
fi

if [[ -z "${ISSUE_BODY_FILE:-}" ]]; then
    echo "ISSUE_BODY_FILE must be set" >&2
    exit 2
fi

if [[ ! -f "${ISSUE_BODY_FILE}" ]]; then
    echo "Issue body file not found: ${ISSUE_BODY_FILE}" >&2
    exit 2
fi

issue_number="$(
    gh issue list \
        --repo "${GITHUB_REPOSITORY}" \
        --state open \
        --search "${ISSUE_TITLE} in:title" \
        --json number,title \
        --jq ".[] | select(.title == \"${ISSUE_TITLE}\") | .number" \
        | head -n 1
)"

if [[ -n "${issue_number}" ]]; then
    gh issue edit "${issue_number}" --repo "${GITHUB_REPOSITORY}" --title "${ISSUE_TITLE}" --body-file "${ISSUE_BODY_FILE}"
else
    gh issue create --repo "${GITHUB_REPOSITORY}" --title "${ISSUE_TITLE}" --body-file "${ISSUE_BODY_FILE}"
fi
