from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


def load_json_lines(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def benchmark_rows(rows: list[dict[str, object]], benchmark_id: str) -> list[dict[str, object]]:
    return [row for row in rows if row.get("benchmark_id") == benchmark_id]


def expected_emitter_pods(statefulset_name: str, replicas: int) -> list[str]:
    return [f"{statefulset_name}-{index}" for index in range(replicas)]


def summarize_rows(rows: list[dict[str, object]]) -> dict[str, object]:
    sources = sorted({str(row.get("pod_name")) for row in rows if row.get("pod_name")})
    event_ids = [str(row.get("event_id")) for row in rows if row.get("event_id")]
    event_counts = Counter(event_ids)
    duplicate_count = sum(count - 1 for count in event_counts.values() if count > 1)
    gap_count = 0
    by_source: dict[str, list[int]] = {}
    for row in rows:
        pod_name = row.get("pod_name")
        seq = row.get("seq")
        if isinstance(pod_name, str) and isinstance(seq, int):
            by_source.setdefault(pod_name, []).append(seq)
    for seqs in by_source.values():
        if not seqs:
            continue
        ordered = sorted(set(seqs))
        gap_count += max(0, (ordered[-1] - ordered[0] + 1) - len(ordered))
    return {
        "row_count": len(rows),
        "sources": sources,
        "duplicate_event_count": duplicate_count,
        "gap_count": gap_count,
    }
