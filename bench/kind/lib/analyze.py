from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
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


@dataclass(frozen=True)
class RowSummary:
    row_count: int
    sources: list[str]
    duplicate_event_count: int
    gap_count: int
    event_ids: set[str]


@dataclass(frozen=True)
class SourceSinkComparison:
    source_row_count: int
    sink_row_count: int
    missing_source_count: int
    missing_event_count: int
    unexpected_event_count: int
    duplicate_event_count: int
    gap_count: int
    observed_sources: list[str]
    expected_sources: list[str]
    missing_sources: list[str]


def summarize_rows(rows: list[dict[str, object]]) -> RowSummary:
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
    return RowSummary(
        row_count=len(rows),
        sources=sources,
        duplicate_event_count=duplicate_count,
        gap_count=gap_count,
        event_ids=set(event_ids),
    )


def compare_source_and_sink(
    *,
    source_rows: list[dict[str, object]],
    sink_rows: list[dict[str, object]],
    expected_sources: list[str],
) -> SourceSinkComparison:
    source_summary = summarize_rows(source_rows)
    sink_summary = summarize_rows(sink_rows)
    missing_sources = sorted(set(expected_sources) - set(sink_summary.sources))
    missing_events = source_summary.event_ids - sink_summary.event_ids
    unexpected_events = sink_summary.event_ids - source_summary.event_ids
    return SourceSinkComparison(
        source_row_count=source_summary.row_count,
        sink_row_count=sink_summary.row_count,
        missing_source_count=len(missing_sources),
        missing_event_count=len(missing_events),
        unexpected_event_count=len(unexpected_events),
        duplicate_event_count=sink_summary.duplicate_event_count,
        gap_count=sink_summary.gap_count,
        observed_sources=sink_summary.sources,
        expected_sources=expected_sources,
        missing_sources=missing_sources,
    )
