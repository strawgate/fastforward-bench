from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


HTTP_413_RE = re.compile(r"\bHTTP\s+413\b|\bstatus(?:=|:)\s*413\b|payload too large", re.IGNORECASE)
INPUT_ROWS_RE = re.compile(r"\b(?:input_rows|output_rows)(?:=|:)\s*(\d+)\b")
BYTES_IN_RE = re.compile(r"\bbytes_in(?:=|:)\s*(\d+)\b")


@dataclass
class DeliveryDiagnostics:
    rejected_batches_total: int = 0
    http_413_count: int = 0
    rejected_rows_estimate: int = 0
    rejected_bytes_estimate: int = 0
    backpressure_warning_count: int = 0
    collector_dropped_batches_total: int | None = None

    def to_dict(self) -> dict[str, int | None]:
        return asdict(self)


def _iter_log_files(artifacts_dir: Path) -> list[Path]:
    if not artifacts_dir.exists():
        return []
    return sorted(
        path
        for pattern in ("collector*-logs.txt", "emitter*-logs.txt", "harness.log")
        for path in artifacts_dir.glob(pattern)
        if path.is_file()
    )


def _batch_span(payload: dict[str, Any]) -> dict[str, Any] | None:
    spans = payload.get("spans")
    if not isinstance(spans, list):
        return None
    for span in spans:
        if isinstance(span, dict) and span.get("name") == "batch":
            return span
    return None


def _as_positive_int(value: Any) -> int:
    if isinstance(value, bool) or value is None:
        return 0
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, float):
        return max(0, int(value))
    if isinstance(value, str):
        try:
            return max(0, int(float(value.strip())))
        except ValueError:
            return 0
    return 0


def _merge_status_dropped_batches(summary: DeliveryDiagnostics, artifacts_dir: Path) -> None:
    status_path = artifacts_dir / "collector-status.json"
    if not status_path.exists():
        return
    try:
        payload = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return

    dropped_total = 0
    pipelines = payload.get("pipelines")
    if isinstance(pipelines, list):
        for pipeline in pipelines:
            if not isinstance(pipeline, dict):
                continue
            batches = pipeline.get("batches")
            if isinstance(batches, dict):
                dropped_total += _as_positive_int(batches.get("dropped_batches_total"))

    summary.collector_dropped_batches_total = dropped_total


def _analyze_json_log_line(summary: DeliveryDiagnostics, line: str) -> bool:
    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, dict):
        return False

    fields = payload.get("fields")
    if not isinstance(fields, dict):
        fields = {}
    message = str(fields.get("message") or "")
    reason = str(fields.get("reason") or "")
    combined = f"{message} {reason}"

    if "input.backpressure" in combined:
        summary.backpressure_warning_count += 1

    is_rejected = "batch rejected" in combined
    is_413 = HTTP_413_RE.search(combined) is not None
    if not is_rejected and not is_413:
        return True

    if is_rejected:
        summary.rejected_batches_total += 1
    if is_413:
        summary.http_413_count += 1

    span = _batch_span(payload)
    if span is not None:
        rows = _as_positive_int(span.get("output_rows")) or _as_positive_int(span.get("input_rows"))
        summary.rejected_rows_estimate += rows
        summary.rejected_bytes_estimate += _as_positive_int(span.get("bytes_in"))
    return True


def _analyze_text_log_line(summary: DeliveryDiagnostics, line: str) -> None:
    if "input.backpressure" in line:
        summary.backpressure_warning_count += 1
    is_rejected = "batch rejected" in line
    is_413 = HTTP_413_RE.search(line) is not None
    if is_rejected:
        summary.rejected_batches_total += 1
    if is_413:
        summary.http_413_count += 1
    if not is_rejected and not is_413:
        return
    rows_match = INPUT_ROWS_RE.search(line)
    if rows_match:
        summary.rejected_rows_estimate += int(rows_match.group(1))
    bytes_match = BYTES_IN_RE.search(line)
    if bytes_match:
        summary.rejected_bytes_estimate += int(bytes_match.group(1))


def analyze_delivery_diagnostics(artifacts_dir: Path) -> DeliveryDiagnostics:
    summary = DeliveryDiagnostics()
    for path in _iter_log_files(artifacts_dir):
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line in lines:
            if not line:
                continue
            if not _analyze_json_log_line(summary, line):
                _analyze_text_log_line(summary, line)
    _merge_status_dropped_batches(summary, artifacts_dir)
    return summary
