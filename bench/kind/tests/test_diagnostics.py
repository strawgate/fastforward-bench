from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
import sys

LIB_DIR = Path(__file__).resolve().parents[1] / "lib"
if str(LIB_DIR) not in sys.path:
    sys.path.insert(0, str(LIB_DIR))

from diagnostics import analyze_delivery_diagnostics  # noqa: E402


class DeliveryDiagnosticsTest(unittest.TestCase):
    def test_structured_log_rejection_counts_rows_bytes_and_413s(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifacts = Path(tmp)
            (artifacts / "collector-logfwd-logs.txt").write_text(
                json.dumps(
                    {
                        "fields": {
                            "message": "worker_pool: batch rejected",
                            "reason": "HTTP 413: OTLP: payload too large",
                        },
                        "spans": [
                            {
                                "name": "batch",
                                "bytes_in": 12_345_678,
                                "input_rows": 100,
                                "output_rows": 95,
                            }
                        ],
                    }
                )
                + "\n"
                + json.dumps({"fields": {"message": "input.backpressure", "input": "input_0"}})
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "collector-status.json").write_text(
                json.dumps(
                    {
                        "pipelines": [
                            {
                                "batches": {
                                    "dropped_batches_total": 7,
                                }
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            summary = analyze_delivery_diagnostics(artifacts)

        self.assertEqual(summary.rejected_batches_total, 1)
        self.assertEqual(summary.http_413_count, 1)
        self.assertEqual(summary.rejected_rows_estimate, 95)
        self.assertEqual(summary.rejected_bytes_estimate, 12_345_678)
        self.assertEqual(summary.backpressure_warning_count, 1)
        self.assertEqual(summary.collector_dropped_batches_total, 7)

    def test_text_log_fallback_detects_payload_too_large(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifacts = Path(tmp)
            (artifacts / "collector-logs.txt").write_text(
                "worker_pool: batch rejected reason=HTTP 413 bytes_in=1024 input_rows=10\n",
                encoding="utf-8",
            )

            summary = analyze_delivery_diagnostics(artifacts)

        self.assertEqual(summary.rejected_batches_total, 1)
        self.assertEqual(summary.http_413_count, 1)
        self.assertEqual(summary.rejected_rows_estimate, 10)
        self.assertEqual(summary.rejected_bytes_estimate, 1024)


if __name__ == "__main__":
    unittest.main()
