#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture NDJSON HTTP payloads for e2e tests.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


class State:
    def __init__(self, capture_path: str) -> None:
        self.capture_path = capture_path
        self.lock = threading.Lock()
        self.requests = 0
        self.lines = 0
        self.bytes = 0

    def append(self, raw_body: bytes) -> int:
        decoded = raw_body.decode("utf-8", errors="replace")
        parsed_lines: list[str] = []
        for raw_line in decoded.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                obj = {"_raw_body": line}
            parsed_lines.append(json.dumps(obj, sort_keys=True))

        if not parsed_lines and decoded.strip():
            try:
                obj = json.loads(decoded)
            except json.JSONDecodeError:
                obj = {"_raw_body": decoded.strip()}
            parsed_lines.append(json.dumps(obj, sort_keys=True))

        with self.lock:
            self.requests += 1
            self.bytes += len(raw_body)
            self.lines += len(parsed_lines)
            if parsed_lines:
                with open(self.capture_path, "a", encoding="utf-8") as handle:
                    for line in parsed_lines:
                        handle.write(line)
                        handle.write("\n")
        return len(parsed_lines)

    def stats(self) -> dict[str, int]:
        with self.lock:
            return {
                "requests": self.requests,
                "lines": self.lines,
                "bytes": self.bytes,
            }

    def dump(self) -> str:
        if not os.path.exists(self.capture_path):
            return ""
        with open(self.capture_path, "r", encoding="utf-8") as handle:
            return handle.read()


def make_handler(state: State):
    class CaptureHandler(BaseHTTPRequestHandler):
        def _send_json(self, payload: dict[str, object], status: int = 200) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self._send_json({"ok": True})
                return
            if self.path == "/stats":
                self._send_json(state.stats())
                return
            if self.path == "/dump":
                body = state.dump().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self._send_json({"error": "not found"}, status=404)

        def do_POST(self) -> None:  # noqa: N802
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length)
            lines = state.append(body)
            self._send_json({"ok": True, "accepted_lines": lines})

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    return CaptureHandler


def main() -> None:
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)
    capture_path = os.path.join(args.output_dir, "captured.ndjson")
    state = State(capture_path)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(state))
    print(
        f"capture-http listening on {args.host}:{args.port}, writing to {capture_path}",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
