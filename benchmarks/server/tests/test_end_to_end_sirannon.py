"""Exercise the whole Sirannon path against a faithful mock of the wire protocol.

The mock answers the same endpoints the real server does, so this drives the driver, the load
generator, the harness aggregation, and the result writer end to end without needing Docker, Node,
or a database. It verifies the mechanics; the SQL itself is exercised by the real Docker run.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from sirannon_bench import cli


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        return

    def _send(self, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._send({})
        else:
            self._send({})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {}
        if self.path.endswith("/query"):
            self._send({"rows": [{"id": 1, "name": "Alice", "age": 30}]})
        elif self.path.endswith("/execute"):
            self._send({"changes": 1, "lastInsertRowId": 1})
        elif self.path.endswith("/transaction"):
            statements = body.get("statements", [])
            self._send({"results": [{"changes": 1, "lastInsertRowId": 1} for _ in statements]})
        else:
            self._send({"rows": []})


def _start_server() -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    return server, f"http://127.0.0.1:{port}"


def test_full_sirannon_run_writes_a_result(tmp_path: Path, monkeypatch):
    server, base_url = _start_server()
    try:
        monkeypatch.setenv("BENCH_SIRANNON_URL", base_url)
        monkeypatch.setenv("BENCH_WORKLOADS", "point-select,batch-update")
        monkeypatch.setenv("BENCH_SCALING_WORKLOADS", "point-select")
        monkeypatch.setenv("BENCH_TARGET_RATES", "200")
        monkeypatch.setenv("BENCH_RUNS", "2")
        monkeypatch.setenv("BENCH_WARMUP_SECONDS", "0.1")
        monkeypatch.setenv("BENCH_MEASURE_SECONDS", "0.3")
        monkeypatch.setenv("BENCH_DATA_SIZE", "40")
        monkeypatch.setenv("BENCH_MAX_IN_FLIGHT", "16")

        exit_code = cli.main([
            "--engine",
            "sirannon",
            "--durability",
            "matched",
            "--results-dir",
            str(tmp_path),
            "--run-id",
            "20260101T000000Z",
        ])
        assert exit_code == 0
    finally:
        server.shutdown()

    result_path = tmp_path / "runs" / "20260101T000000Z" / "engine-sirannon-matched.json"
    manifest_path = tmp_path / "runs" / "20260101T000000Z" / "run.json"
    assert result_path.is_file()
    assert manifest_path.is_file()

    report = json.loads(result_path.read_text(encoding="utf-8"))
    assert report["engine"]["name"] == "sirannon"
    assert report["engine"]["delivery"] == "http"
    workloads = {entry["workload"]: entry for entry in report["workloads"]}
    assert set(workloads) == {"point-select", "batch-update"}
    point = workloads["point-select"]["operating_point"]
    assert point["throughput"]["median_ops"] > 0
    assert point["latency_ms"]["p99"] >= 0
    assert workloads["point-select"]["sweep"]
