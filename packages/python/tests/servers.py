from __future__ import annotations

import contextlib
import json
import os
import queue
import shutil
import signal
import socket
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPOSITORY = Path(__file__).resolve().parents[3]
LAUNCHER = REPOSITORY / "packages" / "ts" / "scripts" / "client-test-server.mjs"
READY_TIMEOUT_SECONDS = 90.0
STOP_TIMEOUT_SECONDS = 15.0
RELAY_CHUNK_BYTES = 65536
SQLITE_BUSY_TIMEOUT_SECONDS = 5.0


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@dataclass
class SirannonNode:
    settings: dict[str, Any]
    process: subprocess.Popen[str] | None = None
    port: int = 0
    grpc_port: int | None = None
    output: list[str] = field(default_factory=list)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> SirannonNode:
        node = shutil.which("node")
        if node is None:
            raise RuntimeError("The tests need Node.js on the PATH to start a Sirannon server")
        environment = {key: value for key, value in os.environ.items() if key != "NODE_OPTIONS"}
        environment["SIRANNON_CLIENT_TEST_SERVER"] = json.dumps(self.settings)
        self.process = subprocess.Popen(
            [node, str(LAUNCHER)],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        lines: queue.Queue[str | None] = queue.Queue()
        stream = self.process.stdout
        assert stream is not None

        def pump() -> None:
            for line in stream:
                self.output.append(line)
                lines.put(line)
            lines.put(None)

        threading.Thread(target=pump, daemon=True).start()
        deadline = time.monotonic() + READY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                line = lines.get(timeout=max(deadline - time.monotonic(), 0.01))
            except queue.Empty:
                break
            if line is None:
                break
            if line.startswith("READY "):
                ready = json.loads(line[len("READY ") :])
                self.port = int(ready["port"])
                self.grpc_port = ready.get("grpcPort")
                return self
        self.kill()
        raise RuntimeError("The Sirannon server did not start:\n" + "".join(self.output[-80:]))

    def kill(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            self.process.wait(STOP_TIMEOUT_SECONDS)

    def stop(self) -> None:
        if self.process is None or self.process.poll() is not None:
            return
        self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(STOP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            self.kill()


class TcpRelay:
    def __init__(self, upstream_port: int) -> None:
        self.upstream_port = upstream_port
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = int(self._listener.getsockname()[1])
        self.url = f"http://127.0.0.1:{self.port}"
        self._lock = threading.Lock()
        self._open: list[socket.socket] = []
        self.refusing = False
        self.accepted = 0
        self._closed = False
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while not self._closed:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            with self._lock:
                self.accepted += 1
                refusing = self.refusing
            if refusing:
                client.close()
                continue
            try:
                upstream = socket.create_connection(("127.0.0.1", self.upstream_port))
            except OSError:
                client.close()
                continue
            with self._lock:
                self._open.extend((client, upstream))
            threading.Thread(target=self._pipe, args=(client, upstream), daemon=True).start()
            threading.Thread(target=self._pipe, args=(upstream, client), daemon=True).start()

    def _pipe(self, source: socket.socket, target: socket.socket) -> None:
        try:
            while True:
                chunk = source.recv(RELAY_CHUNK_BYTES)
                if not chunk:
                    break
                target.sendall(chunk)
        except OSError:
            pass
        finally:
            for end in (source, target):
                with contextlib.suppress(OSError):
                    end.shutdown(socket.SHUT_RDWR)

    def cut(self) -> None:
        with self._lock:
            connections, self._open = self._open, []
        for connection in connections:
            with contextlib.suppress(OSError):
                connection.shutdown(socket.SHUT_RDWR)
            connection.close()

    def close(self) -> None:
        self._closed = True
        self._listener.close()
        self.cut()


def read_rows(path: Path, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=SQLITE_BUSY_TIMEOUT_SECONDS)
    try:
        return connection.execute(sql, params).fetchall()
    finally:
        connection.close()


def prune_change_log(path: Path, through_seq: int) -> int:
    connection = sqlite3.connect(path, timeout=SQLITE_BUSY_TIMEOUT_SECONDS)
    try:
        with connection:
            return connection.execute("DELETE FROM _sirannon_changes WHERE seq <= ?", (through_seq,)).rowcount
    finally:
        connection.close()


def wait_until(check: Any, what: str, timeout_seconds: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.02)
    raise AssertionError(f"Timed out waiting for {what}")
