"""Sirannon-only characterizations that PostgreSQL has no direct equivalent for.

These are framed as characterizations, never as a win over PostgreSQL, because the two systems
do not do the same work here. The change-feed measurement reports the lag from a committed write
to that change arriving at a subscriber over Sirannon's built-in WebSocket feed. PostgreSQL has
no built-in change feed, so the honest comparison would be against PostgreSQL plus an external
Debezium and Kafka pipeline, which is a stack comparison, not an engine one. The number here
describes Sirannon's feed on its own terms.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import websockets

from .core.stats import mean, percentile

_PROBE_TABLE = "cdc_probe"
_SERVER_POLL_INTERVAL_MS = 50


def _ws_url(base_url: str, database_id: str) -> str:
    scheme = "wss" if base_url.startswith("https://") else "ws"
    authority = base_url.split("://", 1)[1].rstrip("/")
    return f"{scheme}://{authority}/db/{database_id}"


async def measure_cdc_latency(
    base_url: str,
    database_id: str,
    samples: int,
    warmup_samples: int,
) -> dict:
    endpoint = f"{base_url.rstrip('/')}/db/{database_id}"
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        await client.post(f"{endpoint}/execute", json={"sql": f"DROP TABLE IF EXISTS {_PROBE_TABLE}"})
        await client.post(
            f"{endpoint}/execute",
            json={"sql": f"CREATE TABLE {_PROBE_TABLE} (id INTEGER PRIMARY KEY, marker INTEGER NOT NULL)"},
        )

        async with websockets.connect(_ws_url(base_url, database_id)) as socket:
            await socket.send(json.dumps({"type": "subscribe", "id": "probe", "table": _PROBE_TABLE}))
            await _await_message(socket, "subscribed")

            latencies_ms: list[float] = []
            loop = asyncio.get_running_loop()
            total = warmup_samples + samples
            for index in range(1, total + 1):
                sent_at = loop.time()
                await client.post(
                    f"{endpoint}/execute",
                    json={"sql": f"INSERT INTO {_PROBE_TABLE} (id, marker) VALUES (?, ?)", "params": [index, index]},
                )
                await _await_change_for(socket, index)
                if index > warmup_samples:
                    latencies_ms.append((loop.time() - sent_at) * 1000.0)

    return {
        "feature": "cdc-latency",
        "description": (
            "Lag from a committed write to the change arriving at a subscriber over Sirannon's "
            "built-in WebSocket change feed. The server polls the change log on a fixed interval, "
            "so the floor is that interval."
        ),
        "samples": len(latencies_ms),
        "server_poll_interval_ms": _SERVER_POLL_INTERVAL_MS,
        "latency_ms": {
            "p50": percentile(latencies_ms, 0.50),
            "p95": percentile(latencies_ms, 0.95),
            "p99": percentile(latencies_ms, 0.99),
            "max": max(latencies_ms) if latencies_ms else 0.0,
            "mean": mean(latencies_ms),
        },
    }


async def _await_message(socket, expected_type: str) -> dict:
    while True:
        message = json.loads(await socket.recv())
        if message.get("type") == "error":
            raise RuntimeError(f"change feed returned an error: {message.get('error')}")
        if message.get("type") == expected_type:
            return message


async def _await_change_for(socket, marker: int) -> dict:
    while True:
        message = await _await_message(socket, "change")
        row = message.get("event", {}).get("row", {})
        if row.get("id") == marker or row.get("marker") == marker:
            return message
