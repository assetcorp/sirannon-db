"""Drive Sirannon through its real HTTP server front-end.

This reproduces exactly what the SirannonClient SDK sends: JSON bodies to
``POST /db/<id>/query``, ``/execute``, and ``/transaction`` with ``?`` placeholders and
positional parameters, no authentication, over a keep-alive connection pool. Charging Sirannon
its real HTTP and JSON cost is the point; that cost is its shipping client path.
"""

from __future__ import annotations

import httpx

from ..core.driver import Driver
from ..core.workloads import SeedTable

_SEED_CHUNK = 500


class SirannonHttpDriver(Driver):
    name = "sirannon"
    delivery = "http"
    dialect = "sqlite"

    def __init__(self, base_url: str, database_id: str, durability: str, max_in_flight: int) -> None:
        self._endpoint = f"{base_url.rstrip('/')}/db/{database_id}"
        self._durability = durability
        self._max_in_flight = max_in_flight
        self._client: httpx.AsyncClient | None = None

    async def connect(self) -> None:
        limits = httpx.Limits(
            max_connections=self._max_in_flight,
            max_keepalive_connections=self._max_in_flight,
        )
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(30.0), limits=limits)
        await self.read("SELECT 1", [])

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Sirannon driver used before connect()")
        return self._client

    async def _post(self, path: str, body: dict) -> dict:
        response = await self._http().post(f"{self._endpoint}{path}", json=body)
        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(f"POST {path} returned {response.status_code}: {response.text}")
        return response.json()

    async def info(self) -> dict:
        # The read pool always opens at synchronous=NORMAL, so a PRAGMA read over HTTP would
        # misreport the writer's durability. The authoritative durability is the requested value,
        # which the server applies to the writer connection; journal_mode is persistent per
        # database, so it reads back accurately from any connection.
        settings: dict[str, str] = {"engine": "sirannon", "delivery": "http", "durability_requested": self._durability}
        journal_rows = (await self._post("/query", {"sql": "PRAGMA journal_mode"})).get("rows") or []
        first = journal_rows[0] if journal_rows else {}
        settings["journal_mode"] = str(next(iter(first.values()), "unknown")) if first else "unknown"
        version_rows = (await self._post("/query", {"sql": "SELECT sqlite_version() AS version"})).get("rows") or []
        settings["version"] = str(version_rows[0].get("version")) if version_rows else "unknown"
        return settings

    async def execute_ddl(self, statements: list[str]) -> None:
        for statement in statements:
            await self._post("/execute", {"sql": statement})

    async def drop_tables(self, tables: list[str]) -> None:
        for table in tables:
            await self._post("/execute", {"sql": f"DROP TABLE IF EXISTS {table}"})

    async def seed(self, tables: list[SeedTable]) -> None:
        for table in tables:
            if not table.rows:
                continue
            insert = self.insert_sql(table)
            for offset in range(0, len(table.rows), _SEED_CHUNK):
                chunk = table.rows[offset : offset + _SEED_CHUNK]
                statements = [{"sql": insert, "params": row} for row in chunk]
                await self._post("/transaction", {"statements": statements})

    async def read(self, sql: str, params: list) -> None:
        await self._post("/query", {"sql": sql, "params": params})

    async def write(self, sql: str, params: list) -> None:
        await self._post("/execute", {"sql": sql, "params": params})

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
