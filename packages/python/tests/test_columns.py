from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Literal

import pytest

from sirannon_client import AsyncSirannonClient, ErrorCodes, SirannonClient, SirannonError
from tests.servers import SirannonNode, read_rows

INSERT = "INSERT INTO items (id, label, amount, payload) VALUES (?, ?, ?, ?)"
REPEATED = (
    'SELECT a.id, b.id, a.amount, a.payload, a.label AS "1", a.amount + 0 AS total '
    "FROM items a JOIN items b ON a.id = b.id ORDER BY a.id"
)
ENDLESS = "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) SELECT i, zeroblob(65536) FROM n"
ROWS = [
    (2**53 + 1, "héllo", 2**63 - 1, bytes(range(256))),
    (-(2**63) + 5, "second", -(2**63), b"\x00\xff"),
]
Transport = Literal["websocket", "http"]
TRANSPORTS: list[Transport] = ["websocket", "http"]


def oracle_row_bytes(path: Path) -> list[int]:
    sizes = read_rows(
        path,
        "SELECT 32 + coalesce(length(CAST(label AS BLOB)), 0) + coalesce(length(payload), 0) FROM items ORDER BY id",
    )
    return [size for (size,) in sizes]


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_sync_columns_read_keeps_repeated_names_and_exact_values_in_column_order(
    node: SirannonNode, directory: Path, database_id: str, transport: Transport
) -> None:
    with SirannonClient(node.url, transport=transport) as client:
        database = client.database(database_id)
        for row in ROWS:
            database.execute(INSERT, list(row))
        answer = database.query_with_columns(REPEATED)

    on_disk = read_rows(directory / f"{database_id}.db", REPEATED)
    assert [column.name for column in answer.columns] == ["id", "id", "amount", "payload", "1", "total"]
    first = answer.columns[0]
    assert (first.declared_type, first.table, first.column, first.database) == ("INTEGER", "items", "id", "main")
    expression = answer.columns[5]
    assert (expression.declared_type, expression.table, expression.column, expression.database) == (None,) * 4
    assert [tuple(row) for row in answer.rows] == on_disk
    assert answer.truncated is None


@pytest.mark.anyio
@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_async_columns_read_stops_at_every_limit_that_a_hostile_read_reaches(
    node: SirannonNode, directory: Path, database_id: str, transport: Transport
) -> None:
    async with AsyncSirannonClient(node.url, transport=transport) as client:
        database = client.database(database_id)
        for position in range(5):
            await database.execute(INSERT, [position + 1, "x" * (100 * (position + 1)), position, b"\xab" * 4096])
        sizes = oracle_row_bytes(directory / f"{database_id}.db")
        budget = sum(sizes[:3])
        select = "SELECT id, label, amount, payload FROM items ORDER BY id"

        short = await database.query_with_columns(select, max_bytes=budget - 1)
        exact = await database.query_with_columns(select, max_bytes=budget)
        every = await database.query_with_columns(select, max_rows=5)
        none = await database.query_with_columns(select, max_rows=0)
        endless_bytes = await database.query_with_columns(ENDLESS, max_bytes=1 << 20)
        endless_rows = await database.query_with_columns(ENDLESS, max_rows=3)

    assert (len(short.rows), short.truncated) == (2, "bytes")
    assert (len(exact.rows), exact.truncated) == (3, "bytes")
    assert (len(every.rows), every.truncated) == (5, None)
    assert (len(none.rows), len(none.columns), none.truncated) == (0, 4, "rows")
    assert (len(endless_bytes.rows), endless_bytes.truncated) == (15, "bytes")
    assert ([row[0] for row in endless_rows.rows], endless_rows.truncated) == ([1, 2, 3], "rows")


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize("limit", [-1, 2**53, True, 1.5, "10"])
def test_columns_read_refuses_a_bad_limit_before_sending_it(
    node: SirannonNode, database_id: str, transport: Transport, limit: Any
) -> None:
    with SirannonClient(node.url, transport=transport) as client:
        database = client.database(database_id)
        with pytest.raises(SirannonError) as refused:
            database.query_with_columns("SELECT 1", max_rows=limit)
        assert refused.value.code == ErrorCodes.INVALID_ARGUMENT
        with pytest.raises(SirannonError) as refused:
            database.query_with_columns("SELECT 1", max_bytes=limit)
        assert refused.value.code == ErrorCodes.INVALID_ARGUMENT


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize(
    "statement",
    ["SELECT * FROM '_sirannon_meta'", "SELECT * FROM `_sirannon_meta`", "SELECT * FROM main.'_sirannon_meta'"],
)
def test_columns_read_and_query_refuse_a_quoted_reserved_table(
    node: SirannonNode, database_id: str, transport: Transport, statement: str
) -> None:
    with SirannonClient(node.url, transport=transport) as client:
        database = client.database(database_id)
        with pytest.raises(SirannonError) as refused:
            database.query_with_columns(statement)
        assert refused.value.code == "FORBIDDEN_SQL"

        with pytest.raises(SirannonError) as refused:
            database.query(statement)
        assert refused.value.code == "FORBIDDEN_SQL"


def test_columns_read_requires_an_explicit_truncation_from_the_server() -> None:
    payload: dict[str, Any] = {"columns": [], "rows": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"capabilities":["query.sql"]}')

        def do_POST(self) -> None:
            self.rfile.read(int(self.headers.get("content-length", "0")))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode())

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        with SirannonClient(f"http://127.0.0.1:{server.server_port}", transport="http") as client:
            database = client.database("columns")
            with pytest.raises(SirannonError) as refused:
                database.query_with_columns("SELECT 1 WHERE 0")
            assert refused.value.code == ErrorCodes.INVALID_RESPONSE
            payload["truncated"] = None
            answer = database.query_with_columns("SELECT 1 WHERE 0")
            assert answer.rows == []
            assert answer.truncated is None
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
