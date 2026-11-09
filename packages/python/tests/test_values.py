from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Literal

import httpx
import pytest

from sirannon_client import (
    AsyncSirannonClient,
    ErrorCodes,
    SirannonClient,
    SirannonError,
)
from tests.generated_operations import SharedOperations
from tests.servers import SirannonNode, read_rows

ITEM_BY_ID = SharedOperations.Reads.itemById
PUT_ITEM = SharedOperations.Writes.putItem
INSERT = "INSERT INTO items (id, label, amount, payload) VALUES (?, ?, ?, ?)"
SELECT = "SELECT id, label, amount, payload FROM items WHERE id = ?"
STATEMENT_ROWS = [
    (2**53 + 1, "just past the safe range", 2**63 - 1, bytes(range(256))),
    (-(2**63) + 5, "near the lowest", -(2**63), b"\x00\x00\xff"),
    (7, "small", 2**53, b""),
]
OPERATION_ROWS = [
    (2**62 + 11, "through putItem", -(2**62) - 3, b"\xde\xad\xbe\xef" * 64),
    (2**61 + 1, "eighteen digits", 1, bytes([0])),
]
Transport = Literal["websocket", "http"]
TRANSPORTS: list[Transport] = ["websocket", "http"]


def stored(path: Path) -> list[tuple[Any, ...]]:
    return read_rows(path, "SELECT id, label, amount, payload FROM items ORDER BY id")


def as_tuple(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (row["id"], row["label"], row["amount"], row["payload"])


def raw_post(url: str, body: dict[str, Any]) -> dict[str, Any]:
    answer: dict[str, Any] = json.loads(httpx.post(url, json=body, timeout=10).content)
    return answer


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_sync_client_returns_64_bit_integers_and_blobs_exactly_as_sqlite_stores_them(
    node: SirannonNode, directory: Path, database_id: str, transport: Transport
) -> None:
    with SirannonClient(node.url, transport=transport) as client:
        database = client.database(database_id)
        for row in STATEMENT_ROWS:
            database.execute(INSERT, list(row))
        for item_id, label, amount, payload in OPERATION_ROWS:
            results = database.execute(PUT_ITEM({"id": item_id, "label": label, "amount": amount, "payload": payload}))
            assert [result.changes for result in results] == [1]
        read_by_statement = [as_tuple(database.query(SELECT, [row[0]])[0]) for row in STATEMENT_ROWS]
        read_by_operation = [as_tuple(database.query(ITEM_BY_ID({"id": row[0]}))[0]) for row in OPERATION_ROWS]

    on_disk = stored(directory / f"{database_id}.db")
    assert on_disk == sorted(STATEMENT_ROWS + OPERATION_ROWS)
    assert read_by_statement == STATEMENT_ROWS
    assert read_by_operation == OPERATION_ROWS


@pytest.mark.anyio
@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_async_client_returns_64_bit_integers_and_blobs_exactly_as_sqlite_stores_them(
    node: SirannonNode, directory: Path, database_id: str, transport: Transport
) -> None:
    async with AsyncSirannonClient(node.url, transport=transport) as client:
        database = client.database(database_id)
        await database.transaction([{"sql": INSERT, "params": list(row)} for row in STATEMENT_ROWS])
        for item_id, label, amount, payload in OPERATION_ROWS:
            await database.execute(PUT_ITEM, {"id": item_id, "label": label, "amount": amount, "payload": payload})
        read_back = await database.query("SELECT id, label, amount, payload FROM items ORDER BY id")
        by_operation = [as_tuple((await database.query(ITEM_BY_ID, {"id": row[0]}))[0]) for row in OPERATION_ROWS]

    on_disk = stored(directory / f"{database_id}.db")
    assert on_disk == sorted(STATEMENT_ROWS + OPERATION_ROWS)
    assert [as_tuple(row) for row in read_back] == on_disk
    assert by_operation == OPERATION_ROWS


def test_generated_route_method_decodes_envelopes_in_query_rows(
    node: SirannonNode, directory: Path, database_id: str
) -> None:
    with SirannonClient(node.url) as client:
        client.batch(database_id, {"sql": INSERT, "paramsBatch": [list(row) for row in STATEMENT_ROWS]})
        answer = client.query(database_id, {"sql": "SELECT id, label, amount, payload FROM items ORDER BY id"})

    assert [as_tuple(row) for row in answer.rows] == stored(directory / f"{database_id}.db")


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_refuses_every_statement_before_sending_it_when_the_server_leaves_out_query_sql(
    sql_refusing_node: SirannonNode, database_id: str, transport: Transport
) -> None:
    path = Path(sql_refusing_node.settings["directory"]) / f"{database_id}.db"
    with SirannonClient(sql_refusing_node.url, transport=transport) as client:
        database = client.database(database_id)
        database.execute(PUT_ITEM, {"id": 1, "label": "registered", "amount": 2**60, "payload": b"\x01"})
        refused: list[Callable[[], object]] = [
            lambda: database.execute(INSERT, [2, "raw", 0, b""]),
            lambda: database.query("SELECT 1"),
            lambda: database.transaction([{"sql": INSERT, "params": [3, "raw", 0, b""]}]),
            lambda: database.batch(INSERT, [[4, "raw", 0, b""]]),
            lambda: database.load(INSERT, [[5, "raw", 0, b""]]),
            lambda: database.load_all(INSERT, [[6, "raw", 0, b""]]),
            lambda: client.execute(database_id, {"sql": INSERT, "params": [7, "raw", 0, b""]}),
        ]
        for attempt in refused:
            with pytest.raises(SirannonError) as caught:
                attempt()
            assert caught.value.code == ErrorCodes.SQL_NOT_ACCEPTED
        assert as_tuple(database.query(ITEM_BY_ID, {"id": 1})[0]) == (1, "registered", 2**60, b"\x01")

    assert stored(path) == [(1, "registered", 2**60, b"\x01")]


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_raises_the_server_error_code_unchanged_and_never_resends_a_failed_write(
    node: SirannonNode, directory: Path, database_id: str, transport: Transport
) -> None:
    with SirannonClient(node.url, transport=transport) as client:
        database = client.database(database_id)
        database.execute(INSERT, [1, "first", 10, b"\x01"])
        with pytest.raises(SirannonError) as caught:
            database.execute(INSERT, [1, "second", 20, b"\x02"])
        with pytest.raises(SirannonError) as missing:
            database.query("SELECT * FROM no_such_table")

    oracle = raw_post(f"{node.url}/db/{database_id}/execute", {"sql": INSERT, "params": [1, "third", 30, None]})
    missing_oracle = raw_post(f"{node.url}/db/{database_id}/query", {"sql": "SELECT * FROM no_such_table"})
    assert caught.value.code == oracle["error"]["code"]
    assert caught.value.message == oracle["error"]["message"]
    assert missing.value.code == missing_oracle["error"]["code"]
    assert stored(directory / f"{database_id}.db") == [(1, "first", 10, b"\x01")]
