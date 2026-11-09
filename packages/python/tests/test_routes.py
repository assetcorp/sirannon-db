from __future__ import annotations

import inspect
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from sirannon_client import AsyncSirannonClient, ErrorCodes, SirannonClient, SirannonError
from sirannon_client._async_operations import AsyncOperations
from sirannon_client._operations import Operations
from tests.conftest import ACCEPTED_BEARER, FORBIDDEN_BEARER
from tests.servers import SirannonNode, read_rows

INSERT = "INSERT INTO items (id, label, amount, payload) VALUES (?, ?, ?, ?)"
SELECT_ALL = {"sql": "SELECT id, label, amount, payload FROM items ORDER BY id"}
BIG = -(2**62) - 3
BLOB = bytes([0, 1, 0xFE, 0xFF])
EXPECTED_ROWS = [
    (1, "execute", BIG, BLOB),
    (2, "transaction", BIG + 1, BLOB),
    (3, "batch", BIG + 2, BLOB),
    (4, "load", BIG + 3, BLOB),
    (5, "operation", BIG + 4, BLOB),
]
ITEM_COLUMNS = ("id", "label", "amount", "payload")
CLIENT_ORIGINATED_CODES = frozenset(
    value for name, value in vars(ErrorCodes).items() if name.isupper() and value != ErrorCodes.SQL_NOT_ACCEPTED
)
AUTHORIZED = {"Authorization": f"Bearer {ACCEPTED_BEARER}"}

RouteCall = tuple[str, tuple[Any, ...], bool]


def writes_and_routes(database_id: str) -> list[RouteCall]:
    sync_batch = {"sourceNodeId": "device", "batchId": "b1", "fromSeq": "1", "toSeq": "1", "checksum": "bad"}
    operation_args = dict(zip(ITEM_COLUMNS, EXPECTED_ROWS[4], strict=True))
    return [
        ("get_capabilities", (), True),
        ("get_health", (), True),
        ("get_readiness", (), False),
        ("execute", (database_id, {"sql": INSERT, "params": list(EXPECTED_ROWS[0])}), True),
        ("transaction", (database_id, {"statements": [{"sql": INSERT, "params": list(EXPECTED_ROWS[1])}]}), True),
        ("batch", (database_id, {"sql": INSERT, "paramsBatch": [list(EXPECTED_ROWS[2])]}), True),
        ("load", (database_id, {"sql": INSERT, "paramsBatch": [list(EXPECTED_ROWS[3])]}), True),
        ("execute_operation", (database_id, "putItem", {"args": operation_args}), True),
        ("query", (database_id, SELECT_ALL), True),
        ("query_operation", (database_id, "itemById", {"args": {"id": 5}}), True),
        ("get_snapshot_page", (database_id, {"table": "items"}), False),
        ("get_cluster_status", (database_id,), False),
        ("list_migrations", (database_id, {}), False),
        ("get_snapshot_manifest", (database_id,), False),
        ("apply_changes", (database_id, {"batch": sync_batch}), False),
        ("get_backup_status", (database_id,), False),
        ("trigger_backup", (database_id,), False),
        ("list_backup_chains", (database_id,), False),
        ("verify_backup", (database_id, {"name": "no-such-backup"}), False),
        ("list_backups_safe_to_delete", (database_id,), False),
        ("get_backup_restore_status", (database_id,), False),
        ("get_encryption_status", (database_id,), False),
        ("change_encryption", (database_id, {"keyRef": "missing-key"}), False),
    ]


def restore_call(database_id: str) -> RouteCall:
    return ("restore_backup", (database_id, {"moment": int(time.time() * 1000)}), False)


def assert_server_answer(name: str, error: SirannonError) -> None:
    assert error.status is not None and error.status >= 400, f"{name} raised {error!r} with no failing HTTP status"
    assert error.code not in CLIENT_ORIGINATED_CODES, f"{name} raised {error!r}, which the client made up"
    for credential in (ACCEPTED_BEARER, FORBIDDEN_BEARER):
        assert credential not in str(error) and credential not in repr(error.details), f"{name} leaked a credential"


def route_names(operations: type) -> set[str]:
    return {name for name, _ in inspect.getmembers(operations, inspect.isfunction) if not name.startswith("_")}


def item_rows(rows: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    return [tuple(row[column] for column in ITEM_COLUMNS) for row in rows]


def assert_answers_match_sqlite(node: SirannonNode, database_id: str, answers: dict[str, Any]) -> None:
    stored = read_rows(
        Path(node.settings["directory"]) / f"{database_id}.db",
        "SELECT id, label, amount, payload FROM items ORDER BY id",
    )
    assert stored == EXPECTED_ROWS
    assert "query.sql" in answers["get_capabilities"].capabilities
    assert answers["get_capabilities"].registry is not None and answers["get_capabilities"].registry.digest != ""
    assert item_rows(answers["query"].rows) == EXPECTED_ROWS
    assert item_rows(answers["query_operation"].rows) == [EXPECTED_ROWS[4]]
    page = answers["get_snapshot_page"]
    assert page is None or len(page.rows) == len(EXPECTED_ROWS)


def settled(name: str, succeeds: bool, error: SirannonError) -> None:
    if succeeds:
        raise error
    assert_server_answer(name, error)


def call_route(client: Any, call: RouteCall) -> Any:
    name, args, succeeds = call
    method: Callable[..., Any] = getattr(client, name)
    try:
        return method(*args)
    except SirannonError as error:
        settled(name, succeeds, error)
        return None


async def await_route(client: Any, call: RouteCall) -> Any:
    name, args, succeeds = call
    method: Callable[..., Any] = getattr(client, name)
    try:
        return await method(*args)
    except SirannonError as error:
        settled(name, succeeds, error)
        return None


def test_every_generated_sync_route_reaches_the_server(secured_node: SirannonNode, database_id: str) -> None:
    calls = writes_and_routes(database_id)
    with SirannonClient(secured_node.url, transport="http", headers=AUTHORIZED) as client:
        answers = {call[0]: call_route(client, call) for call in calls}
        assert_answers_match_sqlite(secured_node, database_id, answers)
        call_route(client, restore_call(database_id))
    untested = route_names(Operations) - {call[0] for call in calls} - {"restore_backup"}
    assert not untested, f"untested routes: {untested}"


@pytest.mark.anyio
async def test_every_generated_async_route_reaches_the_server(secured_node: SirannonNode, database_id: str) -> None:
    calls = writes_and_routes(database_id)
    async with AsyncSirannonClient(secured_node.url, transport="http", headers=AUTHORIZED) as client:
        answers = {call[0]: await await_route(client, call) for call in calls}
        assert_answers_match_sqlite(secured_node, database_id, answers)
        await await_route(client, restore_call(database_id))
    untested = route_names(AsyncOperations) - {call[0] for call in calls} - {"restore_backup"}
    assert not untested, f"untested routes: {untested}"
