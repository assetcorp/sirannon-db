from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from sirannon_client import (
    AsyncSirannonClient,
    ChangeEvent,
    ErrorCodes,
    LiveUpdate,
    SirannonClient,
    SirannonError,
)
from tests.conftest import FORBIDDEN_BEARER
from tests.generated_operations import SharedItemsInOrderRow, SharedOperations
from tests.servers import SirannonNode, TcpRelay, prune_change_log, read_rows, wait_until

INSERT = "INSERT INTO items (id, label, amount, payload) VALUES (?, ?, ?, ?)"
ITEMS_IN_ORDER = SharedOperations.Reads.itemsInOrder
RECONNECT_INTERVAL_MS = 50
WRITES_BEFORE_CUT = 40
WRITES_AFTER_CUT = 60
TOTAL_WRITES = WRITES_BEFORE_CUT + WRITES_AFTER_CUT
CUT_EVERY = 20
SETTLE_SECONDS = 0.3
REFUSED_RECONNECT_WINDOW_SECONDS = 1.0
SEEDED_ROWS = 400
PUT_ITEM = SharedOperations.Writes.putItem


def item(index: int) -> list[Any]:
    return [2**60 + index, f"item-{index}", -(2**62) + index, bytes([index % 256]) * (index % 7)]


def change_log(path: Path) -> list[int]:
    rows = read_rows(path, "SELECT seq FROM _sirannon_changes WHERE table_name = 'items' ORDER BY seq")
    return [row[0] for row in rows]


def stored(path: Path) -> list[tuple[Any, ...]]:
    return read_rows(path, "SELECT id, label, amount, payload FROM items ORDER BY id")


def as_tuple(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (row["id"], row["label"], row["amount"], row["payload"])


def write_with_cuts(url: str, database_id: str, relay: TcpRelay) -> None:
    with SirannonClient(url, transport="http") as writer:
        database = writer.database(database_id)
        for index in range(TOTAL_WRITES):
            if index >= WRITES_BEFORE_CUT and index % CUT_EVERY == 0:
                relay.cut()
            database.execute(INSERT, item(index))


def test_sync_subscription_resumes_after_dropped_connections_with_no_missed_or_repeated_change(
    node: SirannonNode, directory: Path, relay: TcpRelay, database_id: str
) -> None:
    received: list[ChangeEvent] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def collect(event: ChangeEvent) -> None:
        with lock:
            received.append(event)

    with SirannonClient(relay.url, reconnect_interval_ms=RECONNECT_INTERVAL_MS) as client:
        client.database(database_id).on("items").subscribe(collect, on_error=errors.append)
        write_with_cuts(node.url, database_id, relay)
        wait_until(lambda: len(received) >= TOTAL_WRITES, "every change after the cuts")
        time.sleep(SETTLE_SECONDS)

    path = directory / f"{database_id}.db"
    assert errors == []
    assert relay.accepted > 1
    assert [event.seq for event in received] == change_log(path)
    assert [as_tuple(event.row) for event in received] == stored(path)


@pytest.mark.anyio
async def test_async_subscriptions_resume_after_dropped_connections_with_no_missed_or_repeated_change(
    node: SirannonNode, directory: Path, relay: TcpRelay, database_id: str
) -> None:
    everything: list[ChangeEvent] = []
    filtered: list[ChangeEvent] = []
    errors: list[BaseException] = []

    async def collect(event: ChangeEvent) -> None:
        everything.append(event)

    async with AsyncSirannonClient(relay.url, reconnect_interval_ms=RECONNECT_INTERVAL_MS) as client:
        database = client.database(database_id)
        await database.on("items").subscribe(collect, on_error=errors.append)
        await database.on("items").filter({"label": "item-77"}).subscribe(filtered.append, on_error=errors.append)
        await asyncio.to_thread(write_with_cuts, node.url, database_id, relay)
        await asyncio.to_thread(wait_until, lambda: len(everything) >= TOTAL_WRITES, "every change after the cuts")
        await asyncio.sleep(SETTLE_SECONDS)

    path = directory / f"{database_id}.db"
    assert errors == []
    assert relay.accepted > 1
    assert [event.seq for event in everything] == change_log(path)
    assert [as_tuple(event.row) for event in everything] == stored(path)
    assert [as_tuple(event.row) for event in filtered] == [row for row in stored(path) if row[1] == "item-77"]


@pytest.mark.anyio
async def test_calls_on_reset_when_the_change_log_no_longer_holds_what_followed_the_cursor(
    node: SirannonNode, directory: Path, relay: TcpRelay, database_id: str
) -> None:
    received: list[ChangeEvent] = []
    resets: list[bool] = []
    path = directory / f"{database_id}.db"

    async with (
        AsyncSirannonClient(node.url, transport="http") as writer,
        AsyncSirannonClient(relay.url, reconnect_interval_ms=RECONNECT_INTERVAL_MS) as client,
    ):
        await client.database(database_id).on("items").subscribe(received.append, on_reset=lambda: resets.append(True))
        await writer.database(database_id).execute(INSERT, item(0))
        await asyncio.to_thread(wait_until, lambda: len(received) == 1, "the change before the outage")
        relay.refusing = True
        relay.cut()
        for index in range(1, 5):
            await writer.database(database_id).execute(INSERT, item(index))
        pruned = prune_change_log(path, received[0].seq + 2)
        relay.refusing = False
        await asyncio.to_thread(wait_until, lambda: resets == [True], "the reset after the pruning")
        await writer.database(database_id).execute(INSERT, item(5))
        await asyncio.to_thread(wait_until, lambda: len(received) == 2, "the live change after the reset")

    assert pruned == 3
    assert [as_tuple(event.row) for event in received] == [stored(path)[0], stored(path)[-1]]
    assert received[1].seq == change_log(path)[-1]


@pytest.mark.anyio
async def test_live_query_rows_match_the_table_after_dropped_connections(
    node: SirannonNode, directory: Path, relay: TcpRelay, database_id: str
) -> None:
    updates: list[LiveUpdate[SharedItemsInOrderRow]] = []
    async with (
        AsyncSirannonClient(node.url, transport="http") as writer,
        AsyncSirannonClient(relay.url, reconnect_interval_ms=RECONNECT_INTERVAL_MS) as client,
    ):
        await writer.database(database_id).batch(INSERT, [item(index) for index in range(-SEEDED_ROWS, 0)])
        live = await client.database(database_id).live(ITEMS_IN_ORDER)
        live.subscribe(updates.append)
        await writer.database(database_id).execute(INSERT, item(-SEEDED_ROWS - 1))
        await asyncio.to_thread(
            wait_until, lambda: any(update.kind == "ops" for update in updates), "an edit to the live rows"
        )
        assert [as_tuple(row) for row in live.rows] == stored(directory / f"{database_id}.db")
        await asyncio.to_thread(write_with_cuts, node.url, database_id, relay)
        expected = stored(directory / f"{database_id}.db")
        await asyncio.to_thread(
            wait_until,
            lambda: [as_tuple(row) for row in live.rows] == expected and not live.state.revalidating,
            "the live rows to match the table",
        )
        live.close()

    assert live.state.status == "ready"


def test_stops_reconnecting_once_the_server_refuses_the_caller(
    node: SirannonNode, secured_node: SirannonNode, relay: TcpRelay, database_id: str
) -> None:
    errors: list[BaseException] = []
    with SirannonClient(relay.url, reconnect_interval_ms=RECONNECT_INTERVAL_MS) as client:
        database = client.database(database_id)
        database.on("items").subscribe(lambda event: None, on_error=errors.append)
        relay.upstream_port = secured_node.port
        relay.cut()
        wait_until(lambda: len(errors) > 0, "the refusal to reach on_error")
        attempts = relay.accepted
        time.sleep(REFUSED_RECONNECT_WINDOW_SECONDS)
        assert len(errors) == 1
        with pytest.raises(SirannonError) as later:
            item_id, label, amount, payload = item(0)
            database.execute(PUT_ITEM({"id": item_id, "label": label, "amount": amount, "payload": payload}))

    assert isinstance(errors[0], SirannonError)
    assert errors[0].code == ErrorCodes.UNAUTHORIZED
    assert later.value.code == ErrorCodes.UNAUTHORIZED
    assert relay.accepted == attempts


def test_raises_forbidden_and_unauthorized_for_refused_websocket_credentials(
    secured_node: SirannonNode, database_id: str
) -> None:
    for bearer, code in ((FORBIDDEN_BEARER, ErrorCodes.FORBIDDEN), ("wrong", ErrorCodes.UNAUTHORIZED)):
        with SirannonClient(secured_node.url, headers={"authorization": f"Bearer {bearer}"}) as client:
            with pytest.raises(SirannonError) as caught:
                client.database(database_id).on("items").subscribe(lambda event: None)
            assert caught.value.code == code
