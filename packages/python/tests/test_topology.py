from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

import pytest

from sirannon_client import AsyncTopologyAwareClient, SirannonError
from tests.servers import SirannonNode, read_rows

DATABASE_ID = "items"
WRITERS = 4
READERS = 4
WRITES_PER_WRITER = 60
STOP_REPLICA_AFTER_WRITES = 40
INSERT = "INSERT INTO items (label, amount, payload) VALUES (?, ?, ?)"


@pytest.fixture
def group(tmp_path: Path) -> Iterator[tuple[SirannonNode, SirannonNode]]:
    certificates = tmp_path / "certificates"
    primary_directory = tmp_path / "primary"
    replica_directory = tmp_path / "replica"
    primary_directory.mkdir()
    replica_directory.mkdir()
    primary = SirannonNode(
        {"mode": "primary", "directory": str(primary_directory), "certificates": str(certificates)}
    ).start()
    replica = SirannonNode(
        {
            "mode": "replica",
            "directory": str(replica_directory),
            "certificates": str(certificates),
            "primaryGrpcPort": primary.grpc_port,
        }
    )
    try:
        replica.start()
        yield primary, replica
    finally:
        replica.stop()
        primary.stop()


@pytest.mark.anyio
async def test_reads_reach_the_primary_once_the_replica_stops_and_each_acknowledged_write_lands_once(
    group: tuple[SirannonNode, SirannonNode],
) -> None:
    primary, replica = group
    acknowledged: list[str] = []
    unacknowledged: list[str] = []
    read_failures: list[SirannonError] = []
    reads_after_stop = 0
    reads_before_stop = 0
    replica_stopped = asyncio.Event()
    written = 0

    async with AsyncTopologyAwareClient(
        primary=primary.url, replicas=[replica.url], read_preference="replica", reconnect_interval_ms=50
    ) as client:
        database = client.database(DATABASE_ID)

        async def write(writer: int) -> None:
            nonlocal written
            for index in range(WRITES_PER_WRITER):
                label = f"writer-{writer}-{index}"
                try:
                    await database.execute(INSERT, [label, 2**62 + index, label.encode()])
                except SirannonError:
                    unacknowledged.append(label)
                else:
                    acknowledged.append(label)
                written += 1
                if written == STOP_REPLICA_AFTER_WRITES:
                    await asyncio.to_thread(replica.stop)
                    replica_stopped.set()

        async def read() -> None:
            nonlocal reads_after_stop, reads_before_stop
            while len(acknowledged) + len(unacknowledged) < WRITERS * WRITES_PER_WRITER:
                stopped = replica_stopped.is_set()
                try:
                    await database.query("SELECT count(*) AS total FROM items")
                except SirannonError as error:
                    read_failures.append(error)
                else:
                    if stopped:
                        reads_after_stop += 1
                    else:
                        reads_before_stop += 1
                await asyncio.sleep(0)

        await asyncio.gather(*(write(writer) for writer in range(WRITERS)), *(read() for _ in range(READERS)))
        final = await database.query("SELECT label FROM items ORDER BY label")

    stored = Counter(
        row[0] for row in read_rows(Path(primary.settings["directory"]) / "items.db", "SELECT label FROM items")
    )
    assert replica_stopped.is_set()
    assert read_failures == []
    assert reads_before_stop > 0
    assert reads_after_stop > 0
    assert unacknowledged == []
    assert sorted(stored) == sorted(acknowledged)
    assert all(count == 1 for count in stored.values())
    assert [row["label"] for row in final] == sorted(acknowledged)
