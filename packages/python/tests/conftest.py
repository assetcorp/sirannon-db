from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.servers import SirannonNode, TcpRelay

ACCEPTED_BEARER = "client-test-accepted"
FORBIDDEN_BEARER = "client-test-forbidden"


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="session")
def directory(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("sirannon")


@pytest.fixture(scope="session")
def node(directory: Path) -> Iterator[SirannonNode]:
    started = SirannonNode({"directory": str(directory)}).start()
    yield started
    started.stop()


@pytest.fixture(scope="session")
def secured_node(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SirannonNode]:
    settings = {
        "directory": str(tmp_path_factory.mktemp("secured")),
        "token": ACCEPTED_BEARER,
        "forbiddenToken": FORBIDDEN_BEARER,
    }
    started = SirannonNode(settings).start()
    yield started
    started.stop()


@pytest.fixture(scope="session")
def sql_refusing_node(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SirannonNode]:
    started = SirannonNode({"directory": str(tmp_path_factory.mktemp("refusing")), "acceptSql": False}).start()
    yield started
    started.stop()


@pytest.fixture
def relay(node: SirannonNode) -> Iterator[TcpRelay]:
    opened = TcpRelay(node.port)
    yield opened
    opened.close()


@pytest.fixture
def database_id() -> str:
    return f"py-{uuid.uuid4().hex[:12]}"
