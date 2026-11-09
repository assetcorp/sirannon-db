from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from types import TracebackType

import httpx

from sirannon_client._async_client import AsyncSirannonClient
from sirannon_client._async_topology import AsyncTopologyAwareClient
from sirannon_client._database import AsyncRemoteDatabase
from sirannon_client._errors import closed_error
from sirannon_client._http import SyncHttp
from sirannon_client._operations import Operations
from sirannon_client._options import (
    DEFAULT_MAX_RESPONSE_BYTES,
    DEFAULT_RECONNECT_INTERVAL_MS,
    DEFAULT_REQUEST_TIMEOUT_MS,
    TransportKind,
)
from sirannon_client._routing import Discovery, ReadConcernLevel, ReadPreference
from sirannon_client._sync_database import RemoteDatabase
from sirannon_client._sync_runtime import Runtime


async def _open(opener: Callable[[str], AsyncRemoteDatabase], database_id: str) -> AsyncRemoteDatabase:
    return opener(database_id)


class _Handles:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._runtime: Runtime | None = None
        self._databases: dict[str, RemoteDatabase] = {}
        self.closed = False

    def runtime(self) -> Runtime:
        if self._runtime is None:
            self._runtime = Runtime()
        return self._runtime

    def database(
        self, database_id: str, opener: Callable[[Runtime], Callable[[str], AsyncRemoteDatabase]]
    ) -> RemoteDatabase:
        with self._lock:
            if self.closed:
                raise closed_error()
            existing = self._databases.get(database_id)
            if existing is not None and not existing._database._closed:
                return existing
            runtime = self.runtime()
            opened = RemoteDatabase(runtime, runtime.run(_open(opener(runtime), database_id)))
            self._databases[database_id] = opened
            return opened

    def close(self, closer: Callable[[Runtime], None]) -> None:
        with self._lock:
            if self.closed:
                return
            self.closed = True
            runtime, self._runtime = self._runtime, None
            self._databases.clear()
        if runtime is not None:
            try:
                closer(runtime)
            finally:
                runtime.stop()


class SirannonClient(Operations):
    """Connects to one Sirannon server from code that does not use asyncio.

    Each route of the server has a blocking method here, and `database(id)` returns the handle for statements,
    registered operations, change subscriptions, and live queries. The client starts one background thread for
    the connections and one that calls your callbacks, and only once you open a database. Close it with
    `close`, or use it with `with`.
    """

    def __init__(
        self,
        url: str,
        *,
        transport: TransportKind = "websocket",
        headers: Mapping[str, str] | None = None,
        websocket_protocols: str | Sequence[str] | None = None,
        auto_reconnect: bool = True,
        reconnect_interval_ms: int = DEFAULT_RECONNECT_INTERVAL_MS,
        request_timeout_ms: int = DEFAULT_REQUEST_TIMEOUT_MS,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        http_client: httpx.Client | None = None,
    ) -> None:
        """Prepares a client for the server at `url`, and takes the settings that `AsyncSirannonClient` takes.

        Args:
            url: The server's base address, such as `https://db.example.com`.
            transport: `websocket` or `http`, as for `AsyncSirannonClient`.
            headers: The client sends these headers with every HTTP request and WebSocket handshake.
            websocket_protocols: The client offers these subprotocols after `sirannon.v1`.
            auto_reconnect: Reconnect after a dropped WebSocket and restore every subscription.
            reconnect_interval_ms: The client waits this long between reconnection tries.
            request_timeout_ms: The client waits this long for each answer, and 0 waits without a limit.
            max_response_bytes: The client refuses an answer larger than this, and 0 accepts any size.
            http_client: An `httpx.Client` that the route methods send through, which the caller closes.
        """
        self._async = AsyncSirannonClient(
            url,
            transport=transport,
            headers=headers,
            websocket_protocols=websocket_protocols,
            auto_reconnect=auto_reconnect,
            reconnect_interval_ms=reconnect_interval_ms,
            request_timeout_ms=request_timeout_ms,
            max_response_bytes=max_response_bytes,
        )
        self._transport = SyncHttp(self._async.settings, http_client)
        self._handles = _Handles()

    def _opener(self, runtime: Runtime) -> Callable[[str], AsyncRemoteDatabase]:
        return self._async.database

    def database(self, database_id: str) -> RemoteDatabase:
        """Returns the handle for one database, and keeps it, so a later call with the same id returns it again."""
        return self._handles.database(database_id, self._opener)

    def close(self) -> None:
        """Closes every database handle and connection, and fails every call in flight with `TRANSPORT_ERROR`."""
        self._transport.close()
        client = self._async

        def closer(runtime: Runtime) -> None:
            runtime.run(client.aclose())

        self._handles.close(closer)

    def __enter__(self) -> SirannonClient:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()


class TopologyAwareClient:
    """Connects to a replication group from code that does not use asyncio.

    It routes as `AsyncTopologyAwareClient` does: writes go to the primary, a failed write is raised and never
    resent, and a read that cannot reach its node runs once more on another.
    """

    def __init__(
        self,
        *,
        primary: str | None = None,
        replicas: Sequence[str] | None = None,
        endpoints: Sequence[str] | None = None,
        read_preference: ReadPreference = "primary",
        discovery: Discovery = "static",
        read_concern: ReadConcernLevel | None = None,
        transport: TransportKind = "websocket",
        headers: Mapping[str, str] | None = None,
        websocket_protocols: str | Sequence[str] | None = None,
        auto_reconnect: bool = True,
        reconnect_interval_ms: int = DEFAULT_RECONNECT_INTERVAL_MS,
        request_timeout_ms: int = DEFAULT_REQUEST_TIMEOUT_MS,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    ) -> None:
        """Prepares a client for a replication group, with the settings that `AsyncTopologyAwareClient` takes."""
        self._async = AsyncTopologyAwareClient(
            primary=primary,
            replicas=replicas,
            endpoints=endpoints,
            read_preference=read_preference,
            discovery=discovery,
            read_concern=read_concern,
            transport=transport,
            headers=headers,
            websocket_protocols=websocket_protocols,
            auto_reconnect=auto_reconnect,
            reconnect_interval_ms=reconnect_interval_ms,
            request_timeout_ms=request_timeout_ms,
            max_response_bytes=max_response_bytes,
        )
        self._handles = _Handles()

    def _opener(self, runtime: Runtime) -> Callable[[str], AsyncRemoteDatabase]:
        return self._async.database

    def database(self, database_id: str) -> RemoteDatabase:
        """Returns the routed handle for one database, and keeps it, so a later call with the same id returns it."""
        return self._handles.database(database_id, self._opener)

    def close(self) -> None:
        """Closes every database handle and connection, and fails every call in flight with `TRANSPORT_ERROR`."""
        client = self._async

        def closer(runtime: Runtime) -> None:
            runtime.run(client.aclose())

        self._handles.close(closer)

    def __enter__(self) -> TopologyAwareClient:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()
