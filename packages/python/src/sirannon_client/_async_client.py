from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from types import TracebackType

import httpx

from sirannon_client._async_operations import AsyncOperations
from sirannon_client._channels import Channel, HttpChannel, WebSocketChannel
from sirannon_client._database import AsyncRemoteDatabase
from sirannon_client._errors import closed_error
from sirannon_client._exchange import path_segment
from sirannon_client._http import AsyncHttp
from sirannon_client._options import (
    DEFAULT_MAX_RESPONSE_BYTES,
    DEFAULT_RECONNECT_INTERVAL_MS,
    DEFAULT_REQUEST_TIMEOUT_MS,
    ClientSettings,
    TransportKind,
    build_settings,
)
from sirannon_client._ws import AsyncWebSocketTransport


def open_database(
    settings: ClientSettings, http: AsyncHttp, database_id: str, on_close: Callable[[str], None]
) -> AsyncRemoteDatabase:
    segment = path_segment(database_id)
    socket: AsyncWebSocketTransport | None = None
    channel: Channel
    if settings.transport == "websocket":
        socket = AsyncWebSocketTransport(f"{settings.websocket_url}/db/{segment}", settings)
        channel = WebSocketChannel(socket)
    else:
        channel = HttpChannel(http, database_id)
    opened_socket = socket

    async def close() -> None:
        on_close(database_id)
        if opened_socket is not None:
            await opened_socket.aclose()

    return AsyncRemoteDatabase(database_id, channel, http, socket, close)


class AsyncSirannonClient(AsyncOperations):
    """Connects to one Sirannon server from asyncio code.

    The client has one method for each route of the server. Its `database(id)` method returns the handle for
    statements, registered operations, change subscriptions, and live queries. Close the client with `aclose`,
    or use it with `async with`.
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
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        """Prepares a client for the server at `url`, without opening a connection before the first call.

        Args:
            url: The server's base address, such as `https://db.example.com`.
            transport: With `websocket`, the client sends statements over one WebSocket for each database, and
                only this transport supports subscriptions and live queries. With `http`, the client sends each
                statement as an HTTP request.
            headers: The client sends these headers with every HTTP request and WebSocket handshake.
            websocket_protocols: The client offers these subprotocols after `sirannon.v1`, such as a value that
                holds a credential for a server that reads one from the handshake.
            auto_reconnect: When this is true, the client reconnects after a WebSocket drops and restores every
                subscription.
            reconnect_interval_ms: The client waits this long between reconnection tries.
            request_timeout_ms: The client waits this long for each answer, and with 0 it waits without a limit.
            max_response_bytes: The client refuses an answer larger than this, and with 0 it accepts any size.
            http_client: An `httpx.AsyncClient` to send HTTP requests through, which the caller closes.
        """
        self._settings = build_settings(
            url,
            transport=transport,
            headers=headers,
            websocket_protocols=websocket_protocols,
            auto_reconnect=auto_reconnect,
            reconnect_interval_ms=reconnect_interval_ms,
            request_timeout_ms=request_timeout_ms,
            max_response_bytes=max_response_bytes,
        )
        self._transport = AsyncHttp(self._settings, http_client)
        self._databases: dict[str, AsyncRemoteDatabase] = {}
        self._closed = False

    @property
    def settings(self) -> ClientSettings:
        """The validated settings that this client connects with."""
        return self._settings

    def database(self, database_id: str) -> AsyncRemoteDatabase:
        """Returns the handle for one database and keeps it, so a later call with the same id returns that handle."""
        if self._closed:
            raise closed_error()
        existing = self._databases.get(database_id)
        if existing is not None:
            return existing
        opened = open_database(self._settings, self._transport, database_id, self._forget)
        self._databases[database_id] = opened
        return opened

    def _forget(self, database_id: str) -> None:
        self._databases.pop(database_id, None)

    async def aclose(self) -> None:
        """Closes every database handle and connection, so every call in flight fails with `TRANSPORT_ERROR`."""
        if self._closed:
            return
        self._closed = True
        databases, self._databases = list(self._databases.values()), {}
        for database in databases:
            await database.aclose()
        await self._transport.aclose()

    async def __aenter__(self) -> AsyncSirannonClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()
