from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from sirannon_client._errors import ErrorCodes, SirannonError, closed_error
from sirannon_client._events import ChangeEvent, ErrorHandler
from sirannon_client._exchange import path_segment
from sirannon_client._router import TopologyRouter
from sirannon_client._routing import refreshes_routing
from sirannon_client._ws import AsyncWebSocketTransport
from sirannon_client._ws_state import DigestSource, LiveHandlers

Opener = Callable[[AsyncWebSocketTransport], Awaitable[str]]


@dataclass
class Tracked:
    reopen: Opener
    socket: AsyncWebSocketTransport
    remote_id: str


class TopologyStreams:
    def __init__(self, router: TopologyRouter, database_id: str) -> None:
        self._router = router
        self._database_id = database_id
        self._socket: AsyncWebSocketTransport | None = None
        self._url = ""
        self._tracked: dict[str, Tracked] = {}
        self._lock = asyncio.Lock()
        self._counter = 0
        self._closed = False

    async def subscribe(
        self,
        table: str,
        filter_values: dict[str, Any] | None,
        callback: Callable[[ChangeEvent], Any],
        on_error: ErrorHandler | None,
        on_reset: Callable[[], Any] | None,
    ) -> str:
        def opener(socket: AsyncWebSocketTransport) -> Awaitable[str]:
            return socket.subscribe(table, filter_values, callback, on_error, on_reset)

        return await self._track(opener)

    async def open_live(
        self,
        name: str,
        args: dict[str, Any] | None,
        handlers: LiveHandlers,
        digest: DigestSource | None,
    ) -> str:
        def opener(socket: AsyncWebSocketTransport) -> Awaitable[str]:
            return socket.open_live(name, args, handlers, digest)

        return await self._track(opener)

    def unsubscribe(self, subscription_id: str) -> None:
        tracked = self._tracked.pop(subscription_id, None)
        if tracked is not None:
            tracked.socket.unsubscribe(tracked.remote_id)

    async def ack(self, device_id: str, seq: int) -> Any:
        socket = await self._current_socket()
        return await socket.ack(device_id, seq)

    async def _track(self, opener: Opener) -> str:
        try:
            return await self._open_here(opener)
        except SirannonError as error:
            if not (self._router.coordinator and refreshes_routing(error)):
                raise
            if not self._tracked:
                await self._drop_socket()
            await self._router.refresh(self._database_id)
            if not self._tracked:
                await self._drop_socket()
            return await self._open_here(opener)

    async def _open_here(self, opener: Opener) -> str:
        url = await self._router.read_endpoint(self._database_id, self._router.read_concern)
        async with self._lock:
            socket = self._socket_for(url)
            remote_id = await opener(socket)
            self._counter += 1
            tracked_id = f"t_{self._counter}"
            self._tracked[tracked_id] = Tracked(opener, socket, remote_id)
            return tracked_id

    async def _current_socket(self) -> AsyncWebSocketTransport:
        url = await self._router.read_endpoint(self._database_id, self._router.read_concern)
        async with self._lock:
            return self._socket_for(url)

    def _socket_for(self, url: str) -> AsyncWebSocketTransport:
        if self._closed:
            raise closed_error()
        if self._socket is None:
            self._socket = self._open_socket(url)
            self._url = url
        return self._socket

    def _open_socket(self, url: str) -> AsyncWebSocketTransport:
        settings = self._router.endpoint_settings(url)
        return AsyncWebSocketTransport(f"{settings.websocket_url}/db/{path_segment(self._database_id)}", settings)

    async def _drop_socket(self) -> None:
        async with self._lock:
            socket, self._socket, self._url = self._socket, None, ""
        if socket is not None:
            await socket.aclose()

    async def migrate(self) -> None:
        if not self._router.coordinator or not self._tracked:
            return
        url = await self._router.read_endpoint(self._database_id, self._router.read_concern)
        async with self._lock:
            if self._closed or not self._tracked or (self._socket is not None and self._url == url):
                return
            for tracked_id in [key for key, item in self._tracked.items() if not item.socket.holds(item.remote_id)]:
                del self._tracked[tracked_id]
            moving = list(self._tracked.items())
            if not moving:
                return
            fresh = self._open_socket(url)
            reopened: dict[str, str] = {}
            try:
                for tracked_id, tracked in moving:
                    reopened[tracked_id] = await tracked.reopen(fresh)
            except SirannonError as error:
                await fresh.aclose()
                raise SirannonError(
                    ErrorCodes.ROUTING_ERROR,
                    "Could not re-establish active subscriptions on refreshed routing for database "
                    f"'{self._database_id}': {error.message}",
                ) from None
            previous, self._socket, self._url = self._socket, fresh, url
            for tracked_id, remote_id in reopened.items():
                current = self._tracked.get(tracked_id)
                if current is None:
                    fresh.unsubscribe(remote_id)
                else:
                    self._tracked[tracked_id] = Tracked(current.reopen, fresh, remote_id)
        if previous is not None:
            await previous.aclose()

    async def aclose(self) -> None:
        self._closed = True
        self._tracked.clear()
        socket, self._socket = self._socket, None
        if socket is not None:
            await socket.aclose()
