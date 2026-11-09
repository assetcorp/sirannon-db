from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from typing import Any, Final, TypeVar

from sirannon_client._channels import Channel, HttpChannel, Params, WebSocketChannel
from sirannon_client._errors import ErrorCodes, SirannonError
from sirannon_client._exchange import path_segment
from sirannon_client._router import TopologyRouter
from sirannon_client._routing import refreshes_routing
from sirannon_client._ws import AsyncWebSocketTransport

T = TypeVar("T")
UNREACHABLE_FOR_READS: Final = frozenset({ErrorCodes.CONNECTION_ERROR, "HANDLER_CLOSED"})
ReadWork = Callable[[Channel, dict[str, Any] | None], Awaitable[T]]
WriteWork = Callable[[Channel], Awaitable[T]]


class Endpoint:
    def __init__(self, url: str, channel: Channel, socket: AsyncWebSocketTransport | None) -> None:
        self.url = url
        self.channel = channel
        self._socket = socket
        self._in_flight = 0
        self._retired = False

    async def run(self, work: Callable[[Channel], Awaitable[T]]) -> T:
        self._in_flight += 1
        try:
            return await work(self.channel)
        finally:
            self._in_flight -= 1
            if self._retired and self._in_flight == 0:
                await self.aclose()

    async def retire(self) -> None:
        self._retired = True
        if self._in_flight == 0:
            await self.aclose()

    async def aclose(self) -> None:
        if self._socket is not None:
            await self._socket.aclose()


class EndpointPool:
    def __init__(self, router: TopologyRouter, database_id: str) -> None:
        self._router = router
        self._database_id = database_id
        self._open: dict[str, Endpoint] = {}

    def get(self, url: str) -> Endpoint:
        existing = self._open.get(url)
        if existing is not None:
            return existing
        settings = self._router.endpoint_settings(url)
        socket: AsyncWebSocketTransport | None = None
        channel: Channel
        if settings.transport == "websocket":
            socket = AsyncWebSocketTransport(f"{settings.websocket_url}/db/{path_segment(self._database_id)}", settings)
            channel = WebSocketChannel(socket)
        else:
            channel = HttpChannel(self._router.server(url), self._database_id)
        endpoint = Endpoint(url, channel, socket)
        self._open[url] = endpoint
        return endpoint

    async def retire_except(self, keep: set[str]) -> None:
        for url in [url for url in self._open if url not in keep]:
            await self._open.pop(url).retire()

    async def aclose(self) -> None:
        endpoints, self._open = list(self._open.values()), {}
        for endpoint in endpoints:
            await endpoint.aclose()


class TopologyChannel:
    carries_read_concern = False

    def __init__(self, router: TopologyRouter, pool: EndpointPool, database_id: str) -> None:
        self._router = router
        self._pool = pool
        self._database_id = database_id

    def _level(self) -> dict[str, Any] | None:
        return None if self._router.read_concern is None else {"level": self._router.read_concern}

    async def _read(self, work: ReadWork[T]) -> T:
        url = await self._router.read_endpoint(self._database_id, self._router.read_concern)
        level = self._level()
        try:
            return await self._pool.get(url).run(lambda channel: work(channel, level))
        except SirannonError as error:
            unreachable = error.code in UNREACHABLE_FOR_READS
            if self._router.coordinator and refreshes_routing(error):
                if unreachable:
                    self._router.set_aside(url)
                await self._router.refresh(self._database_id)
                retry = await self._router.read_endpoint(self._database_id, self._router.read_concern)
                return await self._pool.get(retry).run(lambda channel: work(channel, level))
            if not unreachable or url == await self._router.write_endpoint(self._database_id):
                raise
            self._router.set_aside(url)
            fallback = await self._router.read_endpoint(self._database_id, None)
            return await self._pool.get(fallback).run(lambda channel: work(channel, None))

    async def _write(self, work: WriteWork[T]) -> T:
        url = await self._router.write_endpoint(self._database_id)
        try:
            return await self._pool.get(url).run(work)
        except SirannonError as error:
            if self._router.coordinator and refreshes_routing(error):
                with suppress(SirannonError):
                    await self._router.refresh(self._database_id)
            raise

    async def query(self, sql: str, params: Params | None, read_concern: dict[str, Any] | None) -> Any:
        return await self._read(lambda channel, level: channel.query(sql, params, level))

    async def execute(self, sql: str, params: Params | None) -> dict[str, Any]:
        return await self._write(lambda channel: channel.execute(sql, params))

    async def transaction(self, statements: list[dict[str, Any]]) -> dict[str, Any]:
        return await self._write(lambda channel: channel.transaction(statements))

    async def batch(self, sql: str, params_batch: list[Any], write_concern: dict[str, Any] | None) -> dict[str, Any]:
        return await self._write(lambda channel: channel.batch(sql, params_batch, write_concern))

    async def load(
        self, sql: str, params_batch: list[Any], durability: str | None, checkpoint: bool | None
    ) -> dict[str, Any]:
        return await self._write(lambda channel: channel.load(sql, params_batch, durability, checkpoint))

    async def query_named(
        self, name: str, args: Mapping[str, Any] | None, read_concern: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        return await self._read(lambda channel, level: channel.query_named(name, args, level))

    async def execute_named(
        self, name: str, args: Mapping[str, Any] | None, write_concern: dict[str, Any] | None
    ) -> dict[str, Any]:
        return await self._write(lambda channel: channel.execute_named(name, args, write_concern))
