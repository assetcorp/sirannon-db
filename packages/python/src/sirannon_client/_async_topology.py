from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import TracebackType

import httpx

from sirannon_client._database import AsyncRemoteDatabase
from sirannon_client._errors import closed_error, invalid_argument
from sirannon_client._http import CapabilityReport
from sirannon_client._options import (
    DEFAULT_MAX_RESPONSE_BYTES,
    DEFAULT_RECONNECT_INTERVAL_MS,
    DEFAULT_REQUEST_TIMEOUT_MS,
    TransportKind,
    build_settings,
    normalise_url,
)
from sirannon_client._router import TopologyRouter
from sirannon_client._routing import READ_CONCERN_LEVELS, Discovery, ReadConcernLevel, ReadPreference
from sirannon_client._topology_channel import EndpointPool, TopologyChannel
from sirannon_client._topology_streams import TopologyStreams


class _RoutedCapabilities:
    def __init__(self, router: TopologyRouter, database_id: str) -> None:
        self._router = router
        self._database_id = database_id

    async def capabilities(self, refresh: bool = False) -> CapabilityReport:
        return await self._router.capabilities(self._database_id, refresh)


def _urls(values: Sequence[str] | None, name: str) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        raise invalid_argument(f"{name} must be a list of URLs")
    return list(dict.fromkeys(normalise_url(value) for value in values))


class AsyncTopologyAwareClient:
    """Connects to a replication group from asyncio code, sending writes to the primary and reads by preference.

    In `static` discovery, the client uses the `primary` and `replicas` that you list. When the client cannot
    reach a replica for a read, it sets that replica aside for five seconds and sends the read once more to
    another node. In `coordinator` discovery, the client fetches `GET /db/{id}/cluster` from the `endpoints`
    that you list and sends writes to the primary in that routing. When the primary changes, the client moves
    every subscription to the new routing. The client raises the error of a failed write without sending the
    write again, so a write reaches the server twice only when you send it twice.
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
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        """Prepares a client for a replication group, without opening a connection before the first call.

        Args:
            primary: The primary's base address, for a group that you list by hand.
            replicas: The replicas' base addresses, for a group that you list by hand.
            endpoints: The nodes that the client asks for routing in `coordinator` discovery.
            read_preference: `primary`, `replica` for a random replica, or `nearest` for the lowest latency.
            discovery: With `static`, the client uses the nodes that you list, while with `coordinator` it
                fetches the routing from the group.
            read_concern: The read concern that every read requires. In `coordinator` discovery, the client
                picks the node for each read by this level, and it sends the level with each read.
            transport: `websocket` or `http`, as for `SirannonClient`.
            headers: The client sends these headers with every request, discovery read, and handshake.
            websocket_protocols: The client offers these subprotocols after `sirannon.v1`.
            auto_reconnect: When this is true, the client reconnects after a WebSocket drops and restores every
                subscription.
            reconnect_interval_ms: The client waits this long between reconnection tries.
            request_timeout_ms: The client waits this long for each answer, and with 0 it waits without a limit.
            max_response_bytes: The client refuses an answer larger than this, and with 0 it accepts any size.
            http_client: An `httpx.AsyncClient` to send HTTP requests through, which the caller closes.
        """
        if read_preference not in ("primary", "replica", "nearest"):
            raise invalid_argument("read_preference must be 'primary', 'replica', or 'nearest'")
        if discovery not in ("static", "coordinator"):
            raise invalid_argument("discovery must be 'static' or 'coordinator'")
        if read_concern is not None and read_concern not in READ_CONCERN_LEVELS:
            raise invalid_argument("read_concern must be 'local', 'majority', or 'linearizable'")
        primary_url = None if primary is None else normalise_url(primary)
        replica_urls = _urls(replicas, "replicas")
        starter_urls = _urls(endpoints, "endpoints")
        if discovery == "static" and primary_url is None and not replica_urls:
            raise invalid_argument("static discovery needs a primary or at least one replica")
        if discovery == "coordinator" and primary_url is None and not replica_urls and not starter_urls:
            raise invalid_argument("coordinator discovery needs at least one endpoint")
        base = primary_url or (replica_urls or starter_urls)[0]
        settings = build_settings(
            base,
            transport=transport,
            headers=headers,
            websocket_protocols=websocket_protocols,
            auto_reconnect=auto_reconnect,
            reconnect_interval_ms=reconnect_interval_ms,
            request_timeout_ms=request_timeout_ms,
            max_response_bytes=max_response_bytes,
        )
        self._owns_http = http_client is None
        self._http_client = http_client if http_client is not None else httpx.AsyncClient(follow_redirects=False)
        self._router = TopologyRouter(
            settings,
            self._http_client,
            primary=primary_url,
            replicas=replica_urls,
            endpoints=starter_urls,
            read_preference=read_preference,
            discovery=discovery,
            read_concern=read_concern,
        )
        self._databases: dict[str, AsyncRemoteDatabase] = {}
        self._closed = False

    def database(self, database_id: str) -> AsyncRemoteDatabase:
        """Returns the routed handle for one database and keeps it, so a later call with the same id returns it."""
        if self._closed:
            raise closed_error()
        existing = self._databases.get(database_id)
        if existing is not None:
            return existing
        pool = EndpointPool(self._router, database_id)
        streams = TopologyStreams(self._router, database_id) if self._router.settings.transport == "websocket" else None
        router = self._router

        async def routing_changed() -> None:
            if streams is not None:
                await streams.migrate()
            routing = await router.routing(database_id)
            await pool.retire_except(routing.urls())

        forget_listener = router.listen(database_id, routing_changed)

        async def close() -> None:
            forget_listener()
            if self._databases.get(database_id) is opened:
                del self._databases[database_id]
            if streams is not None:
                await streams.aclose()
            await pool.aclose()

        opened = AsyncRemoteDatabase(
            database_id,
            TopologyChannel(router, pool, database_id),
            _RoutedCapabilities(router, database_id),
            streams,
            close,
        )
        self._databases[database_id] = opened
        return opened

    async def aclose(self) -> None:
        """Closes every database handle and connection, so every call in flight fails with `TRANSPORT_ERROR`."""
        if self._closed:
            return
        self._closed = True
        databases, self._databases = list(self._databases.values()), {}
        for database in databases:
            await database.aclose()
        await self._router.aclose()
        if self._owns_http:
            await self._http_client.aclose()

    async def __aenter__(self) -> AsyncTopologyAwareClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()
