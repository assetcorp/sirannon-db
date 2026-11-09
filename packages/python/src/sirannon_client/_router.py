from __future__ import annotations

import asyncio
import json
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Final

import httpx

from sirannon_client._errors import ErrorCodes, SirannonError
from sirannon_client._exchange import Exchange, path_segment
from sirannon_client._http import AsyncHttp, CapabilityReport
from sirannon_client._options import ClientSettings
from sirannon_client._routing import (
    ClusterRouting,
    Discovery,
    ReadConcernLevel,
    ReadPreference,
    parse_cluster_routing,
)
from sirannon_client._tasks import shared_task

CLUSTER_DISCOVERY_TIMEOUT_MS: Final = 2_000
LATENCY_PROBE_TIMEOUT_MS: Final = 5_000
LATENCY_TTL_SECONDS: Final = 60.0
SET_ASIDE_SECONDS: Final = 5.0

RoutingListener = Callable[[], Awaitable[None]]


class TopologyRouter:
    def __init__(
        self,
        settings: ClientSettings,
        http_client: httpx.AsyncClient,
        *,
        primary: str | None,
        replicas: list[str],
        endpoints: list[str],
        read_preference: ReadPreference,
        discovery: Discovery,
        read_concern: ReadConcernLevel | None,
    ) -> None:
        self.settings = settings
        self.read_preference = read_preference
        self.discovery = discovery
        self.read_concern = read_concern
        self._http_client = http_client
        self._primary = primary
        self._replicas = replicas
        self._starters = endpoints
        self._base = primary or (replicas[0] if replicas else None) or (endpoints[0] if endpoints else "")
        self._servers: dict[str, AsyncHttp] = {}
        self._set_aside: dict[str, float] = {}
        self._latencies: list[tuple[str, float]] = []
        self._latency_measured_at = 0.0
        self._measuring: asyncio.Task[None] | None = None
        self._routing: dict[str, ClusterRouting] = {}
        self._refreshing: dict[str, asyncio.Task[None]] = {}
        self._listeners: dict[str, RoutingListener] = {}

    @property
    def coordinator(self) -> bool:
        return self.discovery == "coordinator"

    def server(self, url: str) -> AsyncHttp:
        existing = self._servers.get(url)
        if existing is None:
            existing = AsyncHttp(replace(self.settings, base_url=url), self._http_client)
            self._servers[url] = existing
        return existing

    def endpoint_settings(self, url: str) -> ClientSettings:
        return replace(self.settings, base_url=url)

    def listen(self, database_id: str, listener: RoutingListener) -> Callable[[], None]:
        self._listeners[database_id] = listener

        def forget() -> None:
            if self._listeners.get(database_id) is listener:
                del self._listeners[database_id]

        return forget

    def set_aside(self, url: str) -> None:
        self._set_aside[url] = time.monotonic() + SET_ASIDE_SECONDS

    def usable(self, url: str) -> bool:
        until = self._set_aside.get(url)
        if until is None:
            return True
        if time.monotonic() < until:
            return False
        del self._set_aside[url]
        return True

    async def capabilities(self, database_id: str, refresh: bool) -> CapabilityReport:
        if self.coordinator:
            routing = await self.routing(database_id)
            url = routing.current_primary or await self.read_endpoint(database_id, None)
        else:
            url = await self.write_endpoint(database_id)
        return await self.server(url).capabilities(refresh)

    async def write_endpoint(self, database_id: str) -> str:
        if not self.coordinator:
            return self._primary or self._base
        routing = await self.routing(database_id)
        if routing.current_primary is None:
            raise SirannonError(ErrorCodes.NO_SAFE_PRIMARY, "No current primary is available")
        return routing.current_primary

    async def read_endpoint(self, database_id: str, level: str | None) -> str:
        if self.coordinator:
            return await self._routed_read(database_id, level)
        fallback = self._primary or self._base
        if self.read_preference == "primary":
            return fallback
        if self.read_preference == "replica":
            available = [url for url in self._replicas if self.usable(url)]
            return secrets.choice(available) if available else fallback
        await self._measure_latency()
        reachable = [entry for entry in self._latencies if self.usable(entry[0])]
        return min(reachable, key=lambda entry: entry[1])[0] if reachable else fallback

    async def _routed_read(self, database_id: str, level: str | None) -> str:
        routing = await self.routing(database_id)
        wanted = level or self.read_concern or "majority"
        if wanted == "linearizable":
            if routing.current_primary is not None:
                return routing.current_primary
            raise SirannonError(ErrorCodes.NO_SAFE_PRIMARY, "No current primary is available for linearizable reads")
        readable = [
            endpoint.url
            for endpoint in routing.read_endpoints
            if wanted in endpoint.read_concerns and self.usable(endpoint.url)
        ]
        if self.read_preference == "replica" and routing.current_primary is not None:
            readable = [url for url in readable if url != routing.current_primary]
        if self.read_preference != "primary" and readable:
            return readable[0] if self.read_preference == "nearest" else secrets.choice(readable)
        if routing.current_primary is not None:
            return routing.current_primary
        for endpoint in routing.read_endpoints:
            if "local" in endpoint.read_concerns:
                return endpoint.url
        raise SirannonError(ErrorCodes.ROUTING_ERROR, "No usable read endpoint is available")

    async def routing(self, database_id: str) -> ClusterRouting:
        existing = self._routing.get(database_id)
        if existing is not None:
            return existing
        await self.refresh(database_id)
        refreshed = self._routing.get(database_id)
        if refreshed is None:
            raise _undiscovered(database_id)
        return refreshed

    async def refresh(self, database_id: str) -> None:
        running = self._refreshing.get(database_id)
        if running is None or running.done():
            running = shared_task(self._refresh(database_id))
            self._refreshing[database_id] = running
        await asyncio.shield(running)

    async def _refresh(self, database_id: str) -> None:
        encoded = path_segment(database_id)
        for candidate in self._candidates(database_id):
            found = await self._fetch_routing(candidate, database_id, encoded)
            if found is None:
                continue
            previous = self._routing.get(database_id)
            self._routing[database_id] = found
            if previous is not None and previous.fingerprint() == found.fingerprint():
                return
            listener = self._listeners.get(database_id)
            if listener is None:
                return
            try:
                await listener()
            except BaseException:
                if previous is None:
                    self._routing.pop(database_id, None)
                else:
                    self._routing[database_id] = previous
                raise
            return
        raise _undiscovered(database_id)

    async def _fetch_routing(self, base: str, database_id: str, encoded: str) -> ClusterRouting | None:
        exchange = Exchange("GET", f"/db/{encoded}/cluster", timeout_ms=CLUSTER_DISCOVERY_TIMEOUT_MS)
        try:
            status, body = await self.server(base).send(exchange)
        except SirannonError as error:
            if error.code == ErrorCodes.TRANSPORT_ERROR:
                raise
            return None
        if not 200 <= status < 300:
            return None
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            return None
        return parse_cluster_routing(payload, database_id, encoded)

    def _candidates(self, database_id: str) -> list[str]:
        listed: dict[str, None] = dict.fromkeys(self._starters)
        if self._primary is not None:
            listed[self._primary] = None
        listed.update(dict.fromkeys(self._replicas))
        existing = self._routing.get(database_id)
        if existing is not None:
            listed.update(dict.fromkeys(sorted(existing.urls())))
        if self._base:
            listed[self._base] = None
        return list(listed)

    async def _measure_latency(self) -> None:
        if self._latencies and time.monotonic() - self._latency_measured_at < LATENCY_TTL_SECONDS:
            return
        if self._measuring is None or self._measuring.done():
            self._measuring = shared_task(self._probe_all())
        await asyncio.shield(self._measuring)

    async def _probe_all(self) -> None:
        targets = ([self._primary] if self._primary is not None else []) + self._replicas
        measured = await asyncio.gather(*(self._probe(url) for url in targets))
        self._latencies = [entry for entry in measured if entry is not None]
        self._latency_measured_at = time.monotonic()

    async def _probe(self, url: str) -> tuple[str, float] | None:
        started = time.monotonic()
        try:
            status, _ = await self.server(url).send(Exchange("GET", "/health", timeout_ms=LATENCY_PROBE_TIMEOUT_MS))
        except SirannonError:
            return None
        return (url, time.monotonic() - started) if 200 <= status < 300 else None

    async def aclose(self) -> None:
        for task in [self._measuring, *self._refreshing.values()]:
            if task is not None:
                task.cancel()
        for server in list(self._servers.values()):
            await server.aclose()


def _undiscovered(database_id: str) -> SirannonError:
    return SirannonError(ErrorCodes.ROUTING_ERROR, f"Could not discover cluster routing for database '{database_id}'")
