from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Final, TypeVar

import httpx
from pydantic import TypeAdapter, ValidationError

from sirannon_client._errors import (
    SQL_REFUSED_MESSAGE,
    ErrorCodes,
    SirannonError,
    closed_error,
    error_from_body,
    invalid_response,
)
from sirannon_client._exchange import Exchange, encode_json
from sirannon_client._options import ClientSettings, whole_number
from sirannon_client._tasks import shared_task
from sirannon_client._values import decode_values

AnswerT = TypeVar("AnswerT")
CAPABILITIES_PATH: Final = "/capabilities"
SQL_CAPABILITY: Final = "query.sql"
MISSING_ROUTE_CODE: Final = "NOT_FOUND"
JSON_TYPE: Final = "application/json"


@dataclass(frozen=True)
class CapabilityReport:
    accepts_sql: bool
    registry_digest: str | None


@lru_cache(maxsize=256)
def _adapter(answer_type: Any) -> TypeAdapter[Any]:
    return TypeAdapter(answer_type)


def parse_object(body: bytes, where: str) -> dict[str, Any]:
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        raise invalid_response(f"The answer to {where} is not JSON") from None
    if not isinstance(payload, dict):
        raise invalid_response(f"The answer to {where} is not a JSON object")
    decoded = decode_values(payload)
    assert isinstance(decoded, dict)
    return decoded


def parse_answer(status: int, body: bytes, answers: Mapping[int, Any], where: str) -> Any:
    if not 200 <= status < 300:
        raise error_from_body(status, body)
    payload = parse_object(body, where)
    answer_type = answers.get(status)
    if answer_type is None:
        return payload
    try:
        return _adapter(answer_type).validate_python(payload)
    except ValidationError as error:
        first = error.errors(include_url=False, include_input=False)[:1]
        place = ".".join(str(part) for part in first[0]["loc"]) if first else ""
        raise invalid_response(f"The answer to {where} does not hold the documented shape at {place}") from None


def capability_report(status: int, body: bytes) -> CapabilityReport:
    if status == 404:
        refusal = error_from_body(status, body)
        if refusal.code == MISSING_ROUTE_CODE:
            return CapabilityReport(True, None)
        raise refusal
    payload = parse_answer(status, body, {}, f"GET {CAPABILITIES_PATH}")
    listed = payload.get("capabilities")
    if not isinstance(listed, list) or not all(isinstance(item, str) for item in listed):
        raise invalid_response("The capabilities answer carries no list of capability names")
    registry = payload.get("registry")
    digest = registry.get("digest") if isinstance(registry, dict) else None
    return CapabilityReport(SQL_CAPABILITY in listed, digest if isinstance(digest, str) else None)


def refuse_sql(report: CapabilityReport) -> None:
    if not report.accepts_sql:
        raise SirannonError(ErrorCodes.SQL_NOT_ACCEPTED, SQL_REFUSED_MESSAGE)


class _HttpCore:
    def __init__(self, settings: ClientSettings) -> None:
        self.settings = settings
        self.closed = False

    def timeout_ms(self, exchange: Exchange) -> int:
        if exchange.timeout_ms is None:
            return self.settings.request_timeout_ms
        return whole_number(exchange.timeout_ms, "timeout_ms", 0)

    def headers(self, exchange: Exchange) -> dict[str, str]:
        merged = dict(self.settings.headers)
        merged["accept"] = JSON_TYPE
        for key, value in (exchange.headers or {}).items():
            merged[key.lower()] = value
        if exchange.body is not None:
            merged["content-type"] = JSON_TYPE
        return merged

    def content(self, exchange: Exchange) -> bytes | None:
        return None if exchange.body is None else encode_json(exchange.body)

    def add_chunk(self, chunks: list[bytes], chunk: bytes, total: int, where: str) -> int:
        total += len(chunk)
        ceiling = self.settings.max_response_bytes
        if ceiling > 0 and total > ceiling:
            raise invalid_response(f"The answer to {where} is larger than max_response_bytes ({ceiling})")
        chunks.append(chunk)
        return total

    def failure(self, error: Exception, where: str, timeout_ms: int) -> SirannonError:
        if self.closed:
            return closed_error()
        if isinstance(error, httpx.TimeoutException | TimeoutError):
            return SirannonError(ErrorCodes.TIMEOUT, f"{where} took longer than {timeout_ms}ms")
        return SirannonError(ErrorCodes.CONNECTION_ERROR, f"The client could not reach the server for {where}")


class SyncHttp:
    def __init__(self, settings: ClientSettings, http_client: httpx.Client | None = None) -> None:
        self._core = _HttpCore(settings)
        self._owns_client = http_client is None
        self._client = http_client if http_client is not None else httpx.Client(follow_redirects=False)
        self._capability_lock = threading.Lock()
        self._report: CapabilityReport | None = None

    def close(self) -> None:
        self._core.closed = True
        if self._owns_client:
            self._client.close()

    def send(self, exchange: Exchange) -> tuple[int, bytes]:
        if self._core.closed:
            raise closed_error()
        where = f"{exchange.method} {exchange.path}"
        timeout_ms = self._core.timeout_ms(exchange)
        seconds = None if timeout_ms == 0 else timeout_ms / 1000
        deadline = None if seconds is None else time.monotonic() + seconds
        try:
            request = self._client.build_request(
                exchange.method,
                self._core.settings.base_url + exchange.path,
                content=self._core.content(exchange),
                headers=self._core.headers(exchange),
                timeout=httpx.Timeout(seconds),
            )
            response = self._client.send(request, stream=True)
            try:
                chunks: list[bytes] = []
                total = 0
                for chunk in response.iter_bytes():
                    total = self._core.add_chunk(chunks, chunk, total, where)
                    if deadline is not None and time.monotonic() > deadline:
                        raise TimeoutError
                return response.status_code, b"".join(chunks)
            finally:
                response.close()
        except SirannonError:
            raise
        except (httpx.TransportError, TimeoutError, RuntimeError) as error:
            raise self._core.failure(error, where, timeout_ms) from None

    def capabilities(self, refresh: bool = False) -> CapabilityReport:
        with self._capability_lock:
            if self._report is None or refresh:
                status, body = self.send(Exchange(method="GET", path=CAPABILITIES_PATH))
                self._report = capability_report(status, body)
            return self._report

    def json(self, exchange: Exchange, answers: Mapping[int, Any]) -> Any:
        if exchange.sql_statement:
            refuse_sql(self.capabilities())
        status, body = self.send(exchange)
        return parse_answer(status, body, answers, f"{exchange.method} {exchange.path}")


class AsyncHttp:
    def __init__(self, settings: ClientSettings, http_client: httpx.AsyncClient | None = None) -> None:
        self._core = _HttpCore(settings)
        self._owns_client = http_client is None
        self._client = http_client if http_client is not None else httpx.AsyncClient(follow_redirects=False)
        self._in_flight: set[asyncio.Task[tuple[int, bytes]]] = set()
        self._report: CapabilityReport | None = None
        self._reading: asyncio.Task[CapabilityReport] | None = None

    @property
    def closed(self) -> bool:
        return self._core.closed

    async def aclose(self) -> None:
        self._core.closed = True
        for task in list(self._in_flight):
            task.cancel()
        if self._reading is not None:
            self._reading.cancel()
        if self._owns_client:
            await self._client.aclose()

    async def _exchange(self, exchange: Exchange, where: str, timeout_ms: int) -> tuple[int, bytes]:
        seconds = None if timeout_ms == 0 else timeout_ms / 1000
        try:
            async with asyncio.timeout(seconds):
                request = self._client.build_request(
                    exchange.method,
                    self._core.settings.base_url + exchange.path,
                    content=self._core.content(exchange),
                    headers=self._core.headers(exchange),
                    timeout=httpx.Timeout(seconds),
                )
                response = await self._client.send(request, stream=True)
                try:
                    chunks: list[bytes] = []
                    total = 0
                    async for chunk in response.aiter_bytes():
                        total = self._core.add_chunk(chunks, chunk, total, where)
                    return response.status_code, b"".join(chunks)
                finally:
                    await response.aclose()
        except SirannonError:
            raise
        except (httpx.TransportError, TimeoutError, RuntimeError) as error:
            raise self._core.failure(error, where, timeout_ms) from None

    async def send(self, exchange: Exchange) -> tuple[int, bytes]:
        if self._core.closed:
            raise closed_error()
        where = f"{exchange.method} {exchange.path}"
        task = asyncio.ensure_future(self._exchange(exchange, where, self._core.timeout_ms(exchange)))
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)
        return await _awaited(task, self)

    async def capabilities(self, refresh: bool = False) -> CapabilityReport:
        if self._core.closed:
            raise closed_error()
        if refresh:
            self._report = None
        if self._report is not None:
            return self._report
        if self._reading is None:
            self._reading = shared_task(self._read_capabilities())
        reading = self._reading
        try:
            report = await _awaited(asyncio.shield(reading), self)
        finally:
            if self._reading is reading and reading.done():
                self._reading = None
        return report

    async def _read_capabilities(self) -> CapabilityReport:
        status, body = await self.send(Exchange(method="GET", path=CAPABILITIES_PATH))
        report = capability_report(status, body)
        self._report = report
        return report

    async def json(self, exchange: Exchange, answers: Mapping[int, Any]) -> Any:
        if exchange.sql_statement:
            refuse_sql(await self.capabilities())
        status, body = await self.send(exchange)
        return parse_answer(status, body, answers, f"{exchange.method} {exchange.path}")


async def _awaited(awaitable: asyncio.Future[AnswerT], owner: AsyncHttp) -> AnswerT:
    try:
        return await awaitable
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if owner.closed and (current is None or current.cancelling() == 0):
            raise closed_error() from None
        raise


class SyncOperationsBase:
    _transport: SyncHttp


class AsyncOperationsBase:
    _transport: AsyncHttp
