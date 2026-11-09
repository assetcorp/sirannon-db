from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from typing import Any, Final

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus, WebSocketException
from websockets.typing import Subprotocol

from sirannon_client._errors import ErrorCodes, SirannonError, closed_error, error_from_body
from sirannon_client._events import ChangeEvent, ErrorHandler, remote_error, report_failure
from sirannon_client._options import ClientSettings
from sirannon_client._pacing import callbacks_paced
from sirannon_client._tasks import shared_task
from sirannon_client._ws_state import (
    ActiveLive,
    ActiveSubscription,
    DigestSource,
    LiveHandlers,
    restart_live,
    restore_subscriptions,
)

CLOSE_UNAUTHENTICATED: Final = 4401
CLOSE_FORBIDDEN: Final = 4403
REFUSAL_CODES: Final = range(4000, 4100)
NORMAL_CLOSE: Final = 1000


def refusal_for(code: int | None, reason: str) -> SirannonError | None:
    if code is None or (code not in REFUSAL_CODES and code not in (CLOSE_UNAUTHENTICATED, CLOSE_FORBIDDEN)):
        return None
    kind = ErrorCodes.FORBIDDEN if code == CLOSE_FORBIDDEN else ErrorCodes.UNAUTHORIZED
    return SirannonError(kind, f"The server refused the WebSocket connection: {reason or 'no reason given'}")


class AsyncWebSocketTransport:
    def __init__(self, url: str, settings: ClientSettings) -> None:
        self._url = url
        self._settings = settings
        self._socket: ClientConnection | None = None
        self._connecting: asyncio.Task[None] | None = None
        self._reader: asyncio.Task[None] | None = None
        self._reconnecting: asyncio.Task[None] | None = None
        self._restoring: asyncio.Task[None] | None = None
        self._pending: dict[str, asyncio.Future[Any]] = {}
        self._notices: set[asyncio.Task[None]] = set()
        self._subscriptions: dict[str, ActiveSubscription] = {}
        self._lives: dict[str, ActiveLive] = {}
        self._refusal: SirannonError | None = None
        self._restored = True
        self._closed = False
        self._counter = 0

    @property
    def closed(self) -> bool:
        return self._closed

    def next_id(self) -> str:
        self._counter += 1
        return f"c_{self._counter}_{int(time.time() * 1000)}"

    async def request(self, message: dict[str, Any]) -> Any:
        await self._ensure_connected()
        return await self._send(message)

    async def subscribe(
        self,
        table: str,
        filter_values: dict[str, Any] | None,
        callback: Callable[[ChangeEvent], Any],
        on_error: ErrorHandler | None,
        on_reset: Callable[[], Any] | None,
    ) -> str:
        await self._ensure_connected()
        subscription_id = self.next_id()
        subscription = ActiveSubscription(table, filter_values, callback, on_error, on_reset)
        self._subscriptions[subscription_id] = subscription
        try:
            await self._send(subscription.subscribe_message(subscription_id))
        except BaseException:
            self._subscriptions.pop(subscription_id, None)
            raise
        return subscription_id

    async def open_live(
        self,
        name: str,
        args: dict[str, Any] | None,
        handlers: LiveHandlers,
        digest: DigestSource | None,
    ) -> str:
        await self._ensure_connected()
        live_id = self.next_id()
        live = ActiveLive(name, args, handlers, digest)
        self._lives[live_id] = live
        try:
            await live.send(live_id, self._send)
        except BaseException:
            self._lives.pop(live_id, None)
            raise
        return live_id

    def holds(self, subscription_id: str) -> bool:
        return subscription_id in self._subscriptions or subscription_id in self._lives

    def unsubscribe(self, subscription_id: str) -> None:
        self._subscriptions.pop(subscription_id, None)
        self._lives.pop(subscription_id, None)
        socket = self._socket
        if socket is not None and not self._closed:
            task = asyncio.ensure_future(self._quietly_send(socket, {"type": "unsubscribe", "id": subscription_id}))
            self._notices.add(task)
            task.add_done_callback(self._notices.discard)

    async def ack(self, device_id: str, seq: int) -> Any:
        await self._ensure_connected()
        await self._restore()
        return await self._send({"type": "ack", "id": self.next_id(), "deviceId": device_id, "seq": str(seq)})

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        for task in (self._reconnecting, self._restoring, self._connecting):
            if task is not None:
                task.cancel()
        self._fail_pending(closed_error())
        self._subscriptions.clear()
        self._lives.clear()
        socket, self._socket = self._socket, None
        if socket is not None:
            await socket.close(NORMAL_CLOSE, "Client closed")
        if self._reader is not None:
            self._reader.cancel()

    async def _ensure_connected(self) -> None:
        if self._closed:
            raise closed_error()
        if self._refusal is not None:
            raise self._refusal
        if self._socket is not None:
            return
        if self._connecting is None or self._connecting.done():
            self._connecting = shared_task(self._connect())
        connecting = self._connecting
        try:
            await asyncio.shield(connecting)
        except asyncio.CancelledError:
            if self._cancelled_by_close():
                raise closed_error() from None
            raise

    def _cancelled_by_close(self) -> bool:
        return self._closed and _not_cancelled_itself()

    async def _connect(self) -> None:
        timeout_ms = self._settings.request_timeout_ms
        protocols = self._settings.offered_protocols
        try:
            async with asyncio.timeout(None if timeout_ms == 0 else timeout_ms / 1000):
                socket = await connect(
                    self._url,
                    subprotocols=None if protocols is None else [Subprotocol(item) for item in protocols],
                    additional_headers=self._settings.headers or None,
                    max_size=self._settings.max_response_bytes or None,
                    open_timeout=None,
                    compression=None,
                )
        except InvalidStatus as error:
            raise error_from_body(error.response.status_code, bytes(error.response.body or b"")) from None
        except TimeoutError:
            raise SirannonError(
                ErrorCodes.TIMEOUT, f"The WebSocket handshake took longer than {timeout_ms}ms"
            ) from None
        except (OSError, InvalidHandshake, ConnectionClosed):
            raise SirannonError(
                ErrorCodes.CONNECTION_ERROR, "The client could not open a WebSocket connection"
            ) from None
        if self._closed:
            await socket.close(NORMAL_CLOSE, "Client closed")
            raise closed_error()
        self._socket = socket
        self._reader = shared_task(self._read(socket))

    async def _send(self, message: dict[str, Any]) -> Any:
        if self._refusal is not None:
            raise self._refusal
        socket = self._socket
        if socket is None:
            raise SirannonError(ErrorCodes.CONNECTION_ERROR, "The WebSocket is not connected")
        request_id = message["id"]
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        timeout_ms = self._settings.request_timeout_ms
        try:
            await socket.send(json.dumps(message, ensure_ascii=False, allow_nan=False, separators=(",", ":")))
            async with asyncio.timeout(None if timeout_ms == 0 else timeout_ms / 1000):
                return await future
        except TimeoutError:
            raise SirannonError(ErrorCodes.TIMEOUT, f"The server sent no answer within {timeout_ms}ms") from None
        except ConnectionClosed:
            raise self._refusal or SirannonError(ErrorCodes.CONNECTION_ERROR, "The WebSocket disconnected") from None
        finally:
            if self._pending.get(request_id) is future:
                del self._pending[request_id]
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    async def _quietly_send(self, socket: ClientConnection, message: dict[str, Any]) -> None:
        try:
            await socket.send(json.dumps(message))
        except (ConnectionClosed, OSError):
            return

    async def _read(self, socket: ClientConnection) -> None:
        try:
            async for raw in socket:
                if isinstance(raw, str):
                    self._route(raw)
                await callbacks_paced()
        except ConnectionClosed:
            pass
        finally:
            if self._socket is socket:
                self._socket = None
                self._disconnected(socket.close_code, socket.close_reason or "")

    def _route(self, raw: str) -> None:
        try:
            message = json.loads(raw)
        except ValueError:
            return
        if not isinstance(message, dict) or not isinstance(message.get("id"), str):
            return
        kind, message_id = message.get("type"), message["id"]
        if kind == "result":
            self._settle(message_id, result=message.get("data"))
        elif kind == "subscribed":
            subscription = self._subscriptions.get(message_id)
            if subscription is not None:
                subscription.confirmed(message)
            live = self._lives.get(message_id)
            if live is not None:
                live.deliver({"rows": message.get("rows", [])})
            self._settle(message_id, result=None)
        elif kind == "error":
            failure = remote_error(message.get("error"))
            if not self._settle(message_id, error=failure) and message_id in self._lives:
                self._lives[message_id].handlers.on_error(failure)
        elif kind in ("change", "changes"):
            subscription = self._subscriptions.get(message_id)
            events = [message.get("event")] if kind == "change" else message.get("events")
            if subscription is not None and isinstance(events, list):
                for event in events:
                    subscription.deliver(event)
        elif kind == "live" and message_id in self._lives:
            self._lives[message_id].deliver(message)

    def _settle(self, message_id: str, result: Any = None, error: SirannonError | None = None) -> bool:
        future = self._pending.pop(message_id, None)
        if future is None or future.done():
            return False
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(result)
        return True

    def _fail_pending(self, error: SirannonError) -> None:
        pending, self._pending = self._pending, {}
        for future in pending.values():
            if not future.done():
                future.set_exception(error)

    def _restorable(self) -> int:
        return len(self._subscriptions) + len(self._lives)

    def _disconnected(self, code: int | None, reason: str) -> None:
        refusal = refusal_for(code, reason)
        if refusal is not None:
            self._refusal = refusal
        self._fail_pending(self._refusal or SirannonError(ErrorCodes.CONNECTION_ERROR, "The WebSocket disconnected"))
        for live in self._lives.values():
            live.established = False
            live.handlers.on_revalidating()
        for subscription in self._subscriptions.values():
            subscription.established = False
        if self._restorable() > 0:
            self._restored = False
        if self._refusal is not None:
            self._drop_all(self._refusal)
            return
        reconnecting = self._reconnecting is not None and not self._reconnecting.done()
        if self._settings.auto_reconnect and not self._closed and self._restorable() > 0 and not reconnecting:
            self._reconnecting = shared_task(self._reconnect())

    def _drop_all(self, error: SirannonError) -> None:
        subscriptions = list(self._subscriptions.values())
        lives = list(self._lives.values())
        self._subscriptions.clear()
        self._lives.clear()
        for subscription in subscriptions:
            report_failure(subscription.on_error, error)
        for live in lives:
            live.handlers.on_error(error)

    async def _reconnect(self) -> None:
        while not self._closed and self._refusal is None and self._restorable() > 0:
            await asyncio.sleep(self._settings.reconnect_interval_ms / 1000)
            try:
                await self._ensure_connected()
                await self._restore()
            except (SirannonError, OSError, WebSocketException):
                continue
            if self._socket is not None and self._restored:
                return

    async def _restore(self) -> None:
        if self._restored:
            return
        if self._restoring is None or self._restoring.done():
            self._restoring = shared_task(self._restore_all())
        await asyncio.shield(self._restoring)

    async def _restore_all(self) -> None:
        self._restored = True
        try:
            unreachable = await restore_subscriptions(self._subscriptions, self._send, lambda: self._closed)
            await restart_live(self._lives, self._send, lambda: self._closed)
            if unreachable is not None and not self._closed:
                raise unreachable
        except BaseException:
            self._restored = False
            raise


def _not_cancelled_itself() -> bool:
    current = asyncio.current_task()
    return current is None or current.cancelling() == 0
