from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Final, Protocol

from sirannon_client._errors import ErrorCodes, SirannonError, invalid_response
from sirannon_client._events import (
    ChangeEvent,
    ErrorHandler,
    decode_event,
    invoke_callback,
    parse_seq,
    report_failure,
)
from sirannon_client._values import decode_values, encode_values

UNREACHABLE_CODES: Final = frozenset({ErrorCodes.CONNECTION_ERROR, ErrorCodes.TIMEOUT})
DUPLICATE_SUBSCRIPTION: Final = "DUPLICATE_SUBSCRIPTION"
REGISTRY_MISMATCH: Final = "REGISTRY_MISMATCH"

Request = Callable[[dict[str, Any]], Awaitable[Any]]
DigestSource = Callable[[bool], Awaitable[str | None]]


@dataclass
class ActiveSubscription:
    table: str
    filter: dict[str, Any] | None
    callback: Callable[[ChangeEvent], Any]
    on_error: ErrorHandler | None
    on_reset: Callable[[], Any] | None
    last_seq: int | None = None
    epoch: str | None = None
    established: bool = False

    def subscribe_message(self, subscription_id: str) -> dict[str, Any]:
        message: dict[str, Any] = {"type": "subscribe", "id": subscription_id, "table": self.table}
        if self.filter is not None:
            message["filter"] = encode_values(self.filter)
        if self.last_seq is not None:
            message["sinceSeq"] = str(self.last_seq)
        if self.epoch is not None:
            message["epoch"] = self.epoch
        return message

    def confirmed(self, message: dict[str, Any]) -> None:
        self.established = True
        epoch = message.get("epoch")
        if isinstance(epoch, str):
            self.epoch = epoch
        baseline = parse_seq(message.get("seq"))
        if message.get("resync") is True:
            self.last_seq = baseline
            if self.on_reset is not None:
                invoke_callback(self.on_reset, self.on_error)
        elif self.last_seq is None and baseline is not None:
            self.last_seq = baseline

    def deliver(self, wire: Any) -> None:
        try:
            event = decode_event(wire)
        except SirannonError as error:
            report_failure(self.on_error, error)
            return
        if self.last_seq is not None and event.seq <= self.last_seq:
            return
        self.last_seq = event.seq
        invoke_callback(lambda: self.callback(event), self.on_error)


async def restore_subscriptions(
    subscriptions: dict[str, ActiveSubscription],
    request: Request,
    is_closed: Callable[[], bool],
) -> SirannonError | None:
    unreachable: SirannonError | None = None
    for subscription_id, subscription in list(subscriptions.items()):
        if is_closed():
            break
        if subscription.established or subscriptions.get(subscription_id) is not subscription:
            continue
        try:
            await request(subscription.subscribe_message(subscription_id))
        except SirannonError as error:
            if error.code in UNREACHABLE_CODES:
                unreachable = error
                continue
            if error.code == DUPLICATE_SUBSCRIPTION:
                subscription.established = True
                continue
            if subscriptions.get(subscription_id) is subscription:
                del subscriptions[subscription_id]
                report_failure(subscription.on_error, error)
    return unreachable


class LiveHandlers(Protocol):
    def on_rows(self, rows: list[dict[str, Any]]) -> None: ...

    def on_ops(self, ops: list[dict[str, Any]]) -> None: ...

    def on_revalidating(self) -> None: ...

    def on_error(self, error: SirannonError) -> None: ...


@dataclass
class ActiveLive:
    name: str
    args: dict[str, Any] | None
    handlers: LiveHandlers
    digest: DigestSource | None
    established: bool = False

    def subscribe_message(self, live_id: str, registry_digest: str | None) -> dict[str, Any]:
        message: dict[str, Any] = {"type": "subscribe", "id": live_id, "name": self.name}
        if self.args is not None:
            message["args"] = encode_values(self.args)
        if registry_digest is not None:
            message["registryDigest"] = registry_digest
        return message

    async def send(self, live_id: str, request: Request) -> None:
        digest = None if self.digest is None else await self.digest(False)
        try:
            await request(self.subscribe_message(live_id, digest))
        except SirannonError as error:
            if error.code != REGISTRY_MISMATCH or self.digest is None:
                raise
            await request(self.subscribe_message(live_id, await self.digest(True)))
        self.established = True

    def deliver(self, message: dict[str, Any]) -> None:
        try:
            rows = message.get("rows")
            ops = message.get("ops")
            if isinstance(rows, list):
                self.handlers.on_rows(decoded_rows(rows))
            elif isinstance(ops, list):
                self.handlers.on_ops([decoded_op(op) for op in ops])
            elif message.get("revalidating") is True:
                self.handlers.on_revalidating()
            else:
                raise invalid_response("A live message carried no rows, operations, or status")
        except SirannonError as error:
            self.handlers.on_error(error)


def decoded_rows(rows: Any) -> list[dict[str, Any]]:
    decoded = decode_values(rows)
    if not isinstance(decoded, list) or not all(isinstance(row, dict) for row in decoded):
        raise invalid_response("A live message carried rows that are not objects")
    return decoded


def decoded_op(op: Any) -> dict[str, Any]:
    if not isinstance(op, dict) or op.get("op") not in ("insert", "update", "delete"):
        raise invalid_response("A live message carried an operation that is not insert, update, or delete")
    index = op.get("index")
    if isinstance(index, bool) or not isinstance(index, int):
        raise invalid_response("A live operation carried no whole-number index")
    if op["op"] == "delete":
        return {"op": "delete", "index": index}
    row = decode_values(op.get("row"))
    if not isinstance(row, dict):
        raise invalid_response("A live operation carried no row object")
    return {"op": op["op"], "index": index, "row": row}


async def restart_live(lives: dict[str, ActiveLive], request: Request, is_closed: Callable[[], bool]) -> None:
    unreachable: SirannonError | None = None
    for live_id, live in list(lives.items()):
        if is_closed() or live.established:
            continue
        try:
            await live.send(live_id, request)
        except SirannonError as error:
            if error.code in UNREACHABLE_CODES:
                unreachable = error
                continue
            if lives.get(live_id) is live:
                del lives[live_id]
                live.handlers.on_error(error)
    if unreachable is not None and not is_closed():
        raise unreachable
