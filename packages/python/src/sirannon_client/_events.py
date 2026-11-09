from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from sirannon_client._errors import SirannonError, invalid_response
from sirannon_client._pacing import backlog_for
from sirannon_client._values import decode_values

ChangeType = Literal["insert", "update", "delete"]
ErrorHandler = Callable[[BaseException], Any]
_background: set[asyncio.Future[Any]] = set()


@dataclass(frozen=True)
class ChangeEvent:
    """One row change, as the client passes it to a subscription callback."""

    type: str
    """`insert`, `update`, or `delete`."""
    table: str
    """The table that the row belongs to."""
    row: dict[str, Any]
    """The row after the change, or the deleted row for a delete, with integers and BLOBs decoded."""
    seq: int
    """The position of the change in the server's change log."""
    timestamp: float
    """When the change happened, in milliseconds since the Unix epoch."""
    old_row: dict[str, Any] | None = None
    """The row before an update, where the server sends it."""
    extra: dict[str, Any] = field(default_factory=dict)
    """The device sync fields that the server sends, such as `hlc`, `origin`, `rowId`, `txId`, and `txEnd`."""


EXTRA_EVENT_FIELDS = ("hlc", "origin", "rowId", "txId", "txEnd")
DECIMAL_SEQ = re.compile(r"[0-9]{1,20}")


def parse_seq(value: Any) -> int | None:
    if isinstance(value, str) and DECIMAL_SEQ.fullmatch(value) is not None:
        return int(value)
    return None


def decode_event(wire: Any) -> ChangeEvent:
    if not isinstance(wire, dict):
        raise invalid_response("A change message carried no event object")
    kind, table, row, seq, timestamp = (wire.get(key) for key in ("type", "table", "row", "seq", "timestamp"))
    if not isinstance(kind, str) or not isinstance(table, str) or not isinstance(row, dict):
        raise invalid_response("A change event carried no type, table, or row")
    if not isinstance(seq, str) or DECIMAL_SEQ.fullmatch(seq) is None:
        raise invalid_response("A change event carried no decimal seq")
    if isinstance(timestamp, bool) or not isinstance(timestamp, int | float):
        raise invalid_response("A change event carried no timestamp")
    old_row = wire.get("oldRow")
    if old_row is not None and not isinstance(old_row, dict):
        raise invalid_response("A change event carried an oldRow that is not an object")
    return ChangeEvent(
        type=kind,
        table=table,
        row=decode_values(row),
        seq=int(seq),
        timestamp=timestamp,
        old_row=None if old_row is None else decode_values(old_row),
        extra={key: wire[key] for key in EXTRA_EVENT_FIELDS if key in wire},
    )


def report_failure(on_error: ErrorHandler | None, error: BaseException) -> None:
    if on_error is None:
        return
    try:
        outcome = on_error(error)
        if inspect.isawaitable(outcome):
            _keep(asyncio.ensure_future(outcome), None)
    except Exception:
        return


def _settled(task: asyncio.Future[Any], on_error: ErrorHandler | None) -> None:
    _background.discard(task)
    backlog_for(task.get_loop()).finished()
    if task.cancelled():
        return
    failure = task.exception()
    if failure is not None and on_error is not None:
        report_failure(on_error, failure)


def _keep(task: asyncio.Future[Any], on_error: ErrorHandler | None) -> None:
    _background.add(task)
    backlog_for(task.get_loop()).added()
    task.add_done_callback(lambda done: _settled(done, on_error))


def invoke_callback(call: Callable[[], Any], on_error: ErrorHandler | None) -> None:
    try:
        outcome = call()
    except Exception as error:
        report_failure(on_error, error)
        return
    if inspect.isawaitable(outcome):
        _keep(asyncio.ensure_future(outcome), on_error)


def remote_error(payload: Any) -> SirannonError:
    if isinstance(payload, dict):
        code, message = payload.get("code"), payload.get("message")
        if isinstance(code, str) and isinstance(message, str):
            details = payload.get("details")
            return SirannonError(code, message, details if isinstance(details, dict) else None)
    return SirannonError("UNKNOWN_ERROR", "The server sent an error frame that carried no error")
