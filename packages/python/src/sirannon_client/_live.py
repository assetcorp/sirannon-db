from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Generic, Literal, TypeVar, cast

from sirannon_client._errors import SirannonError, invalid_response
from sirannon_client._events import ErrorHandler, invoke_callback

RowT = TypeVar("RowT")
LiveStatus = Literal["pending", "ready", "error"]
UpdateKind = Literal["rows", "ops", "revalidating", "error"]


@dataclass(frozen=True)
class LiveState(Generic[RowT]):
    """What a live query holds at one moment."""

    status: LiveStatus
    """`pending` before the first rows arrive, `ready` once they have, and `error` after a failure."""
    rows: list[RowT] = field(default_factory=list)
    """The current rows, in the order that the read returns them."""
    revalidating: bool = False
    """True while the connection is down, so that the rows can be out of date."""
    error: SirannonError | None = None
    """The failure that ended the query, when `status` is `error`."""


@dataclass(frozen=True)
class LiveUpdate(Generic[RowT]):
    """One change to a live query, which a listener receives."""

    kind: UpdateKind
    """`rows` for a fresh result, `ops` for edits, `revalidating` after a disconnect, and `error`."""
    state: LiveState[RowT]
    """The state after the update."""
    ops: list[dict[str, Any]] = field(default_factory=list)
    """The edits that the server sent, in the order that the client applied them, for an `ops` update."""


class LiveRows(Generic[RowT]):
    def __init__(self, on_error: ErrorHandler | None) -> None:
        self._state: LiveState[RowT] = LiveState("pending")
        self._listeners: list[Callable[[LiveUpdate[RowT]], Any]] = []
        self._on_error = on_error
        self.closed = False

    @property
    def state(self) -> LiveState[RowT]:
        return self._state

    def listen(self, listener: Callable[[LiveUpdate[RowT]], Any]) -> Callable[[], None]:
        self._listeners.append(listener)

        def remove() -> None:
            if listener in self._listeners:
                self._listeners.remove(listener)

        return remove

    def stop(self) -> None:
        self.closed = True
        self._listeners.clear()

    def on_rows(self, rows: list[dict[str, Any]]) -> None:
        if not self.closed:
            self._publish(LiveState("ready", cast(list[RowT], list(rows))), "rows")

    def on_ops(self, ops: list[dict[str, Any]]) -> None:
        if self.closed:
            return
        if self._state.status != "ready":
            self.on_error(invalid_response("A live query received row operations before its first result"))
            return
        rows: list[Any] = list(self._state.rows)
        for op in ops:
            index = op["index"]
            highest = len(rows) if op["op"] == "insert" else len(rows) - 1
            if index < 0 or index > highest:
                self.on_error(invalid_response("A live query received an operation outside its rows"))
                return
            if op["op"] == "insert":
                rows.insert(index, op["row"])
            elif op["op"] == "update":
                rows[index] = op["row"]
            else:
                del rows[index]
        self._publish(LiveState("ready", rows), "ops", ops)

    def on_revalidating(self) -> None:
        if self.closed or self._state.status != "ready" or self._state.revalidating:
            return
        self._publish(LiveState("ready", self._state.rows, revalidating=True), "revalidating")

    def on_error(self, error: SirannonError) -> None:
        if not self.closed:
            self._publish(LiveState("error", error=error), "error")

    def _publish(self, state: LiveState[RowT], kind: UpdateKind, ops: list[dict[str, Any]] | None = None) -> None:
        self._state = state
        update = LiveUpdate(kind, state, ops or [])
        for listener in list(self._listeners):
            invoke_callback(partial(listener, update), self._on_error)
