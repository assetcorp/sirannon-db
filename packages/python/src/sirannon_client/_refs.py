from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from sirannon_client._errors import invalid_argument

ArgsT = TypeVar("ArgsT", bound=Mapping[str, Any])
RowT = TypeVar("RowT", bound=Mapping[str, Any])


@dataclass(frozen=True)
class BoundRead(Generic[RowT]):
    """A registered read with its arguments, which calling a `ReadRef` returns."""

    name: str
    """The name under which the server registers the read."""
    args: Mapping[str, Any]
    """The arguments that the read receives."""


@dataclass(frozen=True)
class BoundWrite:
    """A registered write with its arguments, which calling a `WriteRef` returns."""

    name: str
    """The name under which the server registers the write."""
    args: Mapping[str, Any]
    """The arguments that the write receives."""


@dataclass(frozen=True)
class ReadRef(Generic[ArgsT, RowT]):
    """A registered read on the server, typed with the arguments that it takes and the rows that it returns.

    `sirannon-codegen --python` writes one of these for every registered read. Pass it to `query` or `live`
    with its arguments, or call it with them first, as in `query(ref({"id": 1}))`, so that mypy checks every
    key of a dict literal against the read.
    """

    name: str
    """The name under which the server registers the read."""

    def __call__(self, args: ArgsT) -> BoundRead[RowT]:
        """Returns the read with `args`, ready to pass to `query` or `live`."""
        return BoundRead(self.name, args)


@dataclass(frozen=True)
class WriteRef(Generic[ArgsT]):
    """A registered write on the server, typed with the arguments that it takes.

    `sirannon-codegen --python` writes one of these for every registered write. Pass it to `execute` with its
    arguments, or call it with them first, as in `execute(ref({"id": 1}))`, so that mypy checks every key of a
    dict literal against the write.
    """

    name: str
    """The name under which the server registers the write."""

    def __call__(self, args: ArgsT) -> BoundWrite:
        """Returns the write with `args`, ready to pass to `execute`."""
        return BoundWrite(self.name, args)


def read_call(statement: object, params: object) -> tuple[str, Any] | None:
    if isinstance(statement, ReadRef):
        return statement.name, params
    if isinstance(statement, BoundRead):
        return statement.name, _carried(statement.args, params)
    return None


def write_call(statement: object, params: object) -> tuple[str, Any] | None:
    if isinstance(statement, WriteRef):
        return statement.name, params
    if isinstance(statement, BoundWrite):
        return statement.name, _carried(statement.args, params)
    return None


def _carried(args: Mapping[str, Any], params: object) -> Mapping[str, Any]:
    if params is not None:
        raise invalid_argument("A called operation reference carries its arguments, so pass no params with it")
    return args
