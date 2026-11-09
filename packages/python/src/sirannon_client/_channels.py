from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, TypeAlias

from pydantic import BaseModel

from sirannon_client._errors import ErrorCodes, SirannonError, invalid_argument, invalid_response
from sirannon_client._exchange import Exchange, path_segment
from sirannon_client._http import AsyncHttp, parse_answer
from sirannon_client._values import decode_values, encode_values
from sirannon_client._ws import AsyncWebSocketTransport

Params: TypeAlias = Sequence[Any] | Mapping[str, Any]
ConcernArg: TypeAlias = str | Mapping[str, Any] | BaseModel | None


def concern(value: ConcernArg, name: str) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return {"level": value}
    if isinstance(value, BaseModel):
        return value.model_dump(mode="python", by_alias=True, exclude_none=True)
    if isinstance(value, Mapping):
        return dict(value)
    raise invalid_argument(f"{name} must be a level name, a mapping, or a model")


def bind_values(params: Params | None) -> Any:
    if params is None:
        return None
    if isinstance(params, str | bytes | bytearray):
        raise invalid_argument("Bind parameters must be a sequence or a mapping of values")
    return encode_values(params if isinstance(params, Mapping) else list(params))


def _rows(payload: Any) -> list[dict[str, Any]]:
    rows = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise invalid_response("The answer carried no list of row objects")
    decoded = decode_values(rows)
    assert isinstance(decoded, list)
    return decoded


def _object(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise invalid_response("The answer is not a JSON object")
    return payload


class Channel(Protocol):
    carries_read_concern: bool

    async def query(self, sql: str, params: Params | None, read_concern: dict[str, Any] | None) -> Any: ...

    async def execute(self, sql: str, params: Params | None) -> dict[str, Any]: ...

    async def transaction(self, statements: list[dict[str, Any]]) -> dict[str, Any]: ...

    async def batch(
        self, sql: str, params_batch: list[Any], write_concern: dict[str, Any] | None
    ) -> dict[str, Any]: ...

    async def load(
        self, sql: str, params_batch: list[Any], durability: str | None, checkpoint: bool | None
    ) -> dict[str, Any]: ...

    async def query_named(
        self, name: str, args: Mapping[str, Any] | None, read_concern: dict[str, Any] | None
    ) -> list[dict[str, Any]]: ...

    async def execute_named(
        self, name: str, args: Mapping[str, Any] | None, write_concern: dict[str, Any] | None
    ) -> dict[str, Any]: ...


def _without_none(fields: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in fields.items() if value is not None}


class HttpChannel:
    carries_read_concern = True

    def __init__(self, http: AsyncHttp, database_id: str) -> None:
        self._http = http
        self._base = f"/db/{path_segment(database_id)}"

    async def _post(self, path: str, body: dict[str, Any]) -> Any:
        where = f"POST {self._base}{path}"
        status, answer = await self._http.send(Exchange(method="POST", path=self._base + path, body=body))
        return parse_answer(status, answer, {}, where)

    async def query(self, sql: str, params: Params | None, read_concern: dict[str, Any] | None) -> Any:
        body = _without_none({"sql": sql, "params": bind_values(params), "readConcern": read_concern})
        return _rows(await self._post("/query", body))

    async def execute(self, sql: str, params: Params | None) -> dict[str, Any]:
        return _object(await self._post("/execute", _without_none({"sql": sql, "params": bind_values(params)})))

    async def transaction(self, statements: list[dict[str, Any]]) -> dict[str, Any]:
        return _object(await self._post("/transaction", {"statements": statements}))

    async def batch(self, sql: str, params_batch: list[Any], write_concern: dict[str, Any] | None) -> dict[str, Any]:
        body = _without_none({"sql": sql, "paramsBatch": params_batch, "writeConcern": write_concern})
        return _object(await self._post("/batch", body))

    async def load(
        self, sql: str, params_batch: list[Any], durability: str | None, checkpoint: bool | None
    ) -> dict[str, Any]:
        body = _without_none(
            {"sql": sql, "paramsBatch": params_batch, "durability": durability, "checkpoint": checkpoint}
        )
        return _object(await self._post("/load", body))

    async def query_named(
        self, name: str, args: Mapping[str, Any] | None, read_concern: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        body = _without_none({"args": None if args is None else encode_values(args), "readConcern": read_concern})
        return _rows(await self._post(f"/query/{path_segment(name)}", body))

    async def execute_named(
        self, name: str, args: Mapping[str, Any] | None, write_concern: dict[str, Any] | None
    ) -> dict[str, Any]:
        body = _without_none({"args": None if args is None else encode_values(args), "writeConcern": write_concern})
        return _object(await self._post(f"/execute/{path_segment(name)}", body))


class WebSocketChannel:
    carries_read_concern = True

    def __init__(self, socket: AsyncWebSocketTransport) -> None:
        self._socket = socket

    async def _ask(self, kind: str, fields: dict[str, Any]) -> Any:
        return await self._socket.request({"type": kind, "id": self._socket.next_id(), **_without_none(fields)})

    async def query(self, sql: str, params: Params | None, read_concern: dict[str, Any] | None) -> Any:
        return _rows(await self._ask("query", {"sql": sql, "params": bind_values(params), "readConcern": read_concern}))

    async def execute(self, sql: str, params: Params | None) -> dict[str, Any]:
        return _object(await self._ask("execute", {"sql": sql, "params": bind_values(params)}))

    async def transaction(self, statements: list[dict[str, Any]]) -> dict[str, Any]:
        return _object(await self._ask("transaction", {"statements": statements}))

    async def batch(self, sql: str, params_batch: list[Any], write_concern: dict[str, Any] | None) -> dict[str, Any]:
        return _object(
            await self._ask("batch", {"sql": sql, "paramsBatch": params_batch, "writeConcern": write_concern})
        )

    async def load(
        self, sql: str, params_batch: list[Any], durability: str | None, checkpoint: bool | None
    ) -> dict[str, Any]:
        fields = {"sql": sql, "paramsBatch": params_batch, "durability": durability, "checkpoint": checkpoint}
        return _object(await self._ask("load", fields))

    async def query_named(
        self, name: str, args: Mapping[str, Any] | None, read_concern: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        fields = {"name": name, "args": None if args is None else encode_values(args), "readConcern": read_concern}
        return _rows(await self._ask("query", fields))

    async def execute_named(
        self, name: str, args: Mapping[str, Any] | None, write_concern: dict[str, Any] | None
    ) -> dict[str, Any]:
        fields = {"name": name, "args": None if args is None else encode_values(args), "writeConcern": write_concern}
        return _object(await self._ask("execute", fields))


def transport_unsupported(feature: str) -> SirannonError:
    return SirannonError(ErrorCodes.TRANSPORT_ERROR, f"{feature} need the WebSocket transport")
