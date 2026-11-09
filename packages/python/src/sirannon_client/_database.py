from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable, Mapping, Sequence
from functools import partial
from typing import Any, Final, TypeVar, cast, overload

from pydantic import BaseModel, ValidationError

from sirannon_client._channels import Channel, ConcernArg, Params, bind_values, concern, transport_unsupported
from sirannon_client._contracts import CapabilitySource, Streams
from sirannon_client._errors import closed_error, invalid_argument, invalid_response
from sirannon_client._events import ChangeEvent, ErrorHandler
from sirannon_client._http import refuse_sql
from sirannon_client._live import LiveRows, LiveUpdate
from sirannon_client._refs import BoundRead, BoundWrite, ReadRef, RowT, WriteRef, read_call, write_call
from sirannon_client.models import AckResponse, ExecuteResponse, LoadResponse

DEFAULT_LOAD_BATCH_SIZE: Final = 1000
ModelT = TypeVar("ModelT", bound=BaseModel)


def validated(model: type[ModelT], payload: Any) -> ModelT:
    try:
        return model.model_validate(payload)
    except ValidationError:
        raise invalid_response(f"The answer does not hold the shape of {model.__name__}") from None


def results_of(payload: Mapping[str, Any]) -> list[ExecuteResponse]:
    results = payload.get("results")
    if not isinstance(results, list):
        raise invalid_response("The answer carried no list of results")
    return [validated(ExecuteResponse, result) for result in results]


def statement_list(statements: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    listed: list[dict[str, Any]] = []
    for statement in statements:
        sql = statement.get("sql") if isinstance(statement, Mapping) else None
        if not isinstance(sql, str):
            raise invalid_argument("Every statement of a transaction needs an sql string")
        params = bind_values(statement.get("params"))
        listed.append({"sql": sql} if params is None else {"sql": sql, "params": params})
    return listed


class LoadPlanner:
    def __init__(self, batch_size: int) -> None:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise invalid_argument("load_all batch_size must be a positive whole number")
        self._size = batch_size
        self._held: list[Params] | None = None
        self._batch: list[Params] = []
        self._rows_loaded = 0
        self._changes = 0

    def add(self, row: Params) -> list[Params] | None:
        self._batch.append(row)
        if len(self._batch) < self._size:
            return None
        ready, self._held, self._batch = self._held, self._batch, []
        return ready

    def finish(self) -> list[tuple[list[Params], bool]]:
        if self._batch:
            pending = [] if self._held is None else [(self._held, False)]
            return [*pending, (self._batch, True)]
        return [] if self._held is None else [(self._held, True)]

    def count(self, summary: LoadResponse) -> None:
        self._rows_loaded += int(summary.rows_loaded)
        self._changes += int(summary.changes)

    def totals(self) -> LoadResponse:
        return LoadResponse.model_validate({"rowsLoaded": self._rows_loaded, "changes": self._changes})


def discard(work: Awaitable[Any]) -> None:
    if inspect.iscoroutine(work):
        work.close()


class AsyncSubscription:
    """A change subscription, which delivers events until you call `unsubscribe`."""

    def __init__(self, streams: Streams, subscription_id: str) -> None:
        self._streams = streams
        self.id = subscription_id

    def unsubscribe(self) -> None:
        """Stops the subscription and tells the server to stop sending its events."""
        self._streams.unsubscribe(self.id)


class AsyncSubscriptionBuilder:
    """Builds a change subscription on one table."""

    def __init__(self, database: AsyncRemoteDatabase, table: str, conditions: dict[str, Any] | None = None) -> None:
        self._database = database
        self._table = table
        self._conditions = conditions

    def filter(self, conditions: Mapping[str, Any]) -> AsyncSubscriptionBuilder:
        """Returns a builder that delivers only the changes whose row matches every column value in `conditions`."""
        return AsyncSubscriptionBuilder(self._database, self._table, {**(self._conditions or {}), **conditions})

    async def subscribe(
        self,
        callback: Callable[[ChangeEvent], Any],
        *,
        on_error: ErrorHandler | None = None,
        on_reset: Callable[[], Any] | None = None,
    ) -> AsyncSubscription:
        """Opens the subscription and returns once the server confirms it.

        The client calls `callback` with each change, in the order of the server's change log, and resumes from
        the last change that it delivered after a reconnection. When the server no longer holds the history
        since then, it calls `on_reset`, so read the table again there. A callback may be a coroutine function,
        and the client passes any error that a callback raises to `on_error`.
        """
        streams = self._database._streams_for("Subscriptions")
        subscription_id = await self._database._guarded(
            streams.subscribe(self._table, self._conditions, callback, on_error, on_reset)
        )
        return AsyncSubscription(streams, subscription_id)


class AsyncLiveQuery(LiveRows[RowT]):
    """A registered read whose rows the server keeps current, applying each edit in the order that it arrives."""

    def __init__(self, on_error: ErrorHandler | None) -> None:
        super().__init__(on_error)
        self._close: Callable[[], None] | None = None

    @property
    def rows(self) -> list[RowT]:
        """The current rows."""
        return self.state.rows

    def subscribe(self, listener: Callable[[LiveUpdate[RowT]], Any]) -> Callable[[], None]:
        """Calls `listener` with every update, and returns a function that removes it."""
        return self.listen(listener)

    def close(self) -> None:
        """Ends the live query and tells the server to stop maintaining it."""
        if self.closed:
            return
        self.stop()
        if self._close is not None:
            self._close()


class AsyncRemoteDatabase:
    """One database on a Sirannon server, reached through the client's transport."""

    def __init__(
        self,
        database_id: str,
        channel: Channel,
        capabilities: CapabilitySource,
        streams: Streams | None,
        on_close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.id = database_id
        self._channel = channel
        self._capabilities = capabilities
        self._streams = streams
        self._on_close = on_close
        self._waits: set[asyncio.Task[Any]] = set()
        self._closed = False

    def _streams_for(self, feature: str) -> Streams:
        if self._closed:
            raise closed_error()
        if self._streams is None:
            raise transport_unsupported(feature)
        return self._streams

    async def _guarded(self, work: Awaitable[Any]) -> Any:
        if self._closed:
            discard(work)
            raise closed_error()
        task = asyncio.ensure_future(work)
        self._waits.add(task)
        task.add_done_callback(self._waits.discard)
        try:
            return await task
        except asyncio.CancelledError:
            if self._cancelled_by_close():
                raise closed_error() from None
            raise

    def _cancelled_by_close(self) -> bool:
        current = asyncio.current_task()
        return self._closed and (current is None or current.cancelling() == 0)

    async def _assert_sql(self) -> None:
        refuse_sql(await self._guarded(self._capabilities.capabilities()))

    def _read_concern(self, read_concern: ConcernArg) -> dict[str, Any] | None:
        level = concern(read_concern, "read_concern")
        if level is not None and not self._channel.carries_read_concern:
            raise invalid_argument(
                "The topology-aware client applies its read_concern setting to every read; pass none per query"
            )
        return level

    async def registry_digest(self, refresh: bool = False) -> str | None:
        """Returns the digest of the server's registered operations, or None where the server announces none."""
        report = await self._guarded(self._capabilities.capabilities(refresh))
        return cast(str | None, report.registry_digest)

    @overload
    async def query(
        self, statement: str, params: Params | None = None, *, read_concern: ConcernArg = None
    ) -> list[dict[str, Any]]: ...

    @overload
    async def query(
        self,
        statement: ReadRef[Any, RowT],
        params: Mapping[str, Any] | None = None,
        *,
        read_concern: ConcernArg = None,
    ) -> list[RowT]: ...

    @overload
    async def query(
        self, statement: BoundRead[RowT], params: None = None, *, read_concern: ConcernArg = None
    ) -> list[RowT]: ...

    async def query(self, statement: Any, params: Any = None, *, read_concern: ConcernArg = None) -> list[Any]:
        """Runs a read and returns its rows, with every integer and BLOB as the database stores it.

        Pass a statement and its bind parameters, a registered read and its arguments, or a registered read
        called with its arguments. The client sends a statement only to a server whose capabilities include
        `query.sql`, and raises `SQL_NOT_ACCEPTED` otherwise.
        """
        level = self._read_concern(read_concern)
        operation = read_call(statement, params)
        if operation is not None:
            return cast(list[Any], await self._guarded(self._channel.query_named(*operation, level)))
        await self._assert_sql()
        return cast(list[Any], await self._guarded(self._channel.query(_sql(statement), params, level)))

    @overload
    async def execute(self, statement: str, params: Params | None = None) -> ExecuteResponse: ...

    @overload
    async def execute(
        self,
        statement: WriteRef[Any] | BoundWrite,
        params: Mapping[str, Any] | None = None,
        *,
        write_concern: ConcernArg = None,
    ) -> list[ExecuteResponse]: ...

    async def execute(
        self, statement: Any, params: Any = None, *, write_concern: ConcernArg = None
    ) -> ExecuteResponse | list[ExecuteResponse]:
        """Runs a write. A statement returns one result, and a registered write returns one result per statement.

        `last_insert_row_id` holds the number or the decimal string that the server sends. When a write fails,
        the client raises the server's error and sends nothing again, so resend it yourself where that is safe.
        """
        operation = write_call(statement, params)
        if operation is not None:
            level = concern(write_concern, "write_concern")
            return results_of(await self._guarded(self._channel.execute_named(*operation, level)))
        if write_concern is not None:
            raise invalid_argument("A statement takes no write_concern; pass it with a registered write or batch")
        await self._assert_sql()
        return validated(ExecuteResponse, await self._guarded(self._channel.execute(_sql(statement), params)))

    async def transaction(self, statements: Sequence[Mapping[str, Any]]) -> list[ExecuteResponse]:
        """Runs every statement in one transaction, and returns one result per statement."""
        listed = statement_list(statements)
        await self._assert_sql()
        return results_of(await self._guarded(self._channel.transaction(listed)))

    async def batch(
        self, sql: str, params_batch: Iterable[Params], *, write_concern: ConcernArg = None
    ) -> list[ExecuteResponse]:
        """Runs one statement once per parameter set, in one transaction."""
        encoded = [bind_values(params) for params in params_batch]
        level = concern(write_concern, "write_concern")
        await self._assert_sql()
        return results_of(await self._guarded(self._channel.batch(_sql(sql), encoded, level)))

    async def load(
        self,
        sql: str,
        params_batch: Iterable[Params],
        *,
        durability: str | None = None,
        checkpoint: bool | None = None,
    ) -> LoadResponse:
        """Runs one statement once per parameter set at the bulk-load durability, and returns the totals."""
        encoded = [bind_values(params) for params in params_batch]
        await self._assert_sql()
        payload = await self._guarded(self._channel.load(_sql(sql), encoded, durability, checkpoint))
        return validated(LoadResponse, payload)

    async def load_all(
        self,
        sql: str,
        rows: Iterable[Params] | AsyncIterable[Params],
        *,
        batch_size: int = DEFAULT_LOAD_BATCH_SIZE,
        durability: str | None = None,
    ) -> LoadResponse:
        """Loads rows of any number in batches of `batch_size`, checkpointing only after the last batch."""
        planner = LoadPlanner(batch_size)
        await self._assert_sql()

        async def send(chunk: list[Params], checkpoint: bool) -> None:
            planner.count(await self.load(sql, chunk, durability=durability, checkpoint=checkpoint))

        async for row in _each(rows):
            ready = planner.add(row)
            if ready is not None:
                await send(ready, False)
        for chunk, checkpoint in planner.finish():
            await send(chunk, checkpoint)
        return planner.totals()

    def on(self, table: str) -> AsyncSubscriptionBuilder:
        """Returns a builder for a change subscription on `table`, which needs the WebSocket transport."""
        return AsyncSubscriptionBuilder(self, table)

    async def live(
        self,
        operation: ReadRef[Any, RowT] | BoundRead[RowT] | str,
        args: Mapping[str, Any] | None = None,
        *,
        on_error: ErrorHandler | None = None,
    ) -> AsyncLiveQuery[RowT]:
        """Opens a live query on a registered read and returns once its first rows arrive."""
        streams = self._streams_for("Live queries")
        name, args = read_call(operation, args) or (operation, args)
        if not isinstance(name, str) or not name:
            raise invalid_argument("A live query needs the name of a registered read")
        query: AsyncLiveQuery[RowT] = AsyncLiveQuery(on_error)
        values = None if args is None else dict(args)
        live_id = await self._guarded(streams.open_live(name, values, query, self.registry_digest))
        query._close = partial(streams.unsubscribe, live_id)
        return query

    async def ack(self, device_id: str, seq: int) -> AckResponse:
        """Records that a device has applied every change up to `seq`, which device sync uses for pacing."""
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            raise invalid_argument("ack seq must be a whole number of at least 0")
        streams = self._streams_for("Acknowledgements")
        return validated(AckResponse, await self._guarded(streams.ack(device_id, seq)))

    async def aclose(self) -> None:
        """Cancels every call of this database still in flight, which then fails with `TRANSPORT_ERROR`."""
        if self._closed:
            return
        self._closed = True
        for task in list(self._waits):
            task.cancel()
        if self._on_close is not None:
            await self._on_close()


def _sql(statement: Any) -> str:
    if not isinstance(statement, str) or not statement:
        raise invalid_argument("A statement must be a non-empty SQL string, a ReadRef, or a WriteRef")
    return statement


async def _each(rows: Iterable[Params] | AsyncIterable[Params]) -> AsyncIterable[Params]:
    if isinstance(rows, AsyncIterable):
        async for row in rows:
            yield row
    else:
        for row in rows:
            yield row
