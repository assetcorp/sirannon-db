from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, Generic, overload

from sirannon_client._channels import ConcernArg, Params
from sirannon_client._database import (
    DEFAULT_LOAD_BATCH_SIZE,
    AsyncLiveQuery,
    AsyncRemoteDatabase,
    AsyncSubscription,
    LoadPlanner,
)
from sirannon_client._events import ChangeEvent, ErrorHandler
from sirannon_client._live import LiveState, LiveUpdate
from sirannon_client._refs import BoundRead, BoundWrite, ReadRef, RowT, WriteRef
from sirannon_client._sync_runtime import Runtime
from sirannon_client.models import AckResponse, ExecuteResponse, LoadResponse


async def _call(work: Callable[[], Any]) -> Any:
    return work()


class Subscription:
    """A change subscription, which delivers events until you call `unsubscribe`."""

    def __init__(self, runtime: Runtime, subscription: AsyncSubscription) -> None:
        self._runtime = runtime
        self._subscription = subscription
        self.id = subscription.id

    def unsubscribe(self) -> None:
        """Stops the subscription and tells the server to stop sending its events."""
        self._runtime.run(_call(self._subscription.unsubscribe))


class SubscriptionBuilder:
    """Builds a change subscription on one table."""

    def __init__(self, runtime: Runtime, database: AsyncRemoteDatabase, table: str) -> None:
        self._runtime = runtime
        self._database = database
        self._table = table
        self._conditions: dict[str, Any] = {}

    def filter(self, conditions: Mapping[str, Any]) -> SubscriptionBuilder:
        """Returns a builder that delivers only the changes whose row matches every column value in `conditions`."""
        narrowed = SubscriptionBuilder(self._runtime, self._database, self._table)
        narrowed._conditions = {**self._conditions, **conditions}
        return narrowed

    def subscribe(
        self,
        callback: Callable[[ChangeEvent], Any],
        *,
        on_error: ErrorHandler | None = None,
        on_reset: Callable[[], Any] | None = None,
    ) -> Subscription:
        """Opens the subscription and returns once the server confirms it.

        The client calls `callback`, `on_error`, and `on_reset` on one delivery thread, in the order of the
        server's change log, so a slow callback delays the ones after it and never the connection. After a
        reconnection the client resumes from the last change that it delivered, and it calls `on_reset` when
        the server no longer holds the history since then.
        """
        builder = self._database.on(self._table)
        if self._conditions:
            builder = builder.filter(self._conditions)
        relay = self._runtime.relay
        opened = self._runtime.run(
            builder.subscribe(
                relay(callback, on_error),
                on_error=self._runtime.relay_errors(on_error),
                on_reset=None if on_reset is None else relay(on_reset, on_error),
            )
        )
        return Subscription(self._runtime, opened)


class LiveQuery(Generic[RowT]):
    """A registered read whose rows the server keeps current, applying each edit in the order that it arrives."""

    def __init__(self, runtime: Runtime, query: AsyncLiveQuery[RowT], on_error: ErrorHandler | None) -> None:
        self._runtime = runtime
        self._query = query
        self._on_error = on_error

    @property
    def state(self) -> LiveState[RowT]:
        """The status, rows, and error of the query at this moment."""
        return self._query.state

    @property
    def rows(self) -> list[RowT]:
        """The current rows."""
        return self._query.state.rows

    def subscribe(self, listener: Callable[[LiveUpdate[RowT]], Any]) -> Callable[[], None]:
        """Calls `listener` on the delivery thread with every update, and returns a function that removes it."""
        remove = self._runtime.run(_call(lambda: self._query.subscribe(self._runtime.relay(listener, self._on_error))))

        def unsubscribe() -> None:
            self._runtime.run(_call(remove))

        return unsubscribe

    def close(self) -> None:
        """Ends the live query and tells the server to stop maintaining it."""
        self._runtime.run(_call(self._query.close))


class RemoteDatabase:
    """One database on a Sirannon server, for code that does not use asyncio.

    Every method blocks until the server answers, and is safe to call from several threads at once.
    """

    def __init__(self, runtime: Runtime, database: AsyncRemoteDatabase) -> None:
        self._runtime = runtime
        self._database = database
        self.id = database.id

    def registry_digest(self, refresh: bool = False) -> str | None:
        """Returns the digest of the server's registered operations, or None where the server announces none."""
        return self._runtime.run(self._database.registry_digest(refresh))

    @overload
    def query(
        self, statement: str, params: Params | None = None, *, read_concern: ConcernArg = None
    ) -> list[dict[str, Any]]: ...

    @overload
    def query(
        self,
        statement: ReadRef[Any, RowT],
        params: Mapping[str, Any] | None = None,
        *,
        read_concern: ConcernArg = None,
    ) -> list[RowT]: ...

    @overload
    def query(
        self, statement: BoundRead[RowT], params: None = None, *, read_concern: ConcernArg = None
    ) -> list[RowT]: ...

    def query(self, statement: Any, params: Any = None, *, read_concern: ConcernArg = None) -> list[Any]:
        """Runs a read and returns its rows, as `AsyncRemoteDatabase.query` describes."""
        return self._runtime.run(self._database.query(statement, params, read_concern=read_concern))

    @overload
    def execute(self, statement: str, params: Params | None = None) -> ExecuteResponse: ...

    @overload
    def execute(
        self,
        statement: WriteRef[Any] | BoundWrite,
        params: Mapping[str, Any] | None = None,
        *,
        write_concern: ConcernArg = None,
    ) -> list[ExecuteResponse]: ...

    def execute(
        self, statement: Any, params: Any = None, *, write_concern: ConcernArg = None
    ) -> ExecuteResponse | list[ExecuteResponse]:
        """Runs a write, as `AsyncRemoteDatabase.execute` describes, and never resends one that fails."""
        return self._runtime.run(self._database.execute(statement, params, write_concern=write_concern))

    def transaction(self, statements: Sequence[Mapping[str, Any]]) -> list[ExecuteResponse]:
        """Runs every statement in one transaction, and returns one result per statement."""
        return self._runtime.run(self._database.transaction(statements))

    def batch(
        self, sql: str, params_batch: Iterable[Params], *, write_concern: ConcernArg = None
    ) -> list[ExecuteResponse]:
        """Runs one statement once per parameter set, in one transaction."""
        return self._runtime.run(self._database.batch(sql, list(params_batch), write_concern=write_concern))

    def load(
        self,
        sql: str,
        params_batch: Iterable[Params],
        *,
        durability: str | None = None,
        checkpoint: bool | None = None,
    ) -> LoadResponse:
        """Runs one statement once per parameter set at the bulk-load durability, and returns the totals."""
        batch = list(params_batch)
        return self._runtime.run(self._database.load(sql, batch, durability=durability, checkpoint=checkpoint))

    def load_all(
        self,
        sql: str,
        rows: Iterable[Params],
        *,
        batch_size: int = DEFAULT_LOAD_BATCH_SIZE,
        durability: str | None = None,
    ) -> LoadResponse:
        """Loads rows of any number in batches of `batch_size`, checkpointing only after the last batch.

        The client reads `rows` on the calling thread, so a generator can produce rows of any number.
        """
        planner = LoadPlanner(batch_size)
        self._runtime.run(self._database._assert_sql())
        for row in rows:
            ready = planner.add(row)
            if ready is not None:
                planner.count(self.load(sql, ready, durability=durability, checkpoint=False))
        for chunk, checkpoint in planner.finish():
            planner.count(self.load(sql, chunk, durability=durability, checkpoint=checkpoint))
        return planner.totals()

    def on(self, table: str) -> SubscriptionBuilder:
        """Returns a builder for a change subscription on `table`, which needs the WebSocket transport."""
        return SubscriptionBuilder(self._runtime, self._database, table)

    def live(
        self,
        operation: ReadRef[Any, RowT] | BoundRead[RowT] | str,
        args: Mapping[str, Any] | None = None,
        *,
        on_error: ErrorHandler | None = None,
    ) -> LiveQuery[RowT]:
        """Opens a live query on a registered read and returns once its first rows arrive."""
        query: AsyncLiveQuery[RowT] = self._runtime.run(
            self._database.live(operation, args, on_error=self._runtime.relay_errors(on_error))
        )
        return LiveQuery(self._runtime, query, on_error)

    def ack(self, device_id: str, seq: int) -> AckResponse:
        """Records that a device has applied every change up to `seq`, which device sync uses for pacing."""
        return self._runtime.run(self._database.ack(device_id, seq))

    def close(self) -> None:
        """Fails every call of this database still in flight with `TRANSPORT_ERROR`, and closes its connection."""
        self._runtime.run(self._database.aclose())
