from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import queue
import threading
from collections.abc import Callable, Coroutine
from typing import Any, Final, TypeVar

from sirannon_client._errors import closed_error, invalid_argument
from sirannon_client._events import ErrorHandler
from sirannon_client._pacing import CallbackBacklog, backlog_for

T = TypeVar("T")
STOP: Final = object()


class Delivery:
    def __init__(self, backlog: CallbackBacklog) -> None:
        self._backlog = backlog
        self._stopped = threading.Event()
        self._queue: queue.SimpleQueue[Any] = queue.SimpleQueue()
        self._thread = threading.Thread(target=self._work, name="sirannon-client-delivery", daemon=True)
        self._thread.start()

    def post(self, work: Callable[[], None]) -> None:
        if self._stopped.is_set():
            return
        self._backlog.added()
        self._queue.put(work)

    def _work(self) -> None:
        while True:
            work = self._queue.get()
            if work is STOP or self._stopped.is_set():
                return
            try:
                work()
            finally:
                self._backlog.finished()

    def stop(self) -> None:
        self._stopped.set()
        self._queue.put(STOP)


def report_sync(on_error: ErrorHandler | None, error: BaseException) -> None:
    if on_error is None:
        return
    try:
        outcome = on_error(error)
    except Exception:
        return
    if inspect.iscoroutine(outcome):
        outcome.close()


def call_sync(callback: Callable[..., Any], args: tuple[Any, ...], on_error: ErrorHandler | None) -> None:
    try:
        outcome = callback(*args)
    except Exception as error:
        report_sync(on_error, error)
        return
    if inspect.iscoroutine(outcome):
        outcome.close()
        report_sync(on_error, invalid_argument("The synchronous client calls plain functions, not coroutines"))


class Runtime:
    """One event loop thread that runs the asyncio client, and one thread that calls your callbacks in order."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="sirannon-client-loop", daemon=True)
        self._thread.start()
        self.delivery = Delivery(backlog_for(self._loop))
        self._stopped = False

    def run(self, work: Coroutine[Any, Any, T]) -> T:
        if self._stopped:
            work.close()
            raise closed_error()
        if threading.current_thread() is self._thread:
            work.close()
            raise invalid_argument("A synchronous call cannot run on the client's event loop thread")
        future = asyncio.run_coroutine_threadsafe(work, self._loop)
        try:
            return future.result()
        except concurrent.futures.CancelledError:
            raise closed_error() from None
        except BaseException:
            future.cancel()
            raise

    def relay(self, callback: Callable[..., Any], on_error: ErrorHandler | None) -> Callable[..., None]:
        delivery = self.delivery

        def relayed(*args: Any) -> None:
            delivery.post(lambda: call_sync(callback, args, on_error))

        return relayed

    def relay_errors(self, on_error: ErrorHandler | None) -> ErrorHandler | None:
        if on_error is None:
            return None
        delivery = self.delivery

        def relayed(error: BaseException) -> None:
            delivery.post(lambda: report_sync(on_error, error))

        return relayed

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if threading.current_thread() is not self._thread:
            asyncio.run_coroutine_threadsafe(_cancel_remaining(), self._loop).result()
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()
            self._loop.close()
        self.delivery.stop()


async def _cancel_remaining() -> None:
    current = asyncio.current_task()
    remaining = [task for task in asyncio.all_tasks() if task is not current]
    for task in remaining:
        task.cancel()
    await asyncio.gather(*remaining, return_exceptions=True)
