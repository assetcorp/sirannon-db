from __future__ import annotations

import asyncio
import threading
import weakref
from typing import Final

CALLBACK_BACKLOG_LIMIT: Final = 1024


class CallbackBacklog:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._lock = threading.Lock()
        self._pending = 0
        self._drained: asyncio.Event | None = None

    def added(self) -> None:
        with self._lock:
            self._pending += 1

    def finished(self) -> None:
        with self._lock:
            self._pending = max(0, self._pending - 1)
            drained = self._drained if self._pending < CALLBACK_BACKLOG_LIMIT else None
            if drained is not None:
                self._drained = None
        if drained is None:
            return
        try:
            self._loop.call_soon_threadsafe(drained.set)
        except RuntimeError:
            return

    async def paced(self) -> None:
        while True:
            with self._lock:
                if self._pending < CALLBACK_BACKLOG_LIMIT:
                    return
                if self._drained is None:
                    self._drained = asyncio.Event()
                drained = self._drained
            await drained.wait()


_backlogs: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, CallbackBacklog] = weakref.WeakKeyDictionary()
_backlogs_lock = threading.Lock()


def backlog_for(loop: asyncio.AbstractEventLoop) -> CallbackBacklog:
    with _backlogs_lock:
        backlog = _backlogs.get(loop)
        if backlog is None:
            backlog = CallbackBacklog(loop)
            _backlogs[loop] = backlog
        return backlog


async def callbacks_paced() -> None:
    with _backlogs_lock:
        backlog = _backlogs.get(asyncio.get_running_loop())
    if backlog is not None:
        await backlog.paced()
