from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


def _observe(task: asyncio.Future[Any]) -> None:
    if not task.cancelled():
        task.exception()


def shared_task(work: Coroutine[Any, Any, T]) -> asyncio.Task[T]:
    task = asyncio.ensure_future(work)
    task.add_done_callback(_observe)
    return task
