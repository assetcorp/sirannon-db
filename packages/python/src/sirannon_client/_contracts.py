from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from sirannon_client._events import ChangeEvent, ErrorHandler
from sirannon_client._http import CapabilityReport
from sirannon_client._ws_state import DigestSource, LiveHandlers


class CapabilitySource(Protocol):
    async def capabilities(self, refresh: bool = False) -> CapabilityReport: ...


class Streams(Protocol):
    async def subscribe(
        self,
        table: str,
        filter_values: dict[str, Any] | None,
        callback: Callable[[ChangeEvent], Any],
        on_error: ErrorHandler | None,
        on_reset: Callable[[], Any] | None,
    ) -> str: ...

    async def open_live(
        self,
        name: str,
        args: dict[str, Any] | None,
        handlers: LiveHandlers,
        digest: DigestSource | None,
    ) -> str: ...

    def unsubscribe(self, subscription_id: str) -> None: ...

    async def ack(self, device_id: str, seq: int) -> Any: ...
