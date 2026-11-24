from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, TypeAlias

from sirannon_client._errors import invalid_argument
from sirannon_client.models import LoadResponse

Params: TypeAlias = Sequence[Any] | Mapping[str, Any]


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
