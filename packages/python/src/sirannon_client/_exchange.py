from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from sirannon_client._errors import invalid_argument
from sirannon_client._values import encode_values

DOT_SEGMENTS = frozenset({".", ".."})


@dataclass(frozen=True, slots=True)
class Exchange:
    method: str
    path: str
    body: Any = None
    sql_statement: bool = False
    timeout_ms: int | None = None
    headers: Mapping[str, str] | None = None


def path_segment(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise invalid_argument("A database identifier or operation name must be a non-empty string")
    if value in DOT_SEGMENTS:
        return "%2E" * len(value)
    return quote(value, safe="")


def encode_json(value: Any) -> bytes:
    return json.dumps(encode_values(value), ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
