from __future__ import annotations

from typing import Any, Final

from pydantic import ValidationError

from sirannon_client._errors import invalid_argument, invalid_response
from sirannon_client._values import MAX_SAFE_INTEGER, decode_values
from sirannon_client.models import QueryColumnsResponse

COLUMNS_SHAPE: Final = "columns"
TRUNCATIONS: Final = (None, "rows", "bytes")
MALFORMED_ANSWER: Final = (
    "The server answered a columns read with a body that holds no valid columns, rows, and truncated"
)


def _limit(name: str, value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_SAFE_INTEGER:
        raise invalid_argument(f"{name} must be a whole number from 0 to {MAX_SAFE_INTEGER}")
    return value


def column_limits(max_rows: Any, max_bytes: Any) -> dict[str, int]:
    rows = _limit("max_rows", max_rows)
    size = _limit("max_bytes", max_bytes)
    limits: dict[str, int] = {}
    if rows is not None:
        limits["maxRows"] = rows
    if size is not None:
        limits["maxBytes"] = size
    return limits


def columns_body(sql: str, params: Any, read_concern: dict[str, Any] | None, limits: dict[str, int]) -> dict[str, Any]:
    body: dict[str, Any] = {"sql": sql, "shape": COLUMNS_SHAPE, **limits}
    if params is not None:
        body["params"] = params
    if read_concern is not None:
        body["readConcern"] = read_concern
    return body


def columns_answer(payload: Any) -> QueryColumnsResponse:
    if not isinstance(payload, dict):
        raise invalid_response(MALFORMED_ANSWER)
    columns = payload.get("columns")
    rows = payload.get("rows")
    if not isinstance(columns, list) or not isinstance(rows, list) or payload.get("truncated") not in TRUNCATIONS:
        raise invalid_response(MALFORMED_ANSWER)
    if not all(isinstance(row, list) and len(row) == len(columns) for row in rows):
        raise invalid_response(MALFORMED_ANSWER)
    decoded = {"columns": columns, "rows": decode_values(rows), "truncated": payload.get("truncated")}
    try:
        return QueryColumnsResponse.model_validate(decoded)
    except ValidationError:
        raise invalid_response(MALFORMED_ANSWER) from None
