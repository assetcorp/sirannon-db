from __future__ import annotations

import math
import re
from collections.abc import Mapping
from typing import Any, Final

from pydantic import BaseModel

from sirannon_client._errors import invalid_argument, invalid_response

INTEGER_TAG: Final = "__sirannon_int"
BLOB_TAG: Final = "__sirannon_blob"
MAX_SAFE_INTEGER: Final = 2**53 - 1
MIN_INT64: Final = -(2**63)
MAX_INT64: Final = 2**63 - 1
MAX_NESTING: Final = 256
INTEGER_PAYLOAD: Final = re.compile(r"-?[0-9]{1,19}")
HEX_PAYLOAD: Final = re.compile(r"(?:[0-9A-Fa-f]{2})*")


def encode_values(value: Any, depth: int = 0) -> Any:
    if depth > MAX_NESTING:
        raise invalid_argument(f"The client refuses to send a value nested more than {MAX_NESTING} levels deep")
    if value is None or isinstance(value, bool | str):
        return value
    if isinstance(value, int):
        if not MIN_INT64 <= value <= MAX_INT64:
            raise invalid_argument("SQLite stores integers in 64 bits, and this integer lies outside that range")
        if -MAX_SAFE_INTEGER <= value <= MAX_SAFE_INTEGER:
            return value
        return {INTEGER_TAG: str(value)}
    if isinstance(value, float):
        if not math.isfinite(value):
            raise invalid_argument(f"JSON cannot carry the number {value}")
        return value
    if isinstance(value, bytes | bytearray | memoryview):
        return {BLOB_TAG: bytes(value).hex().upper()}
    if isinstance(value, BaseModel):
        return encode_values(value.model_dump(mode="python", by_alias=True, exclude_unset=True), depth + 1)
    if isinstance(value, Mapping):
        encoded: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise invalid_argument(f"Every key must be a string, and the client received {type(key).__name__}")
            encoded[key] = encode_values(item, depth + 1)
        return encoded
    if isinstance(value, list | tuple):
        return [encode_values(item, depth + 1) for item in value]
    raise invalid_argument(f"The client cannot send a {type(value).__name__} as a Sirannon value")


def _decode_envelope(tag: str, payload: str) -> int | bytes:
    if tag == INTEGER_TAG:
        if INTEGER_PAYLOAD.fullmatch(payload) is None:
            raise invalid_response("The server sent a malformed integer envelope")
        number = int(payload)
        if not MIN_INT64 <= number <= MAX_INT64:
            raise invalid_response("The server sent an integer envelope outside the 64-bit range")
        return number
    if HEX_PAYLOAD.fullmatch(payload) is None:
        raise invalid_response("The server sent a malformed BLOB envelope")
    return bytes.fromhex(payload)


def decode_values(value: Any, depth: int = 0) -> Any:
    if depth > MAX_NESTING:
        raise invalid_response(f"The server sent a value nested more than {MAX_NESTING} levels deep")
    if isinstance(value, list):
        return [decode_values(item, depth + 1) for item in value]
    if not isinstance(value, dict):
        return value
    if len(value) == 1:
        tag, payload = next(iter(value.items()))
        if tag in (INTEGER_TAG, BLOB_TAG) and isinstance(payload, str):
            return _decode_envelope(tag, payload)
    return {key: decode_values(item, depth + 1) for key, item in value.items()}
