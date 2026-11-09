from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final, Literal
from urllib.parse import urlsplit

from sirannon_client._errors import invalid_argument

TransportKind = Literal["websocket", "http"]

SIRANNON_SUBPROTOCOL: Final = "sirannon.v1"
DEFAULT_REQUEST_TIMEOUT_MS: Final = 30_000
DEFAULT_RECONNECT_INTERVAL_MS: Final = 1_000
DEFAULT_MAX_RESPONSE_BYTES: Final = 64 * 1024 * 1024
HEADER_TOKEN: Final = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
WEBSOCKET_SCHEMES: Final = {"http": "ws", "https": "wss"}


@dataclass(frozen=True)
class ClientSettings:
    base_url: str
    headers: dict[str, str]
    transport: TransportKind
    websocket_protocols: tuple[str, ...] | None
    auto_reconnect: bool
    reconnect_interval_ms: int
    request_timeout_ms: int
    max_response_bytes: int

    @property
    def websocket_url(self) -> str:
        scheme, rest = self.base_url.split("://", 1)
        return f"{WEBSOCKET_SCHEMES[scheme]}://{rest}"

    @property
    def offered_protocols(self) -> list[str] | None:
        if self.websocket_protocols is None:
            return None
        return [SIRANNON_SUBPROTOCOL, *(item for item in self.websocket_protocols if item != SIRANNON_SUBPROTOCOL)]


def whole_number(value: object, name: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise invalid_argument(f"{name} must be a whole number of at least {minimum}")
    return value


def normalise_url(url: object) -> str:
    if not isinstance(url, str):
        raise invalid_argument("The server URL must be a string")
    parts = urlsplit(url)
    if parts.scheme not in WEBSOCKET_SCHEMES or not parts.netloc:
        raise invalid_argument("The server URL must be an absolute http:// or https:// URL")
    if parts.query or parts.fragment:
        raise invalid_argument("The server URL must carry no query string or fragment")
    if parts.username is not None or parts.password is not None:
        raise invalid_argument("The server URL must carry no credentials; pass them in headers")
    return url.rstrip("/")


def _headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    merged: dict[str, str] = {}
    for key, value in (headers or {}).items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise invalid_argument("Every header name and value must be a string")
        if HEADER_TOKEN.fullmatch(key) is None or "\r" in value or "\n" in value:
            raise invalid_argument("A header name must be a token and its value must hold no line break")
        merged[key.lower()] = value
    return merged


def _protocols(protocols: str | Sequence[str] | None) -> tuple[str, ...] | None:
    if protocols is None:
        return None
    listed = [protocols] if isinstance(protocols, str) else list(protocols)
    seen: set[str] = set()
    for item in listed:
        if not isinstance(item, str) or HEADER_TOKEN.fullmatch(item) is None or item in seen:
            raise invalid_argument(
                "websocket_protocols must hold distinct values made of the characters that a header token allows"
            )
        seen.add(item)
    return tuple(listed)


def build_settings(
    url: str,
    *,
    transport: TransportKind = "websocket",
    headers: Mapping[str, str] | None = None,
    websocket_protocols: str | Sequence[str] | None = None,
    auto_reconnect: bool = True,
    reconnect_interval_ms: int = DEFAULT_RECONNECT_INTERVAL_MS,
    request_timeout_ms: int = DEFAULT_REQUEST_TIMEOUT_MS,
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
) -> ClientSettings:
    if transport not in ("websocket", "http"):
        raise invalid_argument("transport must be 'websocket' or 'http'")
    if not isinstance(auto_reconnect, bool):
        raise invalid_argument("auto_reconnect must be True or False")
    return ClientSettings(
        base_url=normalise_url(url),
        headers=_headers(headers),
        transport=transport,
        websocket_protocols=_protocols(websocket_protocols),
        auto_reconnect=auto_reconnect,
        reconnect_interval_ms=whole_number(reconnect_interval_ms, "reconnect_interval_ms", 1),
        request_timeout_ms=whole_number(request_timeout_ms, "request_timeout_ms", 0),
        max_response_bytes=whole_number(max_response_bytes, "max_response_bytes", 0),
    )
