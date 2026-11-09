from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Final


class ErrorCodes:
    """The codes that the client raises when it fails before the server answers."""

    CONNECTION_ERROR: Final = "CONNECTION_ERROR"
    """The client could not reach the server, or the connection broke before the client received the answer."""
    TIMEOUT: Final = "TIMEOUT"
    """The request timeout passed before the client received the answer."""
    TRANSPORT_ERROR: Final = "TRANSPORT_ERROR"
    """The call needs the WebSocket transport while the client uses HTTP, or the client is closed."""
    INVALID_RESPONSE: Final = "INVALID_RESPONSE"
    """The answer held no JSON object, a shape other than the one that the route documents, or a bad envelope."""
    ROUTING_ERROR: Final = "ROUTING_ERROR"
    """The topology-aware client found no endpoint to send the call to."""
    NO_SAFE_PRIMARY: Final = "NO_SAFE_PRIMARY"
    """The replication group has no current primary to send a write to."""
    INVALID_ARGUMENT: Final = "INVALID_ARGUMENT"
    """The client refused a setting or a value that the caller passed."""
    UNAUTHORIZED: Final = "UNAUTHORIZED"
    """The server closed the WebSocket connection with code 4401, so the client stops reconnecting."""
    FORBIDDEN: Final = "FORBIDDEN"
    """The server closed the WebSocket connection with code 4403, so the client stops reconnecting."""
    SQL_NOT_ACCEPTED: Final = "SQL_NOT_ACCEPTED"
    """The server's capabilities lack `query.sql`, so the client refused to send the statement."""


SQL_REFUSED_MESSAGE: Final = (
    "This server does not accept SQL over the network. Call a registered operation by name, "
    "or start the server with acceptSql: true."
)


class SirannonError(Exception):
    """The client raises this for every failure, under the code that the server sent.

    A code from :class:`ErrorCodes` other than `SQL_NOT_ACCEPTED` means that the failure happened on this side.
    Any other code is the one that the server sent, passed through unchanged.
    """

    code: str
    """The machine-readable code of the failure, such as `DATABASE_NOT_FOUND`."""
    message: str
    """A description of the failure for a log or a person."""
    details: dict[str, Any]
    """The values behind the failure, where the server sent any."""
    status: int | None
    """The HTTP status of the answer that held this error, or None where no HTTP answer held it."""

    def __init__(
        self,
        code: str,
        message: str,
        details: Mapping[str, Any] | None = None,
        status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})
        self.status = status

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    def __repr__(self) -> str:
        return f"SirannonError(code={self.code!r}, message={self.message!r}, status={self.status!r})"


def invalid_response(message: str) -> SirannonError:
    return SirannonError(ErrorCodes.INVALID_RESPONSE, message)


def invalid_argument(message: str) -> SirannonError:
    return SirannonError(ErrorCodes.INVALID_ARGUMENT, message)


def closed_error() -> SirannonError:
    return SirannonError(ErrorCodes.TRANSPORT_ERROR, "The client is closed")


def error_from_envelope(payload: Any, status: int | None) -> SirannonError | None:
    if not isinstance(payload, dict):
        return None
    envelope = payload.get("error")
    if not isinstance(envelope, dict):
        return None
    code = envelope.get("code")
    message = envelope.get("message")
    if not isinstance(code, str) or not isinstance(message, str):
        return None
    details = envelope.get("details")
    return SirannonError(code, message, details if isinstance(details, dict) else None, status)


def error_from_body(status: int, body: bytes) -> SirannonError:
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        payload = None
    found = error_from_envelope(payload, status)
    if found is not None:
        return found
    return SirannonError(
        ErrorCodes.INVALID_RESPONSE,
        f"The server answered HTTP {status} without a Sirannon error body",
        status=status,
    )
