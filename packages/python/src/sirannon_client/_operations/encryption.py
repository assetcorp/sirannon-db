"""Generated from packages/ts/openapi.json and packages/ts/asyncapi.json by packages/python/generator. Do not edit."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from sirannon_client._exchange import Exchange, path_segment
from sirannon_client._http import SyncOperationsBase
from sirannon_client.models import EncryptionChangeRequest, EncryptionStatus, StartedResponse


class EncryptionOperations(SyncOperationsBase):
    """These methods call the Encryption routes of a Sirannon server."""

    def get_encryption_status(
        self,
        id: str,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> EncryptionStatus:
        """Returns how the database is encrypted and how its last key change went.

        `GET /db/{id}/encryption`

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            EncryptionStatus,
            self._transport.json(
                Exchange(
                    method="GET",
                    path=f"/db/{path_segment(id)}/encryption",
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: EncryptionStatus},
            ),
        )

    def change_encryption(
        self,
        id: str,
        body: EncryptionChangeRequest | Mapping[str, Any],
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> StartedResponse:
        """Starts encrypting the database, changing its key, or removing its encryption, on a
        server that accepts key changes.

        `POST /db/{id}/encryption`

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            body: The body of `POST /db/{id}/encryption`, which encrypts the database, changes
                its key, or removes its encryption.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 202: The server accepted the work and answered before it finished.
        """
        return cast(
            StartedResponse,
            self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/encryption",
                    body=body,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {202: StartedResponse},
            ),
        )
