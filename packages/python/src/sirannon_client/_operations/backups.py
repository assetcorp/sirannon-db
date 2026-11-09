"""Generated from packages/ts/openapi.json and packages/ts/asyncapi.json by packages/python/generator. Do not edit."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from sirannon_client._exchange import Exchange, path_segment
from sirannon_client._http import SyncOperationsBase
from sirannon_client.models import (
    BackupChainResponse,
    BackupRestoreRequest,
    BackupRestoreStatus,
    BackupSafeToDeleteRequest,
    BackupSafeToDeleteResponse,
    BackupStatusResponse,
    BackupTriggerResponse,
    BackupVerifyRequest,
    BackupVerifyResponse,
    StartedResponse,
)


class BackupsOperations(SyncOperationsBase):
    """These methods call the Backups routes of a Sirannon server."""

    def get_backup_status(
        self,
        id: str,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> BackupStatusResponse:
        """Returns what the backup cycle is doing and the results of its recent turns.

        `GET /db/{id}/backup`

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
            BackupStatusResponse,
            self._transport.json(
                Exchange(
                    method="GET",
                    path=f"/db/{path_segment(id)}/backup",
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: BackupStatusResponse},
            ),
        )

    def trigger_backup(
        self,
        id: str,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> BackupTriggerResponse:
        """Starts one turn of the backup checkpoint cycle and answers before the turn finishes.

        `POST /db/{id}/backup`

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 202: The server accepted the work and answered before it finished.
        """
        return cast(
            BackupTriggerResponse,
            self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/backup",
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {202: BackupTriggerResponse},
            ),
        )

    def list_backup_chains(
        self,
        id: str,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> BackupChainResponse:
        """Returns every chain of backups that the destination stores.

        `GET /db/{id}/backup/chain`

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
            BackupChainResponse,
            self._transport.json(
                Exchange(
                    method="GET",
                    path=f"/db/{path_segment(id)}/backup/chain",
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: BackupChainResponse},
            ),
        )

    def verify_backup(
        self,
        id: str,
        body: BackupVerifyRequest | Mapping[str, Any],
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> BackupVerifyResponse:
        """Reads one stored backup back from the destination and checks it.

        `POST /db/{id}/backup/verify`

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            body: The backup to read back from the destination.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            BackupVerifyResponse,
            self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/backup/verify",
                    body=body,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: BackupVerifyResponse},
            ),
        )

    def list_backups_safe_to_delete(
        self,
        id: str,
        body: BackupSafeToDeleteRequest | Mapping[str, Any] | None = None,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> BackupSafeToDeleteResponse:
        """Returns the stored backups that no restore from the given moment onwards needs.

        `POST /db/{id}/backup/safe-to-delete`

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            body: How far back you must still be able to restore.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            BackupSafeToDeleteResponse,
            self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/backup/safe-to-delete",
                    body=body,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: BackupSafeToDeleteResponse},
            ),
        )

    def get_backup_restore_status(
        self,
        id: str,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> BackupRestoreStatus:
        """Returns how the last restore of the database went.

        `GET /db/{id}/backup/restore`

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
            BackupRestoreStatus,
            self._transport.json(
                Exchange(
                    method="GET",
                    path=f"/db/{path_segment(id)}/backup/restore",
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: BackupRestoreStatus},
            ),
        )

    def restore_backup(
        self,
        id: str,
        body: BackupRestoreRequest | Mapping[str, Any] | None = None,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> StartedResponse:
        """Starts rebuilding the database from its backups at a moment, on a server that
        accepts restores.

        `POST /db/{id}/backup/restore`

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            body: The moment to which the server rebuilds the database, and how many change
                pieces it replays in one batch.
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
                    path=f"/db/{path_segment(id)}/backup/restore",
                    body=body,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {202: StartedResponse},
            ),
        )
