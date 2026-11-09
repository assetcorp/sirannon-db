"""Generated from packages/ts/openapi.json and packages/ts/asyncapi.json by packages/python/generator. Do not edit."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from sirannon_client._exchange import Exchange, path_segment
from sirannon_client._http import AsyncOperationsBase
from sirannon_client.models import (
    ChangesRequest,
    ChangesResponse,
    MigrationListRequest,
    MigrationListResponse,
    SnapshotManifestResponse,
    SnapshotPageRequest,
    SnapshotPageResponse,
)


class DeviceSyncOperations(AsyncOperationsBase):
    """These methods call the DeviceSync routes of a Sirannon server."""

    async def apply_changes(
        self,
        id: str,
        body: ChangesRequest | Mapping[str, Any],
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> ChangesResponse:
        """Applies a batch of changes that a device pushes, on a server that accepts device
        sync.

        `POST /db/{id}/changes`

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            body: The body of `POST /db/{id}/changes`, which applies a device's changes to the
                server's database.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            ChangesResponse,
            await self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/changes",
                    body=body,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: ChangesResponse},
            ),
        )

    async def list_migrations(
        self,
        id: str,
        body: MigrationListRequest | Mapping[str, Any],
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> MigrationListResponse:
        """Returns the migrations that the database has applied, on a server that accepts
        device sync.

        `POST /db/{id}/migrations`

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            body: The body of `POST /db/{id}/migrations`, which lists the migrations that the
                database has applied.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            MigrationListResponse,
            await self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/migrations",
                    body=body,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: MigrationListResponse},
            ),
        )

    async def get_snapshot_manifest(
        self,
        id: str,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> SnapshotManifestResponse:
        """Returns the schema, tables, and starting sequence that a device copies the database
        from, on a server that accepts device sync.

        `POST /db/{id}/snapshot`

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
            SnapshotManifestResponse,
            await self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/snapshot",
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: SnapshotManifestResponse},
            ),
        )

    async def get_snapshot_page(
        self,
        id: str,
        body: SnapshotPageRequest | Mapping[str, Any],
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> SnapshotPageResponse:
        """Returns one page of a table for a device to copy, on a server that accepts device
        sync.

        `POST /db/{id}/snapshot/page`

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            body: The body of `POST /db/{id}/snapshot/page`, which asks for one page of a
                table's rows.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            SnapshotPageResponse,
            await self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/snapshot/page",
                    body=body,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: SnapshotPageResponse},
            ),
        )
