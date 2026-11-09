"""Generated from packages/ts/openapi.json and packages/ts/asyncapi.json by packages/python/generator. Do not edit."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from sirannon_client._exchange import Exchange, path_segment
from sirannon_client._http import SyncOperationsBase
from sirannon_client.models import CapabilitiesResponse, ClusterStatusResponse, LivenessResponse, ReadinessResponse


class ServerOperations(SyncOperationsBase):
    """These methods call the Server routes of a Sirannon server."""

    def get_capabilities(
        self,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> CapabilitiesResponse:
        """Returns the capabilities that the server announces, such as query.sql when it
        accepts SQL statements, and the digest of its registered operations.

        `GET /capabilities`

        Args:
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            CapabilitiesResponse,
            self._transport.json(
                Exchange(
                    method="GET",
                    path="/capabilities",
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: CapabilitiesResponse},
            ),
        )

    def get_health(
        self,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> LivenessResponse:
        """Returns ok while the server process runs.

        `GET /health`

        Args:
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            LivenessResponse,
            self._transport.json(
                Exchange(
                    method="GET",
                    path="/health",
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: LivenessResponse},
            ),
        )

    def get_readiness(
        self,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> ReadinessResponse:
        """Returns whether each open database and the replication of this node can serve
        requests.

        `GET /health/ready`

        Args:
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            ReadinessResponse,
            self._transport.json(
                Exchange(
                    method="GET",
                    path="/health/ready",
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: ReadinessResponse},
            ),
        )

    def get_cluster_status(
        self,
        id: str,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> ClusterStatusResponse:
        """Returns the primary, the primary term, and the readable nodes of the database, to a
        caller that the server authorises for routing metadata.

        `GET /db/{id}/cluster`

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
            ClusterStatusResponse,
            self._transport.json(
                Exchange(
                    method="GET",
                    path=f"/db/{path_segment(id)}/cluster",
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: ClusterStatusResponse},
            ),
        )
