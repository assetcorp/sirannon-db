"""Generated from packages/ts/openapi.json and packages/ts/asyncapi.json by packages/python/generator. Do not edit."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from sirannon_client._exchange import Exchange, path_segment
from sirannon_client._http import SyncOperationsBase
from sirannon_client.models import (
    OperationExecuteRequest,
    OperationExecuteResponse,
    OperationQueryRequest,
    QueryResponse,
)


class OperationsOperations(SyncOperationsBase):
    """These methods call the Operations routes of a Sirannon server."""

    def query_operation(
        self,
        id: str,
        name: str,
        body: OperationQueryRequest | Mapping[str, Any] | None = None,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> QueryResponse:
        """Runs a registered read by name and returns its rows.

        `POST /db/{id}/query/{name}`

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            name: The name of the registered operation, percent-encoded.
            body: The body of `POST /db/{id}/query/{name}`, which runs a registered read.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            QueryResponse,
            self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/query/{path_segment(name)}",
                    body=body,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: QueryResponse},
            ),
        )

    def execute_operation(
        self,
        id: str,
        name: str,
        body: OperationExecuteRequest | Mapping[str, Any] | None = None,
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> OperationExecuteResponse:
        """Runs a registered write by name, in one transaction, and returns one result for each
        of its statements.

        `POST /db/{id}/execute/{name}`

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            name: The name of the registered operation, percent-encoded.
            body: The body of `POST /db/{id}/execute/{name}`, which runs a registered write in
                one transaction.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            OperationExecuteResponse,
            self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/execute/{path_segment(name)}",
                    body=body,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: OperationExecuteResponse},
            ),
        )
