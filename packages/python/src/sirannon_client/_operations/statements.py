"""Generated from packages/ts/openapi.json and packages/ts/asyncapi.json by packages/python/generator. Do not edit."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from sirannon_client._exchange import Exchange, path_segment
from sirannon_client._http import SyncOperationsBase
from sirannon_client.models import (
    BatchRequest,
    BatchResponse,
    ExecuteRequest,
    ExecuteResponse,
    LoadRequest,
    LoadResponse,
    QueryRequest,
    QueryResponse,
    TransactionRequest,
    TransactionResponse,
)


class StatementsOperations(SyncOperationsBase):
    """These methods call the Statements routes of a Sirannon server."""

    def query(
        self,
        id: str,
        body: QueryRequest | Mapping[str, Any],
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> QueryResponse:
        """Runs one read statement and returns its rows, on a server that accepts SQL.

        `POST /db/{id}/query`

        The client reads `GET /capabilities` first and raises `SQL_NOT_ACCEPTED` without
        sending the statement when the server leaves out `query.sql`.

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            body: Body of `POST /db/{id}/query`.
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
                    path=f"/db/{path_segment(id)}/query",
                    body=body,
                    sql_statement=True,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: QueryResponse},
            ),
        )

    def execute(
        self,
        id: str,
        body: ExecuteRequest | Mapping[str, Any],
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> ExecuteResponse:
        """Runs one write statement, on a server that accepts SQL.

        `POST /db/{id}/execute`

        The client reads `GET /capabilities` first and raises `SQL_NOT_ACCEPTED` without
        sending the statement when the server leaves out `query.sql`.

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            body: Body of `POST /db/{id}/execute`.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            ExecuteResponse,
            self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/execute",
                    body=body,
                    sql_statement=True,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: ExecuteResponse},
            ),
        )

    def transaction(
        self,
        id: str,
        body: TransactionRequest | Mapping[str, Any],
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> TransactionResponse:
        """Runs every statement in one transaction, so that all succeed or all fail, on a
        server that accepts SQL.

        `POST /db/{id}/transaction`

        The client reads `GET /capabilities` first and raises `SQL_NOT_ACCEPTED` without
        sending the statement when the server leaves out `query.sql`.

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            body: Body of `POST /db/{id}/transaction`, whose statements all succeed or all
                fail.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            TransactionResponse,
            self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/transaction",
                    body=body,
                    sql_statement=True,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: TransactionResponse},
            ),
        )

    def batch(
        self,
        id: str,
        body: BatchRequest | Mapping[str, Any],
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> BatchResponse:
        """Runs one statement once for each parameter set, in one transaction, on a server that
        accepts SQL.

        `POST /db/{id}/batch`

        The client reads `GET /capabilities` first and raises `SQL_NOT_ACCEPTED` without
        sending the statement when the server leaves out `query.sql`.

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            body: The body of `POST /db/{id}/batch`, which the server commits atomically in
                one transaction with one fsync.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            BatchResponse,
            self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/batch",
                    body=body,
                    sql_statement=True,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: BatchResponse},
            ),
        )

    def load(
        self,
        id: str,
        body: LoadRequest | Mapping[str, Any],
        *,
        timeout_ms: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> LoadResponse:
        """Loads rows while the writer runs at relaxed durability, on a server that accepts
        SQL.

        `POST /db/{id}/load`

        The client reads `GET /capabilities` first and raises `SQL_NOT_ACCEPTED` without
        sending the statement when the server leaves out `query.sql`.

        Args:
            id: The identifier of the database, percent-encoded, so that an identifier
                holding a slash reaches the database stored under it.
            body: The body of `POST /db/{id}/load`, which loads rows while the server relaxes
                the writer's durability. The server restores the configured durability
                before it sends the response. When the process crashes during a load, SQLite
                rolls back the uncommitted rows, so the client can send the load again.
            timeout_ms: The client waits this many milliseconds for the answer, in place of the client's
                setting, and 0 waits for as long as the server takes.
            headers: The client adds these headers to this request, replacing any of the same name.

        Returns:
            HTTP 200: The server answered the request.
        """
        return cast(
            LoadResponse,
            self._transport.json(
                Exchange(
                    method="POST",
                    path=f"/db/{path_segment(id)}/load",
                    body=body,
                    sql_statement=True,
                    timeout_ms=timeout_ms,
                    headers=headers,
                ),
                {200: LoadResponse},
            ),
        )
