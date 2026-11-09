from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Final, Literal

from sirannon_client._errors import ErrorCodes, SirannonError, invalid_response
from sirannon_client._options import normalise_url

ReadPreference = Literal["primary", "replica", "nearest"]
Discovery = Literal["static", "coordinator"]
ReadConcernLevel = Literal["local", "majority", "linearizable"]

READ_CONCERN_LEVELS: Final = ("local", "majority", "linearizable")
REFRESH_CODES: Final = frozenset(
    {"STALE_PRIMARY", "AUTHORITY_LOST", "COORDINATOR_UNAVAILABLE", "NO_SAFE_PRIMARY", ErrorCodes.CONNECTION_ERROR}
)


@dataclass(frozen=True)
class ReadEndpoint:
    url: str
    read_concerns: tuple[str, ...]


@dataclass(frozen=True)
class ClusterRouting:
    current_primary: str | None
    primary_term: str | None
    read_endpoints: tuple[ReadEndpoint, ...]

    def fingerprint(self) -> str:
        endpoints = sorted(
            ({"url": endpoint.url, "readConcerns": sorted(endpoint.read_concerns)} for endpoint in self.read_endpoints),
            key=lambda entry: str(entry["url"]),
        )
        return json.dumps(
            {"currentPrimary": self.current_primary, "primaryTerm": self.primary_term, "readEndpoints": endpoints},
            sort_keys=True,
        )

    def urls(self) -> set[str]:
        listed = {endpoint.url for endpoint in self.read_endpoints}
        if self.current_primary is not None:
            listed.add(self.current_primary)
        return listed


def refreshes_routing(error: SirannonError) -> bool:
    return error.code in REFRESH_CODES


def server_base_url(endpoint: str, encoded_id: str) -> str:
    base = normalise_url(endpoint)
    return re.sub(rf"/db/{re.escape(encoded_id)}$", "", base, flags=re.IGNORECASE)


def _discovered_url(endpoint: str, encoded_id: str) -> str:
    try:
        return server_base_url(endpoint, encoded_id)
    except SirannonError as error:
        raise invalid_response(f"Cluster metadata contains an unsafe endpoint: {error.message}") from None


def _read_concerns(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise invalid_response("Cluster metadata readConcerns must be an array")
    concerns: list[str] = []
    for level in value:
        if level not in READ_CONCERN_LEVELS:
            raise invalid_response("Cluster metadata contains an invalid read concern")
        if level not in concerns:
            concerns.append(level)
    return tuple(concerns)


def parse_cluster_routing(payload: Any, database_id: str, encoded_id: str) -> ClusterRouting:
    if not isinstance(payload, dict):
        raise invalid_response("Cluster metadata must be an object")
    if payload.get("databaseId") != database_id:
        raise invalid_response("Cluster metadata database id does not match the request")
    term = payload.get("primaryTerm")
    if term is not None and not isinstance(term, str):
        raise invalid_response("Cluster metadata primaryTerm must be a string")
    primary: str | None = None
    raw_primary = payload.get("currentPrimary")
    if raw_primary is not None:
        if not isinstance(raw_primary, dict) or not isinstance(raw_primary.get("endpoint"), str):
            raise invalid_response("Cluster metadata currentPrimary must be null or an object with an endpoint")
        primary = _discovered_url(raw_primary["endpoint"], encoded_id)
    raw_endpoints = payload.get("readEndpoints", [])
    if not isinstance(raw_endpoints, list):
        raise invalid_response("Cluster metadata readEndpoints must be an array")
    endpoints: list[ReadEndpoint] = []
    for entry in raw_endpoints:
        if not isinstance(entry, dict) or not isinstance(entry.get("endpoint"), str):
            raise invalid_response("Cluster metadata read endpoint must be an object with an endpoint")
        endpoints.append(
            ReadEndpoint(_discovered_url(entry["endpoint"], encoded_id), _read_concerns(entry.get("readConcerns")))
        )
    return ClusterRouting(primary, term, tuple(endpoints))
