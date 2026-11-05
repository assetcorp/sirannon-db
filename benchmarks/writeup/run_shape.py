from __future__ import annotations

import re

from comparison_view import durabilities

POSTGRES_DOCS_ROOT = "https://www.postgresql.org/docs"
_POSTGRES_MAJOR = re.compile(r"PostgreSQL (\d+)")


def over_network(comparison: dict) -> bool:
    delivery = comparison.get("config", {}).get("delivery") or {}
    return delivery.get("transport") == "network"


def concurrency_levels(comparison: dict) -> list[int]:
    levels = comparison.get("config", {}).get("concurrency_levels")
    if not isinstance(levels, list):
        return []
    return [int(level) for level in levels if isinstance(level, (int, float))]


def postgres_major(comparison: dict) -> str | None:
    for _, node in durabilities(comparison):
        version = str((node.get("postgres_engine") or {}).get("version") or "")
        match = _POSTGRES_MAJOR.search(version)
        if match:
            return match.group(1)
    return None


def postgres_docs(comparison: dict) -> str:
    return f"{POSTGRES_DOCS_ROOT}/{postgres_major(comparison) or 'current'}"


def network_round_trip(comparison: dict) -> dict | None:
    for _, node in durabilities(comparison):
        network = node.get("network") or {}
        for engine in ("sirannon", "postgres"):
            measured = network.get(engine)
            if isinstance(measured, dict) and isinstance(measured.get("round_trip_ms"), dict):
                return measured
    return None


def engine_machine(comparison: dict) -> dict:
    machine = comparison.get("engine_machine")
    return machine if isinstance(machine, dict) else {}
