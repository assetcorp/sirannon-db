from __future__ import annotations

from comparison_view import durabilities, durability_label, workload_label
from render import HIGHER_IS_BETTER, LOWER_IS_BETTER, emphasize_best, integer, ms, ops, table
from sources import Source


def _curve_row(point: dict) -> list[str]:
    sirannon_ops_cell, postgres_ops_cell = emphasize_best(
        [point.get("sirannon_ops"), point.get("postgres_ops")],
        HIGHER_IS_BETTER,
        ops,
    )
    sirannon_p99_cell, postgres_p99_cell = emphasize_best(
        [point.get("sirannon_p99_ms"), point.get("postgres_p99_ms")],
        LOWER_IS_BETTER,
        ms,
    )
    return [
        integer(point.get("target_rate")),
        sirannon_ops_cell,
        sirannon_p99_cell,
        postgres_ops_cell,
        postgres_p99_cell,
    ]


def scaling_block(source: Source) -> str:
    recorded = durabilities(source.comparison)
    if not recorded:
        return "No scaling results were recorded."
    name, node = recorded[0]
    scaling = node.get("scaling", [])
    if not scaling:
        return "No scaling results were recorded."
    parts: list[str] = [
        "The tables below show achieved throughput and p99 latency as the offered rate climbs, at "
        f"{durability_label(name).lower()}. PostgreSQL relies on row-level locking and Sirannon on a "
        "single writer, so which one holds throughput as the rate rises depends on the workload.",
        "_Both engines answer the same offered rate on every row, so bold marks the better figure of the two: the "
        "higher achieved throughput and the lower p99. Where the two figures are equal, the row carries no "
        "mark._",
    ]
    has_gap = any(
        any(point.get(key) is None for key in ("sirannon_ops", "postgres_ops"))
        for entry in scaling
        for point in entry.get("curve", [])
    )
    if has_gap:
        parts.append(
            "_An n/a cell marks a rate one engine never ran: its sweep had already stopped at a lower rate it "
            "could not sustain. Where a column ends is that engine's limit._"
        )
    for entry in scaling:
        parts.append(f"### {workload_label(entry.get('workload', 'n/a'))}")
        body = [_curve_row(point) for point in entry.get("curve", [])]
        headers = ["Target ops/s", "Sirannon ops/s", "Sirannon p99 ms", "Postgres ops/s", "Postgres p99 ms"]
        parts.append(table(headers, ["right", "right", "right", "right", "right"], body))
    return "\n\n".join(parts)
