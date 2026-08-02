from __future__ import annotations

from comparison_view import durabilities, durability_label, workload_label
from render import integer, ms, ops, percent, table
from sources import Source


def _soak_row(label: str, engine: str, soak: object) -> list[str] | None:
    if not isinstance(soak, dict):
        return None
    if soak.get("skipped"):
        return [label, engine, "n/a", "n/a", "n/a", "n/a", "n/a", "no rate sustained"]
    latency = soak.get("latency_ms") or {}
    worst = soak.get("worst_window") or {}
    return [
        label,
        engine,
        integer(soak.get("target_rate")),
        ops(soak.get("achieved_rate")),
        ms(latency.get("p99")),
        ms(worst.get("p99_ms")),
        percent(soak.get("error_rate")),
        "yes" if soak.get("held") else "no",
    ]


def soak_block(source: Source) -> str:
    recorded = durabilities(source.comparison)
    headers = ["Workload", "Engine", "Rate held", "Achieved", "p99 ms", "Worst 30 s p99", "Errors", "Held"]
    aligns = ["left", "left", "right", "right", "right", "right", "right", "left"]
    parts: list[str] = []
    for name, node in recorded:
        body: list[list[str]] = []
        for row in node.get("workloads", []):
            label = workload_label(row.get("workload", "n/a"))
            for engine_name, key in (("Sirannon", "sirannon_soak"), ("PostgreSQL", "postgres_soak")):
                rendered = _soak_row(label, engine_name, row.get(key))
                if rendered is not None:
                    body.append(rendered)
        if not body:
            continue
        parts.append(f"### {durability_label(name)}")
        parts.append(table(headers, aligns, body))
    if not parts:
        return "No soak results were recorded."
    intro = (
        "The sweep measures in short windows, so this section holds each engine at its operating point for one "
        "long continuous window instead. The window is long enough to cross both engines' checkpoint cycles, "
        "and the worst-30-second column shows the slowest slice of it, which is where a checkpoint stall "
        "appears. An engine holds when it keeps at least 95% of the rate with under 1% errors across the "
        "whole window."
    )
    return "\n\n".join([intro, *parts])
