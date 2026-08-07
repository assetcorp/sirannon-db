from __future__ import annotations

from comparison_view import durabilities, durability_label, workload_label
from render import HIGHER_IS_BETTER, emphasize_best, emphasize_sole_winner, integer, ms, ops, percent, table
from sources import Source

ACHIEVED_COLUMN = 3
HELD_COLUMN = 7


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
        "yes" if soak.get("held") and soak.get("p99_under_slo") else "no",
    ]


def _emphasize_pair(rows: list[list[str]], soaks: list[object]) -> None:
    if len(rows) != 2:
        return
    achieved = [soak.get("achieved_rate") if isinstance(soak, dict) else None for soak in soaks]
    for row, cell in zip(rows, emphasize_best(achieved, HIGHER_IS_BETTER, ops), strict=True):
        row[ACHIEVED_COLUMN] = cell
    held = emphasize_sole_winner([row[HELD_COLUMN] for row in rows], "yes")
    for row, cell in zip(rows, held, strict=True):
        row[HELD_COLUMN] = cell


def soak_block(source: Source) -> str:
    recorded = durabilities(source.comparison)
    headers = ["Workload", "Engine", "Rate held", "Achieved", "p99 ms", "Worst 30 s p99", "Errors", "Held"]
    aligns = ["left", "left", "right", "right", "right", "right", "right", "left"]
    parts: list[str] = []
    for name, node in recorded:
        body: list[list[str]] = []
        for row in node.get("workloads", []):
            label = workload_label(row.get("workload", "n/a"))
            pair: list[list[str]] = []
            soaks: list[object] = []
            for engine_name, key in (("Sirannon", "sirannon_soak"), ("PostgreSQL", "postgres_soak")):
                rendered = _soak_row(label, engine_name, row.get(key))
                if rendered is not None:
                    pair.append(rendered)
                    soaks.append(row.get(key))
            _emphasize_pair(pair, soaks)
            body.extend(pair)
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
        "appears. An engine holds when it keeps at least 95% of the rate with under 1% errors and a p99 under "
        "the service-level target across the whole window, so an engine that keeps the pace but misses the "
        "latency target reads as a miss. Bold marks the higher rate of the two engines on a workload. It also "
        "marks the engine that held where only one of the two held. Each engine ran this window at its own "
        "operating point, so the latency columns carry no mark."
    )
    return "\n\n".join([intro, *parts])
