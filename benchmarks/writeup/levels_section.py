from __future__ import annotations

from comparison_view import durability_label, workload_label
from render import integer, is_number, ms, ops, table

ENGINE_NAMES = {"sirannon": "Sirannon", "postgres": "PostgreSQL"}


def _cell(entry: dict | None) -> str:
    if entry is None or not is_number(entry.get("target_rate")):
        return "n/a"
    marker = "" if entry.get("under_slo") else " †"
    return f"{ops(entry.get('target_rate'))} ({ms(entry.get('p99_ms'))} ms){marker}"


def _durability_rows(node: dict, levels: list[int]) -> list[list[str]]:
    rows: list[list[str]] = []
    for row in node.get("workloads", []):
        for engine in ("sirannon", "postgres"):
            recorded = row.get(f"{engine}_levels")
            if not isinstance(recorded, list):
                continue
            by_level = {entry.get("concurrency"): entry for entry in recorded}
            peak = row.get(f"{engine}_peak_concurrency")
            rows.append([
                workload_label(row.get("workload", "n/a")),
                ENGINE_NAMES[engine],
                *[_cell(by_level.get(level)) for level in levels],
                integer(peak) if is_number(peak) else "n/a",
            ])
    return rows


def levels_block(recorded: list[tuple[str, dict]], levels: list[int]) -> str:
    if not levels:
        return ""
    parts: list[str] = []
    for name, node in recorded:
        rows = _durability_rows(node, levels)
        if not rows:
            continue
        parts.append(f"#### {durability_label(name)}")
        parts.append(
            table(
                ["Workload", "Engine", *[f"{integer(level)} in flight" for level in levels], "Peak limit"],
                ["left", "left", *["right" for _ in levels], "right"],
                rows,
            )
        )
    if not parts:
        return ""
    lead = (
        "### Operating points at each in-flight limit\n\n"
        "_Each cell gives the operating point that an engine reached with at most that many requests in flight, "
        "with its p99 in brackets. An engine's peak limit is the one where its operating point was highest after "
        "one measurement at each rate. The harness measured every rate again at that limit, so the cell at the "
        "peak limit rests on every repetition, while the engine's other cells rest on one measurement. A † marks "
        "a cell where the engine's p99 exceeded the target at every offered rate._"
    )
    return "\n\n".join([lead, *parts])
