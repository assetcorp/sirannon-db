from __future__ import annotations

from comparison_view import SEARCHED_NOTE, durability_label, searched_mark, workload_label
from render import HIGHER_IS_BETTER, emphasize_best, integer, is_number, ms, ops, table

ENGINE_NAMES = {"sirannon": "Sirannon", "postgres": "PostgreSQL"}


ENGINES = ("sirannon", "postgres")


def _measured(entry: dict | None) -> bool:
    return entry is not None and is_number(entry.get("target_rate"))


def _rate_cells(entries: list[dict | None]) -> list[str]:
    if not all(_measured(entry) for entry in entries):
        return [ops(entry.get("target_rate")) if _measured(entry) else "" for entry in entries]
    rates = [entry.get("target_rate") for entry in entries]
    held = [entry.get("under_slo") is True for entry in entries]
    if all(held):
        return emphasize_best(rates, HIGHER_IS_BETTER, ops)
    cells = [ops(rate) for rate in rates]
    if not any(held):
        return cells
    return [f"**{cell}**" if engine_held else cell for cell, engine_held in zip(cells, held, strict=True)]


def _cell(entry: dict | None, rate_cell: str) -> str:
    if not _measured(entry):
        return "n/a"
    marker = "" if entry.get("under_slo") else " †"
    return f"{rate_cell}{searched_mark(entry)} ({ms(entry.get('p99_ms'))} ms){marker}"


def _durability_rows(node: dict, levels: list[int]) -> list[list[str]]:
    rows: list[list[str]] = []
    for row in node.get("workloads", []):
        by_engine = {
            engine: {entry.get("concurrency"): entry for entry in recorded}
            for engine in ENGINES
            if isinstance(recorded := row.get(f"{engine}_levels"), list)
        }
        rate_cells = {
            level: dict(zip(ENGINES, _rate_cells([by_engine.get(engine, {}).get(level) for engine in ENGINES]), strict=True))
            for level in levels
        }
        for engine, by_level in by_engine.items():
            peak = row.get(f"{engine}_peak_concurrency")
            rows.append([
                workload_label(row.get("workload", "n/a")),
                ENGINE_NAMES[engine],
                *[_cell(by_level.get(level), rate_cells[level][engine]) for level in levels],
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
    if any(
        searched_mark(entry)
        for _, node in recorded
        for row in node.get("workloads", [])
        for engine in ("sirannon", "postgres")
        for entry in row.get(f"{engine}_levels") or []
    ):
        parts.append(SEARCHED_NOTE)
    lead = (
        "### Operating points at each in-flight limit\n\n"
        "_Each cell gives the operating point that an engine reached with at most that many requests in flight, "
        "with its p99 in brackets. An engine's peak limit is the one where its operating point was highest after "
        "one measurement at each rate. The harness measured every rate again at that limit, so the cell at the "
        "peak limit rests on every repetition, while the engine's other cells rest on one measurement. A † marks "
        "a cell where the engine's p99 exceeded the target at every offered rate. Bold marks the higher of the two "
        "operating points on each workload at each in-flight limit, counting only the engines that held the "
        "target. Where both engines held the target at the same rate, neither cell is bold._"
    )
    return "\n\n".join([lead, *parts])
