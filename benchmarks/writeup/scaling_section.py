from __future__ import annotations

from comparison_view import durabilities, durability_label, workload_label
from render import HIGHER_IS_BETTER, LOWER_IS_BETTER, emphasize_best, integer, ms, ops, table
from sources import Source

from chart_paths import figure, sweep_chart


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


def tabled_scaling(comparison: dict, node: dict) -> list[dict]:
    wanted = comparison.get("config", {}).get("scaling_workloads", [])
    entries = node.get("scaling", [])
    if not wanted:
        return entries
    return [entry for entry in entries if entry.get("workload") in wanted]


def scaling_block(source: Source, chart_dir: str) -> str:
    recorded = durabilities(source.comparison)
    if not recorded:
        return "The harness recorded no scaling results."
    name, node = recorded[0]
    scaling = tabled_scaling(source.comparison, node)
    if not scaling:
        return "The harness recorded no scaling results."
    parts: list[str] = [
        "Each figure and table below shows one workload at "
        f"{durability_label(name).lower()}, at every offered rate in the sweep. PostgreSQL locks individual "
        "rows, while Sirannon commits every write through a single writer, so which engine keeps up as the rate "
        "rises depends on the workload. The dotted line on the top panel marks the offered rate, so the point "
        "where an engine's curve falls below that line is the rate at which the engine stopped keeping up.",
        "_Each row gives both engines at the same offered rate, so bold marks the better of the two figures, "
        "which is the higher achieved throughput and the lower p99. Where the two figures are equal, neither "
        "is bold._",
    ]
    has_gap = any(
        any(point.get(key) is None for key in ("sirannon_ops", "postgres_ops"))
        for entry in scaling
        for point in entry.get("curve", [])
    )
    if has_gap:
        parts.append(
            "_An n/a cell marks a rate that the harness never offered to that engine, because the engine's sweep "
            "had stopped after a lower rate that it could not sustain._"
        )
    for entry in scaling:
        workload = entry.get("workload", "n/a")
        label = workload_label(workload)
        parts.append(f"### {label}")
        parts.append(
            figure(
                sweep_chart(chart_dir, name, workload),
                f"Three panels for {label} at {durability_label(name).lower()}: achieved rate against offered "
                "rate, p99 latency against offered rate on a logarithmic scale with the target marked, and "
                "engine cores busy against offered rate.",
            )
        )
        body = [_curve_row(point) for point in entry.get("curve", [])]
        headers = ["Target ops/s", "Sirannon ops/s", "Sirannon p99 ms", "Postgres ops/s", "Postgres p99 ms"]
        parts.append(table(headers, ["right", "right", "right", "right", "right"], body))
    parts.append(
        f"_The directory `{chart_dir}/` contains the same three-panel figure for every workload at both "
        "durability levels._"
    )
    return "\n\n".join(parts)
