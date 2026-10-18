from __future__ import annotations

from comparison_view import durabilities, durability_label, workload_label
from render import HIGHER_IS_BETTER, emphasize_best, is_number, ms, ops, percent, ratio, table
from sources import Source

from chart_paths import figure, operating_points_chart, tail_chart

ERROR_RATE_CEILING = 0.01
HELD_EVERY_RATE = "held every rate"


def _held(side: dict, slo_ms: object) -> bool:
    p99 = side.get("p99_ms")
    error_rate = side.get("error_rate")
    return (
        side.get("sustained") is True
        and is_number(p99)
        and is_number(slo_ms)
        and float(p99) <= float(slo_ms)
        and (not is_number(error_rate) or float(error_rate) < ERROR_RATE_CEILING)
    )


def _missed_cell(curve: list[dict], engine: str, operating_rate: object, slo_ms: object) -> str:
    if not is_number(operating_rate):
        return "n/a"
    for point in curve:
        rate = point.get("target_rate")
        side = point.get(engine) or {}
        if not is_number(rate) or float(rate) <= float(operating_rate) or side.get("ops") is None:
            continue
        if not _held(side, slo_ms):
            return f"{ops(rate)} ({ms(side.get('p99_ms'))} ms)"
    return HELD_EVERY_RATE


def _durability_table(node: dict, slo_ms: object) -> str:
    rows = node.get("workloads", [])
    if not rows:
        return "The harness recorded no workload results."
    curves = {entry.get("workload"): entry.get("curve", []) for entry in node.get("scaling", [])}
    body: list[list[str]] = []
    has_below_slo = False
    for row in rows:
        sirannon = row.get("sirannon", {})
        postgres = row.get("postgres", {})
        curve = curves.get(row.get("workload"), [])
        rate_ratio = row.get("speedup")
        ci_cell = f"[{ops(sirannon.get('ops_ci_low'))}, {ops(sirannon.get('ops_ci_high'))}]"
        if rate_ratio and is_number(rate_ratio.get("point")):
            ratio_cell = (
                f"{ratio(rate_ratio['point'])} "
                f"[{ratio(rate_ratio.get('ci_low'))}, {ratio(rate_ratio.get('ci_high'))}]"
            )
        else:
            ratio_cell = "n/a"
        marker = "" if sirannon.get("under_slo", True) and postgres.get("under_slo", True) else " †"
        if marker:
            has_below_slo = True
        sirannon_ops_cell, postgres_ops_cell = emphasize_best(
            [sirannon.get("ops_median"), postgres.get("ops_median")],
            HIGHER_IS_BETTER,
            ops,
        )
        body.append([
            workload_label(row.get("workload", "n/a")) + marker,
            sirannon_ops_cell,
            ci_cell,
            percent(sirannon.get("ops_cv")),
            ms(sirannon.get("p99_ms")),
            _missed_cell(curve, "sirannon", sirannon.get("target_rate"), slo_ms),
            postgres_ops_cell,
            ms(postgres.get("p99_ms")),
            _missed_cell(curve, "postgres", postgres.get("target_rate"), slo_ms),
            ratio_cell,
        ])
    headers = [
        "Workload",
        "Sirannon ops/s",
        "Sirannon 95% CI",
        "Sirannon CV",
        "Sirannon p99 ms",
        "Sirannon missed at",
        "Postgres ops/s",
        "Postgres p99 ms",
        "Postgres missed at",
        "Rate ratio",
    ]
    aligns = ["left", "right", "right", "right", "right", "right", "right", "right", "right", "right"]
    rendered = table(headers, aligns, body)
    if has_below_slo:
        rendered += (
            "\n\n_A † marks a workload where an engine's p99 exceeded the target at every offered rate, so its "
            "operating point is the highest rate that it sustained._"
        )
    return rendered


def throughput_block(source: Source, chart_dir: str) -> str:
    recorded = durabilities(source.comparison)
    if not recorded:
        return "The harness recorded no engine results."
    slo_ms = source.comparison.get("config", {}).get("slo_p99_ms")
    parts: list[str] = []
    for name, node in recorded:
        label = durability_label(name)
        parts.append(f"### {label}")
        parts.append(
            figure(
                operating_points_chart(chart_dir, name),
                f"Grouped bars of each engine's operating point on every workload at {label.lower()}, "
                "with 95% confidence intervals.",
            )
        )
        parts.append(
            figure(
                tail_chart(chart_dir, name),
                f"Latency at the p50, p95, p99, p99.9 and maximum percentiles for both engines on every "
                f"workload at {label.lower()}, on a logarithmic scale.",
            )
        )
        parts.append(_durability_table(node, slo_ms))
    note = (
        "_Bold marks the higher of the two operating points on each workload. Where both engines held the target "
        "at the same rate, neither cell is bold. Each p99 comes from its own engine's operating point, so the two "
        "p99 columns can describe different offered rates. For both engines at one offered rate, use the sweep "
        "tables below. Each 'missed at' column gives the next offered rate above the operating point at which "
        "that engine missed the target, with its p99 at that rate._\n\n"
        "_Each throughput figure is the median of the repeated measurements at the operating point, which is the "
        "highest offered rate that the engine held at or under the p99 target. The table shows each Sirannon "
        "figure with a 95% bootstrap confidence interval and the coefficient of variation across repetitions. "
        "The rate ratio is "
        "Sirannon's operating point divided by PostgreSQL's, so a ratio above one means that Sirannon held the "
        "target at a higher rate. Every ratio is approximate, because the harness doubles the offered rate at "
        "each step, so an engine's highest sustainable rate lies between its operating point and the rate in its "
        "'missed at' column. For TPC-C-derived, the generator sends two kinds of one-statement write in equal "
        "shares, which are an insert into `orders` and an update of one customer's balance. Neither write is a "
        "full TPC-C transaction. The harness measures YCSB A, B, C, and F, but not D or E._"
    )
    return "\n\n".join([*parts, note])
