from __future__ import annotations

from comparison_view import durabilities, durability_label, workload_label
from render import HIGHER_IS_BETTER, emphasize_best, is_number, ms, ops, percent, ratio, table
from sources import Source


def _durability_table(node: dict) -> str:
    rows = node.get("workloads", [])
    if not rows:
        return "No workload results were recorded."
    body: list[list[str]] = []
    has_below_slo = False
    for row in rows:
        sirannon = row.get("sirannon", {})
        postgres = row.get("postgres", {})
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
            postgres_ops_cell,
            ms(postgres.get("p99_ms")),
            ratio_cell,
        ])
    headers = [
        "Workload",
        "Sirannon ops/s",
        "Sirannon 95% CI",
        "Sirannon CV",
        "Sirannon p99 ms",
        "Postgres ops/s",
        "Postgres p99 ms",
        "Rate ratio",
    ]
    aligns = ["left", "right", "right", "right", "right", "right", "right", "right"]
    rendered = table(headers, aligns, body)
    if has_below_slo:
        rendered += "\n\n_A † marks a workload where an engine could not hold p99 under the target at any offered rate; its operating point is then the best rate it sustained._"
    return rendered


def throughput_block(source: Source) -> str:
    recorded = durabilities(source.comparison)
    if not recorded:
        return "No engine results were recorded."
    parts: list[str] = []
    for name, node in recorded:
        parts.append(f"### {durability_label(name)}")
        parts.append(_durability_table(node))
    note = (
        "_Bold marks the higher of the two operating points on each workload. Where both engines held the target "
        "at the same rate, the row carries no mark. Each p99 belongs to its own engine's operating point, so the "
        "two p99 columns describe different offered rates, and the sweep tables below give tail latency at one "
        "rate._\n\n"
        "_Each throughput figure is the median of several independent runs at the operating point, the highest "
        "offered rate the engine held under the p99 target, shown with a 95% bootstrap confidence interval "
        "and the run-to-run coefficient of variation. The rate ratio is Sirannon's operating point divided by "
        "PostgreSQL's. A ratio above one means Sirannon held the target at a higher rate. Where an engine "
        "delivered a higher rate with p99 above the target, its operating point is the lower rate it last held, "
        "so a ratio compares operating points rather than the work each engine performed. Read every ratio as "
        "approximate too, because the sweep offers rates in doublings and each operating point is the last rung "
        "an engine cleared, so its true ceiling lies between that rung and the next. The sweep tables below give "
        "the rungs themselves. TPC-C-derived is a TPC-C-shaped mix of new-order and payment, not an "
        "audited TPC-C result. The YCSB subset is A, B, C, and F, and leaves out D and E._"
    )
    return "\n\n".join([*parts, note])
