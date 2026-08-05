from __future__ import annotations

from comparison_view import durabilities, durability_label, workload_label
from render import is_number, ms, ops, percent, speedup, table
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
        speed = row.get("speedup")
        ci_cell = f"[{ops(sirannon.get('ops_ci_low'))}, {ops(sirannon.get('ops_ci_high'))}]"
        if speed and is_number(speed.get("point")):
            speed_cell = f"{speedup(speed['point'])} [{speedup(speed.get('ci_low'))}, {speedup(speed.get('ci_high'))}]"
        else:
            speed_cell = "n/a"
        marker = "" if sirannon.get("under_slo", True) and postgres.get("under_slo", True) else " †"
        if marker:
            has_below_slo = True
        body.append([
            workload_label(row.get("workload", "n/a")) + marker,
            ops(sirannon.get("ops_median")),
            ci_cell,
            percent(sirannon.get("ops_cv")),
            ms(sirannon.get("p99_ms")),
            ops(postgres.get("ops_median")),
            ms(postgres.get("p99_ms")),
            speed_cell,
        ])
    headers = [
        "Workload",
        "Sirannon ops/s",
        "Sirannon 95% CI",
        "Sirannon CV",
        "Sirannon p99 ms",
        "Postgres ops/s",
        "Postgres p99 ms",
        "Speedup",
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
        "_Each throughput figure is the median of several independent runs at the operating point, the highest "
        "offered rate the engine sustained under the p99 target, shown with a 95% bootstrap confidence interval "
        "and the run-to-run coefficient of variation. A speedup above one means Sirannon sustained more "
        "throughput than PostgreSQL. Read every speedup as approximate, because the sweep offers rates in "
        "doublings and each operating point is the last rung an engine cleared, so its true ceiling lies "
        "between that rung and the next. The sweep tables below give the rungs themselves. TPC-C-derived is a "
        "TPC-C-shaped mix of new-order and payment, not an "
        "audited TPC-C result. The YCSB subset is A, B, C, and F, and leaves out D and E._"
    )
    return "\n\n".join([*parts, note])
