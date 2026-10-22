from __future__ import annotations

from comparison_view import workload_label
from render import decimal, is_number, table


def _cell(value: object) -> str:
    if not is_number(value):
        return "n/a"
    return decimal(value, 0) if float(value).is_integer() else decimal(value, 2)


def operation_cost_table(recorded: list[tuple[str, dict]]) -> str:
    if not recorded:
        return ""
    _, node = recorded[0]
    body: list[list[str]] = []
    for row in node.get("workloads", []):
        sirannon = row.get("sirannon_operation_cost")
        postgres = row.get("postgres_operation_cost")
        if not isinstance(sirannon, dict) or not isinstance(postgres, dict):
            return ""
        body.append([
            workload_label(row.get("workload", "n/a")),
            _cell(sirannon.get("statements_per_operation")),
            _cell(sirannon.get("round_trips_per_operation")),
            _cell(postgres.get("round_trips_per_operation")),
        ])
    if not body:
        return ""
    lead = (
        "_Each client decides how many network round trips one operation takes. The Sirannon SDK sends a "
        "transaction as one request, while node-postgres sends `BEGIN`, every statement, and `COMMIT` as "
        "separate round trips. The table gives the weighted mean for each workload's mix of operations._"
    )
    rendered = table(
        ["Workload", "Statements per operation", "Sirannon round trips", "PostgreSQL round trips"],
        ["left", "right", "right", "right"],
        body,
    )
    return f"{lead}\n\n{rendered}"
