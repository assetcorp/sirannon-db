from __future__ import annotations

from comparison_view import durabilities, run_date
from render import integer, is_number, ms, ops
from sources import Source

HEADLINE_DURABILITY = "full"
HEADLINE_WORKLOAD = "point-select"

_NO_RUN_NOTICE = (
    "_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit "
    "its run directory under `benchmarks/server/results/runs/` to publish numbers here._"
)


def _headline_workloads(comparison: dict) -> list[dict]:
    for name, node in durabilities(comparison):
        if name == HEADLINE_DURABILITY:
            return list(node.get("workloads", []))
    return []


def _lower_tail_latency(workloads: list[dict]) -> tuple[int, int]:
    postgres_wins = 0
    measured = 0
    for row in workloads:
        sirannon = row.get("sirannon", {}).get("p99_ms")
        postgres = row.get("postgres", {}).get("p99_ms")
        if not (is_number(sirannon) and is_number(postgres)):
            continue
        measured += 1
        if float(postgres) < float(sirannon):
            postgres_wins += 1
    return postgres_wins, measured


def headline_block(source: Source | None) -> str:
    if source is None:
        return _NO_RUN_NOTICE
    comparison = source.comparison
    workloads = _headline_workloads(comparison)
    headline = next((row for row in workloads if row.get("workload") == HEADLINE_WORKLOAD), None)
    if headline is None:
        return _NO_RUN_NOTICE
    sirannon = headline.get("sirannon", {})
    postgres = headline.get("postgres", {})
    postgres_wins, measured = _lower_tail_latency(workloads)
    rows = integer(comparison.get("config", {}).get("data_size"))
    machine = comparison.get("environment", {}).get("machine_label") or "an unrecorded machine"
    date = run_date(comparison) or "an unrecorded date"
    sentences = [
        f"On point-select at {rows} rows, with both engines fsyncing every commit, Sirannon sustained "
        f"{ops(sirannon.get('ops_median'))} operations a second against PostgreSQL's "
        f"{ops(postgres.get('ops_median'))}.",
        f"Postgres held the lower tail latency at those operating points, {ms(postgres.get('p99_ms'))} ms "
        f"against Sirannon's {ms(sirannon.get('p99_ms'))} ms.",
        f"That pattern holds on {postgres_wins} of the {measured} workloads at this durability level, so read "
        "the rate and the latency together.",
        f"The harness recorded both engines in run `{source.run_id}` on {date}, on {machine}.",
        "You will find every workload, both durability levels, and the full method in "
        "[`BENCHMARKS.md`](BENCHMARKS.md).",
    ]
    return " ".join(sentences)


def readme_blocks(source: Source | None) -> dict[str, str]:
    return {"headline": headline_block(source)}
