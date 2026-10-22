from __future__ import annotations

from comparison_view import run_date
from features_section import features_block
from intro_section import intro_block
from methodology_section import methodology_block
from run_section import run_block
from run_shape import over_network
from scaling_section import scaling_block
from soak_section import soak_block
from sources import Source
from throughput_section import throughput_block

from chart_paths import repo_chart_dir, run_report_chart_dir

_NO_RUN_NOTICE = (
    "_Nobody has committed a benchmark run yet. Execute the suite on the disclosed cloud machine, then commit "
    "its run directory under `benchmarks/server/results/runs/` to publish figures here._"
)


def comparison_document(source: Source) -> str:
    date = run_date(source.comparison) or "an unrecorded date"
    charts = run_report_chart_dir()
    network = over_network(source.comparison)
    placement = (
        "on one engine machine, while a second machine sent the load"
        if network
        else "on the same host"
    )
    intro = (
        f"This report gives the results of run `{source.run_id}` of the Sirannon and PostgreSQL benchmark, "
        f"which the harness recorded on {date}. For each workload, it gives the highest rate at which each engine "
        "held the latency target, the tail latency at that rate, and the rate at which the engine stopped keeping "
        f"up. Both engines served the same workloads {placement}, through the client that each engine "
        "provides."
    )
    sections = [
        "# Sirannon and PostgreSQL over a network" if network else "# Sirannon and PostgreSQL on one host",
        intro,
        "## Methodology",
        methodology_block(source.comparison),
        "## Run and machine",
        run_block(source),
        "## Operating points",
        throughput_block(source, charts),
        "## Throughput versus offered load",
        scaling_block(source, charts),
        "## Holding the operating point",
        soak_block(source, charts),
        "## Cold start and change-feed latency",
        features_block(source),
    ]
    return "\n\n".join(sections) + "\n"


def blocks(source: Source | None) -> dict[str, str]:
    if source is None:
        return {
            "intro": intro_block(None),
            "methodology": _NO_RUN_NOTICE,
            "setup": _NO_RUN_NOTICE,
            "comparison": _NO_RUN_NOTICE,
            "scaling": _NO_RUN_NOTICE,
            "soak": _NO_RUN_NOTICE,
            "features": _NO_RUN_NOTICE,
        }
    charts = repo_chart_dir(source.run_id)
    return {
        "intro": intro_block(source),
        "methodology": methodology_block(source.comparison),
        "setup": run_block(source),
        "comparison": throughput_block(source, charts),
        "scaling": scaling_block(source, charts),
        "soak": soak_block(source, charts),
        "features": features_block(source),
    }
