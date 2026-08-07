from __future__ import annotations

from comparison_view import run_date
from features_section import features_block
from methodology_section import METHODOLOGY
from run_section import run_block
from scaling_section import scaling_block
from soak_section import soak_block
from sources import Source
from throughput_section import throughput_block

from chart_paths import repo_chart_dir, run_report_chart_dir

_NO_RUN_NOTICE = (
    "_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit "
    "its run directory under `benchmarks/server/results/runs/` to publish numbers here._"
)


def comparison_document(source: Source) -> str:
    date = run_date(source.comparison) or "an unrecorded date"
    charts = run_report_chart_dir()
    intro = (
        f"This report records one run of the Sirannon and PostgreSQL benchmark, `{source.run_id}` from "
        f"{date}. It measures what each engine does under the same workloads: the rates each one holds, the tail "
        "latency it records at those rates, and the rate where it stops keeping up. Both databases answer those "
        "workloads over the client each provides, on the same host, so the figures come from the two engines "
        "doing the same work."
    )
    sections = [
        "# Sirannon and PostgreSQL on one host",
        intro,
        "## Methodology",
        METHODOLOGY,
        "## Run and machine",
        run_block(source),
        "## Operating points",
        throughput_block(source, charts),
        "## Throughput versus offered load",
        scaling_block(source, charts),
        "## Holding the operating point",
        soak_block(source, charts),
        "## Sirannon-only characterizations",
        features_block(source),
    ]
    return "\n\n".join(sections) + "\n"


def blocks(source: Source | None) -> dict[str, str]:
    if source is None:
        return {
            "methodology": _NO_RUN_NOTICE,
            "setup": _NO_RUN_NOTICE,
            "comparison": _NO_RUN_NOTICE,
            "scaling": _NO_RUN_NOTICE,
            "soak": _NO_RUN_NOTICE,
            "features": _NO_RUN_NOTICE,
        }
    charts = repo_chart_dir(source.run_id)
    return {
        "methodology": METHODOLOGY,
        "setup": run_block(source),
        "comparison": throughput_block(source, charts),
        "scaling": scaling_block(source, charts),
        "soak": soak_block(source, charts),
        "features": features_block(source),
    }
