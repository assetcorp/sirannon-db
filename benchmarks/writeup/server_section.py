from __future__ import annotations

from comparison_view import run_date
from features_section import features_block
from methodology_section import METHODOLOGY
from run_section import run_block
from scaling_section import scaling_block
from soak_section import soak_block
from sources import Source
from throughput_section import throughput_block

_NO_RUN_NOTICE = (
    "_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit "
    "its run directory under `benchmarks/server/results/runs/` to publish numbers here._"
)


def comparison_document(source: Source) -> str:
    date = run_date(source.comparison) or "an unrecorded date"
    intro = (
        f"This report records one run of the Sirannon-versus-PostgreSQL benchmark, `{source.run_id}` from "
        f"{date}. Both databases answer the same workloads over the client each provides, on the same host, so "
        "the figures measure the two engines doing the same work."
    )
    sections = [
        "# Sirannon and PostgreSQL on one host",
        intro,
        "## Methodology",
        METHODOLOGY,
        "## Run and machine",
        run_block(source),
        "## Single-client and sustained-throughput comparison",
        throughput_block(source),
        "## Throughput versus offered load",
        scaling_block(source),
        "## Holding the operating point",
        soak_block(source),
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
    return {
        "methodology": METHODOLOGY,
        "setup": run_block(source),
        "comparison": throughput_block(source),
        "scaling": scaling_block(source),
        "soak": soak_block(source),
        "features": features_block(source),
    }
