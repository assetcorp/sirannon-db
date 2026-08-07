from __future__ import annotations

RUNS_ROOT = "benchmarks/server/results/runs"
CHART_DIRNAME = "charts"


def repo_chart_dir(run_id: str) -> str:
    return f"{RUNS_ROOT}/{run_id}/{CHART_DIRNAME}"


def run_report_chart_dir() -> str:
    return CHART_DIRNAME


def sweep_chart(directory: str, durability: str, workload: str) -> str:
    return f"{directory}/{durability}-{workload}-sweep.svg"


def soak_chart(directory: str, durability: str, workload: str) -> str:
    return f"{directory}/{durability}-{workload}-soak.svg"


def tail_chart(directory: str, durability: str) -> str:
    return f"{directory}/{durability}-tail-profile.svg"


def operating_points_chart(directory: str, durability: str) -> str:
    return f"{directory}/{durability}-operating-points.svg"


def figure(path: str, alt: str) -> str:
    return f'<img src="{path}" alt="{alt}" width="820">'
