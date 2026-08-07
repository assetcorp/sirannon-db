from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from comparison_view import durability_label, workload_label
from matplotlib.ticker import FuncFormatter

from chart_paths import operating_points_chart, repo_chart_dir
from chart_style import (
    BASE_FONT_POINTS,
    FIGURE_WIDTH_INCHES,
    POSTGRES_COLOUR,
    POSTGRES_LABEL,
    SIRANNON_COLOUR,
    SIRANNON_LABEL,
    caption,
    rate_label,
    save,
    style_axes,
)

ENGINES = ((SIRANNON_LABEL, "sirannon", SIRANNON_COLOUR), (POSTGRES_LABEL, "postgres", POSTGRES_COLOUR))
BAR_HEIGHT = 0.38


def _values(rows: list[dict], engine: str) -> tuple[list[float], list[float], list[float]]:
    medians: list[float] = []
    lows: list[float] = []
    highs: list[float] = []
    for row in rows:
        side = row.get(engine) or {}
        median = side.get("ops_median") or 0.0
        medians.append(median)
        lows.append(max(0.0, median - (side.get("ops_ci_low") or median)))
        highs.append(max(0.0, (side.get("ops_ci_high") or median) - median))
    return medians, lows, highs


def operating_points_figure(repo_root: Path, run_id: str, durability: str, rows: list[dict]) -> Path:
    labels = [workload_label(row.get("workload", "n/a")) for row in rows]
    positions = list(range(len(rows)))
    figure, axes = plt.subplots(figsize=(FIGURE_WIDTH_INCHES, 0.62 * len(rows) + 1.9))

    for offset, (label, engine, colour) in zip((BAR_HEIGHT / 2, -BAR_HEIGHT / 2), ENGINES, strict=True):
        medians, lows, highs = _values(rows, engine)
        axes.barh(
            [position + offset for position in positions],
            medians,
            height=BAR_HEIGHT,
            color=colour,
            label=label,
            xerr=[lows, highs],
            error_kw={"ecolor": "#334155", "elinewidth": 0.9, "capsize": 2.5},
        )

    style_axes(axes, "")
    axes.set_yticks(positions)
    axes.set_yticklabels(labels, fontsize=BASE_FONT_POINTS)
    axes.invert_yaxis()
    axes.set_xlim(left=0)
    axes.xaxis.set_major_formatter(FuncFormatter(rate_label))
    axes.set_xlabel("Operating point (requests per second, 95% confidence interval)")
    axes.grid(True, axis="y", alpha=0)
    axes.legend(loc="lower right", ncols=2, fontsize=BASE_FONT_POINTS)

    caption(
        figure,
        "Operating points by workload",
        f"{durability_label(durability)} · highest rate each engine held under the p99 target · run {run_id}",
    )
    return save(figure, repo_root, operating_points_chart(repo_chart_dir(run_id), durability))
