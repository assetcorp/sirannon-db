from __future__ import annotations

from math import ceil
from pathlib import Path

import matplotlib.pyplot as plt
from comparison_view import durability_label, workload_label
from matplotlib.ticker import FuncFormatter

from chart_paths import repo_chart_dir, tail_chart
from chart_style import (
    BASE_FONT_POINTS,
    FIGURE_WIDTH_INCHES,
    POSTGRES_COLOUR,
    POSTGRES_LABEL,
    REFERENCE_COLOUR,
    SIRANNON_COLOUR,
    SIRANNON_LABEL,
    caption,
    millisecond_label,
    save,
    style_axes,
)

PERCENTILE_FIELDS = ("p50_ms", "p95_ms", "p99_ms", "p999_ms", "max_ms")
PERCENTILE_LABELS = ("p50", "p95", "p99", "p99.9", "max")
ENGINES = ((SIRANNON_LABEL, "sirannon", SIRANNON_COLOUR), (POSTGRES_LABEL, "postgres", POSTGRES_COLOUR))
COLUMNS = 3
CAPTION_RESERVE_POINTS = 70


def _profile(row: dict, engine: str) -> list[float | None]:
    side = row.get(engine) or {}
    return [side.get(field) for field in PERCENTILE_FIELDS]


def tail_figure(
    repo_root: Path,
    run_id: str,
    durability: str,
    rows: list[dict],
    target_ms: float | None,
) -> Path:
    count = len(rows)
    columns = min(COLUMNS, count)
    render_rows = ceil(count / columns)
    figure, grid = plt.subplots(
        render_rows,
        columns,
        figsize=(FIGURE_WIDTH_INCHES, 2.3 * render_rows + 1.2),
        sharex=True,
        squeeze=False,
    )
    positions = range(len(PERCENTILE_LABELS))

    for index, row in enumerate(rows):
        axes = grid[index // columns][index % columns]
        for label, engine, colour in ENGINES:
            values = _profile(row, engine)
            drawn = [(x, y) for x, y in zip(positions, values, strict=True) if y is not None]
            axes.plot(
                [x for x, _ in drawn],
                [y for _, y in drawn],
                color=colour,
                marker="o",
                markersize=3.5,
                linewidth=1.6,
                label=label,
            )
        style_axes(axes, "")
        axes.set_yscale("log")
        axes.yaxis.set_major_formatter(FuncFormatter(millisecond_label))
        axes.set_title(workload_label(row.get("workload", "n/a")), fontsize=BASE_FONT_POINTS)
        axes.set_xticks(list(positions))
        axes.set_xticklabels(PERCENTILE_LABELS, fontsize=BASE_FONT_POINTS - 1)
        if target_ms:
            axes.axhline(target_ms, color=REFERENCE_COLOUR, linestyle="--", linewidth=1)
        if index + columns >= count:
            axes.tick_params(labelbottom=True)
        if index % columns == 0:
            axes.set_ylabel("Latency (ms)")

    for empty in range(count, render_rows * columns):
        grid[empty // columns][empty % columns].axis("off")

    caption(
        figure,
        "Latency profile at each engine's operating point",
        f"{durability_label(durability)} · five recorded percentiles · dashed line is the p99 target · run {run_id}",
        CAPTION_RESERVE_POINTS,
    )
    handles, labels = grid[0][0].get_legend_handles_labels()
    figure.legend(handles, labels, loc="upper right", ncols=2, fontsize=BASE_FONT_POINTS - 1)
    figure.subplots_adjust(hspace=0.32, wspace=0.28)
    return save(figure, repo_root, tail_chart(repo_chart_dir(run_id), durability))
