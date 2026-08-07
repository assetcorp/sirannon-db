from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from comparison_view import durability_label, workload_label
from matplotlib.ticker import FuncFormatter

from chart_paths import repo_chart_dir, soak_chart
from chart_style import (
    FIGURE_WIDTH_INCHES,
    POSTGRES_COLOUR,
    POSTGRES_LABEL,
    SIRANNON_COLOUR,
    SIRANNON_LABEL,
    caption,
    latency_axis,
    rate_label,
    save,
    style_axes,
)

ENGINES = ((SIRANNON_LABEL, "sirannon_soak", SIRANNON_COLOUR), (POSTGRES_LABEL, "postgres_soak", POSTGRES_COLOUR))


def _windows(row: dict, key: str) -> list[dict]:
    soak = row.get(key)
    if not isinstance(soak, dict) or soak.get("skipped"):
        return []
    windows = soak.get("windows")
    return windows if isinstance(windows, list) else []


def has_soak(row: dict) -> bool:
    return any(_windows(row, key) for _, key, _colour in ENGINES)


def soak_figure(
    repo_root: Path,
    run_id: str,
    durability: str,
    row: dict,
    target_ms: float | None,
) -> Path:
    figure, (rate_axes, latency_axes) = plt.subplots(
        2,
        1,
        figsize=(FIGURE_WIDTH_INCHES, 5.4),
        sharex=True,
        height_ratios=[1, 1.15],
    )

    for label, key, colour in ENGINES:
        windows = _windows(row, key)
        if not windows:
            continue
        minutes = [window["start_seconds"] / 60 for window in windows]
        achieved = [window.get("achieved_rate") for window in windows]
        p99 = [window.get("p99_ms") for window in windows]
        held = (row.get(key) or {}).get("target_rate")
        legend = f"{label} at {rate_label(held)} ops/s" if held else label
        rate_axes.plot(minutes, achieved, color=colour, linewidth=1.6, label=legend)
        latency_axes.plot(minutes, p99, color=colour, linewidth=1.6, label=legend)

    style_axes(rate_axes, "Achieved ops/s")
    style_axes(latency_axes, "p99 latency (ms)")
    rate_axes.set_ylim(bottom=0)
    rate_axes.yaxis.set_major_formatter(FuncFormatter(rate_label))
    latency_axis(latency_axes, target_ms)
    latency_axes.set_xlabel("Minutes into the window")
    rate_axes.legend(loc="lower left", ncols=2, fontsize=8)

    caption(
        figure,
        f"{workload_label(row.get('workload', 'n/a'))} held at the operating point",
        f"{durability_label(durability)} · 30-second windows · run {run_id}",
    )
    figure.subplots_adjust(hspace=0.14)
    return save(figure, repo_root, soak_chart(repo_chart_dir(run_id), durability, row.get("workload", "unknown")))
