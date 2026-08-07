from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter

SIRANNON_COLOUR = "#0d9488"
POSTGRES_COLOUR = "#b45309"
REFERENCE_COLOUR = "#64748b"
IDEAL_COLOUR = "#94a3b8"
BACKGROUND_COLOUR = "#ffffff"
TEXT_COLOUR = "#334155"
GRID_COLOUR = "#e2e8f0"

SIRANNON_LABEL = "Sirannon"
POSTGRES_LABEL = "PostgreSQL"

FIGURE_WIDTH_INCHES = 8.2
BASE_FONT_POINTS = 9

_RC = {
    "figure.dpi": 100,
    "savefig.dpi": 100,
    "font.size": BASE_FONT_POINTS,
    "font.family": "sans-serif",
    "font.sans-serif": ["DejaVu Sans"],
    "text.color": TEXT_COLOUR,
    "axes.labelcolor": TEXT_COLOUR,
    "axes.edgecolor": GRID_COLOUR,
    "axes.titlesize": BASE_FONT_POINTS + 1,
    "axes.titleweight": "bold",
    "axes.facecolor": BACKGROUND_COLOUR,
    "figure.facecolor": BACKGROUND_COLOUR,
    "savefig.facecolor": BACKGROUND_COLOUR,
    "xtick.color": TEXT_COLOUR,
    "ytick.color": TEXT_COLOUR,
    "grid.color": GRID_COLOUR,
    "grid.linewidth": 0.6,
    "legend.frameon": False,
    "svg.fonttype": "path",
    "svg.hashsalt": "sirannon-benchmarks",
    "path.simplify": False,
}


def apply_style() -> None:
    plt.rcParams.update(_RC)


def rate_label(value: float, _position: float = 0) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:g}M"
    if value >= 1_000:
        return f"{value / 1_000:g}K"
    return f"{value:g}"


def millisecond_label(value: float, _position: float = 0) -> str:
    return f"{value:,.0f}" if value >= 1_000 else f"{value:g}"


def style_axes(axes, ylabel: str) -> None:
    axes.set_ylabel(ylabel)
    axes.grid(True, which="major", axis="both", alpha=0.7)
    axes.set_axisbelow(True)
    for side in ("top", "right"):
        axes.spines[side].set_visible(False)


def rate_axis(axes, rates: Sequence[float]) -> None:
    axes.set_xscale("log", base=2)
    axes.set_xticks(list(rates))
    axes.xaxis.set_major_formatter(FuncFormatter(rate_label))
    axes.minorticks_off()


def latency_axis(axes, target_ms: float | None) -> None:
    axes.set_yscale("log")
    axes.yaxis.set_major_formatter(FuncFormatter(millisecond_label))
    if target_ms:
        axes.axhline(target_ms, color=REFERENCE_COLOUR, linestyle="--", linewidth=1)
        reference_note(axes, target_ms, f"p99 target {millisecond_label(target_ms)} ms")


def reference_note(axes, value: float, text: str) -> None:
    axes.annotate(
        text,
        xy=(1.008, value),
        xycoords=("axes fraction", "data"),
        ha="left",
        va="center",
        fontsize=BASE_FONT_POINTS - 1,
        color=REFERENCE_COLOUR,
        annotation_clip=False,
    )


def caption(figure: Figure, title: str, subtitle: str, reserve_points: float = 46) -> None:
    height_points = figure.get_size_inches()[1] * 72
    figure.text(
        0.008,
        1 - 12 / height_points,
        title,
        ha="left",
        va="top",
        fontsize=BASE_FONT_POINTS + 3,
        fontweight="bold",
    )
    figure.text(
        0.008,
        1 - 28 / height_points,
        subtitle,
        ha="left",
        va="top",
        fontsize=BASE_FONT_POINTS - 1,
        color=REFERENCE_COLOUR,
    )
    figure.subplots_adjust(top=1 - reserve_points / height_points)


def save(figure: Figure, repo_root: Path, relative_path: str) -> Path:
    destination = repo_root / relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(destination, format="svg", bbox_inches="tight", pad_inches=0.12, metadata={"Date": None})
    plt.close(figure)
    return destination
