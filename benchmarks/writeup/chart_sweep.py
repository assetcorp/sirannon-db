from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from comparison_view import durability_label, workload_label
from matplotlib.ticker import FuncFormatter

from chart_paths import repo_chart_dir, sweep_chart
from chart_style import (
    FIGURE_WIDTH_INCHES,
    IDEAL_COLOUR,
    POSTGRES_COLOUR,
    POSTGRES_LABEL,
    REFERENCE_COLOUR,
    SIRANNON_COLOUR,
    SIRANNON_LABEL,
    caption,
    latency_axis,
    rate_axis,
    rate_label,
    reference_note,
    save,
    style_axes,
)

ENGINES = ((SIRANNON_LABEL, "sirannon", SIRANNON_COLOUR), (POSTGRES_LABEL, "postgres", POSTGRES_COLOUR))


def _series(curve: list[dict], engine: str, field: str) -> tuple[list[float], list[float]]:
    rates: list[float] = []
    values: list[float] = []
    for point in curve:
        side = point.get(engine) or {}
        value = side.get(field)
        if value is None:
            continue
        rates.append(point["target_rate"])
        values.append(value)
    return rates, values


def _searched(curve: list[dict], engine: str, field: str) -> tuple[list[float], list[float]]:
    rates: list[float] = []
    values: list[float] = []
    for point in curve:
        side = point.get(engine) or {}
        value = side.get(field)
        if value is None or side.get("searched") is not True:
            continue
        rates.append(point["target_rate"])
        values.append(value)
    return rates, values


def _plot_engine(axes, curve: list[dict], engine: str, field: str, colour: str, label: str) -> None:
    rate_points, values = _series(curve, engine, field)
    axes.plot(rate_points, values, color=colour, marker="o", markersize=4, linewidth=1.8, label=label)
    searched_points, searched_values = _searched(curve, engine, field)
    if searched_points:
        axes.plot(
            searched_points,
            searched_values,
            linestyle="none",
            marker="o",
            markersize=6,
            markerfacecolor="white",
            markeredgecolor=colour,
            markeredgewidth=1.4,
        )


def _band(axes, curve: list[dict], engine: str, colour: str) -> None:
    rates: list[float] = []
    lows: list[float] = []
    highs: list[float] = []
    for point in curve:
        side = point.get(engine) or {}
        low = side.get("ops_ci_low")
        high = side.get("ops_ci_high")
        if low is None or high is None:
            continue
        rates.append(point["target_rate"])
        lows.append(low)
        highs.append(high)
    if len(rates) > 1:
        axes.fill_between(rates, lows, highs, color=colour, alpha=0.18, linewidth=0)


def _cores_allowed(curve: list[dict]) -> float | None:
    for point in curve:
        for _, engine, _colour in ENGINES:
            allowed = (point.get(engine) or {}).get("cores_allowed")
            if allowed:
                return float(allowed)
    return None


def sweep_figure(
    repo_root: Path,
    run_id: str,
    durability: str,
    entry: dict,
    target_ms: float | None,
) -> Path:
    curve = entry.get("curve", [])
    rates = [point["target_rate"] for point in curve]
    listed_rates = [
        point["target_rate"]
        for point in curve
        if (point.get("sirannon") or {}).get("searched") is not True
        and (point.get("postgres") or {}).get("searched") is not True
    ]
    figure, (throughput_axes, latency_ax, cpu_axes) = plt.subplots(
        3,
        1,
        figsize=(FIGURE_WIDTH_INCHES, 7.6),
        sharex=True,
        height_ratios=[1.1, 1.1, 0.8],
    )

    if rates:
        throughput_axes.plot(rates, rates, color=IDEAL_COLOUR, linestyle=":", linewidth=1, label="Offered rate")
    for label, engine, colour in ENGINES:
        _band(throughput_axes, curve, engine, colour)
        _plot_engine(throughput_axes, curve, engine, "ops", colour, label)
        _plot_engine(latency_ax, curve, engine, "p99_ms", colour, label)
        _plot_engine(cpu_axes, curve, engine, "cores_used", colour, label)

    style_axes(throughput_axes, "Achieved ops/s")
    style_axes(latency_ax, "p99 latency (ms)")
    style_axes(cpu_axes, "Engine cores busy")
    throughput_axes.set_yscale("log", base=2)
    if rates:
        throughput_axes.set_yticks(listed_rates or rates)
        throughput_axes.set_ylim(min(rates) * 0.85, max(rates) * 1.18)
    throughput_axes.yaxis.set_major_formatter(FuncFormatter(rate_label))
    throughput_axes.minorticks_off()
    latency_axis(latency_ax, target_ms)
    allowed = _cores_allowed(curve)
    if allowed:
        cpu_axes.axhline(allowed, color=REFERENCE_COLOUR, linestyle="--", linewidth=1)
        cpu_axes.set_ylim(0, allowed * 1.22)
        reference_note(cpu_axes, allowed, f"{allowed:g} cores allowed")
    rate_axis(cpu_axes, listed_rates or rates)
    cpu_axes.set_xlabel("Offered rate (requests per second)")
    throughput_axes.legend(loc="upper left", ncols=3, fontsize=8)

    caption(
        figure,
        f"{workload_label(entry.get('workload', 'n/a'))} sweep",
        f"{durability_label(durability)} · run {run_id}",
    )
    figure.subplots_adjust(hspace=0.16)
    return save(figure, repo_root, sweep_chart(repo_chart_dir(run_id), durability, entry.get("workload", "unknown")))
