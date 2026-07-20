from __future__ import annotations

from sirannon_bench.core import stats


def test_summarize_metric_is_deterministic():
    samples = [1000.0, 1100.0, 900.0, 1050.0, 950.0]
    first = stats.summarize_metric(samples, seed=42)
    second = stats.summarize_metric(samples, seed=42)
    assert first == second
    assert first.median == 1000.0
    assert first.ci_low <= first.median <= first.ci_high
    assert first.runs == 5
    assert first.cv > 0


def test_summarize_metric_single_run_collapses():
    summary = stats.summarize_metric([1234.0], seed=42)
    assert summary.median == 1234.0
    assert summary.ci_low == 1234.0
    assert summary.ci_high == 1234.0
    assert summary.cv == 0.0


def test_percentile_matches_linear_interpolation():
    data = [float(i) for i in range(101)]
    assert stats.percentile(data, 0.50) == 50.0
    assert stats.percentile(data, 0.99) == 99.0
    assert stats.percentile(data, 0.0) == 0.0
    assert stats.percentile(data, 1.0) == 100.0


def test_speedup_interval_direction():
    faster = [2000.0, 2100.0, 1900.0]
    slower = [1000.0, 1050.0, 950.0]
    interval = stats.speedup_interval(faster, slower, seed=42)
    assert interval is not None
    assert interval.point_estimate > 1.8
    assert interval.ci_low <= interval.point_estimate <= interval.ci_high


def test_speedup_interval_guards_zero():
    assert stats.speedup_interval([1.0], [0.0], seed=42) is None
    assert stats.speedup_interval([], [1.0], seed=42) is None
