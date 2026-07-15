from __future__ import annotations

import math
import random
from dataclasses import dataclass


def mean(samples: list[float]) -> float:
    return sum(samples) / len(samples) if samples else 0.0


def median(samples: list[float]) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def sample_stddev(samples: list[float]) -> float:
    n = len(samples)
    if n < 2:
        return 0.0
    avg = mean(samples)
    variance = sum((value - avg) ** 2 for value in samples) / (n - 1)
    return math.sqrt(variance)


def coefficient_of_variation(samples: list[float]) -> float:
    avg = mean(samples)
    return sample_stddev(samples) / avg if avg > 0 else 0.0


def percentile(samples: list[float], fraction: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    rank = fraction * (len(ordered) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[low]
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


@dataclass(frozen=True)
class MetricSummary:
    median: float
    mean: float
    stddev: float
    cv: float
    ci_low: float
    ci_high: float
    confidence: float
    runs: int


_BOOTSTRAP_ITERATIONS = 10_000


def summarize_metric(samples: list[float], confidence: float = 0.95, seed: int = 42) -> MetricSummary:
    n = len(samples)
    if n == 0:
        return MetricSummary(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, confidence, 0)

    med = median(samples)
    avg = mean(samples)
    if n == 1:
        return MetricSummary(med, avg, 0.0, 0.0, med, med, confidence, 1)

    stddev = sample_stddev(samples)
    cv = stddev / avg if avg > 0 else 0.0

    rng = random.Random(seed)
    bootstrap_medians: list[float] = []
    for _ in range(_BOOTSTRAP_ITERATIONS):
        resample = [samples[rng.randrange(n)] for _ in range(n)]
        bootstrap_medians.append(median(resample))
    bootstrap_medians.sort()

    alpha = 1.0 - confidence
    lower_index = math.floor(len(bootstrap_medians) * (alpha / 2.0))
    upper_index = min(len(bootstrap_medians) - 1, math.floor(len(bootstrap_medians) * (1.0 - alpha / 2.0)))
    return MetricSummary(
        median=med,
        mean=avg,
        stddev=stddev,
        cv=cv,
        ci_low=bootstrap_medians[lower_index],
        ci_high=bootstrap_medians[upper_index],
        confidence=confidence,
        runs=n,
    )


@dataclass(frozen=True)
class SpeedupInterval:
    point_estimate: float
    ci_low: float
    ci_high: float
    confidence: float


def speedup_interval(
    sirannon_samples: list[float],
    postgres_samples: list[float],
    confidence: float = 0.95,
    seed: int = 42,
) -> SpeedupInterval | None:
    if not sirannon_samples or not postgres_samples:
        return None
    postgres_mean = mean(postgres_samples)
    if postgres_mean <= 0:
        return None

    rng = random.Random(seed)
    ratios: list[float] = []
    n_sir = len(sirannon_samples)
    n_pg = len(postgres_samples)
    for _ in range(_BOOTSTRAP_ITERATIONS):
        sir = sum(sirannon_samples[rng.randrange(n_sir)] for _ in range(n_sir)) / n_sir
        pg = sum(postgres_samples[rng.randrange(n_pg)] for _ in range(n_pg)) / n_pg
        if pg > 0:
            ratios.append(sir / pg)
    if not ratios:
        return None
    ratios.sort()
    alpha = 1.0 - confidence
    lower_index = math.floor(len(ratios) * (alpha / 2.0))
    upper_index = min(len(ratios) - 1, math.floor(len(ratios) * (1.0 - alpha / 2.0)))
    return SpeedupInterval(
        point_estimate=mean(sirannon_samples) / postgres_mean,
        ci_low=ratios[lower_index],
        ci_high=ratios[upper_index],
        confidence=confidence,
    )
