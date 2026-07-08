"""Join the per-engine result files into one comparison the writeup can render.

For each durability level the comparison pairs Sirannon against PostgreSQL on every workload at
each engine's operating point, the highest offered rate it sustained under the service-level
target. The speedup carries a bootstrap confidence interval on the throughput ratio, so the
head-to-head claim comes with its own uncertainty rather than two error bars a reader has to
compare by eye. The scaling section keeps the full throughput-versus-load curve for the chosen
workloads, and the Sirannon-only features and the cold-start timing ride along unchanged.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from .stats import speedup_interval

_ENGINE_FILE = re.compile(r"^engine-(?P<engine>[a-z0-9]+)-(?P<durability>full|matched)\.json$")
_DURABILITY_ORDER = ["full", "matched"]


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_engine_files(run_dir: Path) -> dict[str, dict[str, dict]]:
    by_durability: dict[str, dict[str, dict]] = {}
    for entry in sorted(run_dir.iterdir()):
        if not entry.is_file():
            continue
        match = _ENGINE_FILE.match(entry.name)
        if not match:
            continue
        by_durability.setdefault(match["durability"], {})[match["engine"]] = _read_json(entry)
    return by_durability


def _workload_index(report: dict) -> dict[str, dict]:
    return {workload["workload"]: workload for workload in report.get("workloads", [])}


def _operating_metrics(workload: dict) -> dict:
    point = workload["operating_point"]
    throughput = point["throughput"]
    latency = point["latency_ms"]
    return {
        "target_rate": point["target_rate"],
        "under_slo": point.get("under_slo", False),
        "ops_median": throughput["median_ops"],
        "ops_ci_low": throughput["ci_low_ops"],
        "ops_ci_high": throughput["ci_high_ops"],
        "ops_cv": throughput["cv"],
        "ops_samples": throughput.get("samples", []),
        "p50_ms": latency["p50"],
        "p99_ms": latency["p99"],
    }


def _workload_rows(sirannon: dict, postgres: dict, order: list[str], seed: int) -> list[dict]:
    sirannon_workloads = _workload_index(sirannon)
    postgres_workloads = _workload_index(postgres)
    rows: list[dict] = []
    for name in order:
        sir = sirannon_workloads.get(name)
        pg = postgres_workloads.get(name)
        if sir is None or pg is None:
            continue
        sir_metrics = _operating_metrics(sir)
        pg_metrics = _operating_metrics(pg)
        interval = speedup_interval(sir_metrics["ops_samples"], pg_metrics["ops_samples"], seed=seed)
        speedup = None
        if interval is not None:
            speedup = {"point": interval.point_estimate, "ci_low": interval.ci_low, "ci_high": interval.ci_high}
        rows.append({
            "workload": name,
            "category": sir.get("category", ""),
            "sirannon": sir_metrics,
            "postgres": pg_metrics,
            "speedup": speedup,
        })
    return rows


def _sweep_point(workload: dict) -> list[dict]:
    return workload.get("sweep", [])


def _scaling_rows(sirannon: dict, postgres: dict, scaling_workloads: list[str]) -> list[dict]:
    sirannon_workloads = _workload_index(sirannon)
    postgres_workloads = _workload_index(postgres)
    scaling: list[dict] = []
    for name in scaling_workloads:
        sir = sirannon_workloads.get(name)
        pg = postgres_workloads.get(name)
        if sir is None or pg is None:
            continue
        pg_by_rate = {point["target_rate"]: point for point in _sweep_point(pg)}
        curve: list[dict] = []
        for sir_point in _sweep_point(sir):
            rate = sir_point["target_rate"]
            pg_point = pg_by_rate.get(rate)
            if pg_point is None:
                continue
            curve.append({
                "target_rate": rate,
                "sirannon_ops": sir_point["throughput"]["median_ops"],
                "postgres_ops": pg_point["throughput"]["median_ops"],
                "sirannon_p99_ms": sir_point["latency_ms"]["p99"],
                "postgres_p99_ms": pg_point["latency_ms"]["p99"],
            })
        scaling.append({"workload": name, "curve": curve})
    return scaling


def _collect_features(by_durability: dict[str, dict[str, dict]]) -> list[dict]:
    seen: dict[str, dict] = {}
    for durability in _DURABILITY_ORDER:
        report = by_durability.get(durability, {}).get("sirannon")
        if not report:
            continue
        for feature in report.get("features", []):
            seen.setdefault(feature["feature"], feature)
    return list(seen.values())


def build_comparison(run_dir: Path, manifest: dict) -> dict:
    by_durability = _load_engine_files(run_dir)
    cold_start_path = run_dir / "cold-start.json"
    cold_start = _read_json(cold_start_path) if cold_start_path.is_file() else None

    reference_config: dict = {}
    durabilities: dict[str, dict] = {}
    for durability in _DURABILITY_ORDER:
        engines = by_durability.get(durability)
        if not engines or "sirannon" not in engines or "postgres" not in engines:
            continue
        sirannon = engines["sirannon"]
        postgres = engines["postgres"]
        config = sirannon.get("config", {})
        if not reference_config:
            reference_config = config
        order = config.get("workloads", [])
        durabilities[durability] = {
            "workloads": _workload_rows(sirannon, postgres, order, int(config.get("seed", 42))),
            "scaling": _scaling_rows(sirannon, postgres, config.get("scaling_workloads", [])),
            "sirannon_engine": sirannon.get("engine", {}),
            "postgres_engine": postgres.get("engine", {}),
        }

    return {
        "run_id": manifest.get("run_id"),
        "created_at": manifest.get("created_at"),
        "environment": manifest.get("environment", {}),
        "config": reference_config,
        "durabilities": durabilities,
        "features": _collect_features(by_durability),
        "cold_start": cold_start,
    }
