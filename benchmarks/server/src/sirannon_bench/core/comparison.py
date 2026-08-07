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
        "p95_ms": latency.get("p95"),
        "p99_ms": latency["p99"],
        "p999_ms": latency.get("p999"),
        "max_ms": latency.get("max"),
        "cores_used": (point.get("engine_cpu") or {}).get("cores_used"),
        "cores_allowed": (point.get("engine_cpu") or {}).get("cores_allowed"),
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
            "sirannon_soak": sir.get("soak"),
            "postgres_soak": pg.get("soak"),
            "speedup": speedup,
        })
    return rows


def _sweep_point(workload: dict) -> list[dict]:
    return workload.get("sweep", [])


def _curve_side(point: dict | None) -> dict:
    if point is None:
        return {
            "ops": None,
            "ops_ci_low": None,
            "ops_ci_high": None,
            "p50_ms": None,
            "p99_ms": None,
            "cores_used": None,
            "cores_allowed": None,
            "sustained": None,
            "error_rate": None,
        }
    throughput = point["throughput"]
    latency = point["latency_ms"]
    cpu = point.get("engine_cpu") or {}
    return {
        "ops": throughput["median_ops"],
        "ops_ci_low": throughput.get("ci_low_ops"),
        "ops_ci_high": throughput.get("ci_high_ops"),
        "p50_ms": latency["p50"],
        "p99_ms": latency["p99"],
        "cores_used": cpu.get("cores_used"),
        "cores_allowed": cpu.get("cores_allowed"),
        "sustained": point.get("sustained"),
        "error_rate": point.get("error_rate"),
    }


def _scaling_rows(sirannon: dict, postgres: dict, order: list[str]) -> list[dict]:
    sirannon_workloads = _workload_index(sirannon)
    postgres_workloads = _workload_index(postgres)
    scaling: list[dict] = []
    for name in order:
        sir = sirannon_workloads.get(name)
        pg = postgres_workloads.get(name)
        if sir is None or pg is None:
            continue
        sir_by_rate = {point["target_rate"]: point for point in _sweep_point(sir)}
        pg_by_rate = {point["target_rate"]: point for point in _sweep_point(pg)}
        curve: list[dict] = []
        for rate in sorted(set(sir_by_rate) | set(pg_by_rate)):
            sir_side = _curve_side(sir_by_rate.get(rate))
            pg_side = _curve_side(pg_by_rate.get(rate))
            curve.append({
                "target_rate": rate,
                "sirannon_ops": sir_side["ops"],
                "postgres_ops": pg_side["ops"],
                "sirannon_p99_ms": sir_side["p99_ms"],
                "postgres_p99_ms": pg_side["p99_ms"],
                "sirannon": sir_side,
                "postgres": pg_side,
            })
        scaling.append({"workload": name, "category": sir.get("category", ""), "curve": curve})
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
            "scaling": _scaling_rows(sirannon, postgres, order),
            "sirannon_engine": sirannon.get("engine", {}),
            "postgres_engine": postgres.get("engine", {}),
            "client_saturation": {
                "sirannon": sirannon.get("client_saturation"),
                "postgres": postgres.get("client_saturation"),
            },
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
