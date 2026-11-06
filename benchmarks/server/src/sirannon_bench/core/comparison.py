from __future__ import annotations

import json
import os
import re
from pathlib import Path

from .stats import speedup_interval

_ENGINE_FILE = re.compile(r"^engine-(?P<engine>[a-z0-9]+)-(?P<durability>full|matched)\.json$")
_UNFINISHED_ENGINE_FILE = re.compile(r"^engine-(?P<engine>[a-z0-9]+)-(?P<durability>full|matched)\.partial\.json$")
_DURABILITY_ORDER = ["full", "matched"]
_COMPARED_ENGINES = ("sirannon", "postgres")
_FIRST_WORKLOAD_REVISION = 1


class IncompleteRunError(Exception):
    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("this run cannot be compared: " + "; ".join(problems))


def _expected_durabilities(run_dir: Path, by_durability: dict[str, dict[str, dict]]) -> list[str]:
    found = set(by_durability) | set(os.environ.get("BENCH_DURABILITIES", "").split())
    for entry in run_dir.iterdir():
        match = _UNFINISHED_ENGINE_FILE.match(entry.name)
        if match:
            found.add(match["durability"])
    return [durability for durability in _DURABILITY_ORDER if durability in found]


def _revision_problems(durability: str, sirannon: dict, postgres: dict) -> list[str]:
    postgres_revisions = {
        workload["workload"]: workload.get("revision", _FIRST_WORKLOAD_REVISION)
        for workload in postgres.get("workloads", [])
    }
    problems: list[str] = []
    for workload in sirannon.get("workloads", []):
        name = workload["workload"]
        sirannon_revision = workload.get("revision", _FIRST_WORKLOAD_REVISION)
        postgres_revision = postgres_revisions.get(name)
        if postgres_revision is not None and postgres_revision != sirannon_revision:
            problems.append(
                f"at {durability} durability sirannon ran {name} revision {sirannon_revision} "
                f"and postgres ran revision {postgres_revision}"
            )
    return problems


def _engine_problems(engine: str, durability: str, report: dict) -> list[str]:
    problems: list[str] = []
    for failed in report.get("failed_workloads", []):
        problems.append(
            f"{engine} at {durability} durability failed {failed.get('workload')} ({failed.get('error_code')})"
        )
    recorded = {workload["workload"] for workload in report.get("workloads", [])}
    for name in report.get("config", {}).get("workloads", []):
        if name not in recorded:
            problems.append(f"{engine} at {durability} durability recorded no result for {name}")
    return problems


def completeness_problems(run_dir: Path, by_durability: dict[str, dict[str, dict]]) -> list[str]:
    problems = [
        f"{entry.name} holds a pass that never finished"
        for entry in sorted(run_dir.iterdir())
        if _UNFINISHED_ENGINE_FILE.match(entry.name)
    ]
    durabilities = _expected_durabilities(run_dir, by_durability)
    if not durabilities:
        problems.append("the run holds no engine results")
    for durability in durabilities:
        engines = by_durability.get(durability, {})
        for engine in _COMPARED_ENGINES:
            report = engines.get(engine)
            if report is None:
                problems.append(f"{engine} has no finished results at {durability} durability")
                continue
            problems.extend(_engine_problems(engine, durability, report))
        sirannon = engines.get("sirannon")
        postgres = engines.get("postgres")
        if sirannon is not None and postgres is not None:
            sirannon_workloads = sirannon.get("config", {}).get("workloads", [])
            postgres_workloads = postgres.get("config", {}).get("workloads", [])
            if sirannon_workloads != postgres_workloads:
                problems.append(
                    f"at {durability} durability sirannon ran {sirannon_workloads} and postgres ran {postgres_workloads}"
                )
            problems.extend(_revision_problems(durability, sirannon, postgres))
    return problems


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
        "searched": point.get("searched", False),
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
        "seed_rows": (workload.get("seed") or {}).get("rows"),
        "concurrency": point.get("concurrency"),
    }


def _level_rows(workload: dict) -> list[dict] | None:
    levels = workload.get("concurrency_levels")
    if not isinstance(levels, list):
        return None
    rows: list[dict] = []
    for level in levels:
        point = level.get("operating_point") or {}
        throughput = point.get("throughput") or {}
        latency = point.get("latency_ms") or {}
        rows.append({
            "concurrency": level.get("concurrency"),
            "client_connections": level.get("client_connections"),
            "runs_per_rate": level.get("runs_per_rate"),
            "target_rate": point.get("target_rate"),
            "searched": point.get("searched", False),
            "under_slo": point.get("under_slo", False),
            "ops_median": throughput.get("median_ops"),
            "p99_ms": latency.get("p99"),
            "error_rate": point.get("error_rate"),
        })
    return rows


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
            "sirannon_operation_cost": sir.get("operation_cost"),
            "postgres_operation_cost": pg.get("operation_cost"),
            "sirannon_peak_concurrency": sir.get("peak_concurrency"),
            "postgres_peak_concurrency": pg.get("peak_concurrency"),
            "sirannon_levels": _level_rows(sir),
            "postgres_levels": _level_rows(pg),
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
            "searched": None,
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
        "searched": point.get("searched", False),
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
    problems = completeness_problems(run_dir, by_durability)
    if problems:
        raise IncompleteRunError(problems)
    cold_start_path = run_dir / "cold-start.json"
    cold_start = _read_json(cold_start_path) if cold_start_path.is_file() else None
    engine_machine_path = run_dir / "engine-machine.json"
    engine_machine = _read_json(engine_machine_path) if engine_machine_path.is_file() else None

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
            "sirannon_environment": sirannon.get("environment", {}),
            "postgres_environment": postgres.get("environment", {}),
            "client_saturation": {
                "sirannon": sirannon.get("client_saturation"),
                "postgres": postgres.get("client_saturation"),
            },
            "network": {
                "sirannon": sirannon.get("network"),
                "postgres": postgres.get("network"),
            },
        }

    return {
        "run_id": manifest.get("run_id"),
        "created_at": manifest.get("created_at"),
        "environment": manifest.get("environment", {}),
        "engine_machine": engine_machine,
        "config": reference_config,
        "durabilities": durabilities,
        "features": _collect_features(by_durability),
        "cold_start": cold_start,
    }
