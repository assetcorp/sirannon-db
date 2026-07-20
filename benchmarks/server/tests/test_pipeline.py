from __future__ import annotations

import json
import sys
from pathlib import Path

from sirannon_bench.core.comparison import build_comparison

_WRITEUP_DIR = Path(__file__).resolve().parents[2] / "writeup"


def _engine_report(engine: str, median_ops: float, p99: float, with_features: bool) -> dict:
    throughput = {
        "median_ops": median_ops,
        "mean_ops": median_ops,
        "stddev_ops": median_ops * 0.02,
        "cv": 0.02,
        "ci_low_ops": median_ops * 0.98,
        "ci_high_ops": median_ops * 1.02,
        "confidence": 0.95,
        "runs": 3,
        "samples": [median_ops * 0.98, median_ops, median_ops * 1.02],
    }
    latency = {"p50": p99 / 3.0, "p95": p99 * 0.9, "p99": p99, "p999": p99 * 1.5, "max": p99 * 2.0, "mean": p99 / 3.0}
    operating_point = {"target_rate": 16000, "under_slo": True, "throughput": throughput, "latency_ms": latency}
    sweep = [
        {"target_rate": 1000, "throughput": throughput, "latency_ms": latency, "sustained": True},
        {"target_rate": 16000, "throughput": throughput, "latency_ms": latency, "sustained": True},
    ]
    report = {
        "environment": {"git_commit": "abcdef123456", "os": "Linux 6.1", "arch": "x86_64"},
        "engine": {"name": engine, "delivery": "http" if engine == "sirannon" else "socket", "version": "3.45.0"},
        "config": {"workloads": ["point-select"], "scaling_workloads": ["point-select"], "seed": 42},
        "workloads": [
            {"workload": "point-select", "category": "micro", "operating_point": operating_point, "sweep": sweep}
        ],
        "features": (
            [{
                "feature": "cdc-latency",
                "samples": 100,
                "server_poll_interval_ms": 50,
                "latency_ms": {"p50": 24.0, "p95": 48.0, "p99": 49.0, "max": 51.0, "mean": 25.0},
            }]
            if with_features
            else []
        ),
    }
    return report


def _write(run_dir: Path, name: str, payload: dict) -> None:
    (run_dir / name).write_text(json.dumps(payload), encoding="utf-8")


def test_build_comparison_joins_engines(tmp_path: Path):
    run_dir = tmp_path / "runs" / "20260101T000000Z"
    run_dir.mkdir(parents=True)
    _write(run_dir, "engine-sirannon-matched.json", _engine_report("sirannon", 50000.0, 2.0, True))
    _write(run_dir, "engine-postgres-matched.json", _engine_report("postgres", 25000.0, 4.0, False))
    _write(run_dir, "cold-start.json", {"sirannon": {"cold_start_ms": 120}, "postgres": {"cold_start_ms": 900}})
    manifest = {"run_id": "20260101T000000Z", "created_at": "2026-07-08T00:00:00+00:00", "environment": {}}

    comparison = build_comparison(run_dir, manifest)
    matched = comparison["durabilities"]["matched"]
    row = matched["workloads"][0]
    assert row["workload"] == "point-select"
    assert row["sirannon"]["ops_median"] == 50000.0
    assert row["postgres"]["ops_median"] == 25000.0
    assert 1.8 < row["speedup"]["point"] < 2.2
    assert matched["scaling"][0]["curve"][0]["sirannon_ops"] == 50000.0
    assert comparison["cold_start"]["sirannon"]["cold_start_ms"] == 120
    assert comparison["features"][0]["feature"] == "cdc-latency"


def test_writeup_renders_numbers(tmp_path: Path):
    run_dir = tmp_path / "runs" / "20260101T000000Z"
    run_dir.mkdir(parents=True)
    _write(run_dir, "engine-sirannon-matched.json", _engine_report("sirannon", 50000.0, 2.0, True))
    _write(run_dir, "engine-postgres-matched.json", _engine_report("postgres", 25000.0, 4.0, False))
    _write(run_dir, "cold-start.json", {"sirannon": {"cold_start_ms": 120}, "postgres": {"cold_start_ms": 900}})
    manifest = {"run_id": "20260101T000000Z", "created_at": "2026-07-08T00:00:00+00:00", "environment": {"os": "Linux"}}
    comparison = build_comparison(run_dir, manifest)

    if str(_WRITEUP_DIR) not in sys.path:
        sys.path.insert(0, str(_WRITEUP_DIR))
    import server_section
    from sources import Source

    source = Source(run_id="20260101T000000Z", report_link="report.md", comparison=comparison)
    rendered = server_section.blocks(source)
    assert "Point-select" in rendered["comparison"]
    assert "50.0K" in rendered["comparison"]
    assert "2.00x" in rendered["comparison"]
    assert "n/a" not in rendered["comparison"]
    assert "Change-feed latency" in rendered["features"]
    assert "Cold start" in rendered["features"]

    document = server_section.comparison_document(source)
    assert "Sirannon and PostgreSQL on one host" in document
    assert "Point-select" in document
