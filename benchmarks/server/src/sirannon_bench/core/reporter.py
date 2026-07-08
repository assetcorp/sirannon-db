"""Assemble the per-engine result file and its disclosure block.

One file records one engine at one durability level: the machine, the engine version and
durability settings in force, the full configuration so a reader can reproduce the run, and the
per-workload numbers. The aggregate step reads these files to build the cross-engine comparison.
"""

from __future__ import annotations

from .config import Config


def delivery_disclosure(config: Config) -> dict:
    return {
        "sirannon": "http",
        "postgres": "socket",
        "transport": "loopback",
        "driver_cpus": config.driver_cpus,
        "engine_cpus": config.engine_cpus,
        "note": (
            "Each engine is driven through its own shipping client: Sirannon over HTTP into its "
            "real server front-end, and PostgreSQL over its binary socket protocol. Both run on "
            "one host with no network between the load driver and the server. The load driver "
            "runs on its own pinned cores so it never competes with the engine under test."
        ),
    }


def config_block(config: Config) -> dict:
    return {
        "data_size": config.data_size,
        "warmup_seconds": config.warmup_seconds,
        "measure_seconds": config.measure_seconds,
        "runs": config.runs,
        "seed": config.seed,
        "slo_p99_ms": config.slo_p99_ms,
        "max_in_flight": config.max_in_flight,
        "target_rates": config.target_rates,
        "workloads": config.workloads,
        "scaling_workloads": config.scaling_workloads,
        "delivery": delivery_disclosure(config),
    }


def build_engine_report(
    environment: dict,
    engine: str,
    delivery: str,
    durability: str,
    engine_info: dict,
    config: Config,
    workloads: list[dict],
    features: list[dict] | None,
) -> dict:
    return {
        "environment": environment,
        "engine": {
            "name": engine,
            "delivery": delivery,
            "durability": durability,
            "version": engine_info.get("version", "unknown"),
            "settings": engine_info,
        },
        "config": config_block(config),
        "workloads": workloads,
        "features": features or [],
    }
