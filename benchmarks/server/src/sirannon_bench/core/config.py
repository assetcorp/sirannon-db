"""Load the benchmark configuration from a TOML file, with environment overrides.

The TOML file holds the stable choices that define the workload: which workloads run, the
target request rates for the open-loop sweep, the warmup and measurement windows, the number of
independent runs, and the seed. Every value is disclosed in the generated report so a reader can
reproduce the run. Environment variables prefixed ``BENCH_`` override individual fields for a
one-off run without editing the file.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    pool_size: int


@dataclass(frozen=True)
class SirannonConfig:
    base_url: str
    database_id: str


@dataclass(frozen=True)
class Config:
    postgres: PostgresConfig
    sirannon: SirannonConfig
    data_size: int
    warmup_seconds: float
    measure_seconds: float
    runs: int
    seed: int
    slo_p99_ms: float
    max_in_flight: int
    workloads: list[str]
    target_rates: list[int]
    scaling_workloads: list[str]
    driver_cpus: float
    engine_cpus: float

    def with_data_size(self, data_size: int) -> "Config":
        return _replace(self, data_size=data_size)


def _replace(config: Config, **changes: object) -> Config:
    values = config.__dict__.copy()
    values.update(changes)
    return Config(**values)


def _env_int(name: str, fallback: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw is not None and raw.strip() else fallback


def _env_float(name: str, fallback: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw is not None and raw.strip() else fallback


def _env_str(name: str, fallback: str) -> str:
    raw = os.environ.get(name)
    return raw if raw is not None and raw.strip() else fallback


def _env_int_list(name: str, fallback: list[int]) -> list[int]:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return fallback
    return [int(part) for part in raw.split(",") if part.strip()]


def _env_str_list(name: str, fallback: list[str]) -> list[str]:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return fallback
    return [part.strip() for part in raw.split(",") if part.strip()]


def load_config(path: Path) -> Config:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    run = raw.get("run", {})
    load = raw.get("load", {})
    scaling = raw.get("scaling", {})
    pg = raw.get("postgres", {})
    sir = raw.get("sirannon", {})
    resources = raw.get("resources", {})

    config = Config(
        postgres=PostgresConfig(
            host=_env_str("BENCH_PG_HOST", pg.get("host", "127.0.0.1")),
            port=_env_int("BENCH_PG_PORT", int(pg.get("port", 5432))),
            user=_env_str("BENCH_PG_USER", pg.get("user", "benchmark")),
            password=_env_str("BENCH_PG_PASSWORD", pg.get("password", "benchmark")),
            database=_env_str("BENCH_PG_DATABASE", pg.get("database", "benchmark")),
            pool_size=_env_int("BENCH_PG_POOL_SIZE", int(pg.get("pool_size", 16))),
        ),
        sirannon=SirannonConfig(
            base_url=_env_str("BENCH_SIRANNON_URL", sir.get("base_url", "http://127.0.0.1:9876")),
            database_id=_env_str("BENCH_SIRANNON_DB", sir.get("database_id", "bench")),
        ),
        data_size=_env_int("BENCH_DATA_SIZE", int(run.get("data_size", 10_000))),
        warmup_seconds=_env_float("BENCH_WARMUP_SECONDS", float(run.get("warmup_seconds", 3.0))),
        measure_seconds=_env_float("BENCH_MEASURE_SECONDS", float(run.get("measure_seconds", 10.0))),
        runs=_env_int("BENCH_RUNS", int(run.get("runs", 5))),
        seed=_env_int("BENCH_SEED", int(run.get("seed", 42))),
        slo_p99_ms=_env_float("BENCH_SLO_P99_MS", float(load.get("slo_p99_ms", 25.0))),
        max_in_flight=_env_int("BENCH_MAX_IN_FLIGHT", int(load.get("max_in_flight", 256))),
        workloads=_env_str_list("BENCH_WORKLOADS", list(run.get("workloads", []))),
        target_rates=_env_int_list("BENCH_TARGET_RATES", list(load.get("target_rates", []))),
        scaling_workloads=_env_str_list("BENCH_SCALING_WORKLOADS", list(scaling.get("workloads", []))),
        driver_cpus=_env_float("BENCH_DRIVER_CPUS", float(resources.get("driver_cpus", 2.0))),
        engine_cpus=_env_float("BENCH_ENGINE_CPUS", float(resources.get("engine_cpus", 2.0))),
    )
    _validate(config)
    return config


def _validate(config: Config) -> None:
    if config.warmup_seconds < 0:
        raise ValueError(f"warmup_seconds must be >= 0, got {config.warmup_seconds}")
    if config.measure_seconds <= 0:
        raise ValueError(f"measure_seconds must be > 0, got {config.measure_seconds}")
    if config.runs < 1:
        raise ValueError(f"runs must be >= 1, got {config.runs}")
    if config.data_size < 1:
        raise ValueError(f"data_size must be >= 1, got {config.data_size}")
    if config.max_in_flight < 1:
        raise ValueError(f"max_in_flight must be >= 1, got {config.max_in_flight}")
    if not config.workloads:
        raise ValueError("at least one workload must be configured")
    if not config.target_rates:
        raise ValueError("at least one target rate must be configured")
    for rate in config.target_rates:
        if rate < 1:
            raise ValueError(f"each target rate must be positive, got {rate}")
    if not 1 <= config.postgres.port <= 65535:
        raise ValueError(f"postgres port must be 1-65535, got {config.postgres.port}")


def default_config_path() -> Path:
    """Find ``benchmark.toml`` across the ways the harness runs.

    The config lives beside the package rather than inside it, so this checks, in order: an
    explicit ``BENCH_CONFIG`` override, the ``config`` directory next to the working directory
    (which is where the Docker image and the source checkout both put it), and the source-tree
    location relative to this file. The first that exists wins; the working-directory path is the
    fallback so a missing file reports a clear location.
    """
    override = os.environ.get("BENCH_CONFIG")
    if override:
        return Path(override)
    candidates = [
        Path.cwd() / "config" / "benchmark.toml",
        Path(__file__).resolve().parents[3] / "config" / "benchmark.toml",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return candidates[0]
