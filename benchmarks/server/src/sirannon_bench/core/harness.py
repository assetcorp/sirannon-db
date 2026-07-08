"""Run one engine through every workload and aggregate the numbers.

For each workload the harness prepares a known table state, then sweeps a set of target request
rates. At each rate it runs several independent measured passes and takes the median throughput
with a bootstrap confidence interval, so one noisy pass cannot set the headline. Latency is the
median of each pass's corrected percentiles. From the sweep it picks an operating point: the
highest offered rate the engine sustained while holding p99 under the disclosed service-level
target. The full sweep is kept as the throughput-versus-load curve.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from .config import Config
from .driver import Driver
from .loadgen import LoadResult, run_open_loop
from .rng import SeededRng, ZipfianGenerator
from .stats import median, summarize_metric
from .workloads import OperationContext, Workload, pick_operation

_ERROR_RATE_CEILING = 0.01


def _make_run_op(driver: Driver, workload: Workload, rng: SeededRng, zipf: ZipfianGenerator, data_size: int):
    operations = workload.operations
    use_sqlite = driver.dialect == "sqlite"

    async def run_op() -> bool:
        operation = pick_operation(rng, operations)
        params = operation.params(OperationContext(rng, zipf, data_size))
        try:
            if operation.kind == "read":
                sql = operation.sqlite_sql if use_sqlite else operation.postgres_sql
                await driver.read(sql, params)
            elif operation.kind == "write":
                sql = operation.sqlite_sql if use_sqlite else operation.postgres_sql
                await driver.write(sql, params)
            else:
                read_sql = operation.sqlite_sql if use_sqlite else operation.postgres_sql
                write_sql = operation.write_sqlite_sql if use_sqlite else operation.write_postgres_sql
                key = params[0]
                value = params[1]
                await driver.read(read_sql, [key])
                await driver.write(write_sql or read_sql, [value, key])
            return True
        except Exception:
            return False

    return run_op


async def _prepare(driver: Driver, workload: Workload, config: Config) -> None:
    await driver.drop_tables(list(workload.tables))
    schema = workload.sqlite_schema if driver.dialect == "sqlite" else workload.postgres_schema
    statements = [part.strip() for part in schema.split(";") if part.strip()]
    await driver.execute_ddl(statements)
    seed_rng = SeededRng(config.seed)
    seed_tables = workload.seed(seed_rng, config.data_size)
    await driver.seed(seed_tables)


async def _measure_rate(
    make_run_op: Callable[[], Callable[[], Awaitable[bool]]],
    target_rate: int,
    config: Config,
) -> dict:
    passes: list[LoadResult] = []
    for _ in range(config.runs):
        run_op = make_run_op()
        result = await run_open_loop(
            run_op,
            target_rate=float(target_rate),
            warmup_seconds=config.warmup_seconds,
            measure_seconds=config.measure_seconds,
            max_in_flight=config.max_in_flight,
        )
        passes.append(result)

    throughput_samples = [result.achieved_rate for result in passes]
    summary = summarize_metric(throughput_samples, confidence=0.95, seed=config.seed)
    sustained_votes = sum(1 for result in passes if result.sustained)
    saturated_votes = sum(1 for result in passes if result.server_saturated)
    client_votes = sum(1 for result in passes if result.client_bound)
    majority = config.runs / 2.0
    return {
        "target_rate": target_rate,
        "throughput": {
            "median_ops": summary.median,
            "mean_ops": summary.mean,
            "stddev_ops": summary.stddev,
            "cv": summary.cv,
            "ci_low_ops": summary.ci_low,
            "ci_high_ops": summary.ci_high,
            "confidence": summary.confidence,
            "runs": summary.runs,
            "samples": throughput_samples,
        },
        "latency_ms": {
            "p50": median([result.p50_ms for result in passes]),
            "p95": median([result.p95_ms for result in passes]),
            "p99": median([result.p99_ms for result in passes]),
            "p999": median([result.p999_ms for result in passes]),
            "max": median([result.max_ms for result in passes]),
            "mean": median([result.mean_ms for result in passes]),
        },
        "sustained": sustained_votes > majority,
        "server_saturated": saturated_votes > majority,
        "client_bound": client_votes > majority,
        "error_rate": median([result.error_rate for result in passes]),
    }


def _select_operating_point(sweep: list[dict], slo_p99_ms: float) -> dict:
    within_slo = [
        rate
        for rate in sweep
        if rate["sustained"] and rate["latency_ms"]["p99"] <= slo_p99_ms and rate["error_rate"] < _ERROR_RATE_CEILING
    ]
    if within_slo:
        chosen = max(within_slo, key=lambda rate: rate["target_rate"])
        return {**chosen, "under_slo": True}
    sustained = [rate for rate in sweep if rate["sustained"] and rate["error_rate"] < _ERROR_RATE_CEILING]
    if sustained:
        chosen = max(sustained, key=lambda rate: rate["target_rate"])
        return {**chosen, "under_slo": False}
    chosen = min(sweep, key=lambda rate: rate["target_rate"])
    return {**chosen, "under_slo": False}


async def run_workload(driver: Driver, workload: Workload, config: Config) -> dict:
    await _prepare(driver, workload, config)
    zipf = ZipfianGenerator(config.data_size)

    def make_run_op() -> Callable[[], Awaitable[bool]]:
        return _make_run_op(driver, workload, SeededRng(config.seed), zipf, config.data_size)

    sweep = [await _measure_rate(make_run_op, rate, config) for rate in config.target_rates]
    operating_point = _select_operating_point(sweep, config.slo_p99_ms)
    return {
        "workload": workload.name,
        "category": workload.category,
        "runs": config.runs,
        "operating_point": operating_point,
        "sweep": sweep,
    }


async def run_engine(driver: Driver, config: Config, workloads: dict[str, Workload]) -> list[dict]:
    results: list[dict] = []
    for name in config.workloads:
        workload = workloads.get(name)
        if workload is None:
            raise ValueError(f"unknown workload {name!r}; known workloads are {sorted(workloads)}")
        results.append(await run_workload(driver, workload, config))
    return results
