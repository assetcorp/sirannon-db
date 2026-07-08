"""Run one engine at one durability level and record the result.

    python -m sirannon_bench.cli --engine sirannon --durability full
    python -m sirannon_bench.cli --engine postgres --durability matched

All engines and durability passes of one run share a single run id, threaded through
``BENCH_RUN_ID`` by the orchestrator, so their files land together under
``results/runs/<run id>/`` for the aggregate step to read.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from .core.config import Config, load_config, default_config_path
from .core.environment import capture_environment
from .core.registry import build_driver
from .core.reporter import build_engine_report, config_block
from .core.run_store import (
    default_results_dir,
    resolve_run_id_for_write,
    run_directory,
    validate_artifact_name,
    write_json,
    write_run_manifest,
)
from .core.workloads import build_workloads
from .features import measure_cdc_latency


async def _run(engine: str, durability: str, config: Config, want_features: bool) -> dict:
    driver = build_driver(engine, config, durability)
    await driver.connect()
    try:
        engine_info = await driver.info()
        workloads = await run_all_workloads(driver, config)
        features: list[dict] = []
        if want_features and engine == "sirannon":
            features.append(
                await measure_cdc_latency(
                    base_url=config.sirannon.base_url,
                    database_id=config.sirannon.database_id,
                    samples=int(os.environ.get("BENCH_CDC_SAMPLES", "200")),
                    warmup_samples=int(os.environ.get("BENCH_CDC_WARMUP", "20")),
                )
            )
        environment = capture_environment()
        return build_engine_report(
            environment, engine, driver.delivery, durability, engine_info, config, workloads, features
        )
    finally:
        await driver.close()


async def run_all_workloads(driver, config: Config) -> list[dict]:
    from .core.harness import run_engine

    return await run_engine(driver, config, build_workloads())


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one engine of the Sirannon-versus-Postgres benchmark.")
    parser.add_argument("--engine", required=True, choices=["sirannon", "postgres"])
    parser.add_argument("--durability", default="matched", choices=["full", "matched"])
    parser.add_argument("--config", type=Path, default=default_config_path())
    parser.add_argument("--results-dir", type=Path, default=default_results_dir())
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--features", action="store_true", help="run the Sirannon-only feature characterizations")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    config = load_config(args.config)

    report = asyncio.run(_run(args.engine, args.durability, config, args.features))

    run_id = resolve_run_id_for_write(args.run_id)
    results_dir: Path = args.results_dir
    write_run_manifest(results_dir, run_id, report["environment"], config_block(config))
    directory = run_directory(results_dir, run_id)
    directory.mkdir(parents=True, exist_ok=True)
    artifact = validate_artifact_name(f"engine-{args.engine}-{args.durability}.json")
    path = write_json(directory / artifact, report)

    sys.stdout.write(f"Recorded {args.engine} ({args.durability} durability) to {path}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
