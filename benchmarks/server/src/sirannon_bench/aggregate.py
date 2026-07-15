from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .core.comparison import build_comparison
from .core.run_store import (
    MANIFEST_NAME,
    default_results_dir,
    latest_run_id,
    run_directory,
    validate_run_id,
    write_json,
)


def _resolve_run_id(results_dir: Path, explicit: str | None) -> str:
    import os

    raw = (explicit or os.environ.get("BENCH_RUN_ID") or "").strip()
    if raw:
        return validate_run_id(raw)
    latest = latest_run_id(results_dir)
    if latest is None:
        raise SystemExit(f"no benchmark runs found under {results_dir / 'runs'}; run the harness first")
    return latest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Aggregate one run into a cross-engine comparison.")
    parser.add_argument("--results-dir", type=Path, default=default_results_dir())
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    run_id = _resolve_run_id(args.results_dir, args.run_id)
    run_dir = run_directory(args.results_dir, run_id)
    manifest_path = run_dir / MANIFEST_NAME
    if not manifest_path.is_file():
        raise SystemExit(f"run {run_id} has no {MANIFEST_NAME}; nothing to aggregate")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    comparison = build_comparison(run_dir, manifest)
    path = write_json(run_dir / "comparison.json", comparison)
    sys.stdout.write(f"Wrote {path}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
