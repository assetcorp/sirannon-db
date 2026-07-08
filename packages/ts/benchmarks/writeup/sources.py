"""Locate and load the latest recorded benchmark run.

Run directories are named with a compact UTC timestamp, so the newest run is the
lexicographic maximum, the same ordering the harness writes them in. The writeup always
reads the latest committed run, so publishing a page means running the suite and committing
its run directory alongside the regenerated page. When no run is committed yet, the loader
returns ``None`` so the generator can emit an honest placeholder rather than fabricate
numbers.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

_BENCHMARKS_DIR = Path(__file__).resolve().parents[1]
_RUNS_ROOT = _BENCHMARKS_DIR / "results" / "runs"
_RUN_ID = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")


@dataclass(frozen=True)
class Source:
    run_id: str
    report_link: str
    engine: dict
    scaling: dict | None
    manifest: dict


def benchmarks_dir() -> Path:
    return _BENCHMARKS_DIR


def _read_json(path: Path) -> dict:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SystemExit(f"benchmark writeup: missing {path}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"benchmark writeup: {path} is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise SystemExit(f"benchmark writeup: {path} must contain a JSON object")
    return parsed


def _read_optional_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    return _read_json(path)


def _latest_run(runs_root: Path) -> str | None:
    if not runs_root.is_dir():
        return None
    ids = [entry.name for entry in runs_root.iterdir() if entry.is_dir() and _RUN_ID.match(entry.name)]
    return max(ids) if ids else None


def load_engine_source() -> Source | None:
    run_id = _latest_run(_RUNS_ROOT)
    if run_id is None:
        return None
    directory = _RUNS_ROOT / run_id
    return Source(
        run_id=run_id,
        report_link=f"results/runs/{run_id}/engine.json",
        engine=_read_json(directory / "engine.json"),
        scaling=_read_optional_json(directory / "engine-scaling.json"),
        manifest=_read_json(directory / "run.json"),
    )
