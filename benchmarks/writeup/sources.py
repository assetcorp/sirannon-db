from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_RUNS_ROOT = _REPO_ROOT / "benchmarks" / "server" / "results" / "runs"
_RUN_ID = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")


@dataclass(frozen=True)
class Source:
    run_id: str
    report_link: str
    comparison: dict


def repo_root() -> Path:
    return _REPO_ROOT


def benchmarks_page() -> Path:
    return _REPO_ROOT / "BENCHMARKS.md"


def comparison_path(run_id: str) -> Path:
    return _RUNS_ROOT / run_id / "comparison.md"


def _latest_run(runs_root: Path) -> str | None:
    if not runs_root.is_dir():
        return None
    ids = [entry.name for entry in runs_root.iterdir() if entry.is_dir() and _RUN_ID.match(entry.name)]
    return max(ids) if ids else None


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


def load_source() -> Source | None:
    run_id = _latest_run(_RUNS_ROOT)
    if run_id is None:
        return None
    comparison_json = _RUNS_ROOT / run_id / "comparison.json"
    if not comparison_json.is_file():
        return None
    return Source(
        run_id=run_id,
        report_link=f"benchmarks/server/results/runs/{run_id}/comparison.md",
        comparison=_read_json(comparison_json),
    )
