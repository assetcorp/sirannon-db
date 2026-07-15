from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

RUN_ID_ENV = "BENCH_RUN_ID"
RUNS_DIRNAME = "runs"
MANIFEST_NAME = "run.json"

# The leading-alphanumeric class rejects '.' and '..'; widening it reopens path traversal.
_SEGMENT = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._-]{0,63}$")


def mint_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def default_results_dir() -> Path:
    override = os.environ.get("BENCH_RESULTS_DIR")
    return Path(override) if override else Path.cwd() / "results"


def _validate_segment(value: str, kind: str) -> str:
    if not isinstance(value, str) or not _SEGMENT.match(value):
        raise ValueError(
            f"invalid {kind} {value!r}: expected 1-64 characters of letters, digits, dot, dash, "
            "or underscore, not starting with a dot or dash"
        )
    return value


def validate_run_id(run_id: str) -> str:
    return _validate_segment(run_id, "run id")


def validate_artifact_name(name: str) -> str:
    return _validate_segment(name, "artifact name")


def runs_root(results_dir: Path) -> Path:
    return results_dir / RUNS_DIRNAME


def run_directory(results_dir: Path, run_id: str) -> Path:
    validate_run_id(run_id)
    root = runs_root(results_dir)
    candidate = root / run_id
    if not candidate.resolve().is_relative_to(root.resolve()):
        raise ValueError(f"run id {run_id!r} resolves outside the results directory")
    return candidate


def latest_run_id(results_dir: Path) -> str | None:
    root = runs_root(results_dir)
    if not root.is_dir():
        return None
    ids = [entry.name for entry in root.iterdir() if entry.is_dir() and _SEGMENT.match(entry.name)]
    return max(ids) if ids else None


def resolve_run_id_for_write(explicit: str | None) -> str:
    raw = (explicit or os.environ.get(RUN_ID_ENV) or "").strip()
    return validate_run_id(raw) if raw else mint_run_id()


def write_json(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    temporary.replace(path)
    return path


def write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)
    return path


def write_run_manifest(results_dir: Path, run_id: str, environment: dict, config: dict) -> Path:
    directory = run_directory(results_dir, run_id)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / MANIFEST_NAME
    created_at = environment.get("captured_at")
    if path.exists():
        try:
            prior = json.loads(path.read_text(encoding="utf-8"))
            created_at = prior.get("created_at") or created_at
        except (OSError, json.JSONDecodeError):
            created_at = environment.get("captured_at")
    manifest = {
        "run_id": run_id,
        "created_at": created_at,
        "harness_version": environment.get("harness_version"),
        "environment": environment,
        "config": config,
    }
    write_json(path, manifest)
    return path
