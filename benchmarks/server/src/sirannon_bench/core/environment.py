"""Capture the machine and build provenance recorded with every run.

Published numbers name the exact host they came from and the exact commit that produced them,
so a reader can reproduce the run and a stale page can be told apart from a fresh one. The CPU
and memory readings come from ``/proc`` on Linux, which is where the cloud runs execute; on
other hosts they fall back to what the platform reports.
"""

from __future__ import annotations

import os
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from .. import __version__


def _cpu_model() -> str | None:
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    return platform.processor() or None


def _total_memory_bytes() -> int | None:
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    return None


def _git(args: list[str]) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def git_provenance() -> dict:
    commit = _git(["rev-parse", "HEAD"])
    if commit is None:
        return {"commit": "unknown", "dirty": False}
    status = _git(["status", "--porcelain"])
    return {"commit": commit, "dirty": bool(status)}


def capture_environment() -> dict:
    provenance = git_provenance()
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "harness_version": __version__,
        "containerized": Path("/.dockerenv").exists(),
        "machine_label": os.environ.get("BENCH_MACHINE_LABEL") or None,
        "os": f"{platform.system()} {platform.release()}",
        "arch": platform.machine(),
        "cpu_model": _cpu_model(),
        "logical_cpus": os.cpu_count(),
        "total_memory_bytes": _total_memory_bytes(),
        "python_version": platform.python_version(),
        "git_commit": provenance["commit"],
        "git_dirty": provenance["dirty"],
    }
