#!/usr/bin/env python3
"""Summarise the load and stress test results.

Reports throughput only. The driver shares unpinned host cores with the
container, so per-request latency from this harness is not a measurement.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

RESULTS = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).parent / "results")


def dominant_kind(point: dict) -> str:
    by_kind = point.get("failures", {}).get("by_kind", {})
    if not by_kind:
        return ""
    kind, count = max(by_kind.items(), key=lambda item: item[1])
    total = sum(by_kind.values())
    return f" {kind}" if count == total else f" {kind} {count / total:.0%}"


def recovery(sweep: list[dict]) -> str:
    if len(sweep) < 2:
        return "n/a"
    trailing = sweep[-1]
    rate = trailing["target_rate"]
    earlier = [p for p in sweep[:-1] if p["target_rate"] == rate]
    if not earlier:
        return "n/a"
    before = earlier[0]["throughput"]["median_ops"]
    after = trailing["throughput"]["median_ops"]
    if before <= 0:
        return "n/a"
    ratio = after / before
    if ratio >= 0.95:
        return f"recovered ({ratio:.0%})"
    if ratio >= 0.5:
        return f"partial ({ratio:.0%})"
    return f"NO ({ratio:.0%})"


def main() -> int:
    cells = sorted(p for p in RESULTS.iterdir() if p.is_dir()) if RESULTS.exists() else []
    if not cells:
        print(f"no results under {RESULTS}")
        return 1

    print(f"{'cell':<44} {'peak ops/s':>10} {'overload':>28} {'recovery':>18}")
    print("-" * 103)
    for cell in cells:
        status = (cell / "status").read_text().strip() if (cell / "status").exists() else "?"
        artifacts = list((cell / "runs" / "cell").glob("engine-sirannon-*.json"))
        if not artifacts:
            print(f"{cell.name:<44} {status:>10}")
            continue
        for artifact in sorted(artifacts):
            data = json.loads(artifact.read_text())
            for w in data["workloads"]:
                sweep = w["sweep"]
                peak = max(p["throughput"]["median_ops"] for p in sweep)
                top = max(sweep, key=lambda p: p["target_rate"])
                overload = (
                    f"{top['throughput']['median_ops']:.0f}/s "
                    f"e={top['error_rate']:.1%}{dominant_kind(top)}"
                )
                label = f"{cell.name}/{w['workload']}"
                print(f"{label:<44} {peak:>10.0f} {overload:>28} {recovery(sweep):>18}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
