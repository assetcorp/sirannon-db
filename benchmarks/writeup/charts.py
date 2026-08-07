from __future__ import annotations

import sys

from comparison_view import durabilities

from chart_bars import operating_points_figure
from chart_soak import has_soak, soak_figure
from chart_style import apply_style
from chart_sweep import sweep_figure
from chart_tail import tail_figure
from sources import Source, load_source, repo_root


def _target_ms(comparison: dict) -> float | None:
    value = comparison.get("config", {}).get("slo_p99_ms")
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def render(source: Source) -> list[str]:
    apply_style()
    root = repo_root()
    target = _target_ms(source.comparison)
    written: list[str] = []
    for durability, node in durabilities(source.comparison):
        rows = node.get("workloads", [])
        if rows:
            written.append(str(operating_points_figure(root, source.run_id, durability, rows)))
            written.append(str(tail_figure(root, source.run_id, durability, rows, target)))
        for entry in node.get("scaling", []):
            written.append(str(sweep_figure(root, source.run_id, durability, entry, target)))
        for row in rows:
            if has_soak(row):
                written.append(str(soak_figure(root, source.run_id, durability, row, target)))
    return written


def main() -> int:
    source = load_source()
    if source is None:
        sys.stderr.write("benchmark charts: no run is committed under benchmarks/server/results/runs/\n")
        return 1
    written = render(source)
    sys.stdout.write(f"Rendered {len(written)} charts from run {source.run_id}.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
