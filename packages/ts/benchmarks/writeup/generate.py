"""Generate the numbers in BENCHMARKS.md from the latest recorded benchmark run.

The page is hand-written prose with generated regions marked by HTML comments. This tool
replaces the text between each ``<!-- BENCH:<id> START -->`` and its matching END marker, so
the narrative stays human while every table and the hardware description come from the
recorded run. With ``--check`` it verifies the committed page matches a fresh generation and
exits non-zero on any drift, which is what continuous integration runs.

    python3 benchmarks/writeup/generate.py            # rewrite BENCHMARKS.md in place
    python3 benchmarks/writeup/generate.py --check     # fail if the page is out of date
"""

from __future__ import annotations

import sys
from pathlib import Path

from docker_section import comparison_document, docker_blocks
from sources import Source, benchmarks_dir, load_engine_source


def _inject(text: str, blocks: dict[str, str]) -> str:
    for block_id, content in blocks.items():
        start = f"<!-- BENCH:{block_id} START -->"
        end = f"<!-- BENCH:{block_id} END -->"
        start_at = text.find(start)
        end_at = text.find(end)
        if start_at == -1 or end_at == -1 or end_at < start_at:
            raise SystemExit(f"benchmark writeup: markers for '{block_id}' are missing or malformed in BENCHMARKS.md")
        head = text[: start_at + len(start)]
        tail = text[end_at:]
        text = f"{head}\n{content}\n{tail}"
    return text


def _comparison_path(source: Source) -> Path:
    return benchmarks_dir() / "results" / "runs" / source.run_id / "comparison.md"


def main(argv: list[str]) -> int:
    check = "--check" in argv
    path = benchmarks_dir() / "BENCHMARKS.md"
    current = path.read_text(encoding="utf-8")
    source = load_engine_source()
    updated = _inject(current, docker_blocks(source))

    fresh_comparison = comparison_document(source) if source is not None else None
    comparison_path = _comparison_path(source) if source is not None else None

    if check:
        stale: list[str] = []
        if updated != current:
            stale.append("BENCHMARKS.md")
        if fresh_comparison is not None and comparison_path is not None:
            existing = comparison_path.read_text(encoding="utf-8") if comparison_path.is_file() else None
            if existing != fresh_comparison:
                stale.append(f"results/runs/{source.run_id}/comparison.md")
        if stale:
            sys.stderr.write(
                f"Benchmark writeup is out of date with the latest run: {', '.join(stale)}. "
                "Run `python3 benchmarks/writeup/generate.py` and commit the result.\n"
            )
            return 1
        sys.stdout.write("Benchmark writeup is up to date with the latest run.\n")
        return 0

    wrote: list[str] = []
    if updated != current:
        path.write_text(updated, encoding="utf-8")
        wrote.append("BENCHMARKS.md")
    if fresh_comparison is not None and comparison_path is not None:
        existing = comparison_path.read_text(encoding="utf-8") if comparison_path.is_file() else None
        if existing != fresh_comparison:
            comparison_path.write_text(fresh_comparison, encoding="utf-8")
            wrote.append(f"results/runs/{source.run_id}/comparison.md")

    if wrote:
        sys.stdout.write(f"Regenerated from the latest run: {', '.join(wrote)}.\n")
    else:
        sys.stdout.write("Benchmark writeup is already up to date.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
