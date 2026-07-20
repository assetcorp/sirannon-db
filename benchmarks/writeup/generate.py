from __future__ import annotations

import sys

from server_section import blocks, comparison_document
from sources import benchmarks_page, comparison_path, load_source


def _inject(text: str, regions: dict[str, str]) -> str:
    for block_id, content in regions.items():
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


def main(argv: list[str]) -> int:
    check = "--check" in argv
    page = benchmarks_page()
    current = page.read_text(encoding="utf-8")
    source = load_source()
    updated = _inject(current, blocks(source))

    fresh_comparison = comparison_document(source) if source is not None else None
    comparison_file = comparison_path(source.run_id) if source is not None else None

    if check:
        stale: list[str] = []
        if updated != current:
            stale.append("BENCHMARKS.md")
        if fresh_comparison is not None and comparison_file is not None:
            existing = comparison_file.read_text(encoding="utf-8") if comparison_file.is_file() else None
            if existing != fresh_comparison:
                stale.append(f"benchmarks/server/results/runs/{source.run_id}/comparison.md")
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
        page.write_text(updated, encoding="utf-8")
        wrote.append("BENCHMARKS.md")
    if fresh_comparison is not None and comparison_file is not None:
        existing = comparison_file.read_text(encoding="utf-8") if comparison_file.is_file() else None
        if existing != fresh_comparison:
            comparison_file.write_text(fresh_comparison, encoding="utf-8")
            wrote.append(f"benchmarks/server/results/runs/{source.run_id}/comparison.md")

    if wrote:
        sys.stdout.write(f"Regenerated from the latest run: {', '.join(wrote)}.\n")
    else:
        sys.stdout.write("Benchmark writeup is already up to date.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
