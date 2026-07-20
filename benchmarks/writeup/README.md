# Benchmark writeup generator

This turns the latest committed benchmark run into the numbers on the root `BENCHMARKS.md`, and writes a self-contained `comparison.md` beside that run's result files.

```sh
python3 benchmarks/writeup/generate.py            # rewrite BENCHMARKS.md and comparison.md
python3 benchmarks/writeup/generate.py --check     # fail if either is out of date
```

The page is hand-written prose with generated regions marked by `<!-- BENCH:<id> START -->` and `<!-- BENCH:<id> END -->` comments. The generator only replaces the text inside those regions, so the narrative stays human while the tables and the machine description come from the recorded run. It reads the newest run under `benchmarks/server/results/runs/`, which is the lexicographic maximum of the run ids. When no run is committed it writes a placeholder instead of numbers.

The `--check` mode is the continuous-integration gate: it fails if the committed page or the per-run `comparison.md` differs from a fresh generation, so a stale page cannot merge.
