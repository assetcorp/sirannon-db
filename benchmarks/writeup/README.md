# Benchmark writeup generator

This turns the latest committed benchmark run into the numbers on the root `BENCHMARKS.md`, and writes a self-contained `comparison.md` beside that run's result files.

```sh
python3 benchmarks/writeup/generate.py            # rewrite BENCHMARKS.md and comparison.md
python3 benchmarks/writeup/generate.py --check     # fail if either is out of date
python3 benchmarks/writeup/charts.py               # redraw every figure into the run's own charts/ directory
```

The page is hand-written prose with generated regions marked by `<!-- BENCH:<id> START -->` and `<!-- BENCH:<id> END -->` comments. The generator only replaces the text inside those regions, so the narrative stays human while the tables and the machine description come from the recorded run. It reads the newest run under `benchmarks/server/results/runs/`, which is the lexicographic maximum of the run ids. When no run is committed it writes a placeholder instead of numbers.

The `--check` mode is the continuous-integration gate: it fails if the committed page or the per-run `comparison.md` differs from a fresh generation, so a stale page cannot merge.

`charts.py` draws the figures both documents embed: a three-panel sweep per workload, a two-panel soak per soaked workload, a latency profile grid, and an operating-point bar chart, each at both durability levels. It writes them into `charts/` inside the run's own directory, so each run keeps the figures drawn from its own numbers, and each figure also prints the run id it came from. Run it after every new run and before `generate.py`, because `--check` fails when the page references a figure that is missing.

`charts.py` needs matplotlib and `generate.py` never imports it, so the gate above stays dependency-free. Install it from inside `benchmarks/server` with `python3 -m pip install -e '.[charts]'`.
