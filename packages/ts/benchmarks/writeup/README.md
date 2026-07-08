# Benchmark writeup generator

`BENCHMARKS.md` in this directory compares Sirannon against Postgres. The prose is written
by hand, but the reference numbers and the hardware description are generated from recorded
runs, so the page cannot quietly fall out of step with the results.

## How it works

`generate.py` reads the latest committed run under `results/runs/` and fills the regions of
`BENCHMARKS.md` marked by `<!-- BENCH:<id> START -->` and `<!-- BENCH:<id> END -->` comments.
The newest run wins, because each run directory is named with a compact UTC timestamp.
Everything outside those markers stays exactly as written.

Each run directory holds a self-describing `run.json` manifest (the machine, the git commit,
durability mode, seed, and both engine versions) beside the `engine.json` and
`engine-scaling.json` result files. The generator reads the manifest for the machine line and
the result files for the numbers, so the published page always names the exact host the run
executed on.

The tool depends only on the Python standard library and reads each run's committed JSON, so
it produces the same page on any machine.

## Commands

Run these from `packages/ts/` after a benchmark run:

```sh
pnpm bench:writeup          # rewrite BENCHMARKS.md from the latest run
pnpm bench:writeup:check    # exit non-zero if the page is out of date
```

Continuous integration runs the check on every push and pull request, so a page that no
longer matches the committed run fails the build.

## Publishing a run

Publishing new numbers takes three steps: run the Docker engine suite on the disclosed cloud
machine, regenerate the page, and commit the run directory together with `BENCHMARKS.md`. The
check reads committed runs, so the run directory that backs the page has to be committed
alongside it. When no run is committed the generated regions render an honest placeholder and
the check still passes.
