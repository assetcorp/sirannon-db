# Sirannon vs PostgreSQL benchmark harness

This harness compares Sirannon against PostgreSQL on the same OLTP workloads, on one host, with matched durability and a load generator that records the full tail latency. One Node load generator drives both engines through the client each ships: Sirannon over its SDK's WebSocket transport, and PostgreSQL over its binary socket protocol through node-postgres. The generator writes one result file per engine; a small Python step joins those files into the cross-engine comparison, and the writeup renders the page.

## What it measures

The core is a server-versus-server comparison on the standard OLTP workloads both engines run identically: point-select, bulk-insert, batch-update, YCSB A/B/C/F, and a TPC-C-shaped transaction mix. For each workload the harness sweeps a set of target request rates and reports the operating point, the highest offered rate the engine sustained while holding p99 latency under the disclosed service-level target. The full sweep is kept as the throughput-versus-load curve.

Alongside the head-to-head, the harness records three Sirannon characterizations PostgreSQL has no direct equivalent for: change-feed latency over Sirannon's built-in WebSocket feed, cold-start time, and the connection-scaling curve.

## Running it

Two profiles cover the common cases. The `cloud` profile is the full run, and a bare `./run-all.sh` uses it by default: 10,000,000 rows across both durability levels in Docker, and it regenerates the page from the fresh numbers.

```sh
./run-all.sh cloud
```

The `smoke` profile checks that the harness works end to end without spending the time a real run needs: 10,000 rows at one durability level with short windows. It never touches the published page, and it keeps its output under `results/.smoke/` (git-ignored) so you can read the numbers and confirm they look sane. Remove that directory yourself once you're satisfied: `rm -rf results/.smoke`.

```sh
./run-all.sh smoke
```

A profile only fills in defaults. Any `BENCH_` variable you export still overrides it, so `BENCH_RUNS=1 ./run-all.sh smoke` keeps a single pass.

To publish credible numbers, run it on the disclosed cloud machine through `benchmarks/cloud`. On macOS a plain fsync does not flush the drive cache, so the full-durability numbers are only valid from the Linux cloud run.

You can also drive a hand-started server and a local PostgreSQL without Docker. Build the SDK the generator imports, install the generator's own dependencies, then point it at the two engines through `BENCH_` variables:

```sh
pnpm --filter @delali/sirannon-db build
pnpm --dir benchmarks/server/driver --ignore-workspace install
node benchmarks/server/driver/src/cli.ts --engine sirannon --durability matched
node benchmarks/server/driver/src/cli.ts --engine postgres --durability matched
pip install -e '.[dev]' && python -m sirannon_bench.aggregate
```

## Results layout

Each run is a self-contained directory under `results/runs/<run id>/`:

- `run.json` records the machine, the commit, and the configuration.
- `engine-<engine>-<durability>.json` records one engine's numbers.
- `cold-start.json` records the cold-start timing for both engines.
- `comparison.json` joins the engine files into the cross-engine comparison.
- `comparison.md` is the human-readable report for that run, written by the writeup generator.

Run directories are committed, so a published page always points at the run it came from. Python caches, virtual environments, and the Node generator's installed `node_modules` are ignored.

## Statistics

Every throughput figure is the median of several independent runs, each a fresh warmup and measurement, shown with a 95% bootstrap confidence interval and the run-to-run coefficient of variation, so one noisy run cannot set the headline and a reader can tell a real difference from noise. The bootstrap reseeds from a fixed seed, so a given set of samples produces the same interval every time.
