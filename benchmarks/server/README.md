# Sirannon vs PostgreSQL benchmark harness

This harness compares Sirannon against PostgreSQL on the same OLTP workloads, with matched durability and a load generator that records the full tail latency. One Node load generator drives both engines through the client each provides: Sirannon over its SDK's WebSocket transport, and PostgreSQL over its binary socket protocol through node-postgres. The generator writes one result file per engine; a small Python step joins those files into the cross-engine comparison, and the writeup renders the page.

## What it measures

The core is a server-versus-server comparison on the standard OLTP workloads both engines run identically: point-select, single-row-insert, single-row-update, YCSB A/B/C/F, a TPC-C-shaped mix of one-statement writes, and the TPC-C new-order transaction. For each workload the harness sweeps a set of target request rates and reports the operating point, the highest offered rate the engine sustained while holding p99 latency under the disclosed service-level target. The full sweep is kept as the throughput-versus-load curve, and `BENCH_SWEEP_STOP_STEPS` can end it a set number of steps past the first unsustained rate instead of running every rate.

The harness sweeps every workload once at each limit on the number of requests in flight (`BENCH_CONCURRENCY_LEVELS`, default 16, 64, and 256). The limit with the highest operating point after that sweep is the engine's peak limit, and the harness sweeps it again until it holds `BENCH_RUNS` measurements at every rate. The PostgreSQL driver opens one pooled connection for each request that the limit allows, so the harness raises `max_connections` to the highest limit plus PostgreSQL's default of 100. Each result also records how many round trips one operation takes on each engine.

A rate proven in short windows can still collapse under the engine's periodic housekeeping, so the harness can also hold the operating point for a long continuous window (`BENCH_SOAK_SECONDS`, on the workloads in `BENCH_SOAK_WORKLOADS`) and report the slowest 30-second slice, so a checkpoint or vacuum that spikes tail latency shows up instead of averaging away.

Every measured request carries a finite timeout, each workload runs under a stall deadline sized to its expected duration, and each schema reset retries engine errors for a bounded budget. A stalled workload ends that engine's pass and keeps the workloads already measured, so one wedged engine cannot burn the machine or contaminate the next workload's numbers. The deadlines, the retry budget, and the timeout are all `BENCH_` variables.

Dropping a ten-million-row table between workloads runs for minutes of random reads, so two limits that would cut it short are lifted for the run. `BENCH_WRITE_TIMEOUT_MS` (default 0) disables the Sirannon writer thread's own deadline, matching PostgreSQL's `statement_timeout` default, and `BENCH_SCHEMA_TIMEOUT_MS` (default 1800000) bounds the schema-reset request itself, which otherwise inherits undici's 300-second header timeout. The workload stall deadline and the per-pass timeout still cap a genuinely wedged engine. Every run report states both values, and each drop's duration appears in the run log.

Alongside the head-to-head, the harness records three Sirannon characterizations PostgreSQL has no direct equivalent for: change-feed latency over Sirannon's built-in WebSocket feed, cold-start time, and the connection-scaling curve.

## Running it

The suite runs both engines as native processes in transient systemd units, so it needs a Linux host with systemd, passwordless sudo, PostgreSQL 18, Node 24, and pnpm; `benchmarks/cloud/remote-bootstrap.sh` installs all of it on the benchmark VM. On one host, the engine under test runs pinned to its own CPU cores under a hard memory ceiling (defaults: cores 4-7 and 2G through `BENCH_ENGINE_CPUSET` and `BENCH_ENGINE_MEMORY`), the load driver runs on disjoint cores (default 0-3 through `BENCH_DRIVER_CPUSET`) with no memory cap, and the script proves at device level that each data directory under `BENCH_DATA_ROOT` is on the expected disk before anything seeds. Each engine process starts fresh for its measured pass and is stopped after it, so neither engine carries a warm buffer pool between passes. Between engine passes a disclosed cooldown (sync, a dirty-page wait, fstrim, then a `BENCH_COOLDOWN_SECONDS` pause, default 30) lets the machine recover, and the page cache is dropped before each measured series.

The cloud runner puts the load generator on a second machine. Set `BENCH_LOAD_ADDRESS` to the load machine's private address and `BENCH_ENGINE_ADDRESS` to the address on which the load machine reaches the engine machine, and the script starts the generator there over SSH as `BENCH_LOAD_USER` with the key in `BENCH_LOAD_SSH_KEY` (default `~/.ssh/bench_load`). In that mode each engine gets every logical CPU of the engine machine, a small HTTP service on port 9877 reports the engine's CPU time to the generator, and the generator times 200 TCP connections to that port before each pass to record the network delay. The engines listen on ports 9876 and 5432 of `BENCH_ENGINE_ADDRESS`, so the network between the machines has to allow those three ports and SSH.

Two profiles cover the common cases. The `cloud` profile is the full run, and a bare `./run-all.sh` uses it by default: 10,000,000 rows across both durability levels, and it regenerates the page from the fresh numbers.

```sh
./run-all.sh cloud
```

The `smoke` profile checks that the harness works end to end without spending the time a real run needs: 10,000 rows at one durability level with short windows. It never touches the published page, and it keeps its output under `results/.smoke/` (git-ignored) so you can read the numbers and confirm they look sane. Remove that directory yourself once you're satisfied: `rm -rf results/.smoke`.

```sh
./run-all.sh smoke
```

A profile only fills in defaults. Any `BENCH_` variable you export still overrides it, so `BENCH_RUNS=1 ./run-all.sh smoke` keeps a single pass.

`BENCH_ENGINES` and `BENCH_DURABILITIES` choose which passes run, and `BENCH_RUN_ID` chooses the run directory they land in. Together they repair a run that lost one pass, because the new pass joins the ones already recorded and the aggregate and the writeup regenerate across all of them:

```sh
BENCH_RUN_ID=20260804T221053Z BENCH_ENGINES=postgres BENCH_DURABILITIES=matched ./run-all.sh cloud
```

To publish credible numbers, run it on the disclosed cloud machine through `benchmarks/cloud`. On macOS a plain fsync does not flush the drive cache, so the full-durability numbers are only valid from the Linux cloud run.

You can also drive a hand-started server and a local PostgreSQL without the orchestration, on any OS. Build the SDK the generator imports, install the generator's own dependencies, then point it at the two engines through `BENCH_` variables:

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
- `engine-machine.json` records the machine that hosted the engines, with the CPUs and the memory ceiling that they had.
- `engine-<engine>-<durability>.json` records one engine's numbers.
- `cold-start.json` records the cold-start timing for both engines.
- `resource-control.log` records each pass's caps read back from the live cgroup, including peak memory.
- `comparison.json` joins the engine files into the cross-engine comparison.
- `comparison.md` is the human-readable report for that run, written by the writeup generator.

Run directories are committed, so a published page always points at the run it came from. Python caches, virtual environments, and the Node generator's installed `node_modules` are ignored.

## Statistics

Every throughput figure is the median of several independent runs, each a fresh warmup and measurement, shown with a 95% bootstrap confidence interval and the run-to-run coefficient of variation, so one noisy run cannot set the headline and a reader can tell a real difference from noise. The bootstrap reseeds from a fixed seed, so a given set of samples produces the same interval every time.
