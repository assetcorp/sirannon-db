# Sirannon vs PostgreSQL benchmark harness

This harness compares Sirannon against PostgreSQL on the same OLTP workloads, on one host, with
matched durability and a load generator that reports the tail latency honestly. It drives each
engine through the client that engine actually ships: Sirannon over HTTP into its real server
front-end, and PostgreSQL over its binary socket protocol through psycopg.

## What it measures

The core is a fair, server-versus-server comparison on the standard OLTP workloads both engines
run identically: point-select, bulk-insert, batch-update, YCSB A/B/C/F, and a TPC-C-shaped
transaction mix. For each workload the harness sweeps a set of target request rates and reports
the operating point, the highest offered rate the engine sustained while holding p99 latency
under the disclosed service-level target. The full sweep is kept as the throughput-versus-load
curve.

Alongside the head-to-head, the harness records characterizations that PostgreSQL has no direct
equivalent for: change-feed latency over Sirannon's built-in WebSocket feed, cold-start time, and
the connection-scaling curve. Each is framed as a property of Sirannon, never as a win over
PostgreSQL, because the two systems do not do the same work there.

## Why the comparison is fair

**Each engine keeps its own client.** Driving each system through its own shipping client, and
disclosing the difference, is what respected efforts do: ClickBench measures the full client
path, YCSB drives every database through its own native binding, and SurrealDB's own comparison
runs each engine on its native protocol and tells readers not to compare across the transport
gap. Sirannon pays HTTP and JSON framing on every request, which is heavier than PostgreSQL's
binary protocol. Charging Sirannon that cost is the point, because it's Sirannon's real client
path. Wrapping PostgreSQL in a foreign HTTP proxy to equalize transport would measure the proxy,
not the database, so the harness never does that.

**Durability is matched, at two levels.** The run reports a full-durability row and a
matched-relaxed row. Full durability sets PostgreSQL `synchronous_commit=on` against SQLite
`synchronous=FULL`, so both fsync every commit. Matched-relaxed sets `synchronous_commit=off`
against `synchronous=NORMAL` in WAL mode, so both defer the fsync and both can lose only the most
recent commits on power loss without corrupting. Running one engine durable and the other relaxed
would measure fsync against no-fsync, so the harness matches at each level.

**The load is open-loop and corrected for coordinated omission.** A closed-loop generator stops
sending when the server stalls, so the slow requests never get measured and the tail looks far
better than it is. This harness fires requests at a fixed rate and charges each request's latency
from the time it was meant to be sent, which is the wrk2 correction. It reports p50, p95, p99, and
p99.9, never an average, and it flags when the load driver rather than the server was the limit.

**The setup is disclosed and capped.** Both engines run in resource-capped containers on one host
with no network between the load driver and the server. The load driver runs on its own pinned
cores so it never competes with the engine under test. Every figure in the report names the
machine, the commit, the durability settings, the workloads, and the seed.

## Running it

The full run drives both engines at both durability levels in Docker:

```sh
./run-all.sh
```

A quick smoke overrides the workload set and the windows:

```sh
BENCH_WORKLOADS=point-select BENCH_TARGET_RATES=1000 BENCH_RUNS=2 \
  BENCH_WARMUP_SECONDS=1 BENCH_MEASURE_SECONDS=2 ./run-all.sh
```

To publish credible numbers, run it on the disclosed cloud machine through `benchmarks/cloud`,
not on a laptop, because laptop clocks throttle under sustained load. On macOS a plain fsync does
not flush the drive cache, so trust the full-durability numbers only from the Linux cloud run.

You can also drive a hand-started server and a local PostgreSQL without Docker:

```sh
pip install -e '.[dev]'
python -m sirannon_bench.cli --engine sirannon --durability matched
python -m sirannon_bench.cli --engine postgres --durability matched
python -m sirannon_bench.aggregate
```

## Results layout

Each run is a self-contained directory under `results/runs/<run id>/`:

- `run.json` records the machine, the commit, and the configuration.
- `engine-<engine>-<durability>.json` records one engine's numbers.
- `cold-start.json` records the cold-start timing for both engines.
- `comparison.json` joins the engine files into the cross-engine comparison.
- `comparison.md` is the human-readable report for that run, written by the writeup generator.

Run directories are committed, so a published page always points at the run it came from. Only
Python caches and virtual environments are ignored.

## Statistics

Every throughput figure is the median of several independent runs, each a fresh warmup and
measurement, shown with a 95% bootstrap confidence interval and the run-to-run coefficient of
variation, so one noisy run cannot set the headline and a reader can tell a real difference from
noise. The bootstrap reseeds from a fixed seed, so a given set of samples produces the same
interval every time.
