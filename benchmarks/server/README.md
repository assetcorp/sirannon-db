# Sirannon vs PostgreSQL benchmark harness

This harness compares Sirannon against PostgreSQL on the same OLTP workloads, on one host, with
matched durability and a load generator that reports the tail latency honestly. One Node load
generator drives both engines through the client each actually ships: Sirannon over its SDK's
WebSocket transport, and PostgreSQL over its binary socket protocol through node-postgres. The
generator writes one result file per engine; a small Python step joins those files into the
cross-engine comparison and the writeup renders the page.

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

**One generator, one client per engine.** A single Node load generator holds the open-loop
scheduler, the coordinated-omission correction, and the statistics, so that instrument behaves
identically for both systems; only a thin per-database adapter differs. Each engine is reached
through the client it actually ships: Sirannon over its SDK's default WebSocket transport, which
multiplexes every concurrent request over one persistent socket the way an application talks to
it, and PostgreSQL over node-postgres on its binary socket protocol. Driving each system through
its own shipping client, and disclosing the difference, is what respected efforts do: ClickBench
measures the full client path, YCSB drives every database through its own native binding, and
SurrealDB's own comparison runs each engine on its native protocol and tells readers not to
compare across the transport gap. Sirannon pays JSON framing on every request, which is heavier
than PostgreSQL's binary protocol. Charging Sirannon that cost is the point, because it's
Sirannon's real client path. Wrapping PostgreSQL in a foreign HTTP proxy to equalize transport
would measure the proxy, not the database, so the harness never does that.

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
p99.9, never an average. Before each sweep it measures each client's own throughput ceiling
against the live engine and discloses it, so a rate that falls short is charged to the database,
and a client that ran out of room is flagged instead of being read as a slow server.

**The setup is disclosed and capped.** Both engines run in resource-capped containers on one host
with no network between the load driver and the server. The load driver runs on its own pinned
cores so it never competes with the engine under test. Every figure in the report names the
machine, the commit, the durability settings, the workloads, and the seed.

## Running it

The full run drives both engines at both durability levels in Docker:

```sh
./run-all.sh
```

Two presets cover the common cases. The `cloud` preset is the real thing: 10,000,000 rows across
both durability levels, and it regenerates the page from the fresh numbers.

```sh
./run-all.sh cloud
```

The `smoke` preset checks that the harness works end to end without spending the time a real run
needs: 10,000 rows at one durability level with short windows. It never touches the published
page, and it keeps its output under `results/.smoke/` (git-ignored) so you can read the numbers
and confirm they look sane. Remove that directory yourself once you're satisfied:
`rm -rf results/.smoke`.

```sh
./run-all.sh smoke
```

A preset only fills in defaults. Any `BENCH_` variable you export still overrides it, so
`BENCH_RUNS=1 ./run-all.sh smoke` keeps a single pass.

To publish credible numbers, run it on the disclosed cloud machine through `benchmarks/cloud`,
not on a laptop, because laptop clocks throttle under sustained load. On macOS a plain fsync does
not flush the drive cache, so trust the full-durability numbers only from the Linux cloud run.

You can also drive a hand-started server and a local PostgreSQL without Docker. Build the SDK the
generator imports, install the generator's own dependencies, then point it at the two engines
through `BENCH_` variables:

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

Run directories are committed, so a published page always points at the run it came from. Python
caches, virtual environments, and the Node generator's installed `node_modules` are ignored.

## Statistics

Every throughput figure is the median of several independent runs, each a fresh warmup and
measurement, shown with a 95% bootstrap confidence interval and the run-to-run coefficient of
variation, so one noisy run cannot set the headline and a reader can tell a real difference from
noise. The bootstrap reseeds from a fixed seed, so a given set of samples produces the same
interval every time.
