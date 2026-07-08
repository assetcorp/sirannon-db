# Benchmarks

Sirannon benchmarks compare it against Postgres 17 across micro-operations, industry-standard workloads (YCSB, TPC-C), concurrency scaling, and Sirannon-specific features like CDC and connection pooling. The published head-to-head reaches each database over its own client-server path on one host, so the comparison measures the engines rather than a network hop; Sirannon's embedded, in-process numbers are reported as a separate deployment-mode claim.

Two benchmark categories exist: **local benchmarks** for quick development feedback, and **Docker-based benchmarks** for fair, reproducible comparisons.

## Prerequisites

- Node.js 22+ (with `--expose-gc` support)
- pnpm
- Docker and Docker Compose
- [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) (for end-to-end Docker benchmarks)
- Python 3 with matplotlib and pandas (for chart generation, optional)

## Local Benchmarks

Local benchmarks use [tinybench](https://github.com/tinylibs/tinybench) with `--expose-gc` enabled for accurate memory management between runs. Sirannon runs natively on the host; Postgres runs in a Docker container.

These benchmarks are useful for quick iteration during development, but the numbers reflect host-vs-container differences and vary across machines.

### Starting Postgres

Most local benchmarks run Sirannon against a Postgres baseline. Start the Postgres container first:

```sh
docker compose -f benchmarks/docker-compose.yml up -d --wait
```

This launches Postgres 17 Alpine on port `5433` with tuned settings (256 MB shared buffers, `synchronous_commit=off` for reduced write latency, SSD-optimized planner costs). The default `matched` durability mode uses `synchronous_commit=off` for Postgres and `PRAGMA synchronous=NORMAL` for SQLite. Both sacrifice durability for write throughput, but at different layers. SQLite with `synchronous=NORMAL` in WAL mode syncs the WAL file at checkpoint boundaries rather than on every commit; a process crash is safe, but a power failure can lose transactions since the last checkpoint. Postgres with `synchronous_commit=off` reports a transaction as committed before the WAL is flushed to disk; a process crash is safe because the OS buffer cache remains intact, but an OS crash or power failure can lose up to ~600ms of recent transactions (3x the `wal_writer_delay` default of 200ms). Set `BENCH_DURABILITY=full` to enable maximum safety on both engines.

| Setting  | Value       |
|----------|-------------|
| Host     | `127.0.0.1` |
| Port     | `5433`      |
| User     | `benchmark` |
| Password | `benchmark` |
| Database | `benchmark` |

To stop and clean up:

```sh
docker compose -f benchmarks/docker-compose.yml down -v
```

### Running Local Benchmarks

All commands run from `packages/ts/`.

**Full suite:**

```sh
pnpm bench
```

Runs every benchmark sequentially. Benchmarks that need Postgres are skipped if the container is not running.

**Individual suites:**

```sh
pnpm bench:micro       # point-select, bulk-insert, batch-update
pnpm bench:ycsb        # YCSB workload-a (50/50 mixed)
pnpm bench:oltp        # TPC-C lite
pnpm bench:scaling     # Concurrency scaling (event-loop + worker threads)
pnpm bench:pool        # Pool size sweep (5, 10, 20 connections)
pnpm bench:cdc         # CDC throughput and latency
pnpm bench:statistical # 4 key benchmarks x 10 runs with full statistics
pnpm bench:charts      # Generate SVG charts from CSV results
```

### Local Benchmark Categories

**Micro** (requires Postgres) - Single-operation latency and throughput:

- `point-select` - Primary-key lookups
- `bulk-insert` - Batch row inserts
- `batch-update` - Batch row updates

**YCSB** (requires Postgres) - Yahoo! Cloud Serving Benchmark workloads:

- `workload-a` - 50% read, 50% update

**OLTP** (requires Postgres) - Online transaction processing:

- `tpc-c-lite` - Simplified TPC-C with 100K customers and 10K products, running new-order (45%), payment (43%), and order-status (12%) transactions

**Scaling** (requires Postgres) - Concurrency scaling from 1 to 64 clients:

- `scaling/concurrency` - Tests two deployment models: single event-loop (Sirannon serializes, Postgres overlaps via async pool) and worker thread pool (N threads with their own connections). Two workloads: read-only (WAL concurrent readers) and 50/50 mixed (single-writer contention).
- `scaling/pool-sweep` - Measures how Postgres connection pool size (5, 10, 20) affects throughput relative to Sirannon. Tests point-select and YCSB-A workloads at each pool size. Sirannon uses a fixed read pool of 4.

**Sirannon** (no Postgres needed) - Sirannon-specific features:

- `cdc-latency` - Change Data Capture event propagation latency
- `connection-pool` - Connection pool throughput and contention
- `cold-start` - Database open and first-query latency
- `multi-tenant` - Multi-database isolation throughput

### Configuration

Override defaults with environment variables:

| Variable                   | Default                     | Description                                                               |
|----------------------------|-----------------------------|---------------------------------------------------------------------------|
| `BENCH_PG_HOST`            | `127.0.0.1`                 | Postgres host                                                             |
| `BENCH_PG_PORT`            | `5433`                      | Postgres port                                                             |
| `BENCH_PG_USER`            | `benchmark`                 | Postgres user                                                             |
| `BENCH_PG_PASSWORD`        | `benchmark`                 | Postgres password                                                         |
| `BENCH_PG_DATABASE`        | `benchmark`                 | Postgres database                                                         |
| `BENCH_PG_MAX_CONNECTIONS` | `10`                        | Postgres connection pool size                                             |
| `BENCH_DURABILITY`         | `matched`                   | `matched` (SQLite NORMAL) or `full` (SQLite + fsync)                      |
| `BENCH_DATA_SIZES`         | `1000,10000,100000,1000000` | Comma-separated row counts to test at each scale                          |
| `BENCH_WARMUP_MS`          | `5000`                      | Warmup duration per task in milliseconds                                  |
| `BENCH_MEASURE_MS`         | `10000`                     | Measurement duration per task in milliseconds                             |
| `BENCH_SEED`               | `42`                        | PRNG seed for reproducible data generation                                |
| `BENCH_RUNS`               | `5`                         | Number of runs per comparison (statistical analysis activates at 5+ runs) |
| `BENCH_RUN_ORDER`          | `random`                    | Engine run order: `random`, `sirannon-first`, or `postgres-first`         |
| `BENCH_SHUFFLE`            | `true`                      | Randomize benchmark execution order in the full suite                     |

The default pool size of 10 connections follows standard Postgres sizing guidance, which recommends roughly 2x CPU cores plus disk spindles. Most production deployments use pools of 5-20 connections. Postgres performance degrades beyond this range because each connection is a separate OS process consuming memory and adding context-switch overhead. Tools like PgBouncer exist to multiplex thousands of application requests through a small connection pool.

The bulk-insert benchmark caps at 10K rows per iteration because each iteration inserts all rows one-by-one in a single transaction. At higher row counts, Postgres iterations take tens of seconds (one TCP round-trip per INSERT), producing too few samples for reliable measurement.

Example with custom settings:

```sh
BENCH_DATA_SIZES=1000,50000 BENCH_MEASURE_MS=20000 pnpm bench:micro
```

## Docker Benchmarks (Fair, Reproducible)

Docker benchmarks run both Sirannon and Postgres inside containers with controlled CPU and memory limits. This produces fair comparisons because both databases operate under identical resource constraints.

### Category 1: End-to-End (E2E) Application Benchmarks

Measures what a client experiences when an HTTP application uses Sirannon vs Postgres. Sirannon's architectural advantage (no network hop between app and DB) is part of the measurement.

**Architecture:**

- **Sirannon app**: Node.js + Sirannon in a single container (2 CPU, 2 GB)
- **Postgres app**: Node.js app container (1 CPU, 1 GB) + Postgres DB container (1 CPU, 1 GB)
- **Total budget per setup**: 2 CPU, 2 GB
- **Load generator**: k6 with `constant-arrival-rate` executor

k6 sends requests at a fixed rate regardless of response time, preventing coordinated omission. The result is a latency-throughput curve at each request rate.

**Workloads:**

- `point-select.js` - Single-row lookups by primary key
- `mixed-readwrite.js` - 80% read, 20% write (realistic web app)
- `transaction.js` - Multi-statement transactions

**Running:**

```sh
pnpm bench:docker:e2e
```

**Configuration:**

| Variable              | Default           | Description                          |
|-----------------------|-------------------|--------------------------------------|
| `BENCH_CPUS`          | `2`               | CPU limit for Sirannon container     |
| `BENCH_MEMORY`        | `2g`              | Memory limit for Sirannon container  |
| `BENCH_PG_APP_CPUS`   | `1`               | CPU limit for Postgres app container |
| `BENCH_PG_APP_MEMORY` | `1g`              | Memory limit for Postgres app        |
| `BENCH_PG_CPUS`       | `1`               | CPU limit for Postgres DB container  |
| `BENCH_PG_MEMORY`     | `1g`              | Memory limit for Postgres DB         |
| `BENCH_DATA_SIZE`     | `10000`           | Rows to seed                         |
| `BENCH_RPS_LEVELS`    | `1000,5000,10000` | Comma-separated target req/s         |
| `BENCH_DURATION`      | `60s`             | Duration per rate level              |

### Category 2: Engine-Level Benchmarks (fair server vs server)

Measures each database doing the same client-server job on one host. A networked load driver reaches Sirannon through the SirannonClient SDK over HTTP into the real server front-end, and reaches Postgres through the native pg driver, both over loopback. Neither side skips a network hop the other pays, which is the fairness rule a client-server comparison has to meet.

**Resource allocation:**

- **Sirannon engine**: SDK load driver and Sirannon server co-located in one container that holds the whole CPU budget (`BENCH_CPUS`, default 2 CPU, 2 GB).
- **Postgres engine**: pg load-driver container (`BENCH_ENGINE_DRIVER_CPUS`, default 1 CPU) plus Postgres DB container (`BENCH_ENGINE_DB_CPUS`, default 1 CPU).
- Both sides run on the same total CPU budget. Sirannon co-locates its driver and server in one container; Postgres splits the identical budget across its driver and database containers. The totals match, so neither side is given more compute than the other.

Sirannon's HTTP request path carries JSON framing on every call, which is heavier than Postgres's binary wire protocol. That is Sirannon's real client cost, disclosed rather than hidden, so the comparison errs toward charging Sirannon more rather than flattering it.

Both engines receive `BENCH_DURABILITY` via environment variable, so the `matched` vs `full` durability mode applies in Docker the same way it does locally.

**Workloads** (same as local benchmarks):

- Micro: point-select, bulk-insert, batch-update
- YCSB: workload A (50/50)
- OLTP: TPC-C lite

**Running:**

```sh
pnpm bench:docker:engine
```

**Running both categories:**

```sh
pnpm bench:docker
```

**Configuration:**

| Variable                   | Default                        | Description                                       |
|----------------------------|--------------------------------|---------------------------------------------------|
| `BENCH_DATA_SIZES`         | `1000,10000`                   | Comma-separated row counts                        |
| `BENCH_WARMUP_MS`          | `5000`                         | Warmup duration per task                          |
| `BENCH_MEASURE_MS`         | `10000`                        | Measurement duration per task                     |
| `BENCH_WORKLOADS`          | `point-select,bulk-insert,...` | Comma-separated workload names                    |
| `BENCH_DURABILITY`         | `matched`                      | Durability mode for both engines                  |
| `BENCH_CPUS`               | `2`                            | Total CPU budget per side                         |
| `BENCH_ENGINE_DRIVER_CPUS` | `1`                            | CPU for the Postgres load-driver container        |
| `BENCH_ENGINE_DB_CPUS`     | `1`                            | CPU for the Postgres database container           |

## Statistical Analysis

### Multi-run methodology

Set `BENCH_RUNS` to a value greater than 1 to enable multi-run analysis. Each run creates a fresh database, seeds it independently, and measures throughput. After all runs complete, the runner calculates:

- **Welch t-test** for statistical significance (unequal variance, N-1 degrees of freedom)
- **95% bootstrap confidence interval** on the speedup ratio (10,000 resamples)
- **IQR-based outlier detection** on per-run ops/sec

The `pnpm bench:statistical` convenience script runs the 4 most representative benchmarks (point-select, batch-update, workload-a, tpc-c-lite) with `BENCH_RUNS=10` at data sizes 1,000 and 10,000.

### Reading statistical columns

The console table and CSV files include these columns when multi-run data is available:

- **Sig**: Significance stars. `***` = p < 0.001, `**` = p < 0.01, `*` = p < 0.05, `n/s` = not significant, `-` = single run.
- **CI**: 95% confidence interval on the speedup ratio, e.g. `[4.2, 5.8]` means the true speedup is between 4.2x and 5.8x with 95% confidence.
- **Runs**: Number of independent runs used for the calculation.

### Speedup ratio as the primary metric

Absolute ops/sec varies by hardware, OS, thermal throttling, and background load. The **speedup ratio** (Sirannon ops/sec / Postgres ops/sec) cancels out most machine-specific factors and stays stable across different systems. When comparing results from different machines, focus on the ratio and CI, not raw throughput.

## Results

### Console output

Each run prints a comparison table to stdout with speedup and CI columns first:

```text
Workload | N Rows | Speedup | CI | Sig | Sirannon ops/s | Postgres ops/s | P50 | P99 | CV | [Runs]
```

- **Speedup** - Sirannon ops/s divided by Postgres ops/s
- **CI** - 95% bootstrap confidence interval on speedup (multi-run only)
- **Sig** - Statistical significance (multi-run only)
- **P50 / P99** - Sirannon median and 99th-percentile latency
- **CV** - Coefficient of variation; results marked `[!]` have CV > 10% and may be unreliable

### JSON and CSV files

Raw results are written to `benchmarks/results/` as timestamped JSON and CSV files. Each file contains system information, configuration, and per-workload results for both engines.

CSV files can be loaded directly into R, pandas, or any spreadsheet tool for custom analysis. When multi-run data is available, a separate `*-per-run-*.csv` file contains individual run data.

### Charts

Generate SVG charts from CSV results:

```sh
pnpm bench:charts
```

This reads all CSV files from `benchmarks/results/` and writes charts to `benchmarks/results/charts/`. Five chart types are generated:

- **Speedup bar chart** - Horizontal bars showing speedup ratio per workload, with CI error bars when available
- **Scaling line chart** - Ops/sec vs concurrency level for event-loop and worker-thread models
- **Latency comparison** - Grouped bars comparing P50 and P99 latencies between engines
- **Feature bar chart** - Ops/sec for Sirannon-only benchmarks (CDC, connection pool, cold start, multi-tenant)
- **Per-run box plot** - Distribution of ops/sec across runs, showing median, quartiles, and outliers for each workload

Requires Python 3 with matplotlib and pandas (`pip install matplotlib pandas`).

### Reference Results

The numbers below are generated from the latest committed Docker engine run and describe the exact machine that ran it. The Docker engine track is the fair, reproducible comparison: a networked client reaches each database through the database's own server over loopback, both co-located in resource-capped containers on one host, so the figures measure the engines doing the same job rather than one system's missing network hop. Regenerate this section with `pnpm bench:writeup` after committing a run directory under `results/runs/`.

#### Why this comparison is fair

<!-- BENCH:engine-methodology START -->
_No benchmark run is committed yet. Run the Docker engine suite on the disclosed cloud machine and commit its run directory under `results/runs/` to publish numbers here._
<!-- BENCH:engine-methodology END -->

#### Run and machine

<!-- BENCH:engine-setup START -->
_No benchmark run is committed yet. Run the Docker engine suite on the disclosed cloud machine and commit its run directory under `results/runs/` to publish numbers here._
<!-- BENCH:engine-setup END -->

#### Single-client comparison

Throughput and latency per workload at each measured row count, each database reached over its own client-server path on the same host. A speedup above one means Sirannon was faster than Postgres for that workload.

<!-- BENCH:engine-comparison START -->
_No benchmark run is committed yet. Run the Docker engine suite on the disclosed cloud machine and commit its run directory under `results/runs/` to publish numbers here._
<!-- BENCH:engine-comparison END -->

#### Concurrency scaling (server vs server)

Throughput as concurrent clients increase, with both databases reached over their own client-server path. This is the head-to-head result and includes the region where Postgres pulls ahead.

<!-- BENCH:engine-scaling START -->
_No benchmark run is committed yet. Run the Docker engine suite on the disclosed cloud machine and commit its run directory under `results/runs/` to publish numbers here._
<!-- BENCH:engine-scaling END -->

#### Embedded deployment mode (separate claim)

Sirannon's in-process, no-network-hop numbers. This is a property of the embedded deployment, not a head-to-head win over a server database.

<!-- BENCH:engine-embedded START -->
_No benchmark run is committed yet. Run the Docker engine suite on the disclosed cloud machine and commit its run directory under `results/runs/` to publish numbers here._
<!-- BENCH:engine-embedded END -->

### Interpreting results

- A **speedup > 1** means Sirannon was faster than Postgres for that workload.
- **CV > 10%** indicates high variance in per-engine latency samples. For Sirannon's sub-10-microsecond operations (point-select, YCSB reads), high CV is expected: most samples complete in 2us, but occasional GC pauses, OS scheduler interrupts, and CPU frequency transitions inject multi-millisecond spikes into the tail. These outliers inflate the standard deviation far beyond the mean, producing CV values of 200-600% even though the median latency is stable. The **speedup ratio and CI** are the reliable metrics because both engines experience the same OS-level disruptions, and the ratio cancels out shared noise. If CV is high on operations that take milliseconds or more, close background apps and re-run with a longer `BENCH_MEASURE_MS`.
- Compare JSON files across runs to track regressions. System info is captured so you can account for hardware differences.
- **Docker benchmarks on macOS** use Docker Desktop with a Linux VM, adding overhead to both sides equally. Linux gives more accurate absolute numbers due to native Docker execution. Relative speedups should be consistent across platforms.

## Concurrency and Scaling Trade-offs

The single-client comparison above reaches each database over its own client-server path on the same host, so it measures the engines, not a network hop. This section explains the architecture behind the results and how the picture shifts as concurrent clients increase.

### Why single-client performance is high

Both databases answer the query over loopback on the same host, so neither is paying for a network between machines. Behind Sirannon's server, SQLite executes the query in the server process against its B-tree engine and returns; behind Postgres's server, the query is parsed, planned, executed, and serialised across Postgres's connection model. For a trivial point-select that finishes in microseconds, SQLite's shorter execution path inside the server shows up as a throughput lead, and Sirannon's own HTTP and JSON framing is charged against that lead rather than hidden.

The speedup hierarchy in the comparison table follows from this: the less compute each query does, the more the per-request overhead each server carries dominates, and the larger the gap. Batch and multi-statement transactions amplify the difference; mixed read-update workloads with real per-query computation narrow it.

### What happens under concurrency

SQLite uses a single-writer model. WAL mode allows any number of concurrent readers, but only one connection writes at a time, and other writers queue behind it. This is a fundamental architectural choice, not a bug to work around.

The scaling table tests concurrency from 1 to 64 clients over the client-server path both sides share. Sirannon's server overlaps concurrent reads through its read pool while its single-writer lock serialises writes; Postgres overlaps both reads and writes across its connection pool. As the client count climbs, Postgres's row-level locking lets write-heavy mixed workloads pull ahead of SQLite's single writer, and the scaling table records that crossover rather than hiding it. Read-heavy workloads keep Sirannon ahead further up the range because WAL readers do not contend.

### Where Postgres pulls ahead

On write-heavy mixed workloads at higher client counts, Postgres overtakes Sirannon: every mutation Sirannon runs waits on the single-writer lock, so added clients only lengthen the write queue, while Postgres parallelises writes across connections through row-level locking. The scaling table shows the concurrency level where this crossover occurs for the recorded run. Read-only workloads do not cross over within the tested range because WAL mode serves concurrent readers without contention.

### Practical guidance

Read the scaling table for the crossover point on your workload mix: Sirannon leads on read-heavy and light mixed workloads across the tested range, and Postgres leads on write-heavy mixed workloads once the client count passes the recorded crossover.

**Web applications at standard scale** with a connection pool of 10-50 concurrent database queries usually fall on the read-heavy and light-mixed side where Sirannon leads. Sirannon also removes the operational overhead of running a separate database server when deployed embedded, because connection pooling and configuration happen in-process rather than through external infrastructure.

**Multi-tenant architectures** are a natural fit. Each tenant gets its own SQLite database, so write serialisation applies per tenant rather than across the whole system. A tenant with 10 concurrent users gets the full single-client performance profile, and tenants do not contend with each other.

**Read-heavy workloads at scale** benefit from SQLite's WAL mode, which allows concurrent readers, so read throughput scales until hardware bandwidth limits it. For horizontal read scaling beyond a single machine, WAL replication tools like Litestream can stream changes to read replicas via S3 for durability, or through faster transports like Redis Streams for sub-second replication lag.

**Where Postgres is the better fit:**

- **Extremely write-heavy workloads with high concurrent writer counts on shared rows.** SQLite serialises writes through a single-writer lock, so Sirannon's write throughput stays flat as concurrency increases while Postgres scales with its row-level locking. For applications where hundreds of clients mutate the same tables simultaneously (high-frequency trading systems, real-time bidding platforms, shared counters with hundreds of concurrent writers), Postgres's ability to parallelise writes across connections is a genuine advantage. For most applications, SQLite's single writer processes mutations fast enough that the queue never becomes a bottleneck.
- **Data exceeding a single machine's disk.** SQLite operates on a single file on a single machine. If your dataset outgrows local storage, Postgres handles that natively with partitioning, tablespaces, and distributed extensions.

### What these benchmarks do and don't measure

These benchmarks measure client-server throughput at concurrency levels that match real production deployments (1 to 64 concurrent clients), with both databases reached over their own server on the same host. Sirannon leads on light and read-heavy workloads with the margin widest on batch operations; Postgres pulls ahead on write-heavy mixed workloads once the client count passes the crossover the scaling table records.

The benchmarks do not measure replication, failover, or distributed transactions. SQLite replication is available through external tools (Litestream for WAL streaming, cr-sqlite for CRDT-based multi-writer), while Postgres includes these capabilities as built-in features. The trade-off is between Sirannon's deployment simplicity and Postgres's integrated feature set; both approaches achieve the same end result through different means.

## Tips

- Close resource-heavy applications before running benchmarks.
- Run the full suite at least twice; use the second run's numbers since the first run warms OS and Docker caches.
- For shorter feedback loops during development, reduce data sizes and measurement time:

  ```sh
  BENCH_DATA_SIZES=1000 BENCH_WARMUP_MS=1000 BENCH_MEASURE_MS=3000 pnpm bench:micro
  ```

- The `--expose-gc` flag is already included in the pnpm scripts, so manual garbage collection calls in benchmarks work correctly.
- Docker benchmarks build images on first run. Subsequent runs reuse cached layers unless source files change.

## File Structure

```txt
benchmarks/
  config.ts, runner.ts, reporter.ts     # Shared infrastructure
  schemas.ts, engine.ts                 # Schema definitions, engine interface
  sirannon-engine.ts, postgres-engine.ts # Engine implementations
  run-all.ts                            # Local benchmark orchestrator
  run-statistical.ts                    # Multi-run statistical benchmark script
  micro/                                # Local micro benchmarks
  ycsb/                                 # Local YCSB benchmarks
  oltp/                                 # Local OLTP benchmarks
  sirannon/                             # Sirannon-only benchmarks (CDC, pool, etc.)
  scaling/                              # Concurrency and pool scaling benchmarks
  scripts/
    generate-charts.py                  # Chart generation from CSV results
  docker/                               # Docker-based fair benchmarks
    docker-compose.yml                  # Orchestrates all containers
    Dockerfile.sirannon-app             # Category 1: Sirannon HTTP server
    Dockerfile.postgres-app             # Category 1: Postgres-backed HTTP server
    Dockerfile.engine                   # Category 2: Engine benchmark runner

  e2e/                                  # Category 1 support files
    sirannon-app.ts                     # Sirannon HTTP server entrypoint
    postgres-app.ts                     # Postgres HTTP server entrypoint
    postgres-server.ts                  # Postgres HTTP server implementation
    seed.ts                             # Shared seeding utilities

  k6/                                   # Category 1 load scripts
    point-select.js                     # Single-row lookups
    mixed-readwrite.js                  # 80/20 read/write mix
    transaction.js                      # Multi-statement transactions
    helpers/zipfian.js                  # Zipfian distribution for k6

  engine/                               # Category 2 support files
    control-server.ts                   # Control API (setup, benchmark, cleanup)
    workloads.ts                        # Workload definitions

  run-e2e.ts                            # Orchestrate Category 1
  run-engine.ts                         # Orchestrate Category 2
  run-docker.ts                         # Run both categories
  results/                              # Output directory for JSON/CSV results and charts
```
