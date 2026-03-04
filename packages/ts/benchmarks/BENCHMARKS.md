# Benchmarks

Sirannon benchmarks compare embedded SQLite performance against Postgres 17 across micro-operations, industry-standard workloads (YCSB, TPC-C), concurrency scaling, and Sirannon-specific features like CDC and connection pooling.

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

This launches Postgres 17 Alpine on port `5433` with tuned settings (256 MB shared buffers, `synchronous_commit=off` for reduced write latency, SSD-optimized planner costs). The default `matched` durability mode uses `synchronous_commit=off` for Postgres and `PRAGMA synchronous=NORMAL` for SQLite. Both approaches survive process crashes but use different WAL flush strategies; they are similar but not identical trade-offs.

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
pnpm bench:cdc         # CDC latency
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

- `tpc-c-lite` - Simplified TPC-C with new-order and payment transactions

**Scaling** (requires Postgres) - Concurrency scaling from 1 to 64 clients:

- `scaling/concurrency` - Tests two deployment models: single event-loop (Sirannon serializes, Postgres overlaps via async pool) and worker thread pool (N threads with their own connections). Two workloads: read-only (WAL concurrent readers) and 50/50 mixed (single-writer contention).

**Sirannon** (no Postgres needed) - Sirannon-specific features:

- `cdc-latency` - Change Data Capture event propagation latency
- `connection-pool` - Connection pool throughput and contention
- `cold-start` - Database open and first-query latency
- `multi-tenant` - Multi-database isolation throughput

### Configuration

Override defaults with environment variables:

| Variable                   | Default             | Description                                                           |
|----------------------------|---------------------|-----------------------------------------------------------------------|
| `BENCH_PG_HOST`            | `127.0.0.1`         | Postgres host                                                         |
| `BENCH_PG_PORT`            | `5433`              | Postgres port                                                         |
| `BENCH_PG_USER`            | `benchmark`         | Postgres user                                                         |
| `BENCH_PG_PASSWORD`        | `benchmark`         | Postgres password                                                     |
| `BENCH_PG_DATABASE`        | `benchmark`         | Postgres database                                                     |
| `BENCH_PG_MAX_CONNECTIONS` | `10`                | Postgres connection pool size                                         |
| `BENCH_DURABILITY`         | `matched`           | `matched` (SQLite NORMAL) or `full` (SQLite + fsync)                  |
| `BENCH_DATA_SIZES`         | `1000,10000,100000` | Comma-separated row counts to test at each scale                      |
| `BENCH_WARMUP_MS`          | `5000`              | Warmup duration per task in milliseconds                              |
| `BENCH_MEASURE_MS`         | `10000`             | Measurement duration per task in milliseconds                         |
| `BENCH_SEED`               | `42`                | PRNG seed for reproducible data generation                            |
| `BENCH_RUNS`               | `1`                 | Number of runs per comparison (enables significance testing when > 1) |
| `BENCH_RUN_ORDER`          | `random`            | Engine run order: `random`, `sirannon-first`, or `postgres-first`     |
| `BENCH_SHUFFLE`            | `true`              | Randomize benchmark execution order in the full suite                 |

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

### Category 2: Engine-Level Benchmarks

Measures raw query execution performance under identical constraints. No network latency in the measurement; each container runs tinybench internally.

**Resource allocation:**

- **Sirannon engine**: Sirannon + tinybench in a single container (2 CPU, 2 GB)
- **Postgres engine**: pg.Pool + tinybench in a client container (2 CPU, 2 GB) connected to a Postgres DB container (2 CPU, 2 GB)
- Sirannon uses 2 CPUs total. Postgres uses 4 CPUs total (2 client + 2 database). Sirannon wins with half the total CPU resources.

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

| Variable           | Default                          | Description                          |
|--------------------|----------------------------------|--------------------------------------|
| `BENCH_DATA_SIZES` | `1000,10000`                     | Comma-separated row counts           |
| `BENCH_WARMUP_MS`  | `5000`                           | Warmup duration per task             |
| `BENCH_MEASURE_MS` | `10000`                          | Measurement duration per task        |
| `BENCH_WORKLOADS`  | `point-select,bulk-insert,...`   | Comma-separated workload names       |
| `BENCH_DURABILITY` | `matched`                        | Durability mode for both engines     |

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

This reads all CSV files from `benchmarks/results/` and writes charts to `benchmarks/results/charts/`. Three chart types are generated:

- **Speedup bar chart** - Horizontal bars showing speedup ratio per workload, with CI error bars when available
- **Scaling line chart** - Ops/sec vs concurrency level for event-loop and worker-thread models
- **Latency comparison** - Grouped bars comparing P50 and P99 latencies between engines

Requires Python 3 with matplotlib and pandas (`pip install matplotlib pandas`).

### Interpreting results

- A **speedup > 1** means Sirannon was faster than Postgres for that workload.
- **CV > 10%** indicates high variance. Close background apps and re-run with a longer `BENCH_MEASURE_MS` for more stable numbers.
- Compare JSON files across runs to track regressions. System info is captured so you can account for hardware differences.
- **Docker benchmarks on macOS** use Docker Desktop with a Linux VM, adding overhead to both sides equally. Linux gives more accurate absolute numbers due to native Docker execution. Relative speedups should be consistent across platforms.

## Concurrency and Scaling Trade-offs

The single-client benchmarks show Sirannon outperforming Postgres by 22x to 244x depending on the workload. These numbers are accurate, but they tell only half the story. The concurrency scaling benchmark (`pnpm bench:scaling`) reveals how these advantages shift as concurrent clients increase.

### Why single-client performance is so high

Sirannon runs SQLite in-process. A query travels from your application code into SQLite's B-tree engine and back without leaving the process boundary. Postgres receives the same query over a TCP socket, parses it, plans it, executes it, serializes the result, and sends it back over the network. For a trivial point-select that takes microseconds to execute, the network round-trip dominates Postgres's total response time. Sirannon skips that round-trip entirely.

This explains the speedup hierarchy across workloads:

| Workload | Speedup | Why |
| --- | --- | --- |
| batch-update (1K) | 244x | Multi-statement transactions multiply the network cost per transaction |
| point-select (1K) | 91x | Trivial query where network overhead is nearly all of Postgres's latency |
| tpc-c-lite (1K) | 44x | Multi-step transactions, but heavier per-step computation |
| ycsb-a (1K) | 23x | Mixed read/update with Zipfian distribution adds real computation |

The pattern is clear: the less work each query does, the more network overhead dominates, and the larger Sirannon's advantage.

### What happens under concurrency

SQLite uses a single-writer model. WAL mode allows any number of concurrent readers, but only one connection can write at a time. All other writers queue behind it. This is a fundamental architectural choice, not a bug or limitation to work around.

The scaling benchmark tests concurrency from 1 to 64 clients and reveals the consequences:

**Event-loop model (single Node.js thread, async Postgres pool):**

| Concurrency | Sirannon ops/s | Postgres ops/s | Speedup | Workload |
| --- | --- | --- | --- | --- |
| 1 | 422,067 | 6,413 | 65.8x | read-only |
| 16 | 411,990 | 25,083 | 16.4x | read-only |
| 64 | 411,674 | 30,694 | 13.4x | read-only |
| 1 | 138,375 | 6,281 | 22.0x | mixed 50/50 |
| 16 | 138,029 | 25,008 | 5.5x | mixed 50/50 |
| 64 | 144,627 | 28,858 | 5.0x | mixed 50/50 |

Sirannon's throughput stays flat regardless of how many concurrent clients are running. It can't go faster because the single event loop serializes all operations anyway, and SQLite's single-writer lock prevents write parallelism. Postgres, on the other hand, scales from 6,400 to 30,700 ops/sec on reads as its connection pool overlaps queries across the async event loop. Each additional Postgres connection does real parallel work on the server side.

The speedup ratio compresses from 66x down to 13x for reads, and from 22x down to 5x for mixed workloads. Sirannon still wins at every concurrency level tested, but the margin narrows as Postgres gains ground.

**Worker-thread model (N threads, each with its own connection):**

| Concurrency | Sirannon ops/s | Postgres ops/s | Speedup | Workload |
| --- | --- | --- | --- | --- |
| 1 | 382,617 | 6,313 | 60.6x | read-only |
| 4 | 926,611 | 17,234 | 53.8x | read-only |
| 8 | 927,309 | 22,104 | 42.0x | read-only |
| 1 | 111,677 | 6,187 | 18.1x | mixed 50/50 |
| 4 | 123,366 | 16,828 | 7.3x | mixed 50/50 |
| 16 | 118,935 | 24,606 | 4.8x | mixed 50/50 |

Worker threads unlock SQLite's read parallelism. WAL mode allows multiple threads to read simultaneously, and Sirannon's throughput jumps from 383K to 927K ops/sec at 4 threads for read-only workloads. Beyond 4 threads, the gains plateau because the hardware's memory bandwidth and cache contention become the bottleneck rather than SQLite's locking.

Mixed workloads tell a different story. Even with worker threads, write throughput stays flat at around 112-123K ops/sec because SQLite's write lock serializes all mutations regardless of how many threads are waiting. The extra threads don't help with writes; they only add contention overhead.

### The crossover point

Postgres never catches Sirannon in absolute throughput within the 64-client range tested here. Even at 64 concurrent mixed clients, Sirannon delivers 5x the throughput. But the trend is clear: Postgres throughput grows linearly with concurrency while Sirannon's stays constant.

Extrapolating the event-loop mixed workload curve, Postgres would need roughly 200-300 concurrent connections doing real parallel work to match Sirannon's single-threaded throughput. Whether your application reaches that concurrency level depends on your deployment model.

### Practical guidance

The benchmarks show Sirannon outperforming Postgres across every workload and concurrency level tested, from 91x on single-client point-selects down to 5x at 64 concurrent mixed-workload clients. These results cover a wide range of application profiles.

**Single-client and low-concurrency applications** (CLI tools, desktop apps, mobile apps, edge functions, typical web backends with 5-50 concurrent users) see the largest gains. Sirannon eliminates the network round-trip between application and database, and the benchmarks confirm this translates to 22x-244x speedups depending on workload. Deployment is simpler too: no connection strings, no connection pooling, no database server to manage.

**Multi-tenant architectures** are a natural fit. Each tenant gets its own SQLite database, so write serialization applies per tenant rather than across the whole system. A tenant with 10 concurrent users gets the full single-client performance profile, and tenants don't contend with each other.

**Read-heavy workloads at scale** benefit from SQLite's WAL mode, which allows concurrent readers. The worker-thread benchmarks show Sirannon scaling from 383K to 927K read ops/sec at 4 threads. For horizontal read scaling beyond a single machine, WAL replication tools like Litestream can stream changes to read replicas via S3 for durability, or through faster transports like Redis Streams for sub-second replication lag.

**Where Postgres is the better fit:**

- **Hundreds of concurrent writers contending on the same rows.** SQLite serializes writes through a single-writer lock. The scaling benchmarks show Sirannon's write throughput stays flat as concurrency increases, while Postgres scales linearly. At 64 concurrent mixed clients, Sirannon still leads 5x, but the gap closes. Applications with hundreds of writers hitting the same tables (high-frequency event ingestion, shared inventory with concurrent checkout processes) will eventually hit the single-writer ceiling.
- **Data exceeding a single machine's disk.** SQLite operates on a single file on a single machine. If your dataset outgrows local storage, Postgres with its partitioning, tablespaces, and distributed extensions handles that natively.

### What these benchmarks do and don't measure

These benchmarks measure raw query throughput on a single machine. They confirm that Sirannon is faster than Postgres across every tested scenario, with the advantage ranging from 5x under heavy concurrency to 244x for batch operations.

The benchmarks do not measure replication, failover, or distributed transactions. SQLite replication is available through external tools (Litestream, cr-sqlite), while Postgres includes these capabilities as built-in features. The operational trade-off is between Sirannon's simplicity (fewer moving parts, no database server) and Postgres's integrated feature set.

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
  scaling/                              # Concurrency scaling benchmarks
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
