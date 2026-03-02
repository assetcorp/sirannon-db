# Benchmarks

Sirannon benchmarks compare embedded SQLite performance against Postgres 17 across micro-operations, industry-standard workloads (YCSB, TPC-C), and Sirannon-specific features like CDC and connection pooling.

Two benchmark categories exist: **local benchmarks** for quick development feedback, and **Docker-based benchmarks** for fair, reproducible comparisons.

## Prerequisites

- Node.js 22+ (with `--expose-gc` support)
- pnpm
- Docker and Docker Compose
- [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) (for end-to-end Docker benchmarks)

## Local Benchmarks

Local benchmarks use [tinybench](https://github.com/tinylibs/tinybench) with `--expose-gc` enabled for accurate memory management between runs. Sirannon runs natively on the host; Postgres runs in a Docker container.

These benchmarks are useful for quick iteration during development, but the numbers reflect host-vs-container differences and vary across machines.

### Starting Postgres

Most local benchmarks run Sirannon against a Postgres baseline. Start the Postgres container first:

```sh
docker compose -f benchmarks/docker-compose.yml up -d --wait
```

This launches Postgres 17 Alpine on port `5433` with tuned settings (256 MB shared buffers, `synchronous_commit=off` to match SQLite's `NORMAL` durability, SSD-optimized planner costs).

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
pnpm bench:micro    # point-select, range-select, bulk-insert, batch-update
pnpm bench:ycsb     # YCSB workload-b (read-heavy)
pnpm bench:oltp     # TPC-C lite
pnpm bench:server   # HTTP throughput
pnpm bench:cdc      # CDC latency
```

### Local Benchmark Categories

**Micro** (requires Postgres) - Single-operation latency and throughput:

- `point-select` - Primary-key lookups
- `range-select` - Range scans with LIMIT
- `bulk-insert` - Batch row inserts
- `batch-update` - Batch row updates

**YCSB** (requires Postgres) - Yahoo! Cloud Serving Benchmark workloads:

- `workload-a` - 50% read, 50% update
- `workload-b` - 95% read, 5% update
- `workload-c` - 100% read

**OLTP** (requires Postgres) - Online transaction processing:

- `tpc-c-lite` - Simplified TPC-C with new-order and payment transactions

**Sirannon** (no Postgres needed) - Sirannon-specific features:

- `cdc-latency` - Change Data Capture event propagation latency
- `connection-pool` - Connection pool throughput and contention
- `cold-start` - Database open and first-query latency
- `multi-tenant` - Multi-database isolation throughput

**Server** (no Postgres needed):

- `http-throughput` - HTTP request handling throughput

### Configuration

Override defaults with environment variables:

| Variable                   | Default             | Description                                          |
|----------------------------|---------------------|------------------------------------------------------|
| `BENCH_PG_HOST`            | `127.0.0.1`         | Postgres host                                        |
| `BENCH_PG_PORT`            | `5433`              | Postgres port                                        |
| `BENCH_PG_USER`            | `benchmark`         | Postgres user                                        |
| `BENCH_PG_PASSWORD`        | `benchmark`         | Postgres password                                    |
| `BENCH_PG_DATABASE`        | `benchmark`         | Postgres database                                    |
| `BENCH_PG_MAX_CONNECTIONS` | `10`                | Postgres connection pool size                        |
| `BENCH_DURABILITY`         | `matched`           | `matched` (SQLite NORMAL) or `full` (SQLite + fsync) |
| `BENCH_DATA_SIZES`         | `1000,10000,100000` | Comma-separated row counts to test at each scale     |
| `BENCH_WARMUP_MS`          | `5000`              | Warmup duration per task in milliseconds             |
| `BENCH_MEASURE_MS`         | `10000`             | Measurement duration per task in milliseconds        |

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

| Variable             | Default | Description                          |
|----------------------|---------|--------------------------------------|
| `BENCH_CPUS`         | `2`     | CPU limit for Sirannon container     |
| `BENCH_MEMORY`       | `2g`    | Memory limit for Sirannon container  |
| `BENCH_PG_APP_CPUS`  | `1`     | CPU limit for Postgres app container |
| `BENCH_PG_APP_MEMORY`| `1g`    | Memory limit for Postgres app        |
| `BENCH_PG_CPUS`      | `1`     | CPU limit for Postgres DB container  |
| `BENCH_PG_MEMORY`    | `1g`    | Memory limit for Postgres DB         |
| `BENCH_DATA_SIZE`    | `10000` | Rows to seed                         |
| `BENCH_RPS_LEVELS`   | `1000,5000,10000` | Comma-separated target req/s  |
| `BENCH_DURATION`     | `60s`   | Duration per rate level              |

### Category 2: Engine-Level Benchmarks

Measures raw query execution performance under identical constraints. Both database engines get the same CPU and memory. No network latency in the measurement; each container runs tinybench internally.

**Architecture:**

- **Sirannon engine**: Sirannon + tinybench in a container (2 CPU, 2 GB)
- **Postgres engine**: pg.Pool + tinybench in a thin client container (0.5 CPU, 2 GB) connected to a Postgres DB container (2 CPU, 2 GB)
- Both database engines get 2 CPU, 2 GB

**Workloads** (same as local benchmarks):

- Micro: point-select, range-select, bulk-insert, batch-update
- YCSB: workload A (50/50), B (95/5), C (100% read)
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
| `BENCH_WORKLOADS`  | `point-select,range-select,...`  | Comma-separated workload names       |

## Results

### Console output

Each run prints a comparison table to stdout:

```text
Workload                  | N Rows | Sirannon ops/s | Postgres ops/s |  Speedup |        S P50 |        S P99 |    S CV
```

- **Speedup** - Sirannon ops/s divided by Postgres ops/s
- **P50 / P99** - Sirannon median and 99th-percentile latency
- **CV** - Coefficient of variation; results marked `[!]` have CV > 10% and may be unreliable

### JSON files

Raw results are written to `benchmarks/results/` as timestamped JSON files. Each file contains system information, configuration, and per-workload results for both engines.

### Interpreting results

- A **speedup > 1** means Sirannon was faster than Postgres for that workload.
- **CV > 10%** indicates high variance. Close background apps and re-run with a longer `BENCH_MEASURE_MS` for more stable numbers.
- Compare JSON files across runs to track regressions. System info is captured so you can account for hardware differences.
- **Docker benchmarks on macOS** use Docker Desktop with a Linux VM, adding overhead to both sides equally. Linux gives more accurate absolute numbers due to native Docker execution. Relative speedups should be consistent across platforms.

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

```
benchmarks/
  config.ts, runner.ts, reporter.ts     # Shared infrastructure
  schemas.ts, engine.ts                 # Schema definitions, engine interface
  sirannon-engine.ts, postgres-engine.ts # Engine implementations
  run-all.ts                            # Local benchmark orchestrator
  micro/                                # Local micro benchmarks
  ycsb/                                 # Local YCSB benchmarks
  oltp/                                 # Local OLTP benchmarks
  sirannon/                             # Sirannon-only benchmarks (CDC, pool, etc.)
  server/                               # HTTP throughput benchmark

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
  results/                              # Output directory for JSON results
```
