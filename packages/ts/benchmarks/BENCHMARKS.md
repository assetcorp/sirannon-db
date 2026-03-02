# Benchmarks

Sirannon benchmarks compare embedded SQLite performance against Postgres 17 across micro-operations, industry-standard workloads (YCSB, TPC-C), and Sirannon-specific features like CDC and connection pooling.

All benchmarks use [tinybench](https://github.com/tinylibs/tinybench) with `--expose-gc` enabled for accurate memory management between runs.

## Prerequisites

- Node.js (with `--expose-gc` support)
- pnpm
- Docker and Docker Compose (for Postgres-dependent benchmarks)

## Starting Postgres

Most benchmarks run Sirannon against a Postgres baseline. Start the Postgres container before running them:

```sh
docker compose -f benchmarks/docker-compose.yml up -d --wait
```

This launches Postgres 17 Alpine on port `5433` with tuned settings for benchmarking (256 MB shared buffers, `synchronous_commit=off` to match SQLite's `NORMAL` durability, SSD-optimized planner costs). The default credentials are:

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

## Running Benchmarks

All commands run from `packages/ts/`.

### Full suite

```sh
pnpm bench
```

Runs every benchmark sequentially. Benchmarks that need Postgres are skipped if the container isn't running.

### Individual suites

```sh
pnpm bench:micro    # point-select, range-select, bulk-insert, batch-update
pnpm bench:ycsb     # YCSB workload-b (read-heavy)
pnpm bench:oltp     # TPC-C lite
pnpm bench:server   # HTTP throughput
pnpm bench:cdc      # CDC latency
```

### Benchmark categories

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

## Configuration

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

## Results

### Console output

Each run prints a comparison table to stdout:

```text
Workload                       | N Rows | Sirannon ops/s | Postgres ops/s |  Speedup |           P50 |           P99 |        CV
```

- **Speedup** - Sirannon ops/s divided by Postgres ops/s
- **P50 / P99** - Sirannon median and 99th-percentile latency
- **CV** - Coefficient of variation; results marked `[!]` have CV > 10% and may be unreliable

### JSON files

Raw results are written to `benchmarks/results/` as timestamped JSON files (e.g., `micro-2026-03-02T12-00-00-000Z.json`). Each file contains:

```json
{
  "category": "micro",
  "timestamp": "2026-03-02T12:00:00.000Z",
  "system": {
    "os": "Darwin 25.0.0",
    "cpu": "Apple M3 Pro",
    "cpuCores": 12,
    "ramGb": 36,
    "nodeVersion": "v22.0.0",
    "v8Version": "12.4.254.21",
    "sqliteVersion": "3.45.0",
    "postgresVersion": "16.2",
    "durability": "matched"
  },
  "results": [
    {
      "workload": "point-select",
      "dataSize": 1000,
      "framing": "...",
      "speedup": 4.21,
      "sirannon": {
        "opsPerSec": 142000,
        "meanNs": 7042,
        "p50Ns": 6800,
        "p99Ns": 12400,
        "samples": 1420000
      },
      "postgres": { "..." : "..." }
    }
  ]
}
```

### Interpreting results

- A **speedup > 1** means Sirannon was faster than Postgres for that workload.
- **CV > 10%** indicates high variance. Close background apps and re-run with a longer `BENCH_MEASURE_MS` for more stable numbers.
- Compare JSON files across runs to track regressions. System info is captured in each file so you can account for hardware differences.

## Tips

- Close resource-heavy applications before running benchmarks.
- Run the full suite at least twice; use the second run's numbers since the first run warms OS and Docker caches.
- For shorter feedback loops during development, reduce data sizes and measurement time:

  ```sh
  BENCH_DATA_SIZES=1000 BENCH_WARMUP_MS=1000 BENCH_MEASURE_MS=3000 pnpm bench:micro
  ```

- The `--expose-gc` flag is already included in the pnpm scripts, so manual garbage collection calls in benchmarks work correctly.
