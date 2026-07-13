# Sirannon benchmarks

Sirannon is a Postgres alternative built on SQLite, so this page reports a head-to-head comparison against PostgreSQL on the same OLTP workloads. Both engines run on one host in resource-capped containers, each driven through the client it ships, at matched durability, under an open-loop load generator that records the full tail latency.

The numbers on this page come from the latest committed run under `benchmarks/server/results/runs/`. The prose is written by hand; every table and the machine description come from that run, so the words and the numbers match. When no run is committed, the page shows a placeholder instead of numbers.

## Methodology

<!-- BENCH:methodology START -->
_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit its run directory under `benchmarks/server/results/runs/` to publish numbers here._
<!-- BENCH:methodology END -->

## The run

<!-- BENCH:setup START -->
_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit its run directory under `benchmarks/server/results/runs/` to publish numbers here._
<!-- BENCH:setup END -->

## Head-to-head: Sirannon versus PostgreSQL

The table pairs the two engines on every shared workload at each engine's operating point, the highest offered request rate it sustained while holding p99 latency under the disclosed target. Each speedup includes its own confidence interval.

<!-- BENCH:comparison START -->
_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit its run directory under `benchmarks/server/results/runs/` to publish numbers here._
<!-- BENCH:comparison END -->

## Throughput versus offered load

The curve below shows achieved throughput and p99 latency as the offered rate climbs, so you can see where each engine's tail latency breaks down.

<!-- BENCH:scaling START -->
_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit its run directory under `benchmarks/server/results/runs/` to publish numbers here._
<!-- BENCH:scaling END -->

## Sirannon-only characterizations

These measure Sirannon on its own terms, because PostgreSQL either has no built-in equivalent or reaches the same goal a different way.

<!-- BENCH:features START -->
_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit its run directory under `benchmarks/server/results/runs/` to publish numbers here._
<!-- BENCH:features END -->

## Reproducing this

The harness is in `benchmarks/server/`, and its README explains the method, the durability matching, the coordinated-omission correction, and how to run the suite in Docker or against a hand-started server. To publish credible numbers, run it on the disclosed cloud machine through `benchmarks/cloud/`.
