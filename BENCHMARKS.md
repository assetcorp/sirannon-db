# Sirannon benchmarks

Sirannon is a database server, and PostgreSQL is the server most applications would otherwise use, so this page compares the two on the same standard OLTP workloads. Both engines run on one host as native processes pinned to dedicated cores under a hard memory ceiling, each driven through the client it provides, at matched durability, under an open-loop load generator that records the full tail latency.

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

## Holding the operating point

An operating point found in short windows is only proven when the engine holds it through its periodic housekeeping, so this section holds each engine at that rate for one long continuous window and shows the slowest slice of it.

<!-- BENCH:soak START -->
_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit its run directory under `benchmarks/server/results/runs/` to publish numbers here._
<!-- BENCH:soak END -->

## Sirannon-only characterizations

These measure Sirannon on its own terms, because PostgreSQL either has no built-in equivalent or reaches the same goal a different way.

<!-- BENCH:features START -->
_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit its run directory under `benchmarks/server/results/runs/` to publish numbers here._
<!-- BENCH:features END -->

## Reproducing this

The harness is in `benchmarks/server/`, and its README explains the method, the durability matching, the coordinated-omission correction, and how to run the suite on the benchmark VM or against a hand-started server. To publish credible numbers, run it on the disclosed cloud machine through `benchmarks/cloud/`.
