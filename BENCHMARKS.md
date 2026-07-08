# Sirannon benchmarks

Sirannon is a Postgres alternative built on SQLite, so the benchmark that matters is a fair,
head-to-head comparison against PostgreSQL on the same OLTP workloads. This page reports that
comparison. Both engines run on one host in resource-capped containers, each driven through the
client it actually ships, at matched durability, under an open-loop load generator that reports
the tail latency honestly.

The numbers on this page are generated from the latest committed run under
`benchmarks/server/results/runs/`. The prose is written by hand; every table and the machine
description come from the recorded run, so the two never disagree. When no run is committed, the
tables say so rather than show invented numbers.

## How to read these numbers

<!-- BENCH:methodology START -->
_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit its run directory under `benchmarks/server/results/runs/` to publish numbers here._
<!-- BENCH:methodology END -->

## The run

<!-- BENCH:setup START -->
_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit its run directory under `benchmarks/server/results/runs/` to publish numbers here._
<!-- BENCH:setup END -->

## Head-to-head: Sirannon versus PostgreSQL

The table pairs the two engines on every shared workload at each engine's operating point, the
highest offered request rate it sustained while holding p99 latency under the disclosed target.
The speedup carries its own confidence interval, so the head-to-head claim comes with its
uncertainty attached.

<!-- BENCH:comparison START -->
_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit its run directory under `benchmarks/server/results/runs/` to publish numbers here._
<!-- BENCH:comparison END -->

## Throughput versus offered load

Peak throughput on its own hides where an engine's tail latency breaks down. The curve below
shows achieved throughput and p99 latency as the offered rate climbs, so you can see the knee for
each engine rather than a single peak number.

<!-- BENCH:scaling START -->
_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit its run directory under `benchmarks/server/results/runs/` to publish numbers here._
<!-- BENCH:scaling END -->

## Sirannon-only characterizations

These describe Sirannon on its own terms, because PostgreSQL either has no built-in equivalent or
reaches the same goal through a different mechanism. Read them as properties of Sirannon, not as a
win over PostgreSQL.

<!-- BENCH:features START -->
_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit its run directory under `benchmarks/server/results/runs/` to publish numbers here._
<!-- BENCH:features END -->

## Reproducing this

The harness is a Python project under `benchmarks/server/`. Its README explains the fair-comparison
method, the durability matching, the coordinated-omission correction, and how to run the suite,
both in Docker and against a hand-started server. To publish credible numbers, run it on the
disclosed cloud machine through `benchmarks/cloud/`, not on a laptop, because laptop clocks
throttle under sustained load.
