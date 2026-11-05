from __future__ import annotations

from run_shape import over_network, postgres_major
from sources import Source

_PURPOSE = (
    "For each workload, you can read the highest rate at which each engine held a latency target, the tail "
    "latency at that rate, and the rate at which the engine stopped keeping up."
)

_ONE_HOST = (
    "Both engines operate at matched durability as native processes on one host, each pinned to its own logical "
    "CPUs under a hard memory ceiling. One open-loop load generator, which records the full tail latency, drives "
    "each engine through the client that the engine provides."
)

_TWO_MACHINES = (
    "Both engines operate at matched durability as native processes on one engine machine, one after the other, "
    "and each engine has every logical CPU of that machine under a hard memory ceiling. One open-loop load "
    "generator on a second machine, which records the full tail latency, drives each engine over the private "
    "network between the two machines, through the client that the engine provides."
)


def intro_block(source: Source | None) -> str:
    major = postgres_major(source.comparison) if source is not None else None
    postgres = f"PostgreSQL {major}" if major else "PostgreSQL"
    opening = f"The benchmark harness measures Sirannon and {postgres} on the same OLTP workloads. {_PURPOSE}"
    placement = _TWO_MACHINES if source is not None and over_network(source.comparison) else _ONE_HOST
    return f"{opening}\n\n{placement}"
