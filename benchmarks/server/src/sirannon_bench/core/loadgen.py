"""Open-loop load generation with coordinated-omission correction.

A closed-loop generator sends the next request only after the previous one returns, so when the
server stalls the generator stops sending and the slow requests never get measured. That
undercount can hide the tail by orders of magnitude, which is the single failure a database
reviewer attacks first. This generator instead fires requests at a fixed target rate regardless
of whether earlier requests have returned, and it records each request's latency from the time
it was *meant* to be sent, not the time a backlog let it start. A request delayed by a stall
therefore carries the full delay, which is the wrk2 correction for coordinated omission.

Concurrency is bounded by a semaphore so a stalled server cannot spawn unbounded work. When the
semaphore blocks, the intended-time measurement still charges the wait to the delayed requests,
so the correction holds while memory stays bounded. The result records whether the target rate
was sustained and, when it was not, whether the bound was the server (the in-flight cap was hit)
or the driver itself (the cap was never reached), so a reader can tell a real result from a
generator that ran out of headroom.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .stats import mean, percentile


@dataclass(frozen=True)
class LoadResult:
    target_rate: float
    achieved_rate: float
    completed: int
    errors: int
    error_rate: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    p999_ms: float
    max_ms: float
    mean_ms: float
    max_in_flight_observed: int
    sustained: bool
    server_saturated: bool
    client_bound: bool


_SUSTAINED_FRACTION = 0.95


async def run_open_loop(
    run_op: Callable[[], Awaitable[bool]],
    target_rate: float,
    warmup_seconds: float,
    measure_seconds: float,
    max_in_flight: int,
) -> LoadResult:
    """Drive ``run_op`` at ``target_rate`` requests per second.

    ``run_op`` performs one operation and returns ``True`` on success or ``False`` on an
    application error; it must not raise, so a single failed request cannot tear down the run.
    Latencies from the warmup window are discarded; latencies from the measurement window are
    corrected for coordinated omission and returned as percentiles.
    """
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_in_flight)
    interval = 1.0 / target_rate

    start = loop.time()
    warmup_end = start + warmup_seconds
    measure_end = warmup_end + measure_seconds

    latencies_ms: list[float] = []
    served = 0
    errors = 0
    in_flight = 0
    max_in_flight_observed = 0
    tasks: set[asyncio.Task] = set()

    def in_measure_window(instant: float) -> bool:
        return warmup_end <= instant <= measure_end

    async def fire(intended_time: float, intended_in_window: bool) -> None:
        # Two accounting rules run here on purpose. Throughput counts responses that arrive within
        # the measurement window, so a saturated server whose backlog drains after the window is
        # not credited for work it did late. Latency is charged from the intended send time for
        # every request meant to be sent in the window, so a stalled request carries its full
        # delay. That split is the coordinated-omission correction.
        nonlocal served, errors, in_flight, max_in_flight_observed
        in_flight += 1
        max_in_flight_observed = max(max_in_flight_observed, in_flight)
        try:
            ok = await run_op()
        except asyncio.CancelledError:
            raise
        except Exception:
            ok = False
        finally:
            in_flight -= 1
            semaphore.release()
        arrival = loop.time()
        if intended_in_window and ok:
            latencies_ms.append((arrival - intended_time) * 1000.0)
        if in_measure_window(arrival):
            if ok:
                served += 1
            else:
                errors += 1

    index = 0
    while True:
        intended = start + index * interval
        if intended > measure_end:
            break
        delay = intended - loop.time()
        if delay > 0:
            await asyncio.sleep(delay)
        await semaphore.acquire()
        task = loop.create_task(fire(intended, intended >= warmup_end))
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        index += 1

    # Wait for the backlog to drain, but not forever: at a saturating rate the tail would otherwise
    # take far longer than the run itself. Requests still outstanding after a grace window are
    # cancelled, which the saturation flag already reflects.
    if tasks:
        _, pending = await asyncio.wait(tasks, timeout=measure_seconds)
        for task in pending:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    achieved_rate = served / measure_seconds if measure_seconds > 0 else 0.0
    total = served + errors
    error_rate = errors / total if total > 0 else 0.0
    sustained = achieved_rate >= target_rate * _SUSTAINED_FRACTION
    reached_cap = max_in_flight_observed >= max_in_flight
    return LoadResult(
        target_rate=target_rate,
        achieved_rate=achieved_rate,
        completed=served,
        errors=errors,
        error_rate=error_rate,
        p50_ms=percentile(latencies_ms, 0.50),
        p95_ms=percentile(latencies_ms, 0.95),
        p99_ms=percentile(latencies_ms, 0.99),
        p999_ms=percentile(latencies_ms, 0.999),
        max_ms=max(latencies_ms) if latencies_ms else 0.0,
        mean_ms=mean(latencies_ms),
        max_in_flight_observed=max_in_flight_observed,
        sustained=sustained,
        server_saturated=(not sustained) and reached_cap,
        client_bound=(not sustained) and not reached_cap,
    )
