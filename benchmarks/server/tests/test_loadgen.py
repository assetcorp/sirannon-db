from __future__ import annotations

import asyncio

from sirannon_bench.core.loadgen import run_open_loop


def _run(coro):
    return asyncio.run(coro)


def test_sustained_load_reports_low_tail():
    async def scenario():
        async def run_op() -> bool:
            await asyncio.sleep(0.0005)
            return True

        return await run_open_loop(
            run_op, target_rate=200.0, warmup_seconds=0.1, measure_seconds=0.4, max_in_flight=32
        )

    result = _run(scenario())
    assert result.sustained
    assert not result.server_saturated
    assert not result.client_bound
    assert result.error_rate == 0.0
    assert result.p99_ms < 15.0


def test_coordinated_omission_charges_the_stall():
    # Each op takes 5 ms and only two may run at once, so the server can serve about 400 per
    # second. Asking for 2000 per second builds a backlog. Because latency is charged from the
    # intended send time, the delayed requests carry the backlog, so the tail climbs far above the
    # 5 ms service time a closed-loop generator would report.
    async def scenario():
        async def run_op() -> bool:
            await asyncio.sleep(0.005)
            return True

        return await run_open_loop(
            run_op, target_rate=2000.0, warmup_seconds=0.1, measure_seconds=0.5, max_in_flight=2
        )

    result = _run(scenario())
    assert not result.sustained
    assert result.server_saturated
    assert result.p99_ms > 20.0


def test_application_errors_are_counted_not_raised():
    async def scenario():
        async def run_op() -> bool:
            await asyncio.sleep(0.0005)
            return False

        return await run_open_loop(
            run_op, target_rate=200.0, warmup_seconds=0.05, measure_seconds=0.2, max_in_flight=16
        )

    result = _run(scenario())
    assert result.completed == 0
    assert result.errors > 0
    assert result.error_rate == 1.0
