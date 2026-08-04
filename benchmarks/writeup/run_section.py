from __future__ import annotations

from comparison_view import durabilities, durability_label, run_date, workload_label
from render import decimal, gigabytes, humanized_list, integer, is_number, ops, speedup
from sources import Source


def _engine_versions(comparison: dict) -> tuple[str, str, str]:
    for _, node in durabilities(comparison):
        sirannon_engine = node.get("sirannon_engine", {})
        sirannon = sirannon_engine.get("version")
        sqlite = (sirannon_engine.get("settings") or {}).get("sqlite_version")
        postgres = node.get("postgres_engine", {}).get("version")
        if sirannon or sqlite or postgres:
            short_postgres = str(postgres or "n/a").removeprefix("PostgreSQL ").split(" ")[0]
            return str(sirannon or "n/a"), str(sqlite or "n/a"), short_postgres
    return "n/a", "n/a", "n/a"


SIRANNON_DEFAULT_WRITE_TIMEOUT_MS = 30000


def _writer_deadline_ms(comparison: dict) -> float | None:
    for _, node in durabilities(comparison):
        settings = (node.get("sirannon_engine", {}) or {}).get("settings") or {}
        value = settings.get("writer_worker_write_timeout_ms")
        if is_number(value):
            return float(value)
    return None


def _client_ceilings(comparison: dict) -> tuple[dict, dict]:
    for _, node in durabilities(comparison):
        saturation = node.get("client_saturation") or {}
        sirannon = saturation.get("sirannon") or {}
        postgres = saturation.get("postgres") or {}
        if sirannon or postgres:
            return sirannon, postgres
    return {}, {}


def run_block(source: Source) -> str:
    comparison = source.comparison
    environment = comparison.get("environment", {})
    config = comparison.get("config", {})
    delivery = config.get("delivery", {})

    commit = str(environment.get("git_commit") or "")[:12] or "unknown"
    dirty = ", with uncommitted changes" if environment.get("git_dirty") else ""
    label = environment.get("machine_label")
    host = (
        f"{environment.get('cpu_model') or 'an unspecified CPU'} with "
        f"{integer(environment.get('logical_cpus'))} logical cores, "
        f"{gigabytes(environment.get('total_memory_bytes'))} GB of memory, on "
        f"{environment.get('os') or 'an unknown OS'} ({environment.get('arch') or 'unknown arch'})"
    )
    machine = f"The run executed on {label}, which reports {host}." if label else f"The run host reports {host}."

    sirannon_version, sqlite_version, postgres_version = _engine_versions(comparison)
    durability_names = humanized_list([durability_label(name) for name, _ in durabilities(comparison)])
    rates = humanized_list([integer(rate) for rate in config.get("target_rates", [])])
    engine_cpus = delivery.get("engine_cpus")
    driver_cpus = delivery.get("driver_cpus")
    resource_control = environment.get("resource_control") or {}
    memory_cap = resource_control.get("engine_memory_max")
    cap_text = f"a {memory_cap} memory ceiling" if memory_cap else "a hard memory ceiling"
    sirannon_ceiling, postgres_ceiling = _client_ceilings(comparison)

    bullets = [
        f"- **Run.** These figures come from run `{source.run_id}`, recorded on {run_date(comparison)} from commit "
        f"`{commit}`{dirty}. The full per-run report is in [the run report]({source.report_link}).",
        f"- **Machine.** {machine}",
        f"- **Engines.** Sirannon {sirannon_version} (storage engine SQLite {sqlite_version}); PostgreSQL "
        f"{postgres_version}. Both run as "
        f"native processes on dedicated cores under {cap_text} (cgroup v2), at "
        f"{durability_names or 'the recorded durability levels'}.",
        "- **Delivery.** One Node load generator drove both engines through the client each provides: Sirannon over "
        "its SDK's WebSocket transport, which multiplexes every request over one persistent socket, and PostgreSQL "
        f"over node-postgres on its binary socket protocol, both on one host over loopback. Each engine ran on "
        f"{integer(engine_cpus)} pinned cores and the load generator on {integer(driver_cpus)} of its own.",
    ]
    if sirannon_ceiling or postgres_ceiling:
        bullets.append(
            "- **Load-client headroom.** Run on its own against the live engines, the Sirannon SDK "
            f"sustained {ops(sirannon_ceiling.get('ceiling_ops'))} and node-postgres "
            f"{ops(postgres_ceiling.get('ceiling_ops'))}, {speedup(sirannon_ceiling.get('headroom_factor'))} and "
            f"{speedup(postgres_ceiling.get('headroom_factor'))} the fastest rate offered. The load generator stays "
            "well above both engines, so every reported number reflects the database's speed."
        )
    stop_steps = config.get("sweep_stop_steps", -1)
    if is_number(stop_steps) and stop_steps >= 0:
        if stop_steps == 0:
            ending = "then stops"
        elif stop_steps == 1:
            ending = "runs one more rate, and stops"
        else:
            ending = f"runs {integer(stop_steps)} more rates, and stops"
        sweep_text = (
            f"sweeping target rates drawn from {rates or 'n/a'} requests per second. Each engine climbs the list "
            f"until it fails to sustain a rate, {ending}, so the two engines can end their sweeps at different rates"
        )
    else:
        sweep_text = f"sweeping target rates of {rates or 'n/a'} requests per second"
    bullets.append(
        f"- **Workloads.** Every workload ran at {integer(config.get('data_size'))} rows, {sweep_text}, with a "
        f"{decimal(config.get('warmup_seconds'), 0)} s warmup and a "
        f"{decimal(config.get('measure_seconds'), 0)} s measurement window under seed `{config.get('seed', 'n/a')}`. "
        f"Every rate ran {integer(config.get('runs'))} independent times, and each figure is the median with a "
        f"95% confidence interval. The service-level target for the operating point is a p99 under "
        f"{decimal(config.get('slo_p99_ms'), 0)} ms."
    )
    soak_seconds = config.get("soak_seconds")
    soak_workloads = humanized_list([workload_label(name) for name in config.get("soak_workloads", [])])
    if is_number(soak_seconds) and soak_seconds > 0 and soak_workloads:
        if soak_seconds % 60 == 0:
            duration = f"{integer(soak_seconds / 60)}-minute"
        else:
            duration = f"{integer(soak_seconds)}-second"
        bullets.append(
            f"- **Soak.** After the sweep, {soak_workloads} held each engine at its operating point for one "
            f"continuous {duration} window, reported in 30-second slices."
        )
    deadline_ms = _writer_deadline_ms(comparison)
    in_flight = integer(config.get("max_in_flight"))
    if deadline_ms == 0:
        bullets.append(
            "- **Writer deadline.** Sirannon ran with its writer deadline disabled, so a single write had no time "
            "limit and a stalled writer would have been caught by the workload stall deadline instead."
        )
    elif deadline_ms is not None:
        raised = ""
        if deadline_ms > SIRANNON_DEFAULT_WRITE_TIMEOUT_MS:
            raised = (
                f", raised from the {integer(SIRANNON_DEFAULT_WRITE_TIMEOUT_MS / 1000)}-second default because "
                f"dropping a table at this row count takes one to two minutes"
            )
        bullets.append(
            f"- **Writer deadline.** Sirannon gave a single write {integer(deadline_ms / 1000)} seconds before "
            f"reporting the writer unresponsive{raised}. Measured operations finish in milliseconds, with at most "
            f"{in_flight} requests in flight."
        )
    return "\n".join(bullets)
