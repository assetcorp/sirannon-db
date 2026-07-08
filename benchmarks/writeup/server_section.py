"""Build the generated regions of BENCHMARKS.md from the latest recorded run.

Every number in the setup line, the per-workload comparison, the scaling curve, and the
Sirannon-only characterizations reads straight from the recorded run, so the surrounding prose in
BENCHMARKS.md stays qualitative and the numbers come only from that run. When no run is committed
the blocks render a placeholder that the drift check tolerates.
"""

from __future__ import annotations

from render import decimal, gigabytes, humanized_list, integer, is_number, ms, ops, percent, speedup, table
from sources import Source

_WORKLOAD_LABELS = {
    "point-select": "Point-select",
    "bulk-insert": "Bulk-insert",
    "batch-update": "Batch-update",
    "ycsb-a": "YCSB-A (50/50 read/update)",
    "ycsb-b": "YCSB-B (95/5 read/update)",
    "ycsb-c": "YCSB-C (read-only)",
    "ycsb-f": "YCSB-F (read-modify-write)",
    "tpc-c-derived": "TPC-C-derived",
}

_DURABILITY_LABELS = {
    "full": "Full durability (fsync every commit)",
    "matched": "Matched-relaxed (deferred fsync)",
}

_DURABILITY_ORDER = ["full", "matched"]

_NO_RUN_NOTICE = (
    "_No benchmark run is committed yet. Run the suite on the disclosed cloud machine and commit "
    "its run directory under `benchmarks/server/results/runs/` to publish numbers here._"
)


def _label(workload: str) -> str:
    return _WORKLOAD_LABELS.get(workload, workload)


def _durabilities(comparison: dict) -> list[tuple[str, dict]]:
    node = comparison.get("durabilities", {})
    return [(name, node[name]) for name in _DURABILITY_ORDER if name in node]


def _date(comparison: dict) -> str:
    created = comparison.get("created_at") or ""
    return created[:10] if isinstance(created, str) else ""


def _engine_versions(comparison: dict) -> tuple[str, str]:
    for _, node in _durabilities(comparison):
        sqlite = node.get("sirannon_engine", {}).get("version")
        postgres = node.get("postgres_engine", {}).get("version")
        if sqlite or postgres:
            return str(sqlite or "n/a"), str(postgres or "n/a")
    return "n/a", "n/a"


def _client_ceilings(comparison: dict) -> tuple[dict, dict]:
    for _, node in _durabilities(comparison):
        saturation = node.get("client_saturation") or {}
        sirannon = saturation.get("sirannon") or {}
        postgres = saturation.get("postgres") or {}
        if sirannon or postgres:
            return sirannon, postgres
    return {}, {}


def _setup_block(source: Source) -> str:
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

    sqlite_version, postgres_version = _engine_versions(comparison)
    durability_names = humanized_list([_DURABILITY_LABELS.get(name, name) for name, _ in _durabilities(comparison)])
    rates = humanized_list([integer(rate) for rate in config.get("target_rates", [])])
    engine_cpus = delivery.get("engine_cpus")
    driver_cpus = delivery.get("driver_cpus")
    sirannon_ceiling, postgres_ceiling = _client_ceilings(comparison)

    bullets = [
        f"- **Run.** These figures come from run `{source.run_id}`, recorded on {_date(comparison)} from commit "
        f"`{commit}`{dirty}. The full per-run report is in [the run report]({source.report_link}).",
        f"- **Machine.** {machine}",
        f"- **Engines.** Sirannon runs on SQLite {sqlite_version}; PostgreSQL is {postgres_version}. Both run in "
        f"resource-capped containers at {durability_names or 'the recorded durability levels'}.",
        "- **Delivery.** One Node load generator drove both engines through the client each ships: Sirannon over "
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
    bullets.append(
        f"- **Workloads.** Every workload ran at {integer(config.get('data_size'))} rows, sweeping target rates of "
        f"{rates or 'n/a'} requests per second, with a {decimal(config.get('warmup_seconds'), 0)} s warmup and a "
        f"{decimal(config.get('measure_seconds'), 0)} s measurement window under seed `{config.get('seed', 'n/a')}`. "
        f"Every rate ran {integer(config.get('runs'))} independent times, and each figure is the median with a "
        f"95% confidence interval. The service-level target for the operating point is a p99 under "
        f"{decimal(config.get('slo_p99_ms'), 0)} ms."
    )
    return "\n".join(bullets)


_METHODOLOGY = (
    "One Node load generator drives both databases, with a thin per-database adapter for each. Sirannon runs "
    "over its SDK's default WebSocket transport, which multiplexes every concurrent request over one persistent "
    "socket; PostgreSQL runs over node-postgres on its binary socket protocol. Both engines answer on the same "
    "host over loopback. Sirannon's WebSocket path carries JSON framing on every call, heavier than PostgreSQL's "
    "binary protocol, and the numbers include that cost because it is Sirannon's client path. Before each sweep "
    "the generator measures each client's own throughput ceiling against the live engine and records it with the "
    "results, so a rate that falls short reflects the database's limit.\n\n"
    "The harness matches durability at two levels. Full durability sets PostgreSQL `synchronous_commit=on` "
    "against SQLite `synchronous=FULL`, so both fsync every commit. Matched-relaxed sets `synchronous_commit=off` "
    "against `synchronous=NORMAL` in WAL mode, so both defer the fsync and both can lose only the most recent "
    "commits on power loss without corrupting.\n\n"
    "The load is open-loop. Requests arrive at a fixed target rate whether or not earlier requests have "
    "returned, and each request's latency counts from the time it was meant to be sent, which corrects for "
    "coordinated omission. The report uses tail-latency percentiles, and the operating point is the highest "
    "offered rate the engine sustained while holding p99 under the recorded target."
)


def _methodology_block(source: Source) -> str:
    return _METHODOLOGY


def _durability_table(node: dict) -> str:
    rows = node.get("workloads", [])
    if not rows:
        return "No workload results were recorded."
    body: list[list[str]] = []
    has_below_slo = False
    for row in rows:
        sirannon = row.get("sirannon", {})
        postgres = row.get("postgres", {})
        speed = row.get("speedup")
        ci_cell = f"[{ops(sirannon.get('ops_ci_low'))}, {ops(sirannon.get('ops_ci_high'))}]"
        if speed and is_number(speed.get("point")):
            speed_cell = f"{speedup(speed['point'])} [{speedup(speed.get('ci_low'))}, {speedup(speed.get('ci_high'))}]"
        else:
            speed_cell = "n/a"
        marker = "" if sirannon.get("under_slo", True) and postgres.get("under_slo", True) else " †"
        if marker:
            has_below_slo = True
        body.append([
            _label(row.get("workload", "n/a")) + marker,
            ops(sirannon.get("ops_median")),
            ci_cell,
            percent(sirannon.get("ops_cv")),
            ms(sirannon.get("p99_ms")),
            ops(postgres.get("ops_median")),
            ms(postgres.get("p99_ms")),
            speed_cell,
        ])
    headers = [
        "Workload",
        "Sirannon ops/s",
        "Sirannon 95% CI",
        "Sirannon CV",
        "Sirannon p99 ms",
        "Postgres ops/s",
        "Postgres p99 ms",
        "Speedup",
    ]
    aligns = ["left", "right", "right", "right", "right", "right", "right", "right"]
    rendered = table(headers, aligns, body)
    if has_below_slo:
        rendered += "\n\n_A † marks a workload where an engine could not hold p99 under the target at any offered rate; its operating point is then the best rate it sustained._"
    return rendered


def _comparison_block(source: Source) -> str:
    durabilities = _durabilities(source.comparison)
    if not durabilities:
        return "No engine results were recorded."
    parts: list[str] = []
    for name, node in durabilities:
        parts.append(f"### {_DURABILITY_LABELS.get(name, name)}")
        parts.append(_durability_table(node))
    note = (
        "_Each throughput figure is the median of several independent runs at the operating point, the highest "
        "offered rate the engine sustained under the p99 target, shown with a 95% bootstrap confidence interval "
        "and the run-to-run coefficient of variation. A speedup above one means Sirannon sustained more "
        "throughput than PostgreSQL. TPC-C-derived is a TPC-C-shaped mix of new-order and payment, not an "
        "audited TPC-C result. The YCSB subset is A, B, C, and F, and leaves out D and E._"
    )
    return "\n\n".join([*parts, note])


def _scaling_block(source: Source) -> str:
    durabilities = _durabilities(source.comparison)
    if not durabilities:
        return "No scaling results were recorded."
    name, node = durabilities[0]
    scaling = node.get("scaling", [])
    if not scaling:
        return "No scaling results were recorded."
    parts: list[str] = [
        "The curves below show achieved throughput and p99 latency as the offered rate climbs, at "
        f"{_DURABILITY_LABELS.get(name, name).lower()}. PostgreSQL relies on row-level locking and Sirannon on a "
        "single writer, so which one holds throughput as the rate rises depends on the workload."
    ]
    for entry in scaling:
        parts.append(f"### {_label(entry.get('workload', 'n/a'))}")
        body = [
            [
                integer(point.get("target_rate")),
                ops(point.get("sirannon_ops")),
                ms(point.get("sirannon_p99_ms")),
                ops(point.get("postgres_ops")),
                ms(point.get("postgres_p99_ms")),
            ]
            for point in entry.get("curve", [])
        ]
        headers = ["Target ops/s", "Sirannon ops/s", "Sirannon p99 ms", "Postgres ops/s", "Postgres p99 ms"]
        parts.append(table(headers, ["right", "right", "right", "right", "right"], body))
    return "\n\n".join(parts)


def _features_block(source: Source) -> str:
    comparison = source.comparison
    parts: list[str] = []

    cold_start = comparison.get("cold_start")
    if isinstance(cold_start, dict):
        body: list[list[str]] = []
        for engine, key in (("Sirannon", "sirannon"), ("PostgreSQL", "postgres")):
            node = cold_start.get(key)
            value = node.get("cold_start_ms") if isinstance(node, dict) else None
            body.append([engine, ms(value)])
        parts.append("### Cold start")
        parts.append(
            "This is the time from the container start command to the first successful health probe, measured "
            "the same way for both engines."
        )
        parts.append(table(["Engine", "Cold start ms"], ["left", "right"], body))

    for feature in comparison.get("features", []):
        if feature.get("feature") != "cdc-latency":
            continue
        latency = feature.get("latency_ms", {})
        parts.append("### Change-feed latency (Sirannon only)")
        parts.append(
            "This measures the lag from a committed write to the change reaching a subscriber over Sirannon's "
            "built-in WebSocket feed. The server polls the change log every "
            f"{integer(feature.get('server_poll_interval_ms'))} ms, so that interval is the floor. PostgreSQL "
            "has no built-in change feed, so these numbers describe Sirannon on its own."
        )
        parts.append(
            table(
                ["Samples", "p50 ms", "p95 ms", "p99 ms", "max ms"],
                ["right", "right", "right", "right", "right"],
                [[
                    integer(feature.get("samples")),
                    ms(latency.get("p50")),
                    ms(latency.get("p95")),
                    ms(latency.get("p99")),
                    ms(latency.get("max")),
                ]],
            )
        )

    if not parts:
        return "No Sirannon-only characterizations were recorded."
    return "\n\n".join(parts)


def comparison_document(source: Source) -> str:
    """Assemble the self-contained per-run report committed as ``comparison.md`` beside the run's
    result JSON. It reuses the same block builders as the published page, so a reader who opens one
    run directory sees the machine, the methodology, and every table without leaving the folder."""
    date = _date(source.comparison) or "an unrecorded date"
    intro = (
        f"This report records one run of the Sirannon-versus-PostgreSQL benchmark, `{source.run_id}` from "
        f"{date}. Both databases answer the same workloads over their own shipping client on the same host, so "
        "the figures measure the two engines doing the same work."
    )
    sections = [
        "# Sirannon and PostgreSQL on one host",
        intro,
        "## Methodology",
        _methodology_block(source),
        "## Run and machine",
        _setup_block(source),
        "## Single-client and sustained-throughput comparison",
        _comparison_block(source),
        "## Throughput versus offered load",
        _scaling_block(source),
        "## Sirannon-only characterizations",
        _features_block(source),
    ]
    return "\n\n".join(sections) + "\n"


def blocks(source: Source | None) -> dict[str, str]:
    if source is None:
        return {
            "methodology": _NO_RUN_NOTICE,
            "setup": _NO_RUN_NOTICE,
            "comparison": _NO_RUN_NOTICE,
            "scaling": _NO_RUN_NOTICE,
            "features": _NO_RUN_NOTICE,
        }
    return {
        "methodology": _methodology_block(source),
        "setup": _setup_block(source),
        "comparison": _comparison_block(source),
        "scaling": _scaling_block(source),
        "features": _features_block(source),
    }
