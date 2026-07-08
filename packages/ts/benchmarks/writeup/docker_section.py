"""Build the generated regions of BENCHMARKS.md from the latest Docker engine run.

Every number in the setup line, the per-workload comparison, and the concurrency-scaling
tables reads straight from the recorded run, so the surrounding prose in BENCHMARKS.md stays
qualitative and never disagrees with the data. When no run is committed the blocks render an
honest placeholder that the drift check tolerates.
"""

from __future__ import annotations

from render import decimal, integer, is_number, latency_ms, ops, speedup, table
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

_NO_RUN_NOTICE = (
    "_No benchmark run is committed yet. Run the Docker engine suite on the disclosed cloud "
    "machine and commit its run directory under `results/runs/` to publish numbers here._"
)


def _label(workload: str) -> str:
    return _WORKLOAD_LABELS.get(workload, workload)


def _date(source: Source) -> str:
    created = source.manifest.get("createdAt") or ""
    return created[:10] if isinstance(created, str) else ""


def _engine_rows(payload: dict, side: str) -> list[dict]:
    node = payload.get(side)
    rows = node.get("results") if isinstance(node, dict) else None
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _first_metric(row: dict, key: str) -> float | None:
    results = row.get("results")
    if not isinstance(results, list) or not results:
        return None
    first = results[0]
    value = first.get(key) if isinstance(first, dict) else None
    return float(value) if is_number(value) else None


def _match(rows: list[dict], workload: str, data_size: object) -> dict | None:
    return next(
        (row for row in rows if row.get("workload") == workload and row.get("dataSize") == data_size),
        None,
    )


def _engine_config(source: Source) -> dict:
    config = source.engine.get("config") if isinstance(source.engine, dict) else None
    return config if isinstance(config, dict) else {}


def _delivery(source: Source) -> dict:
    delivery = _engine_config(source).get("delivery")
    return delivery if isinstance(delivery, dict) else {}


def _setup_block(source: Source) -> str:
    manifest = source.manifest
    environment = manifest.get("environment") or {}
    engines = manifest.get("engines") or {}
    config = manifest.get("config") or {}
    engine_config = _engine_config(source)

    commit = str(manifest.get("gitCommit") or "")[:12] or "unknown"
    dirty = ", with uncommitted changes" if manifest.get("gitDirty") else ""
    label = manifest.get("machineLabel")
    host = (
        f"{environment.get('cpu') or 'an unspecified CPU'} with {integer(environment.get('cpuCores'))} logical "
        f"cores, {decimal(environment.get('ramGb'), 1)} GB of memory, on {environment.get('os') or 'an unknown OS'}"
    )
    machine = f"The run executed on {label}, which reports {host}." if label else f"The run host reports {host}."

    sizes = ", ".join(integer(size) for size in (engine_config.get("dataSizes") or []))
    postgres_version = engines.get("postgresVersion") or "n/a"
    sqlite_version = engines.get("sqliteVersion") or "n/a"

    delivery = _delivery(source)
    cpus = delivery.get("cpusPerSide")
    cpu_line = (
        f"Each side ran on the same total budget of {integer(cpus)} CPUs, split between its load "
        "driver and its database server."
        if is_number(cpus)
        else "Each side ran on the same total CPU budget, split between its load driver and its database server."
    )

    return "\n".join([
        f"- **Run.** These figures come from run `{source.run_id}`, recorded on {_date(source)} from commit "
        f"`{commit}`{dirty}. The raw per-workload results are in [the run report]({source.report_link}).",
        f"- **Machine.** {machine}",
        f"- **Engines.** Sirannon on SQLite {sqlite_version} against {postgres_version}, each in a "
        f"resource-capped Docker container under the {config.get('durability') or 'matched'} durability mode.",
        "- **Delivery.** The load driver reached Sirannon through the SirannonClient SDK over HTTP into the "
        "real server front-end, and reached Postgres through the native pg driver, both over loopback on one "
        f"host. {cpu_line}",
        f"- **Workloads.** Measured at {sizes or 'n/a'} rows with a {integer(engine_config.get('warmupMs'))} ms "
        f"warmup and {integer(engine_config.get('measureMs'))} ms measurement window, seed "
        f"`{config.get('seed') or 'n/a'}`.",
    ])


_METHODOLOGY = (
    "Both databases do the same client-server job on the same host. The load driver reaches Sirannon "
    "through its real client SDK over HTTP into the real server front-end, and reaches Postgres through "
    "the native pg driver, both over loopback with no network between them to bias the result. This is "
    "the comparison that counts, because it measures the two engines doing the same work rather than one "
    "engine skipping a network hop the other pays.\n\n"
    "Sirannon's HTTP request path carries JSON framing on every call, which is heavier than Postgres's "
    "binary wire protocol. The numbers err in that direction on purpose: charging Sirannon its real client "
    "cost keeps the comparison honest rather than flattering.\n\n"
    "The embedded figures reported separately below are a different claim. They measure Sirannon called "
    "in-process with no network hop at all, which is how an application that embeds the engine reaches it. "
    "That is a property of the embedded deployment mode, so it is never presented as Sirannon beating a "
    "server database; the fair server-vs-server tables are the head-to-head result."
)


def _methodology_block(source: Source) -> str:
    delivery = _delivery(source)
    sirannon_split = delivery.get("sirannonSplit")
    postgres_split = delivery.get("postgresSplit")
    if not isinstance(sirannon_split, str) or not isinstance(postgres_split, str):
        return _METHODOLOGY
    split = (
        f"\n\nCPU budget per side: Sirannon runs its {sirannon_split}; Postgres runs its {postgres_split}. "
        "The totals match, so neither side is given more compute than the other."
    )
    return f"{_METHODOLOGY}{split}"


def _comparison_block(source: Source) -> str:
    sirannon_rows = _engine_rows(source.engine, "sirannon")
    postgres_rows = _engine_rows(source.engine, "postgres")
    if not sirannon_rows:
        return "No engine results were recorded."

    body: list[list[str]] = []
    for srow in sirannon_rows:
        prow = _match(postgres_rows, srow.get("workload") or "", srow.get("dataSize"))
        sops = _first_metric(srow, "opsPerSec")
        pops = _first_metric(prow, "opsPerSec") if prow else None
        ratio = sops / pops if is_number(sops) and is_number(pops) and pops > 0 else None
        body.append([
            _label(srow.get("workload") or "n/a"),
            integer(srow.get("dataSize")),
            ops(sops),
            ops(pops),
            speedup(ratio, 1),
            latency_ms(_first_metric(srow, "p50Ns")),
            latency_ms(_first_metric(srow, "p99Ns")),
        ])

    headers = ["Workload", "Rows", "Sirannon ops/s", "Postgres ops/s", "Speedup", "Sirannon p50 ms", "Sirannon p99 ms"]
    aligns = ["left", "right", "right", "right", "right", "right", "right"]
    note = (
        "\n\n_Each YCSB workload runs its full weighted operation mix as a single measurement. "
        "TPC-C-derived is a TPC-C-shaped transaction mix (new-order plus payment), not an audited "
        "TPC-C result. The YCSB subset run is A, B, C, and F; D (read-latest) and E (short-range-scan) "
        "are omitted._"
    )
    return table(headers, aligns, body) + note


def _read_label(read_ratio: object) -> str:
    if read_ratio == 1.0:
        return "read-only"
    if read_ratio == 0.5:
        return "mixed 50/50"
    return f"read-{round(float(read_ratio) * 100)}%" if is_number(read_ratio) else "n/a"


def _scaling_points(payload: dict, side: str, model: str) -> list[dict]:
    node = payload.get(side)
    points = node.get("results") if isinstance(node, dict) else None
    if not isinstance(points, list):
        return []
    return [p for p in points if isinstance(p, dict) and p.get("model") == model]


def _scaling_model_table(scaling: dict, model: str) -> str:
    sirannon = _scaling_points(scaling, "sirannon", model)
    postgres = _scaling_points(scaling, "postgres", model)
    if not sirannon:
        return f"No {model} scaling results were recorded."

    body: list[list[str]] = []
    for sp in sirannon:
        pp = next(
            (
                p
                for p in postgres
                if p.get("concurrency") == sp.get("concurrency") and p.get("readRatio") == sp.get("readRatio")
            ),
            None,
        )
        sops = sp.get("opsPerSec")
        pops = pp.get("opsPerSec") if pp else None
        ratio = sops / pops if is_number(sops) and is_number(pops) and pops > 0 else None
        body.append([
            _read_label(sp.get("readRatio")),
            integer(sp.get("concurrency")),
            ops(sops),
            ops(pops),
            speedup(ratio, 1),
        ])

    headers = ["Workload", "Clients", "Sirannon ops/s", "Postgres ops/s", "Speedup"]
    return table(headers, ["left", "right", "right", "right", "right"], body)


def _scaling_block(source: Source) -> str:
    scaling = source.scaling
    if not isinstance(scaling, dict):
        return "No concurrency-scaling results were recorded."
    return "\n\n".join([
        "The load driver holds N in-flight requests against each server over loopback: Sirannon through "
        "the SDK over HTTP, Postgres across an async pool of N connections. This is where Postgres's "
        "row-level locking overtakes SQLite's single-writer model as the client count climbs.",
        _scaling_model_table(scaling, "event-loop"),
    ])


def _embedded_block(source: Source) -> str:
    scaling = source.scaling
    if not isinstance(scaling, dict):
        return "No embedded-deployment results were recorded."
    return "\n\n".join([
        "Separate claim about the embedded deployment mode, not a head-to-head against a server database. "
        "Here Sirannon is called in-process across worker threads with no network hop, which is how an "
        "application that embeds the engine reaches it. Postgres is still reached over its client-server "
        "path, so this table describes Sirannon's embedded mode rather than declaring a winner.",
        _scaling_model_table(scaling, "worker-threads"),
    ])


def docker_blocks(source: Source | None) -> dict[str, str]:
    if source is None:
        return {
            "engine-methodology": _NO_RUN_NOTICE,
            "engine-setup": _NO_RUN_NOTICE,
            "engine-comparison": _NO_RUN_NOTICE,
            "engine-scaling": _NO_RUN_NOTICE,
            "engine-embedded": _NO_RUN_NOTICE,
        }
    return {
        "engine-methodology": _methodology_block(source),
        "engine-setup": _setup_block(source),
        "engine-comparison": _comparison_block(source),
        "engine-scaling": _scaling_block(source),
        "engine-embedded": _embedded_block(source),
    }
