from __future__ import annotations

from comparison_view import durabilities, durability_label, run_date, workload_label
from render import decimal, gigabytes, humanized_list, integer, is_number, ops, ratio
from sources import Source

SIRANNON_DEFAULT_WRITE_TIMEOUT_MS = 30000
NODE_POSTGRES_DEFAULT_POOL_SIZE = 10
COMMIT_DIGITS = 12
ENGINE_NAMES = {"sirannon": "Sirannon", "postgres": "PostgreSQL"}
CLIENT_NAMES = {"sirannon": "the Sirannon SDK", "postgres": "node-postgres"}
PASS_NAMES = {"full": "full-durability", "matched": "matched-relaxed"}


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


def _engine_setting(comparison: dict, engine: str, key: str) -> object:
    for _, node in durabilities(comparison):
        settings = (node.get(f"{engine}_engine", {}) or {}).get("settings") or {}
        if settings.get(key) is not None:
            return settings[key]
    return None


def _client_ceilings(comparison: dict) -> tuple[dict, dict]:
    for _, node in durabilities(comparison):
        saturation = node.get("client_saturation") or {}
        sirannon = saturation.get("sirannon") or {}
        postgres = saturation.get("postgres") or {}
        if sirannon or postgres:
            return sirannon, postgres
    return {}, {}


def _pass_commits(comparison: dict) -> list[tuple[str, bool, list[str]]]:
    fallback = comparison.get("environment", {})
    groups: dict[tuple[str, bool], list[str]] = {}
    for name, node in durabilities(comparison):
        for engine in ("sirannon", "postgres"):
            environment = node.get(f"{engine}_environment") or fallback
            commit = str(environment.get("git_commit") or "")[:COMMIT_DIGITS] or "unknown"
            pass_name = f"{ENGINE_NAMES[engine]} {PASS_NAMES.get(name, name)}"
            groups.setdefault((commit, bool(environment.get("git_dirty"))), []).append(pass_name)
    return [(commit, dirty, passes) for (commit, dirty), passes in groups.items()]


def _commit_sentence(comparison: dict) -> str:
    groups = _pass_commits(comparison)
    if len(groups) <= 1:
        environment = comparison.get("environment", {})
        commit = groups[0][0] if groups else str(environment.get("git_commit") or "")[:COMMIT_DIGITS] or "unknown"
        dirty = groups[0][1] if groups else bool(environment.get("git_dirty"))
        changes = ", with uncommitted changes" if dirty else ""
        return f"For every pass, the harness recorded commit `{commit}`{changes}."
    sentences = []
    for index, (commit, dirty, passes) in enumerate(groups):
        changes = ", with uncommitted changes" if dirty else ""
        noun = "passes" if len(passes) > 1 else "pass"
        if index == 0:
            sentences.append(f"For the {humanized_list(passes)} {noun}, the harness recorded commit `{commit}`{changes}.")
        else:
            sentences.append(f"For the {humanized_list(passes)} {noun}, however, it recorded commit `{commit}`{changes}.")
    return " ".join(sentences)


def _highest_operating_rate(comparison: dict, engine: str) -> float | None:
    rates = [
        float(row[engine]["target_rate"])
        for _, node in durabilities(comparison)
        for row in node.get("workloads", [])
        if is_number((row.get(engine) or {}).get("target_rate"))
    ]
    return max(rates) if rates else None


def _headroom_bullet(comparison: dict, top_rate: object) -> str | None:
    sirannon_ceiling, postgres_ceiling = _client_ceilings(comparison)
    if not (sirannon_ceiling or postgres_ceiling):
        return None
    sentences = [
        "When the harness measured each client alone against its live engine, the Sirannon SDK sustained "
        f"{ops(sirannon_ceiling.get('ceiling_ops'))} requests a second and node-postgres sustained "
        f"{ops(postgres_ceiling.get('ceiling_ops'))}, which are {ratio(sirannon_ceiling.get('headroom_factor'))} "
        f"and {ratio(postgres_ceiling.get('headroom_factor'))} the fastest offered rate."
    ]
    limited: list[str] = []
    for engine, ceiling in (("sirannon", sirannon_ceiling), ("postgres", postgres_ceiling)):
        highest = _highest_operating_rate(comparison, engine)
        ceiling_ops = ceiling.get("ceiling_ops")
        if is_number(ceiling_ops) and highest is not None and float(ceiling_ops) <= highest:
            limited.append(engine)
    if limited:
        for engine in limited:
            sentences.append(
                f"The ceiling of {CLIENT_NAMES[engine]} is at or below the highest operating point of "
                f"{ENGINE_NAMES[engine]}, so that client can limit {ENGINE_NAMES[engine]}'s operating points."
            )
    else:
        sentences.append(
            "Each client's ceiling is above every operating point that its engine reached, so neither client "
            "limited a reported operating point."
        )
    for engine, ceiling in (("sirannon", sirannon_ceiling), ("postgres", postgres_ceiling)):
        factor = ceiling.get("headroom_factor")
        if is_number(factor) and float(factor) < 1 and is_number(top_rate):
            sentences.append(
                f"Since {CLIENT_NAMES[engine]} sustained less than the fastest offered rate, the "
                f"{ENGINE_NAMES[engine]} figures at rates above {ops(ceiling.get('ceiling_ops'))} a second "
                f"show the limit of {CLIENT_NAMES[engine]}."
            )
    return "- **Load-client headroom.** " + " ".join(sentences)


def _seed_sentences(comparison: dict, data_size: str) -> str:
    seeded: list[str] = []
    empty: list[str] = []
    recorded = durabilities(comparison)
    for row in recorded[0][1].get("workloads", []) if recorded else []:
        rows = (row.get("sirannon") or {}).get("seed_rows")
        if not is_number(rows):
            return f"Every workload draws its keys from a range of {data_size} rows."
        (seeded if rows > 0 else empty).append(workload_label(row.get("workload", "n/a")))
    if not empty:
        return f"Every workload starts from a table that the harness seeds with {data_size} rows."
    total = len(seeded) + len(empty)
    empty_names = humanized_list(empty)
    return (
        f"Of the {integer(total)} workloads, {integer(len(seeded))} start from a table that the harness seeds with "
        f"{data_size} rows. For the {empty_names} workload, the table starts empty, so every row in it comes from "
        f"the inserts that the generator sends during the sweep, with keys between 1 and {data_size}."
    )


def _sweep_sentences(config: dict) -> str:
    rates = humanized_list([integer(rate) for rate in config.get("target_rates", [])]) or "n/a"
    offered = f"The harness offers target rates of {rates} requests a second."
    stop_steps = config.get("sweep_stop_steps", -1)
    if not (is_number(stop_steps) and stop_steps >= 0):
        return offered
    if stop_steps == 0:
        ending = "it stops"
    elif stop_steps == 1:
        ending = "it measures one more rate and stops"
    else:
        ending = f"it measures {integer(stop_steps)} more rates and stops"
    return (
        f"{offered} It raises each engine's rate along that list until the engine fails to sustain one. After "
        f"that failure, {ending}, so the two sweeps can end at different rates."
    )


def _failed_requests(comparison: dict) -> bool:
    for _, node in durabilities(comparison):
        for entry in node.get("scaling", []):
            for point in entry.get("curve", []):
                for engine in ("sirannon", "postgres"):
                    rate = (point.get(engine) or {}).get("error_rate")
                    if is_number(rate) and rate > 0:
                        return True
    return False


def _timeout_bullet(comparison: dict) -> str | None:
    deadline_ms = _engine_setting(comparison, "sirannon", "writer_worker_write_timeout_ms")
    statement_timeout = _engine_setting(comparison, "postgres", "statement_timeout")
    request_timeout_ms = _engine_setting(comparison, "postgres", "request_timeout_ms")
    schema_ms = _engine_setting(comparison, "sirannon", "schema_timeout_ms")
    sentences: list[str] = []
    default_seconds = integer(SIRANNON_DEFAULT_WRITE_TIMEOUT_MS / 1000)
    if deadline_ms == 0:
        sentences.append(f"Sirannon's writer deadline was off, although its default is {default_seconds} seconds.")
    elif is_number(deadline_ms):
        sentences.append(
            f"Sirannon gave a single write {integer(float(deadline_ms) / 1000)} seconds before it reported the "
            f"writer unresponsive, against a default of {default_seconds} seconds."
        )
    if statement_timeout is not None and is_number(request_timeout_ms):
        sentences.append(
            "The PostgreSQL driver set `statement_timeout` to the load generator's request timeout of "
            f"{integer(request_timeout_ms)} ms, which PostgreSQL reported as `{statement_timeout}`, while the "
            "PostgreSQL default of 0 turns that timeout off."
        )
    if not sentences:
        return None
    if not _failed_requests(comparison):
        sentences.append("No request failed at any rate in either engine's sweep, so neither limit ended a request.")
    if is_number(schema_ms) and schema_ms > 0:
        sentences.append(
            f"The Sirannon driver gave each schema reset between workloads a "
            f"{integer(float(schema_ms) / 60000)}-minute request limit."
        )
    return "- **Timeouts.** " + " ".join(sentences)


def _engines_bullet(comparison: dict, environment: dict, delivery: dict) -> str:
    sirannon_version, sqlite_version, postgres_version = _engine_versions(comparison)
    levels = humanized_list([f"at {durability_label(name).lower()}" for name, _ in durabilities(comparison)])
    levels_text = f" {levels}" if levels else ""
    resource_control = environment.get("resource_control") or {}
    memory_cap = resource_control.get("engine_memory_max")
    cap_text = f"a {memory_cap} memory ceiling" if memory_cap else "a hard memory ceiling"
    engine_set = resource_control.get("engine_allowed_cpus")
    driver_set = resource_control.get("driver_allowed_cpus")
    if engine_set and driver_set:
        pinning = f"pinned to logical CPUs {engine_set}, while the load generator had logical CPUs {driver_set}"
    else:
        pinning = (
            f"pinned to {integer(delivery.get('engine_cpus'))} logical CPUs, while the load generator had "
            f"{integer(delivery.get('driver_cpus'))} other logical CPUs"
        )
    return (
        f"- **Engines.** The harness measured Sirannon {sirannon_version}, whose storage engine is SQLite "
        f"{sqlite_version}, and PostgreSQL {postgres_version}{levels_text}. Each engine operated as a native "
        f"process under {cap_text} that cgroup v2 enforces, {pinning} to itself."
    )


def _delivery_bullet(comparison: dict, config: dict) -> str:
    pool_size = _engine_setting(comparison, "postgres", "pool_size")
    if is_number(pool_size):
        pool = (
            f"a pool of {integer(pool_size)} connections, against a node-postgres default of "
            f"{integer(NODE_POSTGRES_DEFAULT_POOL_SIZE)}"
        )
    else:
        pool = "a pool of connections"
    return (
        "- **Delivery.** One Node load generator on the same host sent every request over loopback, through the "
        "client that each engine provides. The Sirannon SDK sent its requests to Sirannon over one WebSocket, "
        f"while node-postgres sent its requests to PostgreSQL over {pool}. With either client, the generator "
        f"kept at most {integer(config.get('max_in_flight'))} requests in flight at once."
    )


def run_block(source: Source) -> str:
    comparison = source.comparison
    environment = comparison.get("environment", {})
    config = comparison.get("config", {})
    delivery = config.get("delivery", {})

    label = environment.get("machine_label")
    host = (
        f"{integer(environment.get('logical_cpus'))} logical CPUs "
        f"({environment.get('cpu_model') or 'model unrecorded'}), "
        f"{gigabytes(environment.get('total_memory_bytes'))} GB of memory, and "
        f"{environment.get('os') or 'an unrecorded OS'} on {environment.get('arch') or 'an unrecorded architecture'}"
    )
    machine = (
        f"The benchmark machine was {label}, where the harness recorded {host}."
        if label
        else f"The harness recorded {host}."
    )
    date = run_date(comparison) or "an unrecorded date"
    data_size = integer(config.get("data_size"))
    runs = integer(config.get("runs"))
    bullets = [
        f"- **Run.** [The run report]({source.report_link}) gives the full results of run `{source.run_id}`, "
        f"which the harness recorded on {date}. {_commit_sentence(comparison)}",
        f"- **Machine.** {machine}",
        _engines_bullet(comparison, environment, delivery),
        _delivery_bullet(comparison, config),
    ]
    target_rates = config.get("target_rates") or []
    headroom = _headroom_bullet(comparison, max(target_rates) if target_rates else None)
    if headroom:
        bullets.append(headroom)
    bullets.append(
        f"- **Workloads.** {_seed_sentences(comparison, data_size)} {_sweep_sentences(config)} Each measurement "
        f"has a {decimal(config.get('warmup_seconds'), 0)} s warmup and a "
        f"{decimal(config.get('measure_seconds'), 0)} s window under seed `{config.get('seed', 'n/a')}`. The "
        f"harness measures every rate {runs} independent times, so each figure is the median of those {runs} "
        "repetitions with a 95% confidence interval. The service-level target for an operating point is a p99 "
        f"of at most {decimal(config.get('slo_p99_ms'), 0)} ms."
    )
    soak_seconds = config.get("soak_seconds")
    soak_workloads = humanized_list([workload_label(name) for name in config.get("soak_workloads", [])])
    if is_number(soak_seconds) and soak_seconds > 0 and soak_workloads:
        if soak_seconds % 60 == 0:
            duration = f"{integer(soak_seconds / 60)}-minute"
        else:
            duration = f"{integer(soak_seconds)}-second"
        bullets.append(
            f"- **Soak.** After the sweep, the harness held each engine at its operating point on {soak_workloads} "
            f"for one continuous {duration} window, which the page reports in 30-second slices."
        )
    timeouts = _timeout_bullet(comparison)
    if timeouts:
        bullets.append(timeouts)
    return "\n".join(bullets)
