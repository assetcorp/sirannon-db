from __future__ import annotations

from comparison_view import durabilities, durability_label
from render import gigabytes, humanized_list, integer, is_number, ms, ops, ratio
from run_shape import concurrency_levels, engine_machine, network_round_trip, over_network

NODE_POSTGRES_DEFAULT_POOL_SIZE = 10
ENGINE_NAMES = {"sirannon": "Sirannon", "postgres": "PostgreSQL"}
CLIENT_NAMES = {"sirannon": "the Sirannon SDK", "postgres": "node-postgres"}


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


def engine_setting(comparison: dict, engine: str, key: str) -> object:
    for _, node in durabilities(comparison):
        settings = (node.get(f"{engine}_engine", {}) or {}).get("settings") or {}
        if settings.get(key) is not None:
            return settings[key]
    return None


def _host_description(machine: dict) -> str:
    return (
        f"{integer(machine.get('logical_cpus'))} logical CPUs "
        f"({machine.get('cpu_model') or 'model unrecorded'}), "
        f"{gigabytes(machine.get('total_memory_bytes'))} GB of memory, and "
        f"{machine.get('os') or 'an unrecorded OS'} on {machine.get('arch') or 'an unrecorded architecture'}"
    )


def machine_bullet(comparison: dict) -> str:
    environment = comparison.get("environment", {})
    label = environment.get("machine_label")
    host = _host_description(environment)
    if not over_network(comparison):
        machine = (
            f"The benchmark machine was {label}, where the harness recorded {host}."
            if label
            else f"The harness recorded {host}."
        )
        return f"- **Machine.** {machine}"
    engine = engine_machine(comparison)
    engine_label = engine.get("machine_label")
    sentences = [
        f"The engine machine was {engine_label}, where the harness recorded {_host_description(engine)}."
        if engine_label
        else f"On the engine machine, the harness recorded {_host_description(engine)}."
    ]
    sentences.append(
        f"The load generator ran on {label}, where the harness recorded {host}."
        if label
        else f"On the load machine, the harness recorded {host}."
    )
    measured = network_round_trip(comparison)
    if measured is not None:
        round_trip = measured.get("round_trip_ms") or {}
        sentences.append(
            f"Before each pass, the load generator opened {integer(measured.get('samples'))} TCP connections to "
            f"the engine machine, and each connection took a median of {ms(round_trip.get('p50'))} ms to open, "
            f"with a p99 of {ms(round_trip.get('p99'))} ms."
        )
    return "- **Machine.** " + " ".join(sentences)


def engines_bullet(comparison: dict, environment: dict, delivery: dict) -> str:
    sirannon_version, sqlite_version, postgres_version = _engine_versions(comparison)
    levels = humanized_list([f"at {durability_label(name).lower()}" for name, _ in durabilities(comparison)])
    levels_text = f" {levels}" if levels else ""
    resource_control = environment.get("resource_control") or {}
    memory_cap = resource_control.get("engine_memory_max") or engine_machine(comparison).get("engine_memory_max")
    cap_text = f"a {memory_cap} memory ceiling" if memory_cap else "a hard memory ceiling"
    engine_set = resource_control.get("engine_allowed_cpus")
    driver_set = resource_control.get("driver_allowed_cpus")
    if over_network(comparison):
        engine_cpus = engine_machine(comparison).get("engine_allowed_cpus") or engine_set
        pinning = (
            f"with logical CPUs {engine_cpus} of the engine machine"
            if engine_cpus
            else "with every logical CPU of the engine machine"
        )
    elif engine_set and driver_set:
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


def delivery_bullet(comparison: dict, config: dict) -> str:
    levels = concurrency_levels(comparison)
    if not levels:
        pool_size = engine_setting(comparison, "postgres", "pool_size")
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
    route = (
        "on the load machine sent every request over the private network"
        if over_network(comparison)
        else "on the same host sent every request over loopback"
    )
    limits = humanized_list([integer(level) for level in levels])
    return (
        f"- **Delivery.** One Node load generator {route}, through the client that each engine provides. The "
        "Sirannon SDK sent its requests to Sirannon over one WebSocket, while node-postgres opened one pooled "
        "connection to PostgreSQL for each request in flight. The harness measured every workload with at most "
        f"{limits} requests in flight. The tables report each engine at its peak limit, which is the limit where "
        "its operating point was highest after one measurement at each rate."
    )


def _client_ceilings(comparison: dict) -> tuple[dict, dict]:
    for _, node in durabilities(comparison):
        saturation = node.get("client_saturation") or {}
        sirannon = saturation.get("sirannon") or {}
        postgres = saturation.get("postgres") or {}
        if sirannon or postgres:
            return sirannon, postgres
    return {}, {}


def _operating_rates(comparison: dict, engine: str) -> list[tuple[float, object]]:
    rates: list[tuple[float, object]] = []
    for _, node in durabilities(comparison):
        for row in node.get("workloads", []):
            side = row.get(engine) or {}
            if is_number(side.get("target_rate")):
                rates.append((float(side["target_rate"]), side.get("concurrency")))
    return rates


def _limited_sentences(limited: list[str]) -> list[str]:
    if not limited:
        return [
            "Each client's ceiling is above every operating point that its engine reached, so neither client "
            "limited a reported operating point."
        ]
    return [
        f"The ceiling of {CLIENT_NAMES[engine]} is at or below the highest operating point of "
        f"{ENGINE_NAMES[engine]}, so that client can limit {ENGINE_NAMES[engine]}'s operating points."
        for engine in limited
    ]


def _headroom_by_level(comparison: dict, sirannon: dict, postgres: dict) -> str:
    def ceiling_at(saturation: dict, level: object) -> object:
        for entry in saturation.get("levels") or []:
            if entry.get("concurrency") == level:
                return entry.get("ceiling_ops")
        return None

    levels = concurrency_levels(comparison)
    limits = humanized_list([integer(level) for level in levels])
    sentences = [
        f"With {limits} requests in flight, the Sirannon SDK alone sustained "
        f"{humanized_list([ops(ceiling_at(sirannon, level)) for level in levels])} requests a second against its "
        "live engine.",
        f"At the same limits, node-postgres sustained "
        f"{humanized_list([ops(ceiling_at(postgres, level)) for level in levels])} requests a second.",
    ]
    limited: list[str] = []
    for engine, saturation in (("sirannon", sirannon), ("postgres", postgres)):
        for rate, level in _operating_rates(comparison, engine):
            ceiling = ceiling_at(saturation, level)
            if is_number(ceiling) and float(ceiling) <= rate:
                limited.append(engine)
                break
    sentences.extend(_limited_sentences(limited))
    return "- **Load-client headroom.** " + " ".join(sentences)


def headroom_bullet(comparison: dict, top_rate: object) -> str | None:
    sirannon_ceiling, postgres_ceiling = _client_ceilings(comparison)
    if not (sirannon_ceiling or postgres_ceiling):
        return None
    if "levels" in sirannon_ceiling or "levels" in postgres_ceiling:
        return _headroom_by_level(comparison, sirannon_ceiling, postgres_ceiling)
    sentences = [
        "When the harness measured each client alone against its live engine, the Sirannon SDK sustained "
        f"{ops(sirannon_ceiling.get('ceiling_ops'))} requests a second and node-postgres sustained "
        f"{ops(postgres_ceiling.get('ceiling_ops'))}, which are {ratio(sirannon_ceiling.get('headroom_factor'))} "
        f"and {ratio(postgres_ceiling.get('headroom_factor'))} the fastest offered rate."
    ]
    limited: list[str] = []
    for engine, ceiling in (("sirannon", sirannon_ceiling), ("postgres", postgres_ceiling)):
        rates = [rate for rate, _ in _operating_rates(comparison, engine)]
        highest = max(rates) if rates else None
        ceiling_ops = ceiling.get("ceiling_ops")
        if is_number(ceiling_ops) and highest is not None and float(ceiling_ops) <= highest:
            limited.append(engine)
    sentences.extend(_limited_sentences(limited))
    for engine, ceiling in (("sirannon", sirannon_ceiling), ("postgres", postgres_ceiling)):
        factor = ceiling.get("headroom_factor")
        if is_number(factor) and float(factor) < 1 and is_number(top_rate):
            sentences.append(
                f"Since {CLIENT_NAMES[engine]} sustained less than the fastest offered rate, the "
                f"{ENGINE_NAMES[engine]} figures at rates above {ops(ceiling.get('ceiling_ops'))} a second "
                f"show the limit of {CLIENT_NAMES[engine]}."
            )
    return "- **Load-client headroom.** " + " ".join(sentences)
