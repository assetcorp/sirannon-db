from __future__ import annotations

from comparison_view import durabilities, run_date, workload_label
from render import decimal, humanized_list, integer, is_number
from run_setup_section import (
    ENGINE_NAMES,
    delivery_bullet,
    engine_setting,
    engines_bullet,
    headroom_bullet,
    machine_bullet,
)
from run_shape import concurrency_levels
from sources import Source

SIRANNON_DEFAULT_WRITE_TIMEOUT_MS = 30000
COMMIT_DIGITS = 12
PASS_NAMES = {"full": "full-durability", "matched": "matched-relaxed"}


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


def _repetition_sentences(comparison: dict, config: dict) -> str:
    runs = integer(config.get("runs"))
    if not concurrency_levels(comparison):
        return (
            f"The harness measures every rate {runs} independent times, so each figure is the median of those {runs} "
            "repetitions with a 95% confidence interval."
        )
    extra = config.get("runs")
    more = float(extra) - 1 if is_number(extra) else None
    times = "n/a more times" if more is None else f"{integer(more)} more time{'' if more == 1 else 's'}"
    return (
        f"At each in-flight limit, the harness measures every rate once. It then measures every rate {times} at "
        "the engine's peak limit, so each figure that the tables report at that limit is the median of those "
        f"{runs} repetitions with a 95% confidence interval."
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
    deadline_ms = engine_setting(comparison, "sirannon", "writer_worker_write_timeout_ms")
    statement_timeout = engine_setting(comparison, "postgres", "statement_timeout")
    request_timeout_ms = engine_setting(comparison, "postgres", "request_timeout_ms")
    schema_ms = engine_setting(comparison, "sirannon", "schema_timeout_ms")
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


def run_block(source: Source) -> str:
    comparison = source.comparison
    environment = comparison.get("environment", {})
    config = comparison.get("config", {})
    delivery = config.get("delivery", {})

    date = run_date(comparison) or "an unrecorded date"
    data_size = integer(config.get("data_size"))
    bullets = [
        f"- **Run.** [The run report]({source.report_link}) gives the full results of run `{source.run_id}`, "
        f"which the harness recorded on {date}. {_commit_sentence(comparison)}",
        machine_bullet(comparison),
        engines_bullet(comparison, environment, delivery),
        delivery_bullet(comparison, config),
    ]
    target_rates = config.get("target_rates") or []
    headroom = headroom_bullet(comparison, max(target_rates) if target_rates else None)
    if headroom:
        bullets.append(headroom)
    bullets.append(
        f"- **Workloads.** {_seed_sentences(comparison, data_size)} {_sweep_sentences(config)} Each measurement "
        f"has a {decimal(config.get('warmup_seconds'), 0)} s warmup and a "
        f"{decimal(config.get('measure_seconds'), 0)} s window under seed `{config.get('seed', 'n/a')}`. "
        f"{_repetition_sentences(comparison, config)} The service-level target for an operating point is a p99 "
        f"of at most {decimal(config.get('slo_p99_ms'), 0)} ms."
    )
    soak_seconds = config.get("soak_seconds")
    soak_workloads = humanized_list([workload_label(name) for name in config.get("soak_workloads", [])])
    if is_number(soak_seconds) and soak_seconds > 0 and soak_workloads:
        if soak_seconds % 60 == 0:
            duration = f"{integer(soak_seconds / 60)}-minute"
        else:
            duration = f"{integer(soak_seconds)}-second"
        held_at = (
            "its operating point, with the in-flight limit at which it reached that point,"
            if concurrency_levels(comparison)
            else "its operating point"
        )
        bullets.append(
            f"- **Soak.** After the sweep, the harness held each engine at {held_at} on {soak_workloads} "
            f"for one continuous {duration} window, which the page reports in 30-second slices."
        )
    timeouts = _timeout_bullet(comparison)
    if timeouts:
        bullets.append(timeouts)
    return "\n".join(bullets)
