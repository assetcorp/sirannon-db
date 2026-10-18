from __future__ import annotations

from render import LOWER_IS_BETTER, emphasize_best, integer, ms, table
from sources import Source


def features_block(source: Source) -> str:
    comparison = source.comparison
    parts: list[str] = []

    cold_start = comparison.get("cold_start")
    if isinstance(cold_start, dict):
        engines = [("Sirannon", "sirannon"), ("PostgreSQL", "postgres")]
        values = [
            cold_start[key].get("cold_start_ms") if isinstance(cold_start.get(key), dict) else None
            for _, key in engines
        ]
        cells = emphasize_best(values, LOWER_IS_BETTER, ms)
        body = [[engine, cell] for (engine, _), cell in zip(engines, cells, strict=True)]
        parts.append("### Cold start")
        parts.append(
            "Cold start is the time from the command that starts the engine process to the first successful "
            "health probe, which the harness measures in the same way for both engines. Bold marks the faster "
            "start."
        )
        parts.append(table(["Engine", "Cold start ms"], ["left", "right"], body))

    for feature in comparison.get("features", []):
        if feature.get("feature") != "cdc-latency":
            continue
        latency = feature.get("latency_ms", {})
        parts.append("### Change-feed latency (Sirannon only)")
        parts.append(
            "This table gives the delay between the client sending a write and a subscriber receiving the change "
            "over Sirannon's built-in WebSocket feed. The server polls the change log every "
            f"{integer(feature.get('server_poll_interval_ms'))} ms. The harness sends each probe write as soon as "
            "the subscriber receives the previous change, so each write commits shortly after a poll and its "
            "change waits close to one full interval for the next poll. The harness measures no PostgreSQL "
            "equivalent, so the table gives Sirannon's figures alone."
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
        return "The harness recorded no cold-start or change-feed measurements."
    return "\n\n".join(parts)
