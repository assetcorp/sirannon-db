from __future__ import annotations

from render import integer, ms, table
from sources import Source


def features_block(source: Source) -> str:
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
            "This is the time from the process start command to the first successful health probe, measured "
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
