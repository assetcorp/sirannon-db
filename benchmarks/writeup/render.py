"""Deterministic formatting for the generated benchmark writeup.

Everything here is deterministic so the continuous-integration drift check stays stable: numbers
render ``n/a`` when absent rather than raising, tables come out byte-identical for the same run,
and no value depends on the wall clock or the host locale.
"""

from __future__ import annotations

from collections.abc import Sequence


def is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def integer(value: object) -> str:
    return f"{round(value):,}" if is_number(value) else "n/a"


def decimal(value: object, places: int) -> str:
    return f"{value:.{places}f}" if is_number(value) else "n/a"


def speedup(value: object, places: int = 2) -> str:
    return f"{value:.{places}f}x" if is_number(value) else "n/a"


def ops(value: object) -> str:
    if not is_number(value):
        return "n/a"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:.0f}"


def ms(value: object) -> str:
    if not is_number(value):
        return "n/a"
    if abs(value) >= 100:
        return f"{round(value):,}"
    return f"{value:.3f}"


def percent(fraction: object, places: int = 1) -> str:
    return f"{fraction * 100:.{places}f}%" if is_number(fraction) else "n/a"


def gigabytes(byte_count: object, places: int = 1) -> str:
    return f"{byte_count / (1024 ** 3):.{places}f}" if is_number(byte_count) else "n/a"


def humanized_list(parts: Sequence[str]) -> str:
    items = [part for part in parts if part]
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + ", and " + items[-1]


def table(headers: Sequence[str], aligns: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    divider = ["---:" if align == "right" else "---" for align in aligns]

    def render(cells: Sequence[str]) -> str:
        return "| " + " | ".join(cells) + " |"

    return "\n".join([render(headers), render(divider), *(render(row) for row in rows)])
