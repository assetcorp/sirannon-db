from __future__ import annotations

import keyword
import re

_WORD = re.compile(r"[A-Z]+(?=[A-Z][a-z]|[0-9]|$|[^A-Za-z0-9])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+")


def words(name: str) -> list[str]:
    found: list[str] = []
    for chunk in re.split(r"[^A-Za-z0-9]+", name):
        found.extend(match.group(0) for match in _WORD.finditer(chunk))
    merged: list[str] = []
    for word in found:
        if word.isdigit() and merged:
            merged[-1] += word
        else:
            merged.append(word)
    return merged


def pascal(name: str) -> str:
    return "".join(word[:1].upper() + word[1:] for word in words(name))


def snake(name: str) -> str:
    return "_".join(word.lower() for word in words(name))


def python_identifier(name: str, reserved: frozenset[str]) -> str:
    candidate = snake(name)
    if not candidate or candidate[0].isdigit():
        candidate = f"field_{candidate}"
    if keyword.iskeyword(candidate) or candidate in reserved:
        candidate += "_"
    return candidate
