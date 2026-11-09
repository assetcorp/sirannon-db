from __future__ import annotations

import textwrap

DOCSTRING_WIDTH = 100
GENERATED_MODULE_DOCSTRING = (
    '"""Generated from packages/ts/openapi.json and packages/ts/asyncapi.json by packages/python/generator.'
    ' Do not edit."""'
)


def escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')


def wrap(text: str, indent: str, width: int = DOCSTRING_WIDTH) -> list[str]:
    if not text:
        return []
    return textwrap.wrap(
        escape(text),
        width=max(width - len(indent), 40),
        initial_indent=indent,
        subsequent_indent=indent,
        break_long_words=False,
        break_on_hyphens=False,
    )


def docstring(paragraphs: list[str | list[str]], indent: str) -> list[str]:
    body: list[str] = []
    for paragraph in paragraphs:
        if not paragraph:
            continue
        if body:
            body.append("")
        if isinstance(paragraph, list):
            body.extend(f"{indent}{line}" if line else "" for line in paragraph)
        else:
            body.extend(wrap(paragraph, indent))
    if not body:
        return []
    first = body[0][len(indent) :]
    if len(body) == 1 and len(indent) + len(first) + 6 <= DOCSTRING_WIDTH and not first.endswith('"'):
        return [f'{indent}"""{first}"""']
    return [f'{indent}"""{first}', *body[1:], f'{indent}"""']


def ensure_sentence(text: str) -> str:
    stripped = text.strip()
    if not stripped:
        return stripped
    if stripped[-1] in ".!?`)":
        return stripped
    return stripped + "."
