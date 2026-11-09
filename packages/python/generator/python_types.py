from __future__ import annotations

from dataclasses import dataclass, field

from generator.ir import (
    AliasDef,
    Definition,
    ListOf,
    Lit,
    MapOf,
    ModelDef,
    Nullable,
    Prim,
    Ref,
    TupleOf,
    TypeRef,
    UnionOf,
)

PRIMITIVES = {
    "str": "str",
    "number": "int | float",
    "bool": "bool",
    "null": "None",
    "any": "Any",
    "open_object": "dict[str, Any]",
}


@dataclass
class Rendered:
    text: str
    typing_names: set[str] = field(default_factory=set)
    model_names: set[str] = field(default_factory=set)
    uses_open_enum: bool = False


def _literal(value: str | int | float | bool) -> str:
    if isinstance(value, str):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return repr(value)


def render_type(ref: TypeRef, out: Rendered) -> str:
    if isinstance(ref, Prim):
        text = PRIMITIVES[ref.kind]
        if "Any" in text:
            out.typing_names.add("Any")
        return text
    if isinstance(ref, Lit):
        out.typing_names.add("Literal")
        literal = "Literal[" + ", ".join(_literal(value) for value in ref.values) + "]"
        if len(ref.values) > 1 and all(isinstance(value, str) for value in ref.values):
            out.typing_names.add("Annotated")
            out.uses_open_enum = True
            return f"Annotated[{literal}, OPEN_ENUM]"
        return literal
    if isinstance(ref, Ref):
        out.model_names.add(ref.name)
        return ref.name
    if isinstance(ref, ListOf):
        return f"list[{render_type(ref.item, out)}]"
    if isinstance(ref, TupleOf):
        inner = render_type(ref.item, out)
        return "tuple[" + ", ".join([inner] * ref.size) + "]"
    if isinstance(ref, MapOf):
        return f"dict[str, {render_type(ref.value, out)}]"
    if isinstance(ref, UnionOf):
        return " | ".join(render_type(option, out) for option in ref.options)
    if isinstance(ref, Nullable):
        return f"{render_type(ref.inner, out)} | None"
    raise TypeError(f"Unknown type reference {ref!r}")


def references(ref: TypeRef) -> set[str]:
    if isinstance(ref, Ref):
        return {ref.name}
    if isinstance(ref, ListOf | TupleOf):
        return references(ref.item)
    if isinstance(ref, MapOf):
        return references(ref.value)
    if isinstance(ref, UnionOf):
        found: set[str] = set()
        for option in ref.options:
            found |= references(option)
        return found
    if isinstance(ref, Nullable):
        return references(ref.inner)
    return set()


def definition_references(definition: Definition) -> set[str]:
    if isinstance(definition, AliasDef):
        return references(definition.type)
    found: set[str] = set()
    for model_field in definition.fields:
        found |= references(model_field.type)
    return found


def strongly_connected(definitions: dict[str, Definition]) -> list[list[str]]:
    index_of: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    groups: list[list[str]] = []
    counter = 0
    order = list(definitions)

    def visit(name: str) -> None:
        nonlocal counter
        index_of[name] = low[name] = counter
        counter += 1
        stack.append(name)
        on_stack.add(name)
        for target in sorted(definition_references(definitions[name])):
            if target not in index_of:
                visit(target)
                low[name] = min(low[name], low[target])
            elif target in on_stack:
                low[name] = min(low[name], index_of[target])
        if low[name] == index_of[name]:
            group: list[str] = []
            while True:
                member = stack.pop()
                on_stack.discard(member)
                group.append(member)
                if member == name:
                    break
            groups.append(sorted(group, key=order.index))

    for name in definitions:
        if name not in index_of:
            visit(name)
    return groups


def is_recursive(group: list[str], definitions: dict[str, Definition]) -> bool:
    if len(group) > 1:
        return True
    only = group[0]
    return only in definition_references(definitions[only])


def model_names(definitions: dict[str, Definition]) -> set[str]:
    return {name for name, definition in definitions.items() if isinstance(definition, ModelDef)}
