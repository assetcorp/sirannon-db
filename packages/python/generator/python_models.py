from __future__ import annotations

from pydantic import BaseModel

from generator.identifiers import python_identifier
from generator.ir import AliasDef, Api, FieldDef, ModelDef
from generator.python_types import Rendered, is_recursive, render_type, strongly_connected
from generator.text import GENERATED_MODULE_DOCSTRING, docstring

RESERVED_FIELD_NAMES = frozenset(name for name in dir(BaseModel) if not name.startswith("_"))
TYPING_ORDER = ("Annotated", "Any", "Literal", "TypeAlias")


def field_lines(model_field: FieldDef, out: Rendered) -> list[str]:
    attribute = python_identifier(model_field.json_name, RESERVED_FIELD_NAMES)
    annotation = render_type(model_field.type, out)
    aliased = attribute != model_field.json_name
    if model_field.required:
        default = f' = Field(alias="{model_field.json_name}")' if aliased else ""
    else:
        if not annotation.endswith("| None") and annotation not in ("Any", "None"):
            annotation += " | None"
        default = f' = Field(default=None, alias="{model_field.json_name}")' if aliased else " = None"
    if aliased:
        out.typing_names.add("Field")
    return [f"    {attribute}: {annotation}{default}", *docstring([model_field.description], "    ")]


def model_lines(model: ModelDef, out: Rendered) -> list[str]:
    lines = [f"class {model.name}(ApiModel):"]
    lines.extend(docstring([model.description], "    "))
    if model.fields:
        if len(lines) > 1:
            lines.append("")
        for model_field in model.fields:
            lines.extend(field_lines(model_field, out))
    elif len(lines) == 1:
        lines.append("    pass")
    return lines


def alias_lines(alias: AliasDef, out: Rendered, recursive: bool) -> list[str]:
    value = render_type(alias.type, out)
    if recursive:
        out.typing_names.add("TypeAliasType")
        quote = "'" if '"' in value else '"'
        lines = [f'{alias.name} = TypeAliasType("{alias.name}", {quote}{value}{quote})']
    else:
        out.typing_names.add("TypeAlias")
        lines = [f"{alias.name}: TypeAlias = {value}"]
    return lines + docstring([alias.description], "")


def render_models(api: Api) -> dict[str, str]:
    definitions = api.definitions
    out = Rendered("")
    body: list[str] = []
    for group in strongly_connected(definitions):
        recursive = is_recursive(group, definitions)
        for name in sorted(group, key=lambda member: isinstance(definitions[member], AliasDef)):
            definition = definitions[name]
            body.extend(["", ""])
            if isinstance(definition, ModelDef):
                body.extend(model_lines(definition, out))
            else:
                body.extend(alias_lines(definition, out, recursive))
    header = [GENERATED_MODULE_DOCSTRING, "", "from __future__ import annotations", ""]
    standard = sorted(name for name in out.typing_names if name in TYPING_ORDER)
    if standard:
        header.extend([f"from typing import {', '.join(standard)}", ""])
    if "Field" in out.typing_names:
        header.append("from pydantic import Field")
    if "TypeAliasType" in out.typing_names:
        header.append("from typing_extensions import TypeAliasType")
    header.append("")
    model_imports = ["ApiModel"] + (["OPEN_ENUM"] if out.uses_open_enum else [])
    header.append(f"from sirannon_client._model import {', '.join(sorted(model_imports, key=str.lower))}")
    exported = sorted(definitions)
    trailer = ["", "", "__all__ = [" + ", ".join(f'"{name}"' for name in exported) + "]"]
    return {"__init__.py": "\n".join([*header, *body, *trailer]) + "\n"}
