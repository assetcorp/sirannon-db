from __future__ import annotations

from dataclasses import dataclass

from generator.identifiers import pascal, python_identifier
from generator.ir import Api, ModelDef, Operation, Param, Ref, SpecError
from generator.python_types import Rendered, render_type
from generator.text import GENERATED_MODULE_DOCSTRING, docstring, ensure_sentence, wrap

ARG_WIDTH = 76
RESERVED_METHOD_NAMES = frozenset({"close", "aclose", "database", "self"})
RESERVED_PARAMETER_NAMES = frozenset({"self", "body", "timeout_ms", "headers"})
BODY_FALLBACK = "The route reads this JSON body."
OPTION_DOCS = [
    "timeout_ms: The client waits this many milliseconds for the answer, in place of the client's",
    "    setting, and 0 waits for as long as the server takes.",
    "headers: The client adds these headers to this request, replacing any of the same name.",
]
STATEMENT_NOTE = (
    "The client reads `GET /capabilities` first and raises `SQL_NOT_ACCEPTED` without sending the "
    "statement when the server leaves out `query.sql`."
)


@dataclass(frozen=True)
class Flavour:
    package: str
    base: str
    transport_module: str
    def_keyword: str
    await_keyword: str
    combined: str


SYNC = Flavour("_operations", "SyncOperationsBase", "_http", "def", "", "Operations")
ASYNC = Flavour("_async_operations", "AsyncOperationsBase", "_http", "async def", "await ", "AsyncOperations")


@dataclass
class MethodText:
    lines: list[str]
    rendered: Rendered
    helpers: set[str]


def _param_name(param: Param) -> str:
    return python_identifier(param.json_name, RESERVED_PARAMETER_NAMES)


def _path_expression(operation: Operation) -> str:
    path = operation.path
    for param in operation.params:
        path = path.replace("{" + param.json_name + "}", "{path_segment(" + _param_name(param) + ")}")
    return f'f"{path}"' if "{" in path else f'"{path}"'


def _answers(operation: Operation, out: Rendered) -> tuple[str, str]:
    typed = [answer for answer in operation.answers if answer.type is not None]
    mapping = "{" + ", ".join(f"{answer.status}: {render_type(answer.type, out)}" for answer in typed if answer.type)
    mapping += "}"
    returns: list[str] = []
    for answer in typed:
        if answer.type is None or not 200 <= answer.status < 300:
            continue
        rendered = render_type(answer.type, out)
        if rendered not in returns:
            returns.append(rendered)
    if any(answer.type is None and 200 <= answer.status < 300 for answer in operation.answers):
        returns.append("None")
    return mapping, " | ".join(returns) or "None"


def _body_parameter(operation: Operation, api: Api, out: Rendered) -> tuple[str, str]:
    body = operation.body
    if body is None:
        return "", ""
    annotation = render_type(body.type, out)
    description = body.description or BODY_FALLBACK
    if isinstance(body.type, Ref):
        target = api.definitions.get(body.type.name)
        if isinstance(target, ModelDef):
            out.typing_names.update({"Mapping", "Any"})
            annotation = f"{annotation} | Mapping[str, Any]"
            description = body.description or target.description or BODY_FALLBACK
    if not body.required:
        annotation += " | None = None"
    return f"body: {annotation},", ensure_sentence(description)


def _wrap_arg(entry: str) -> list[str]:
    head, _, rest = entry.partition(": ")
    lines = wrap(rest, "", ARG_WIDTH)
    if not lines:
        return [f"{head}:"]
    return [f"{head}: {lines[0]}", *[f"    {line}" for line in lines[1:]]]


def method_text(operation: Operation, api: Api, flavour: Flavour) -> MethodText:
    out = Rendered("")
    name = python_identifier(operation.operation_id, RESERVED_METHOD_NAMES)
    signature = [f"    {flavour.def_keyword} {name}(", "        self,"]
    args_doc: list[str] = []
    for param in operation.params:
        signature.append(f"        {_param_name(param)}: str,")
        args_doc.append(f"{_param_name(param)}: {ensure_sentence(param.description)}")
    body_line, body_doc = _body_parameter(operation, api, out)
    if body_line:
        signature.append(f"        {body_line}")
        args_doc.append(f"body: {body_doc}")
    signature.extend(
        ["        *,", "        timeout_ms: int | None = None,", "        headers: Mapping[str, str] | None = None,"]
    )
    out.typing_names.update({"Mapping", "cast"})
    mapping, returns = _answers(operation, out)
    signature.append(f"    ) -> {returns}:")
    args_block: list[str] = []
    for entry in args_doc:
        args_block.extend(_wrap_arg(entry))
    args_block.extend(OPTION_DOCS)
    returns_doc = [
        f"HTTP {answer.status}: {ensure_sentence(answer.description)}"
        for answer in operation.answers
        if answer.description
    ]
    doc = docstring(
        [
            ensure_sentence(operation.summary),
            f"`{operation.method} {operation.path}`",
            STATEMENT_NOTE if operation.sql_statement else "",
            ["Args:", *[f"    {line}" for line in args_block]],
            ["Returns:", *[f"    {line}" for line in returns_doc]] if returns_doc else "",
        ],
        "        ",
    )
    call = [
        f"        return cast({returns}, {flavour.await_keyword}self._transport.json(",
        "            Exchange(",
        f'                method="{operation.method}",',
        f"                path={_path_expression(operation)},",
    ]
    if operation.body is not None:
        call.append("                body=body,")
    if operation.sql_statement:
        call.append("                sql_statement=True,")
    call.extend(
        [
            "                timeout_ms=timeout_ms,",
            "                headers=headers,",
            "            ),",
            f"            {mapping},",
            "        ))",
        ]
    )
    helpers = {"Exchange"}
    if operation.params:
        helpers.add("path_segment")
    return MethodText([*signature, *doc, *call], out, helpers)


def _render_class(tag: str, class_name: str, methods: list[MethodText], flavour: Flavour) -> str:
    typing_names: set[str] = set()
    models: set[str] = set()
    helpers: set[str] = set()
    for method in methods:
        typing_names |= method.rendered.typing_names
        models |= method.rendered.model_names
        helpers |= method.helpers
    lines = [GENERATED_MODULE_DOCSTRING, "", "from __future__ import annotations", ""]
    if "Mapping" in typing_names:
        lines.append("from collections.abc import Mapping")
    typing = sorted(name for name in typing_names if name in ("Annotated", "Any", "Literal", "cast"))
    if typing:
        lines.append(f"from typing import {', '.join(typing)}")
    lines.append("")
    if any(method.rendered.uses_open_enum for method in methods):
        lines.append("from sirannon_client._model import OPEN_ENUM")
    lines.append(f"from sirannon_client._exchange import {', '.join(sorted(helpers))}")
    lines.append(f"from sirannon_client.{flavour.transport_module} import {flavour.base}")
    if models:
        lines.append(f"from sirannon_client.models import {', '.join(sorted(models))}")
    lines.extend(["", "", f"class {class_name}({flavour.base}):"])
    lines.extend(docstring([f"These methods call the {tag} routes of a Sirannon server."], "    "))
    for method in methods:
        lines.append("")
        lines.extend(method.lines)
    return "\n".join(lines) + "\n"


def render_operations(api: Api, flavour: Flavour) -> dict[str, str]:
    by_tag: dict[str, list[Operation]] = {}
    for operation in api.operations:
        by_tag.setdefault(operation.tag, []).append(operation)
    output: dict[str, str] = {}
    classes: list[tuple[str, str]] = []
    owners: dict[str, str] = {}
    for tag, operations in by_tag.items():
        module = python_identifier(tag, frozenset())
        if module in owners:
            raise SpecError(
                f"The tags {owners[module]} and {tag} would both be written to {flavour.package}/{module}.py"
            )
        owners[module] = tag
        class_name = f"{pascal(tag)}Operations"
        methods = [method_text(operation, api, flavour) for operation in operations]
        output[f"{module}.py"] = _render_class(tag, class_name, methods, flavour)
        classes.append((module, class_name))
    index = [GENERATED_MODULE_DOCSTRING, ""]
    index.extend(f"from sirannon_client.{flavour.package}.{module} import {name}" for module, name in sorted(classes))
    bases = ", ".join(name for _, name in classes)
    index.extend(["", "", f"class {flavour.combined}({bases}):"])
    index.extend(docstring(["These methods call every route of a Sirannon server, one method for each route."], "    "))
    output["__init__.py"] = "\n".join(index) + "\n"
    return output
