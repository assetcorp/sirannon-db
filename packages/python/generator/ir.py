from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from generator.identifiers import pascal

SCHEMA_PREFIX = "#/components/schemas/"
HTTP_METHODS = ("get", "put", "post", "patch", "delete")
SQL_STATEMENT_EXTENSION = "x-sirannon-sql-statement"
JSON_CONTENT = "application/json"
UNION_KEYWORDS = ("anyOf", "oneOf")


@dataclass(frozen=True)
class Prim:
    kind: str


@dataclass(frozen=True)
class Lit:
    values: tuple[str | int | float | bool, ...]


@dataclass(frozen=True)
class ListOf:
    item: TypeRef


@dataclass(frozen=True)
class TupleOf:
    item: TypeRef
    size: int


@dataclass(frozen=True)
class MapOf:
    value: TypeRef


@dataclass(frozen=True)
class Ref:
    name: str


@dataclass(frozen=True)
class UnionOf:
    options: tuple[TypeRef, ...]


@dataclass(frozen=True)
class Nullable:
    inner: TypeRef


TypeRef = Prim | Lit | ListOf | TupleOf | MapOf | Ref | UnionOf | Nullable

STRING = Prim("str")
NUMBER = Prim("number")
BOOLEAN = Prim("bool")
NULL = Prim("null")
ANY = Prim("any")
OPEN_OBJECT = Prim("open_object")


@dataclass(frozen=True)
class FieldDef:
    json_name: str
    type: TypeRef
    required: bool
    description: str


@dataclass(frozen=True)
class ModelDef:
    name: str
    description: str
    fields: tuple[FieldDef, ...]


@dataclass(frozen=True)
class AliasDef:
    name: str
    description: str
    type: TypeRef


Definition = ModelDef | AliasDef


@dataclass(frozen=True)
class Param:
    json_name: str
    description: str


@dataclass(frozen=True)
class Body:
    type: TypeRef
    required: bool
    description: str


@dataclass(frozen=True)
class Answer:
    status: int
    type: TypeRef | None
    description: str


@dataclass(frozen=True)
class Operation:
    operation_id: str
    method: str
    path: str
    tag: str
    summary: str
    params: tuple[Param, ...]
    body: Body | None
    answers: tuple[Answer, ...]
    sql_statement: bool


@dataclass
class Api:
    definitions: dict[str, Definition] = field(default_factory=dict)
    operations: list[Operation] = field(default_factory=list)


class SpecError(Exception):
    pass


def _description(schema: dict[str, Any]) -> str:
    text = schema.get("description", "")
    return " ".join(text.split()) if isinstance(text, str) else ""


class _Builder:
    def __init__(self, schemas: dict[str, Any], responses: dict[str, Any]) -> None:
        self.schemas = schemas
        self.responses = responses
        self.api = Api()

    def define(self, name: str, definition: Definition) -> None:
        if name in self.api.definitions:
            raise SpecError(f"Two schemas would both become the type {name}")
        self.api.definitions[name] = definition

    def ref_name(self, ref: str) -> str:
        if not ref.startswith(SCHEMA_PREFIX):
            raise SpecError(f"The generator reads only component schema references, and found {ref}")
        name = ref[len(SCHEMA_PREFIX) :]
        if name not in self.schemas:
            raise SpecError(f"The files hold no schema for the reference {ref}")
        return name

    def is_model(self, schema: dict[str, Any]) -> bool:
        if "properties" not in schema:
            return False
        extra = schema.get("additionalProperties")
        return extra is not True and extra != {}

    def top_level(self, name: str, schema: dict[str, Any]) -> None:
        if self.is_model(schema):
            self.model(name, schema)
            return
        self.define(name, AliasDef(name, _description(schema), self.convert(schema, name)))

    def model(self, name: str, schema: dict[str, Any], where: str = "") -> Ref:
        required = set(schema.get("required", []))
        fields = tuple(
            FieldDef(
                json_name=key,
                type=self.convert(value, name + pascal(key), f"the `{key}` field of `{name}`"),
                required=key in required,
                description=_description(value),
            )
            for key, value in schema["properties"].items()
        )
        description = _description(schema) or (f"The shape of {where}." if where else "")
        self.define(name, ModelDef(name, description, fields))
        return Ref(name)

    def union(self, options: list[dict[str, Any]], hint: str, where: str = "") -> TypeRef:
        nullable = any(option.get("type") == "null" for option in options)
        rest = [option for option in options if option.get("type") != "null"]
        objects = [option for option in rest if self.is_model(option)]
        converted: list[TypeRef] = []
        for position, option in enumerate(rest):
            if self.is_model(option):
                suffix = "Object" if len(objects) == 1 else f"Variant{position + 1}"
                converted.append(self.convert(option, hint + suffix, f"one form of {where or f'`{hint}`'}"))
            else:
                converted.append(self.convert(option, hint, where))
        if not converted:
            return NULL
        merged: TypeRef = converted[0] if len(converted) == 1 else UnionOf(tuple(converted))
        return Nullable(merged) if nullable else merged

    def convert(self, schema: Any, hint: str, where: str = "") -> TypeRef:
        if schema is True or schema == {}:
            return ANY
        if not isinstance(schema, dict):
            raise SpecError(f"The schema for {hint} is not an object")
        if "$ref" in schema:
            return Ref(self.ref_name(schema["$ref"]))
        for union_keyword in UNION_KEYWORDS:
            if union_keyword in schema:
                return self.union(schema[union_keyword], hint, where)
        if "allOf" in schema:
            raise SpecError(f"The schema for {hint} uses allOf, which this generator does not read")
        if "const" in schema:
            return Lit((schema["const"],))
        if "enum" in schema:
            return Lit(tuple(schema["enum"]))
        kind = schema.get("type")
        if kind == "string":
            return STRING
        if kind in ("number", "integer"):
            return NUMBER
        if kind == "boolean":
            return BOOLEAN
        if kind == "null":
            return NULL
        if kind == "array":
            item = self.convert(schema.get("items", True), hint + "Item", f"one item of {where or f'`{hint}`'}")
            size = schema.get("minItems")
            if isinstance(size, int) and size == schema.get("maxItems") and size > 0:
                return TupleOf(item, size)
            return ListOf(item)
        if kind == "object" or "properties" in schema or "additionalProperties" in schema:
            if self.is_model(schema):
                return self.model(hint, schema, where)
            extra = schema.get("additionalProperties")
            if isinstance(extra, dict) and extra:
                return MapOf(self.convert(extra, hint + "Value", f"each value of {where or f'`{hint}`'}"))
            return OPEN_OBJECT
        if kind is None:
            return ANY
        raise SpecError(f"The schema for {hint} has type {kind!r}, which this generator does not read")

    def json_type(self, content: dict[str, Any], hint: str) -> TypeRef:
        if set(content) != {JSON_CONTENT}:
            raise SpecError(f"{hint} offers {sorted(content)}, and the generator reads only {JSON_CONTENT}")
        return self.convert(content[JSON_CONTENT].get("schema", {}), hint)

    def answers(self, operation_id: str, responses: dict[str, Any]) -> tuple[Answer, ...]:
        found: list[Answer] = []
        for status, response in responses.items():
            if status == "default":
                continue
            if "$ref" in response:
                response = self.responses[response["$ref"].rsplit("/", 1)[-1]]
            content = response.get("content")
            typed = self.json_type(content, pascal(operation_id) + "Response" + status) if content else None
            found.append(Answer(int(status), typed, _description(response)))
        if not any(200 <= answer.status < 300 for answer in found):
            raise SpecError(f"Operation {operation_id} documents no successful response")
        return tuple(found)

    def operation(self, path: str, method: str, raw: dict[str, Any], shared: list[Any]) -> Operation:
        operation_id = raw.get("operationId")
        if not isinstance(operation_id, str) or not operation_id:
            raise SpecError(f"{method.upper()} {path} has no operationId, so it would have no method")
        params: list[Param] = []
        for param in [*shared, *raw.get("parameters", [])]:
            if param.get("in") != "path":
                raise SpecError(f"Operation {operation_id} takes a {param.get('in')} parameter, which is not read")
            params.append(Param(json_name=param["name"], description=_description(param)))
        body = None
        if "requestBody" in raw:
            request = raw["requestBody"]
            typed = self.json_type(request["content"], pascal(operation_id) + "Body")
            body = Body(typed, bool(request.get("required", False)), _description(request))
        tags = raw.get("tags") or ["Default"]
        return Operation(
            operation_id=operation_id,
            method=method.upper(),
            path=path,
            tag=tags[0],
            summary=" ".join(str(raw.get("summary", "")).split()),
            params=tuple(params),
            body=body,
            answers=self.answers(operation_id, raw.get("responses", {})),
            sql_statement=raw.get(SQL_STATEMENT_EXTENSION) is True,
        )


def merged_schemas(openapi: dict[str, Any], asyncapi: dict[str, Any]) -> dict[str, Any]:
    schemas: dict[str, Any] = dict(openapi.get("components", {}).get("schemas", {}))
    for name, schema in asyncapi.get("components", {}).get("schemas", {}).items():
        if name in schemas and schemas[name] != schema:
            raise SpecError(f"The OpenAPI and AsyncAPI files describe {name} with two different shapes")
        schemas.setdefault(name, schema)
    return schemas


def build_api(openapi: dict[str, Any], asyncapi: dict[str, Any]) -> Api:
    builder = _Builder(merged_schemas(openapi, asyncapi), openapi.get("components", {}).get("responses", {}))
    for name, schema in builder.schemas.items():
        builder.top_level(name, schema)
    seen: set[str] = set()
    for path, item in openapi.get("paths", {}).items():
        shared = item.get("parameters", [])
        for method in HTTP_METHODS:
            if method not in item:
                continue
            operation = builder.operation(path, method, item[method], shared)
            if operation.operation_id in seen:
                raise SpecError(f"Two operations share the operationId {operation.operation_id}")
            seen.add(operation.operation_id)
            builder.api.operations.append(operation)
    return builder.api
