from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError, ValidatorFunctionWrapHandler, WrapValidator


class ApiModel(BaseModel):
    """Every request, response, and message model of the Sirannon API derives from this class.

    A model accepts each field under its snake_case attribute name or under the camelCase name that the server
    writes. It also keeps every field that it does not declare, so that an answer from a newer server still
    parses.
    """

    model_config = ConfigDict(
        extra="allow",
        validate_by_name=True,
        validate_by_alias=True,
        serialize_by_alias=True,
        protected_namespaces=(),
        arbitrary_types_allowed=True,
    )


def _keep_unknown_string(value: Any, handler: ValidatorFunctionWrapHandler) -> Any:
    try:
        return handler(value)
    except ValidationError:
        if isinstance(value, str):
            return value
        raise


OPEN_ENUM = WrapValidator(_keep_unknown_string)
