from __future__ import annotations

from typing import (
    List,
    Optional,
    Sequence,
)

from click.exceptions import ClickException
from snowflake.cli.api.project.schemas.snowpark.argument import Argument
from snowflake.cli.api.project.util import (
    is_valid_string_literal,
    to_identifier,
    to_string_literal,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.models import (
    ExtensionFunctionTypeEnum,
    NativeAppExtensionFunction,
)


class MalformedExtensionFunctionError(ClickException):
    """Required extension function attribute is missing."""

    def __init__(self, message: str):
        super().__init__(message=message)


def get_sql_object_type(extension_fn: NativeAppExtensionFunction) -> Optional[str]:
    if extension_fn.function_type == ExtensionFunctionTypeEnum.PROCEDURE:
        return "PROCEDURE"
    elif extension_fn.function_type in (
        ExtensionFunctionTypeEnum.FUNCTION,
        ExtensionFunctionTypeEnum.TABLE_FUNCTION,
    ):
        return "FUNCTION"
    elif extension_fn.function_type == extension_fn.function_type.AGGREGATE_FUNCTION:
        return "AGGREGATE FUNCTION"
    else:
        return None


def get_sql_argument_signature(arg: Argument) -> str:
    formatted = f"{arg.name} {arg.arg_type}"
    if arg.default is not None:
        formatted = f"{formatted} DEFAULT {arg.default_value}"
    return formatted


def get_qualified_object_name(extension_fn: NativeAppExtensionFunction) -> str:
    qualified_name = to_identifier(extension_fn.name)
    if extension_fn.schema_name:
        qualified_name = f"{to_identifier(extension_fn.schema_name)}.{qualified_name}"
    return qualified_name


def _is_single_quoted(name: str) -> bool:
    """
    Helper function to do a generic check on whether the provided string is surrounded by single quotes.
    """
    return name.startswith("'") and name.endswith("'")


def _ensure_single_quoted(value: str) -> str:
    if is_valid_string_literal(value):
        return value
    return to_string_literal(value)


def _ensure_all_single_quoted(values: Sequence[str]) -> List[str]:
    """
    Helper function to ensure that a list of object strings is transformed to a list of object strings surrounded by single quotes.
    """
    return [_ensure_single_quoted(value) for value in values]
