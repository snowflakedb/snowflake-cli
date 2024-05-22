from __future__ import annotations

from typing import (
    List,
    Optional,
    Sequence,
)

from click.exceptions import ClickException
from snowflake.cli.api.project.schemas.snowpark.argument import Argument
from snowflake.cli.api.project.util import (
    is_valid_identifier,
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
        formatted = f"{formatted} DEFAULT {arg.default}"
    return formatted


def get_qualified_object_name(extension_fn: NativeAppExtensionFunction) -> str:
    qualified_name = to_identifier(extension_fn.name)
    if extension_fn.schema_name:
        if is_valid_identifier(extension_fn.schema_name):
            qualified_name = f"{extension_fn.schema_name}.{qualified_name}"
        else:
            full_schema = ".".join(
                [
                    to_identifier(schema_part)
                    for schema_part in extension_fn.schema_name.split(".")
                ]
            )
            qualified_name = f"{full_schema}.{qualified_name}"

    return qualified_name


def ensure_string_literal(value: str) -> str:
    """
    Returns the string literal representation of the given value, or the value itself if
    it was already a valid string literal.
    """
    if is_valid_string_literal(value):
        return value
    return to_string_literal(value)


def ensure_all_string_literals(values: Sequence[str]) -> List[str]:
    """
    Ensures that all provided values are valid string literals.

    Returns:
        A list with all values transformed to be valid string literals (as necessary).
    """
    return [ensure_string_literal(value) for value in values]
