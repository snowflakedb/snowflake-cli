import pytest
import snowflake.cli.plugins.nativeapp.codegen.snowpark.extension_function_utils as ef_utils
from snowflake.cli.plugins.nativeapp.codegen.snowpark.models import (
    NativeAppExtensionFunction,
)


@pytest.mark.parametrize(
    "function_type, expected",
    [
        ("PROCEDURE", "PROCEDURE"),
        ("FUNCTION", "FUNCTION"),
        ("AGGREGATE_FUNCTION", "AGGREGATE FUNCTION"),
        ("TABLE_FUNCTION", "FUNCTION"),
    ],
)
def test_get_object_type_as_text(
    function_type, expected, native_app_extension_function_raw_data
):
    native_app_extension_function_raw_data["function_type"] = function_type
    extension_fn = NativeAppExtensionFunction(**native_app_extension_function_raw_data)
    assert ef_utils.get_sql_object_type(extension_fn) == expected


def test_get_qualified_object_name(native_app_extension_function):
    native_app_extension_function.name = "foo"
    native_app_extension_function.schema_name = None

    assert ef_utils.get_qualified_object_name(native_app_extension_function) == "foo"

    native_app_extension_function.name = "foo"
    native_app_extension_function.schema_name = "my_schema"

    assert (
        ef_utils.get_qualified_object_name(native_app_extension_function)
        == "my_schema.foo"
    )

    native_app_extension_function.name = "foo"
    native_app_extension_function.schema_name = "my schema"

    assert (
        ef_utils.get_qualified_object_name(native_app_extension_function)
        == '"my schema".foo'
    )

    native_app_extension_function.name = "foo"
    native_app_extension_function.schema_name = "my.full.schema"

    assert (
        ef_utils.get_qualified_object_name(native_app_extension_function)
        == "my.full.schema.foo"
    )

    native_app_extension_function.name = "foo"
    native_app_extension_function.schema_name = "my.full schema.with special chars"

    assert (
        ef_utils.get_qualified_object_name(native_app_extension_function)
        == 'my."full schema"."with special chars".foo'
    )


def test_ensure_string_literal():
    assert ef_utils.ensure_string_literal("") == "''"
    assert ef_utils.ensure_string_literal("abc") == "'abc'"
    assert ef_utils.ensure_string_literal("'abc'") == "'abc'"
    assert ef_utils.ensure_string_literal("'abc def'") == "'abc def'"
    assert ef_utils.ensure_string_literal("'abc") == r"'\'abc'"
    assert ef_utils.ensure_string_literal("abc'") == r"'abc\''"


def test_ensure_all_string_literals():
    assert ef_utils.ensure_all_string_literals([]) == []
    assert ef_utils.ensure_all_string_literals(["", "foo", "'bar'"]) == [
        "''",
        "'foo'",
        "'bar'",
    ]
