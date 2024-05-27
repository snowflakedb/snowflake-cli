from textwrap import dedent

import pytest
import snowflake.cli.plugins.nativeapp.codegen.snowpark.extension_function_utils as ef_utils
from snowflake.cli.api.project.schemas.snowpark.argument import Argument
from snowflake.cli.plugins.nativeapp.codegen.snowpark.models import (
    NativeAppExtensionFunction,
)


@pytest.mark.parametrize(
    "function_type, expected",
    [
        ("procedure", "PROCEDURE"),
        ("function", "FUNCTION"),
        ("aggregate function", "AGGREGATE FUNCTION"),
        ("table function", "FUNCTION"),
    ],
)
def test_get_sql_object_type(
    function_type, expected, native_app_extension_function_raw_data
):
    native_app_extension_function_raw_data["type"] = function_type
    extension_fn = NativeAppExtensionFunction(**native_app_extension_function_raw_data)
    assert ef_utils.get_sql_object_type(extension_fn) == expected


def test_get_sql_argument_signature():
    arg = Argument(name="foo", type="int")
    assert ef_utils.get_sql_argument_signature(arg) == "foo int"

    arg = Argument(name="foo", type="int", default="42")
    assert ef_utils.get_sql_argument_signature(arg) == "foo int DEFAULT 42"


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


def test_get_function_type_signature_for_grant(native_app_extension_function):
    assert (
        ef_utils.get_function_type_signature_for_grant(native_app_extension_function)
        == "int"
    )

    native_app_extension_function.signature = []
    assert (
        ef_utils.get_function_type_signature_for_grant(native_app_extension_function)
        == ""
    )

    native_app_extension_function.signature = [
        Argument(name="foo", type="int", default="42"),
        Argument(name="bar", type="varchar"),
    ]
    assert (
        ef_utils.get_function_type_signature_for_grant(native_app_extension_function)
        == "int, varchar"
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


TEST_SNOWPARK_CODE = dedent(
    """
import snowflake.snowpark

from typing import Optional
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, sum, sproc


@custom
@sproc(native_app_params={'schema': 'math', 'application_roles': ['app_public', 'app_admin']})
def sproc_sum(session: Session, first: int, second: int) -> int:
    return first + second

@udf(native_app_params={
    'schema': 'math',
    'application_roles': ['app_public', 'app_admin']
})
@module.annotation
def udf_sum(first: int, second: int) -> int:
    return first + second
    
@custom
def helper():
    pass
"""
).strip()


def test_deannotate_module_source_removes_all_annotations(snapshot):
    sproc = NativeAppExtensionFunction(
        type="procedure",
        handler="math_fns.sproc_sum",
        returns="int",
        signature=[
            Argument(name="first", type="int"),
            Argument(name="second", type="int"),
        ],
    )
    udf = NativeAppExtensionFunction(
        type="function",
        handler="math_fns.udf_sum",
        returns="int",
        signature=[
            Argument(name="first", type="int"),
            Argument(name="second", type="int"),
        ],
    )
    assert (
        ef_utils.deannotate_module_source(TEST_SNOWPARK_CODE, [sproc, udf]) == snapshot
    )


def test_deannotate_module_source_preserves_specified_annotations(snapshot):
    sproc = NativeAppExtensionFunction(
        type="procedure",
        handler="math_fns.sproc_sum",
        returns="int",
        signature=[
            Argument(name="first", type="int"),
            Argument(name="second", type="int"),
        ],
    )
    udf = NativeAppExtensionFunction(
        type="function",
        handler="math_fns.udf_sum",
        returns="int",
        signature=[
            Argument(name="first", type="int"),
            Argument(name="second", type="int"),
        ],
    )
    assert (
        ef_utils.deannotate_module_source(
            TEST_SNOWPARK_CODE, [sproc, udf], annotations_to_preserve=["custom"]
        )
        == snapshot
    )
    assert (
        ef_utils.deannotate_module_source(
            TEST_SNOWPARK_CODE,
            [sproc, udf],
            annotations_to_preserve=["module.annotation"],
        )
        == snapshot
    )


def test_deannotate_module_source_is_identity_when_no_functions_present(snapshot):
    sproc = NativeAppExtensionFunction(
        type="procedure",
        handler="math_fns.sproc_sum",
        returns="int",
        signature=[
            Argument(name="first", type="int"),
            Argument(name="second", type="int"),
        ],
    )
    udf = NativeAppExtensionFunction(
        type="function",
        handler="math_fns.udf_sum",
        returns="int",
        signature=[
            Argument(name="first", type="int"),
            Argument(name="second", type="int"),
        ],
    )

    non_annotated_code = dedent(
        """
    if __name__ == "__main__":
        print("All your base are belong to us")
    """
    )

    assert (
        ef_utils.deannotate_module_source(
            non_annotated_code, [sproc, udf], annotations_to_preserve=["custom"]
        )
        == non_annotated_code
    )


def test_deannotate_module_source_is_identity_when_no_annotated_functions_present(
    snapshot,
):
    sproc = NativeAppExtensionFunction(
        type="procedure",
        handler="math_fns.sproc_sum",
        returns="int",
        signature=[
            Argument(name="first", type="int"),
            Argument(name="second", type="int"),
        ],
    )
    udf = NativeAppExtensionFunction(
        type="function",
        handler="math_fns.udf_sum",
        returns="int",
        signature=[
            Argument(name="first", type="int"),
            Argument(name="second", type="int"),
        ],
    )

    non_annotated_code = dedent(
        """
    def foo():
        pass
    """
    )

    assert (
        ef_utils.deannotate_module_source(
            non_annotated_code, [sproc, udf], annotations_to_preserve=["custom"]
        )
        == non_annotated_code
    )


def test_deannotate_module_source_is_identity_when_extension_function_does_not_match(
    snapshot,
):
    sproc = NativeAppExtensionFunction(
        type="procedure",
        handler="math_fns.my_sproc_sum",  # trigger mismatch
        returns="int",
        signature=[
            Argument(name="first", type="int"),
            Argument(name="second", type="int"),
        ],
    )
    udf = NativeAppExtensionFunction(
        type="function",
        handler="math_fns.my_udf_sum",  # trigger mismatch
        returns="int",
        signature=[
            Argument(name="first", type="int"),
            Argument(name="second", type="int"),
        ],
    )

    assert (
        ef_utils.deannotate_module_source(TEST_SNOWPARK_CODE, [sproc, udf])
        == TEST_SNOWPARK_CODE
    )
