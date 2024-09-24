# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from textwrap import dedent

import pytest
import snowflake.cli._plugins.nativeapp.codegen.snowpark.extension_function_utils as ef_utils
from snowflake.cli._plugins.nativeapp.codegen.snowpark.models import (
    NativeAppExtensionFunction,
)
from snowflake.cli.api.project.schemas.v1.snowpark.argument import Argument


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
    
    
@custom
@udtf(
    name="alt_int",
    replace=True,
    output_schema=StructType([StructField("number", IntegerType())]),
    input_types=[IntegerType()],
)
@module.annotation
class Alternator:
    def __init__(self):
        self._positive = True

    def process(self, n):
        for i in range(n):
            if self._positive:
                yield (1,)
            else:
                yield (-1,)
            self._positive = not self._positive
"""
).strip()


def test_deannotate_module_source_removes_all_annotations(os_agnostic_snapshot):
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
    udtf = NativeAppExtensionFunction(
        type="function",
        handler="math_fns.Alternator",
        returns="TABLE",
        signature=[
            Argument(name="n", type="int"),
        ],
    )
    assert (
        ef_utils.deannotate_module_source(TEST_SNOWPARK_CODE, [sproc, udf, udtf])
        == os_agnostic_snapshot
    )


def test_deannotate_module_source_preserves_specified_annotations(os_agnostic_snapshot):
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
    udtf = NativeAppExtensionFunction(
        type="function",
        handler="math_fns.Alternator",
        returns="TABLE",
        signature=[
            Argument(name="n", type="int"),
        ],
    )
    assert (
        ef_utils.deannotate_module_source(
            TEST_SNOWPARK_CODE, [sproc, udf, udtf], annotations_to_preserve=["custom"]
        )
        == os_agnostic_snapshot
    )
    assert (
        ef_utils.deannotate_module_source(
            TEST_SNOWPARK_CODE,
            [sproc, udf, udtf],
            annotations_to_preserve=["module.annotation"],
        )
        == os_agnostic_snapshot
    )


def test_deannotate_module_source_is_identity_when_no_functions_present(
    os_agnostic_snapshot,
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
    udtf = NativeAppExtensionFunction(
        type="function",
        handler="math_fns.Alternator",
        returns="TABLE",
        signature=[
            Argument(name="n", type="int"),
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
            non_annotated_code, [sproc, udf, udtf], annotations_to_preserve=["custom"]
        )
        == non_annotated_code
    )


def test_deannotate_module_source_is_identity_when_no_annotated_functions_present(
    os_agnostic_snapshot,
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
    udtf = NativeAppExtensionFunction(
        type="function",
        handler="math_fns.Alternator",
        returns="TABLE",
        signature=[
            Argument(name="n", type="int"),
        ],
    )

    non_annotated_code = dedent(
        """
    def foo():
        pass
        
    class Bar:
        pass
    """
    )

    assert (
        ef_utils.deannotate_module_source(
            non_annotated_code, [sproc, udf, udtf], annotations_to_preserve=["custom"]
        )
        == non_annotated_code
    )


def test_deannotate_module_source_is_identity_when_extension_function_does_not_match(
    os_agnostic_snapshot,
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
    udtf = NativeAppExtensionFunction(
        type="function",
        handler="math_fns.MyAlternator",  # trigger mismatch
        returns="TABLE",
        signature=[
            Argument(name="n", type="int"),
        ],
    )

    assert (
        ef_utils.deannotate_module_source(TEST_SNOWPARK_CODE, [sproc, udf, udtf])
        == TEST_SNOWPARK_CODE
    )
