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

from snowflake.snowpark.functions import sproc, udf
from snowflake.snowpark.types import IntegerType


def helper_fn(data: str) -> str:
    return "infile: " + data


@udf(
    name="echo_fn_1",
    native_app_params={
        "schema": "ext_code_schema",
        "application_roles": ["app_instance_role"],
    },
)
def echo_fn_1(echo: str) -> str:
    return "echo_fn: " + helper_fn(echo)


# UDF name is given, imports and packages are empty, native_app_params empty, should be in the final output
@udf(
    name="echo_fn_2",
    native_app_params={
        "schema": "ext_code_schema",
        "application_roles": ["app_instance_role"],
    },
)
def echo_fn_2(echo: str) -> str:
    return "echo_fn: " + echo


# Inconsequential UDF Params, should have no effect on the DDL, should be in the final output
@udf(
    name="echo_fn_4",
    is_permanent=True,
    stage_location="@some_stage",
    replace=False,
    if_not_exists=True,
    session=None,
    parallel=4,
    max_batch_size=2,
    statement_params={},
    strict=True,
    secure=True,
    immutable=True,
    comment="some comment",
    native_app_params={
        "schema": "ext_code_schema",
        "application_roles": ["app_instance_role"],
    },
)
def echo_fn_4(echo: str) -> str:
    return "echo_fn: " + echo


@sproc(
    return_type=IntegerType(),
    input_types=[IntegerType(), IntegerType()],
    packages=["snowflake-snowpark-python"],
    native_app_params={
        "schema": "ext_code_schema",
        "application_roles": ["app_instance_role"],
    },
)
def add_sp(session_, x, y):
    return x + y
