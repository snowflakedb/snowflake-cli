# from cli_gen.helper import return_with_hw
from snowflake.snowpark.functions import sproc, udf
from snowflake.snowpark.types import IntegerType


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


# UDF name is given, additional imports are given, packages is empty, should be in the final output
# @udf(
#     name="echo_fn_3",
#     imports=["cli_gen/helper.py"],
#     native_app_params={
#         "schema": "ext_code_schema",
#         "application_roles": ["app_instance_role"],
#     },
# )
# def echo_fn_3(echo: str) -> str:
#     return return_with_hw(echo)


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
