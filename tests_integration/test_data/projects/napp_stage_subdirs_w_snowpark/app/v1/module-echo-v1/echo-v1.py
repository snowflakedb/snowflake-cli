# This is where you can create python functions, which can further
# be used to create Snowpark UDFs and Stored Procedures in your setup_script.sql file.

from snowflake.snowpark.functions import udf

# UDF example:
# decorated example
@udf(
    name="echo_fn",
    native_app_params={"schema": "core", "application_roles": ["app_public"]},
)
def echo_fn(data: str) -> str:
    return "echo_fn, v1 implementation: " + data
