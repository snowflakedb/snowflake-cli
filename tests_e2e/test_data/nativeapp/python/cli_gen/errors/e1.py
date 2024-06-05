from snowflake.snowpark.functions import udf


# Should be rejected by Snowpark since type hints are absent, sql should not be in the final output.
@udf(name="echo_fn_2")
def echo_fn_2(echo) -> str:
    return "echo_fn: " + echo


"""
Warning message generated by the CLI on skipping this file:
Could not fetch Snowpark objects from /Users/bgoel/snowcli/tests_e2e/test_data/nativeapp/python/cli_gen/errors/e1.py due to the following Snowpark-internal error:
 An exception occurred while executing file:  the number of arguments (1) is different from the number of argument type hints (0)
"""
