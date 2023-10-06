from snowcli.cli.common.flags import ConnectionOption
from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager
from snowcli.snow_connector import connect_to_snowflake


def get_token():
    """
    Get token to authenticate with registry.
    """
    conn = snow_cli_global_context_manager.get_connection()
    conn.execute_string(
        "alter session set PYTHON_CONNECTOR_QUERY_RESULT_FORMAT = 'json'"
    )
    # disable session deletion
    conn._all_async_queries_finished = lambda: False
    if conn._rest is None:
        raise Exception("Failed to connect to Snowflake to retrieve token.")
    # obtain and create the token
    token_data = conn._rest._token_request("ISSUE")

    return {
        "token": token_data["data"]["sessionToken"],
        "expires_in": token_data["data"]["validityInSecondsST"],
    }
