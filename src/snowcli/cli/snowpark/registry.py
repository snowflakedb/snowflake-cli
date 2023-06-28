from __future__ import annotations

import json
import sys

import typer
from rich import print
from snowcli import config
from snowcli.cli import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.common.flags import ConnectionOption
from snowcli.config import connect_to_snowflake

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS, name="registry", help="Manage registry"
)


@app.command("token")
def get_token(
    environment: str = ConnectionOption,
):
    """
    Get token to authenticate with registry.
    """
    conn = connect_to_snowflake(
        connection_name=environment,
        # to support registry login
        session_parameters={"PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "json"},
    )
    if config.is_auth():
        # disable session deletion
        conn.ctx._all_async_queries_finished = lambda: False
        if conn.ctx._rest is None:
            raise Exception("error in connection object")
        # obtain and create the token
        token_data = conn.ctx._rest._token_request("ISSUE")
        token = {
            "token": token_data["data"]["sessionToken"],
            "expires_in": token_data["data"]["validityInSecondsST"],
        }
        sys.stdout.write(json.dumps(token))
