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

from __future__ import annotations

import logging
import os.path

import typer
from click import ClickException, Context, Parameter  # type: ignore
from click.core import ParameterSource  # type: ignore
from click.types import StringParamType
from snowflake.cli._plugins.connection.util import (
    strip_and_check_if_exists,
    strip_if_value_present,
)
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.flags import (
    PLAIN_PASSWORD_MSG,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.config import (
    ConnectionConfig,
    add_connection,
    connection_exists,
    get_all_connections,
    get_connection_dict,
    get_default_connection_name,
    set_config_value,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    ObjectResult,
)
from snowflake.connector import ProgrammingError
from snowflake.connector.config_manager import CONFIG_MANAGER

app = SnowTyperFactory(
    name="connection",
    help="Manages connections to Snowflake.",
)
log = logging.getLogger(__name__)


class EmptyInput:
    def __repr__(self):
        return "optional"


class OptionalPrompt(StringParamType):
    def convert(self, value, param, ctx):
        return None if isinstance(value, EmptyInput) else value


def _mask_password(connection_params: dict):
    if "password" in connection_params:
        connection_params["password"] = "****"
    return connection_params


@app.command(name="list")
def list_connections(**options) -> CommandResult:
    """
    Lists configured connections.
    """
    connections = get_all_connections()
    default_connection = get_default_connection_name()
    result = (
        {
            "connection_name": connection_name,
            "parameters": _mask_password(
                connection_config.to_dict_of_known_non_empty_values()
            ),
            "is_default": connection_name == default_connection,
        }
        for connection_name, connection_config in connections.items()
    )
    return CollectionResult(result)


def require_integer(field_name: str):
    def callback(value: str):
        if value is None:
            return None
        if value.strip().isdigit():
            return value.strip()
        raise ClickException(f"Value of {field_name} must be integer")

    return callback


def _password_callback(ctx: Context, param: Parameter, value: str):
    if value and ctx.get_parameter_source(param.name) == ParameterSource.COMMANDLINE:  # type: ignore
        cli_console.warning(PLAIN_PASSWORD_MSG)

    return value


@app.command()
def add(
    connection_name: str = typer.Option(
        None,
        "--connection-name",
        "-n",
        prompt="Name for this connection",
        help="Name of the new connection.",
        show_default=False,
        callback=strip_if_value_present,
    ),
    account: str = typer.Option(
        None,
        "--account",
        "-a",
        "--accountname",
        prompt="Snowflake account name",
        help="Account name to use when authenticating with Snowflake.",
        show_default=False,
        callback=strip_if_value_present,
    ),
    user: str = typer.Option(
        None,
        "--user",
        "-u",
        "--username",
        prompt="Snowflake username",
        show_default=False,
        help="Username to connect to Snowflake.",
        callback=strip_if_value_present,
    ),
    password: str = typer.Option(
        EmptyInput(),
        "--password",
        "-p",
        click_type=OptionalPrompt(),
        callback=_password_callback,
        prompt="Snowflake password",
        help="Snowflake password.",
        hide_input=True,
    ),
    role: str = typer.Option(
        EmptyInput(),
        "--role",
        "-r",
        click_type=OptionalPrompt(),
        prompt="Role for the connection",
        help="Role to use on Snowflake.",
        callback=strip_if_value_present,
    ),
    warehouse: str = typer.Option(
        EmptyInput(),
        "--warehouse",
        "-w",
        click_type=OptionalPrompt(),
        prompt="Warehouse for the connection",
        help="Warehouse to use on Snowflake.",
        callback=strip_if_value_present,
    ),
    database: str = typer.Option(
        EmptyInput(),
        "--database",
        "-d",
        click_type=OptionalPrompt(),
        prompt="Database for the connection",
        help="Database to use on Snowflake.",
        callback=strip_if_value_present,
    ),
    schema: str = typer.Option(
        EmptyInput(),
        "--schema",
        "-s",
        click_type=OptionalPrompt(),
        prompt="Schema for the connection",
        help="Schema to use on Snowflake.",
        callback=strip_if_value_present,
    ),
    host: str = typer.Option(
        EmptyInput(),
        "--host",
        "-h",
        click_type=OptionalPrompt(),
        prompt="Connection host",
        help="Host name the connection attempts to connect to Snowflake.",
        callback=strip_if_value_present,
    ),
    port: int = typer.Option(
        EmptyInput(),
        "--port",
        "-P",
        click_type=OptionalPrompt(),
        prompt="Connection port",
        help="Port to communicate with on the host.",
        callback=require_integer(field_name="port"),
    ),
    region: str = typer.Option(
        EmptyInput(),
        "--region",
        "-R",
        click_type=OptionalPrompt(),
        prompt="Snowflake region",
        help="Region name if not the default Snowflake deployment.",
        callback=strip_if_value_present,
    ),
    authenticator: str = typer.Option(
        EmptyInput(),
        "--authenticator",
        "-A",
        click_type=OptionalPrompt(),
        prompt="Authentication method",
        help="Chosen authenticator, if other than password-based",
    ),
    private_key_file: str = typer.Option(
        EmptyInput(),
        "--private-key",
        "--private-key-path",
        "-k",
        click_type=OptionalPrompt(),
        prompt="Path to private key file",
        help="Path to file containing private key",
        callback=strip_and_check_if_exists,
    ),
    token_file_path: str = typer.Option(
        EmptyInput(),
        "--token-file-path",
        "-t",
        click_type=OptionalPrompt(),
        prompt="Path to token file",
        help="Path to file with an OAuth token that should be used when connecting to Snowflake",
        callback=strip_and_check_if_exists,
    ),
    set_as_default: bool = typer.Option(
        False,
        "--default",
        is_flag=True,
        help="If provided the connection will be configured as default connection.",
    ),
    **options,
) -> CommandResult:
    """Adds a connection to configuration file."""
    if connection_exists(connection_name):
        raise ClickException(f"Connection {connection_name} already exists")

    add_connection(
        connection_name,
        ConnectionConfig(
            account=account,
            user=user,
            password=password,
            host=host,
            region=region,
            port=port,
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role,
            authenticator=authenticator,
            private_key_file=private_key_file,
            token_file_path=token_file_path,
        ),
    )
    if set_as_default:
        set_config_value(
            section=None, key="default_connection_name", value=connection_name
        )

    return MessageResult(
        f"Wrote new connection {connection_name} to {CONFIG_MANAGER.file_path}"
    )


@app.command(requires_connection=True)
def test(
    **options,
) -> CommandResult:
    """
    Tests the connection to Snowflake.
    """

    # Test connection
    cli_context = get_cli_context()
    conn = cli_context.connection

    # Test session attributes
    om = ObjectManager()
    try:
        # "use database" operation changes schema to default "public",
        # so to test schema set up by user we need to copy it here:
        schema = conn.schema

        if conn.role:
            om.use(object_type=ObjectType.ROLE, name=f'"{conn.role}"')
        if conn.database:
            om.use(object_type=ObjectType.DATABASE, name=f'"{conn.database}"')
        if schema:
            om.use(object_type=ObjectType.SCHEMA, name=f'"{schema}"')
        if conn.warehouse:
            om.use(object_type=ObjectType.WAREHOUSE, name=f'"{conn.warehouse}"')

    except ProgrammingError as err:
        raise ClickException(str(err))

    conn_ctx = cli_context.connection_context
    result = {
        "Connection name": conn_ctx.connection_name,
        "Status": "OK",
        "Host": conn.host,
        "Account": conn.account,
        "User": conn.user,
        "Role": f'{conn.role or "not set"}',
        "Database": f'{conn.database or "not set"}',
        "Warehouse": f'{conn.warehouse or "not set"}',
    }

    if conn_ctx.enable_diag:
        result["Diag Report Location"] = os.path.join(
            conn_ctx.diag_log_path, "SnowflakeConnectionTestReport.txt"
        )

    return ObjectResult(result)


@app.command(requires_connection=False)
def set_default(
    name: str = typer.Argument(
        help="Name of the connection, as defined in your `config.toml`",
        show_default=False,
    ),
    **options,
):
    """Changes default connection to provided value."""
    get_connection_dict(connection_name=name)
    set_config_value(section=None, key="default_connection_name", value=name)
    return MessageResult(f"Default connection set to: {name}")
