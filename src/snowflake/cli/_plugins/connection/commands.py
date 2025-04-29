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
from copy import deepcopy
from pathlib import Path
from typing import Dict, Optional, Tuple

import typer
from click import (  # type: ignore
    ClickException,
    Context,
    Parameter,
    UsageError,
)
from click.core import ParameterSource  # type: ignore
from snowflake import connector
from snowflake.cli._app.snow_connector import connect_to_snowflake
from snowflake.cli._plugins.auth.keypair.commands import KEY_PAIR_DEFAULT_PATH
from snowflake.cli._plugins.auth.keypair.manager import AuthManager
from snowflake.cli._plugins.connection.util import (
    strip_if_value_present,
)
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api import exceptions
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.flags import (
    PLAIN_PASSWORD_MSG,
    AccountOption,
    AuthenticatorOption,
    DatabaseOption,
    HostOption,
    NoInteractiveOption,
    PasswordOption,
    PortOption,
    PrivateKeyPathOption,
    RoleOption,
    SchemaOption,
    TokenFilePathOption,
    UserOption,
    WarehouseOption,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.config import (
    ConnectionConfig,
    add_connection_to_proper_file,
    connection_exists,
    get_all_connections,
    get_connection_dict,
    get_default_connection_name,
    set_config_value,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    ObjectResult,
)
from snowflake.cli.api.secret import SecretType
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector import ProgrammingError
from snowflake.connector.constants import CONNECTIONS_FILE

app = SnowTyperFactory(
    name="connection",
    help="Manages connections to Snowflake.",
)
log = logging.getLogger(__name__)


class EmptyInput:
    def __repr__(self):
        return "optional"


def _mask_sensitive_parameters(connection_params: dict):
    if "password" in connection_params:
        connection_params["password"] = "****"
    if "oauth_client_secret" in connection_params:
        connection_params["oauth_client_secret"] = "****"
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
            "parameters": _mask_sensitive_parameters(
                connection_config.to_dict_of_known_non_empty_values()
            ),
            "is_default": connection_name == default_connection,
        }
        for connection_name, connection_config in connections.items()
    )

    if CONNECTIONS_FILE.exists():
        cli_console.warning(
            f"Reading connections from {CONNECTIONS_FILE}. Entries from config.toml are ignored."
        )
    return CollectionResult(result)


def require_integer(field_name: str):
    def callback(ctx: Context, param: Parameter, value: str):
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
        help="Name of the new connection.",
        show_default=False,
    ),
    account: str = typer.Option(
        None,
        "-a",
        *AccountOption.param_decls,
        help="Account name to use when authenticating with Snowflake.",
        show_default=False,
    ),
    user: str = typer.Option(
        None,
        "-u",
        *UserOption.param_decls,
        show_default=False,
        help="Username to connect to Snowflake.",
    ),
    password: Optional[str] = typer.Option(
        None,
        "-p",
        *PasswordOption.param_decls,
        callback=_password_callback,
        help="Snowflake password.",
        hide_input=True,
    ),
    role: Optional[str] = typer.Option(
        None,
        "-r",
        *RoleOption.param_decls,
        help="Role to use on Snowflake.",
    ),
    warehouse: Optional[str] = typer.Option(
        None,
        "-w",
        *WarehouseOption.param_decls,
        help="Warehouse to use on Snowflake.",
    ),
    database: Optional[str] = typer.Option(
        None,
        "-d",
        *DatabaseOption.param_decls,
        help="Database to use on Snowflake.",
    ),
    schema: Optional[str] = typer.Option(
        None,
        "-s",
        *SchemaOption.param_decls,
        help="Schema to use on Snowflake.",
    ),
    host: Optional[str] = typer.Option(
        None,
        "-h",
        *HostOption.param_decls,
        help="Host name the connection attempts to connect to Snowflake.",
    ),
    port: Optional[int] = typer.Option(
        None,
        "-P",
        *PortOption.param_decls,
        help="Port to communicate with on the host.",
    ),
    region: Optional[str] = typer.Option(
        None,
        "--region",
        "-R",
        help="Region name if not the default Snowflake deployment.",
    ),
    authenticator: Optional[str] = typer.Option(
        None,
        "-A",
        *AuthenticatorOption.param_decls,
        help="Chosen authenticator, if other than password-based",
    ),
    private_key_file: Optional[str] = typer.Option(
        None,
        "--private-key",
        "-k",
        *PrivateKeyPathOption.param_decls,
        help="Path to file containing private key",
    ),
    token_file_path: Optional[str] = typer.Option(
        None,
        "-t",
        *TokenFilePathOption.param_decls,
        help="Path to file with an OAuth token that should be used when connecting to Snowflake",
    ),
    set_as_default: bool = typer.Option(
        False,
        "--default",
        is_flag=True,
        help="If provided the connection will be configured as default connection.",
    ),
    no_interactive: bool = NoInteractiveOption,
    **options,
) -> CommandResult:
    """Adds a connection to configuration file."""
    connection_options = {
        "connection_name": connection_name,
        "account": account,
        "user": user,
        "password": password,
        "role": role,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
        "host": host,
        "port": port,
        "region": region,
        "authenticator": authenticator,
        "private_key_file": private_key_file,
        "token_file_path": token_file_path,
    }

    if not no_interactive:
        for option in connection_options:
            if connection_options[option] is None:
                connection_options[option] = typer.prompt(
                    f"Enter {option.replace('_', ' ')}",
                    default="",
                    value_proc=lambda x: None if not x else x,
                    hide_input=option == "password",
                    show_default=False,
                )
            if isinstance(connection_options[option], str):
                connection_options[option] = strip_if_value_present(
                    connection_options[option]
                )

    if (value := connection_options["port"]) is not None:
        connection_options["port"] = int(value)

    if (path := connection_options["private_key_file"]) is not None:
        if not Path(str(path)).exists():
            raise UsageError(f"Path {path} does not exist.")

    if (path := connection_options["token_file_path"]) is not None:
        if not Path(str(path)).exists():
            raise UsageError(f"Path {path} does not exist.")

    connection_name = str(connection_options["connection_name"])
    del connection_options["connection_name"]

    if connection_exists(connection_name):
        raise UsageError(f"Connection {connection_name} already exists")

    if FeatureFlag.ENABLE_AUTH_KEYPAIR.is_enabled() and not no_interactive:
        connection_options, keypair_error = _extend_add_with_key_pair(
            connection_name, connection_options
        )
    else:
        keypair_error = ""

    connections_file = add_connection_to_proper_file(
        connection_name,
        ConnectionConfig(**connection_options),
    )
    if set_as_default:
        set_config_value(path=["default_connection_name"], value=connection_name)

    if keypair_error:
        return MessageResult(
            f"Wrote new password-based connection {connection_name} to {connections_file}, "
            f"however there were some issues during key pair setup. Review the following error and check 'snow auth keypair' "
            f"commands to setup key pair authentication:\n * {keypair_error}"
        )
    return MessageResult(
        f"Wrote new connection {connection_name} to {connections_file}"
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
        help="Name of the connection, as defined in your `config.toml` file",
        show_default=False,
    ),
    **options,
):
    """Changes default connection to provided value."""
    get_connection_dict(connection_name=name)
    set_config_value(path=["default_connection_name"], value=name)
    return MessageResult(f"Default connection set to: {name}")


@app.command(requires_connection=True)
def generate_jwt(
    **options,
) -> CommandResult:
    """Generate a JWT token, which will be printed out and displayed.."""
    connection_details = get_cli_context().connection_context.update_from_config()

    msq_template = (
        "{} is not set in the connection context, but required for JWT generation."
    )
    if not connection_details.user:
        raise UsageError(msq_template.format("User"))
    if not connection_details.account:
        raise UsageError(msq_template.format("Account"))
    if not connection_details.private_key_file:
        raise UsageError(msq_template.format("Private key file"))

    passphrase = os.getenv("PRIVATE_KEY_PASSPHRASE", None)

    def _decrypt(passphrase: str | None):
        return connector.auth.get_token_from_private_key(
            user=connection_details.user,
            account=connection_details.account,
            privatekey_path=connection_details.private_key_file,
            key_password=passphrase,
        )

    try:
        if passphrase is None:
            try:
                token = _decrypt(passphrase=None)
                return MessageResult(token)
            except TypeError:
                passphrase = typer.prompt(
                    "Enter private key file password (press enter for empty)",
                    hide_input=True,
                    type=str,
                    default="",
                )
        token = _decrypt(passphrase=passphrase)
        return MessageResult(token)
    except (ValueError, TypeError) as err:
        raise ClickException(str(err))


def _extend_add_with_key_pair(
    connection_name: str, connection_options: Dict
) -> Tuple[Dict, str]:
    if not _should_extend_with_key_pair(connection_options):
        return connection_options, ""

    configure_key_pair = typer.confirm(
        "Do you want to configure key pair authentication?",
        default=False,
    )
    if not configure_key_pair:
        return connection_options, ""

    key_length = typer.prompt(
        "Key length",
        default=2048,
        show_default=True,
    )

    output_path = typer.prompt(
        "Output path",
        default=KEY_PAIR_DEFAULT_PATH,
        show_default=True,
        value_proc=lambda value: SecurePath(value),
    )
    private_key_passphrase = typer.prompt(
        "Private key passphrase",
        default="",
        hide_input=True,
        show_default=False,
        value_proc=lambda value: SecretType(value),
    )
    connection = connect_to_snowflake(temporary_connection=True, **connection_options)
    try:
        connection_options = AuthManager(connection=connection).extend_connection_add(
            connection_name=connection_name,
            connection_options=deepcopy(connection_options),
            key_length=key_length,
            output_path=output_path,
            private_key_passphrase=private_key_passphrase,
        )
    except exceptions.CouldNotSetKeyPairError:
        return connection_options, "The public key is set already."
    except Exception as e:
        return connection_options, str(e)
    return connection_options, ""


def _should_extend_with_key_pair(connection_options: Dict) -> bool:
    return (
        connection_options.get("password") is not None
        and connection_options.get("private_key_file") is None
        and connection_options.get("private_key_path") is None
    )
