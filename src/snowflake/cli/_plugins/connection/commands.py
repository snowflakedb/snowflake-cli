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
from pathlib import Path
from typing import Optional

import typer
from click import (  # type: ignore
    ClickException,
    Context,
    Parameter,
    UsageError,
)
from click.core import ParameterSource  # type: ignore
from snowflake import connector
from snowflake.cli._plugins.connection.diagnostic import (
    collect_network_policy,
    run_diagnostic,
    status_line,
)
from snowflake.cli._plugins.connection.util import (
    strip_if_value_present,
)
from snowflake.cli._plugins.object.manager import ObjectManager
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
    ProtocolOption,
    RoleOption,
    SchemaOption,
    SecondaryRolesOption,
    TokenFilePathOption,
    UserOption,
    WarehouseOption,
    WorkloadIdentityProviderOption,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.config import (
    ConnectionConfig,
    add_connection_to_proper_file,
    connection_exists,
    get_all_connections,
    get_connection_dict,
    get_default_connection_name,
    remove_connection_from_proper_file,
    set_config_value,
    unset_config_value,
)
from snowflake.cli.api.config_ng.masking import mask_sensitive_value
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB, ObjectType
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    ObjectResult,
)
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


def mask_sensitive_parameters(connection_params: dict):
    return {
        key: mask_sensitive_value(key, value)
        for key, value in connection_params.items()
    }


@app.command(name="list")
def list_connections(
    all_sources: bool = typer.Option(
        False,
        "--all",
        "-a",
        help="Include connections from all sources (environment variables, SnowSQL config). "
        "By default, only shows connections from configuration files.",
    ),
    **options,
) -> CommandResult:
    """
    Lists configured connections.
    """
    from snowflake.cli.api.config_provider import (
        get_config_provider_singleton,
        is_alternative_config_enabled,
    )

    # Use provider directly for config_ng to pass the flag
    if is_alternative_config_enabled():
        provider = get_config_provider_singleton()
        connections = provider.get_all_connections(include_env_connections=all_sources)
    else:
        # Legacy provider ignores the flag
        connections = get_all_connections()

    default_connection = get_default_connection_name()
    result = (
        {
            "connection_name": connection_name,
            "parameters": mask_sensitive_parameters(
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
    protocol: Optional[str] = typer.Option(
        None,
        *ProtocolOption.param_decls,
        help="Protocol to use for the connection, for example `https`.",
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
    workload_identity_provider: Optional[str] = typer.Option(
        None,
        "-W",
        *WorkloadIdentityProviderOption.param_decls,
        help="Workload identity provider type",
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
    secondary_roles: Optional[str] = typer.Option(
        None,
        *SecondaryRolesOption.param_decls,
        help=(
            "Secondary roles mode applied when the session starts. "
            "Supported values are `ALL` and `NONE`; pass `NONE` to run the "
            "session only with the primary role."
        ),
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
        "protocol": protocol,
        "region": region,
        "authenticator": authenticator,
        "workload_identity_provider": workload_identity_provider,
        "private_key_file": private_key_file,
        "token_file_path": token_file_path,
        "secondary_roles": secondary_roles,
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

    connections_file = add_connection_to_proper_file(
        connection_name,
        ConnectionConfig(**connection_options),
    )
    if set_as_default:
        set_config_value(path=["default_connection_name"], value=connection_name)

    return MessageResult(
        f"Wrote new connection {connection_name} to {connections_file}"
    )


@app.command(requires_connection=False)
def remove(
    connection_name: str = typer.Argument(
        help="Name of the connection to remove.",
        show_default=False,
    ),
    **options,
):
    """Removes a connection from configuration file."""
    if not connection_exists(connection_name):
        raise UsageError(f"Connection {connection_name} does not exist.")

    is_default = get_default_connection_name() == connection_name
    if is_default:
        unset_config_value(path=["default_connection_name"])

    connections_file = remove_connection_from_proper_file(connection_name)

    return MessageResult(
        f"Removed connection {connection_name} from {connections_file}."
        f"{' It was the default connection, so default connection is now unset.' if is_default else ''}"
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
        "Role": f"{conn.role or 'not set'}",
        "Database": f"{conn.database or 'not set'}",
        "Warehouse": f"{conn.warehouse or 'not set'}",
    }

    if conn_ctx.enable_diag:
        result["Diag Report Location"] = os.path.join(
            conn_ctx.diag_log_path, "SnowflakeConnectionTestReport.txt"
        )
        return _connection_test_with_diag(conn, conn_ctx, result)

    return ObjectResult(result)


def _connection_test_with_diag(
    conn,
    conn_ctx,
    connection_summary: dict,
) -> CommandResult:
    """Run the SnowCD-style per-endpoint diagnostic and assemble the full result.

    Streams `Checking <TYPE>: <host> <icon>` lines for human (TABLE) output and
    appends a per-endpoint table plus a `Results: ...` summary line. For JSON
    output the streaming is suppressed and the structured payload carries the
    diagnostic data instead.
    """
    from snowflake.cli.api.output.formats import OutputFormat
    from snowflake.cli.api.output.types import MultipleResults

    is_table_output = get_cli_context().output_format == OutputFormat.TABLE

    if is_table_output:
        cli_console.message("Fetching allowlist from Snowflake...")

    def _stream(check):
        if is_table_output:
            cli_console.message(status_line(check))

    report = run_diagnostic(
        conn=conn,
        allowlist_path=conn_ctx.diag_allowlist_path,
        on_check=_stream,
    )

    if is_table_output:
        cli_console.message("Inspecting network policies...")
    policy = collect_network_policy(conn, user=conn.user)

    diagnostic_payload = {
        "checks": [c.to_dict() for c in report.checks],
        "healthy": report.healthy,
        "unhealthy": report.unhealthy,
        "skipped": report.skipped,
        "tested": report.tested,
        "network_policy": policy.to_dict(),
    }
    connection_summary["Diagnostic"] = diagnostic_payload

    if not is_table_output:
        return ObjectResult(connection_summary)

    results = MultipleResults()
    results.add(ObjectResult(connection_summary))
    tested_rows = [
        {
            "url": c.host,
            "type": c.type,
            "status": c.status,
            "latency_ms": c.latency_ms if c.latency_ms is not None else "",
            "issuer": c.cert_issuer or "",
            "cert_expires": c.cert_expires or "",
        }
        for c in report.checks
        if c.status != "Skipped"
    ]
    if tested_rows:
        results.add(CollectionResult(tested_rows))
    if policy.has_policy():
        results.add(
            ObjectResult(
                {
                    "Effective network policy": policy.effective_policy,
                    "Source": "user" if policy.user_policy else "account",
                    "Account-level": policy.account_policy or "(none)",
                    "User-level": policy.user_policy or "(none)",
                    "Current IP": policy.current_ip or "(unknown)",
                    "Allowed IPs": ", ".join(policy.allowed_ip_list) or "(none)",
                    "Blocked IPs": ", ".join(policy.blocked_ip_list) or "(none)",
                    "Allowed network rules": ", ".join(policy.allowed_rule_list)
                    or "(none)",
                    "Blocked network rules": ", ".join(policy.blocked_rule_list)
                    or "(none)",
                }
            )
        )
        if policy.rules:
            results.add(
                CollectionResult(
                    [
                        {
                            "rule": r.name,
                            "mode": r.mode,
                            "type": r.type,
                            "values": ", ".join(r.values),
                        }
                        for r in policy.rules
                    ]
                )
            )
    elif policy.current_ip:
        results.add(
            MessageResult(
                f"No network policy in effect (current IP: {policy.current_ip})."
            )
        )
    results.add(MessageResult(report.summary_line()))
    return results


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

    # `PRIVATE_KEY_PASSPHRASE` env var takes precedence over the config-sourced
    # `private_key_passphrase` (populated from `private_key_file_pwd` or
    # `private_key_passphrase` in connections.toml) for back-compat.
    env_passphrase = os.getenv("PRIVATE_KEY_PASSPHRASE")
    passphrase = (
        env_passphrase
        if env_passphrase is not None
        else connection_details.private_key_passphrase
    )

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


@app.command(requires_connection=True)
def generate_workload_identity_token(
    **options,
) -> CommandResult:
    """Generate a workload identity token for authenticating to Snowflake via a Workload Identity Federation (WIF) provider (AWS, AZURE, GCP, or OIDC). The token is printed to stdout."""
    from snowflake.connector.wif_util import AttestationProvider, create_attestation

    connection_details = get_cli_context().connection_context.update_from_config()

    if not connection_details.workload_identity_provider:
        raise UsageError(
            "Workload identity provider is not set in the connection context, "
            "but required for workload identity token generation."
        )

    try:
        provider = AttestationProvider.from_string(
            connection_details.workload_identity_provider
        )

        token = None
        if provider == AttestationProvider.OIDC:
            if connection_details.token:
                token = connection_details.token
            elif connection_details.token_file_path:
                token = (
                    SecurePath(connection_details.token_file_path)
                    .read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB)
                    .strip()
                )
            else:
                raise UsageError(
                    "OIDC provider requires a token. "
                    "Set --token-file-path or configure 'token' in the connection."
                )

        attestation = create_attestation(provider=provider, token=token)
        return MessageResult(attestation.credential)
    except (UsageError, ClickException):
        raise
    except (ValueError, TypeError, ProgrammingError) as err:
        raise ClickException(str(err))
