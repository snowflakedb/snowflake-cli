from __future__ import annotations

from typing import Any, Callable, Optional

import typer
from snowflake.cli.api.cli_global_context import cli_context_manager
from snowflake.cli.api.output.formats import OutputFormat

DEFAULT_CONTEXT_SETTINGS = {"help_option_names": ["--help", "-h"]}

_CONNECTION_SECTION = "Connection configuration"
_CLI_BEHAVIOUR = "Global configuration"


def _callback(provide_setter: Callable[[], Callable[[Any], Any]]):
    def callback(value):
        set_value = provide_setter()
        set_value(value)
        return value

    return callback


ConnectionOption = typer.Option(
    None,
    "--connection",
    "-c",
    "--environment",
    help=f"Name of the connection, as defined in your `config.toml`. Default: `default`.",
    callback=_callback(
        lambda: cli_context_manager.connection_context.set_connection_name
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

TemporaryConnectionOption = typer.Option(
    False,
    "--temporary-connection",
    "-x",
    help="Uses connection defined with command line parameters, instead of one defined in config",
    callback=_callback(
        lambda: cli_context_manager.connection_context.set_temporary_connection
    ),
    is_flag=True,
    rich_help_panel=_CONNECTION_SECTION,
)

AccountOption = typer.Option(
    None,
    "--account",
    "--accountname",
    help="Name assigned to your Snowflake account. Overrides the value specified for the connection.",
    callback=_callback(lambda: cli_context_manager.connection_context.set_account),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

UserOption = typer.Option(
    None,
    "--user",
    "--username",
    help="Username to connect to Snowflake. Overrides the value specified for the connection.",
    callback=_callback(lambda: cli_context_manager.connection_context.set_user),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

PasswordOption = typer.Option(
    None,
    "--password",
    help="Snowflake password. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_callback(lambda: cli_context_manager.connection_context.set_password),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

AuthenticatorOption = typer.Option(
    None,
    "--authenticator",
    help="Snowflake authenticator. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_callback(
        lambda: cli_context_manager.connection_context.set_authenticator
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

PrivateKeyPathOption = typer.Option(
    None,
    "--private-key-path",
    help="Snowflake private key path. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_callback(
        lambda: cli_context_manager.connection_context.set_private_key_path
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    file_okay=True,
    dir_okay=False,
)

DatabaseOption = typer.Option(
    None,
    "--database",
    "--dbname",
    help="Database to use. Overrides the value specified for the connection.",
    callback=_callback(lambda: cli_context_manager.connection_context.set_database),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

SchemaOption = typer.Option(
    None,
    "--schema",
    "--schemaname",
    help="Database schema to use. Overrides the value specified for the connection.",
    callback=_callback(lambda: cli_context_manager.connection_context.set_schema),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

RoleOption = typer.Option(
    None,
    "--role",
    "--rolename",
    help="Role to use. Overrides the value specified for the connection.",
    callback=_callback(lambda: cli_context_manager.connection_context.set_role),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

WarehouseOption = typer.Option(
    None,
    "--warehouse",
    help="Warehouse to use. Overrides the value specified for the connection.",
    callback=_callback(lambda: cli_context_manager.connection_context.set_warehouse),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

OutputFormatOption = typer.Option(
    OutputFormat.TABLE.value,
    "--format",
    help="Specifies the output format.",
    case_sensitive=False,
    callback=_callback(lambda: cli_context_manager.set_output_format),
    rich_help_panel=_CLI_BEHAVIOUR,
)

SilentOption = typer.Option(
    False,
    "--silent",
    help="Turns off intermediate output to console.",
    callback=_callback(lambda: cli_context_manager.set_silent),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
)

VerboseOption = typer.Option(
    False,
    "--verbose",
    "-v",
    help="Displays log entries for log levels `info` and higher.",
    callback=_callback(lambda: cli_context_manager.set_verbose),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
)

DebugOption = typer.Option(
    False,
    "--debug",
    help="Displays log entries for log levels `debug` and higher; debug logs contains additional information.",
    callback=_callback(lambda: cli_context_manager.set_enable_tracebacks),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
)

LikeOption = typer.Option(
    "%%",
    "--like",
    "-l",
    help='Regular expression for filtering objects by name. For example, `list --like "my%"` lists all objects that begin with “my”.',
)


def experimental_option(
    experimental_behaviour_description: Optional[str] = None,
) -> typer.Option:
    help_text = (
        f"Turns on experimental behaviour of the command: {experimental_behaviour_description}"
        if experimental_behaviour_description
        else "Turns on experimental behaviour of the command."
    )
    return typer.Option(
        False,
        "--experimental",
        help=help_text,
        hidden=True,
        callback=_callback(lambda: cli_context_manager.set_experimental),
        is_flag=True,
        rich_help_panel=_CLI_BEHAVIOUR,
    )


def identifier_argument(sf_object: str, example: str) -> typer.Argument:
    return typer.Argument(
        ..., help=f"Identifier of the {sf_object}. For example: {example}"
    )


def execution_identifier_argument(sf_object: str, example: str) -> typer.Argument:
    return typer.Argument(
        ..., help=f"Execution identifier of the {sf_object}. For example: {example}"
    )


def project_definition_option(project_name: str):
    from snowflake.cli.api.exceptions import NoProjectDefinitionError
    from snowflake.cli.api.project.definition_manager import DefinitionManager

    def _callback(project_path: Optional[str]):
        dm = DefinitionManager(project_path)
        project_definition = dm.project_definition.get(project_name)
        project_root = dm.project_root

        if not project_definition:
            raise NoProjectDefinitionError(
                project_type=project_name, project_file=project_path
            )

        cli_context_manager.set_project_definition(project_definition)
        cli_context_manager.set_project_root(project_root)
        return project_definition

    return typer.Option(
        None,
        "-p",
        "--project",
        help=f"Path where the {project_name.replace('_', ' ').capitalize()} project resides. "
        f"Defaults to current working directory.",
        callback=_callback,
        show_default=False,
    )
