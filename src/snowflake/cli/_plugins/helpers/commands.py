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

import json
import logging
import os
import platform
from dataclasses import asdict
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, List, Optional

import click
import typer
import yaml
from snowflake.cli._app.version_check import (
    get_version_info,
    record_version_check_displayed,
    suppress_new_version_banner,
)
from snowflake.cli._plugins.helpers.installer_path import clean_installer_path_files
from snowflake.cli._plugins.helpers.snowsl_vars_reader import check_env_vars
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.command_docs import (
    AdmonitionType,
    CommandDocs,
    Example,
    admonition,
    code,
    link,
    plain_text,
    ref,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.config import (
    ConnectionConfig,
    add_connection_to_proper_file,
    get_all_connections,
    get_encoding_diagnostics,
    get_file_io_encoding,
    set_config_value,
)
from snowflake.cli.api.config_provider import ALTERNATIVE_CONFIG_ENV_VAR
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    MultipleResults,
    ObjectResult,
)
from snowflake.cli.api.project.definition_conversion import (
    convert_project_definition_to_v2,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.schemas.project_definition import (
    get_version_map,
)
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

app = SnowTyperFactory(
    name="helpers",
    help="Helper commands.",
)

_HELPERS_RELATED = (
    link("/developer-guide/snowflake-cli/index"),
    link(
        "/developer-guide/snowflake-cli/command-reference/overview",
        "Snowflake CLI command reference",
    ),
    link("/developer-guide/snowflake-cli/command-reference/helpers-commands/overview"),
)


def _format_paths(paths: Iterable[Path]) -> str:
    return ", ".join(sanitize_for_terminal(str(path)) for path in paths)


@app.command(name="clean-installer-path", requires_connection=False)
def clean_installer_path(
    apply_changes: bool = typer.Option(
        False,
        "--apply",
        help="Remove historical installer PATH entries. The default is a dry run.",
    ),
    **options,
) -> CommandResult:
    """Remove PATH entries added by historical macOS installers."""
    if platform.system() != "Darwin":
        return MessageResult(
            "This command is intended only for macOS. "
            "Shell startup files were not scanned."
        )

    get_euid = getattr(os, "geteuid", None)
    if get_euid is not None and get_euid() == 0:
        raise CliError(
            "This command must not be run as root. "
            "Run it as the user whose shell files should be cleaned."
        )

    cleanup = clean_installer_path_files(Path.home(), apply_changes)
    action = "Removed" if apply_changes else "Would remove"
    messages: list[str] = []
    found_entries = False
    for file_cleanup in cleanup.files:
        safe_path = sanitize_for_terminal(str(file_cleanup.path))
        for line_number in file_cleanup.unpaired_comment_lines:
            found_entries = True
            messages.append(
                "Unpaired historical installer comment in "
                f"{safe_path} at line {line_number}; leaving it unchanged."
            )
        if file_cleanup.removed_pairs:
            found_entries = True
            noun = "pair" if file_cleanup.removed_pairs == 1 else "pairs"
            messages.append(
                f"{action} {file_cleanup.removed_pairs} historical installer "
                f"PATH {noun} from {safe_path}."
            )

    for skipped_file in cleanup.skipped_files:
        safe_path = sanitize_for_terminal(str(skipped_file))
        messages.append(f"Skipped {safe_path}; the file could not be read or written.")

    for skipped_symlink in cleanup.skipped_symlinks:
        safe_path = sanitize_for_terminal(str(skipped_symlink))
        messages.append(
            f"Skipped {safe_path}: it is a symlink and is not modified by this command."
        )

    if not found_entries:
        scope = (
            " in the files that could be scanned"
            if cleanup.skipped_files or cleanup.skipped_symlinks
            else ""
        )
        messages.append(f"No historical installer PATH entries found{scope}.")

    # Always account for every shell startup file the command looks at, so a run
    # that finds nothing still tells the user what was actually examined.
    if cleanup.files:
        scanned = _format_paths(entry.path for entry in cleanup.files)
        messages.append(f"Scanned {scanned}.")
    if cleanup.missing_files:
        messages.append(f"Not present: {_format_paths(cleanup.missing_files)}.")

    for message in messages:
        log.debug(message)

    summary = "\n".join(messages)
    if apply_changes and (cleanup.skipped_files or cleanup.skipped_symlinks):
        suffix = (
            "Some shell startup files could not be cleaned. "
            "Check their permissions and run the command again."
        )
        raise CliError(f"{summary}\n{suffix}")
    return MessageResult(summary)


@app.command(
    docs=CommandDocs(
        related=_HELPERS_RELATED,
        usage_notes=(
            plain_text(
                ref("sf-cli"),
                " 3.0 introduced support for V2 project definition files. If you have "
                "existing V1.x project definition files, you can use the ",
                code("snow helpers v1-to-v2"),
                " command to convert the files to the V2 version. The command preserves "
                "the original version in a ",
                code("snowflake_V1.yml"),
                " file.",
            ),
            plain_text(
                "You must run this command in the same directory as the ",
                code("snowflake.yml"),
                " file.",
            ),
            admonition(
                AdmonitionType.ATTENTION,
                "With the change in how ",
                ref("sf-cli"),
                " 3.0 handles project definition templates, Snowflake cannot guarantee "
                "that project definition files using ",
                link(
                    "/developer-guide/snowflake-cli/project-definitions/create-templates",
                    "templates",
                ),
                " will work correctly after conversion. By default, this command generates "
                "an error if you try convert a 1.x file that contains templates. You can "
                "force the command to convert these types of files by using the ",
                code("--accept-templates"),
                " option. Then you must manually update any templates to their V2 "
                "equivalents.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Convert a version 1.x project definition file."
                ),
                command="cd <project-directory>\nsnow helpers v1-to-v2",
                output="Project definition migrated to version 2.",
            ),
            Example(
                description=plain_text("Convert a version 2 project definition file."),
                command="cd <project-directory>\nsnow helpers v1-to-v2",
                output="Project definition is already at version 2.",
            ),
            Example(
                description=plain_text(
                    "Convert a version 1 project definition that contains templates "
                    "without the ",
                    code("--accept-templates"),
                    " option.",
                ),
                command="cd <project-directory>\nsnow helpers v1-to-v2",
                output=(
                    "+- Error---------------------------------------------------------------------+\n"
                    "| Project definition contains templates. They may not be migrated correctly, |\n"
                    "| and require manual migration.You can try again with --accept-templates     |\n"
                    "| option, to attempt automatic migration.                                    |\n"
                    "+----------------------------------------------------------------------------+"
                ),
            ),
            Example(
                description=plain_text(
                    "Convert a version 1 project definition with the ",
                    code("--accept-templates"),
                    " option.",
                ),
                command="cd <project-directory>\nsnow helpers v1-to-v2",
                output=(
                    "WARNING  snowflake.cli._plugins.workspace.commands:commands.py:60 "
                    "Your V1 definition contains templates. We cannot guarantee the "
                    "correctness of the migration.\n"
                    "Project definition migrated to version 2"
                ),
            ),
        ),
    ),
)
def v1_to_v2(
    accept_templates: bool = typer.Option(
        False, "-t", "--accept-templates", help="Allows the migration of templates."
    ),
    migrate_local_yml: Optional[bool] = typer.Option(
        None,
        "-l",
        "--migrate-local-overrides/--no-migrate-local-overrides",
        help=(
            "Merge values in snowflake.local.yml into the main project definition. "
            "The snowflake.local.yml file will not be migrated, "
            "instead its values will be reflected in the output snowflake.yml file. "
            "If unset and snowflake.local.yml is present, an error will be raised."
        ),
        show_default=False,
    ),
    **options,
):
    """Migrates the Snowpark, Streamlit, and Native App project definition files from V1 to V2."""
    manager = DefinitionManager()
    local_yml_path = manager.project_root / "snowflake.local.yml"
    has_local_yml = local_yml_path in manager.project_config_paths
    if has_local_yml:
        if migrate_local_yml is None:
            raise click.ClickException(
                "snowflake.local.yml file detected, "
                "please specify --migrate-local-overrides to include "
                "or --no-migrate-local-overrides to exclude its values."
            )
        if not migrate_local_yml:
            # If we don't want the local file,
            # remove it from the list of paths to load
            manager.project_config_paths.remove(local_yml_path)

    pd = manager.unrendered_project_definition

    if pd.meets_version_requirement("2"):
        return MessageResult("Project definition is already at version 2.")

    pd_v2 = convert_project_definition_to_v2(
        manager.project_root, pd, accept_templates, manager.template_context
    )

    SecurePath("snowflake.yml").rename("snowflake_V1.yml")
    if has_local_yml:
        SecurePath("snowflake.local.yml").rename("snowflake_V1.local.yml")
    with open("snowflake.yml", "w", encoding=get_file_io_encoding()) as file:
        yaml.dump(
            pd_v2.model_dump(
                exclude_unset=True, exclude_none=True, mode="json", by_alias=True
            ),
            file,
            sort_keys=False,
            width=float("inf"),  # Don't break lines
        )
    return MessageResult("Project definition migrated to version 2.")


@app.command(
    name="import-snowsql-connections",
    requires_connection=False,
    docs=CommandDocs(
        related=_HELPERS_RELATED
        + (
            link(
                "/developer-guide/snowflake-cli/connecting/configure-connections#label-snow"
                + "cli-import-connections-snowsql",
                "Import connections from SnowSQL",
            ),
        ),
        usage_notes=(
            plain_text(
                "The ",
                code("snow helpers import-snowsql-connections"),
                " command imports existing connection definitions from SnowSQL into your ",
                code("config.toml"),
                " configuration file.",
            ),
            plain_text(
                "By default, the command reads the SnowSQL configuration files in the order "
                "described in the ",
                link(
                    "/user-guide/snowsql-config#label-configuring-snowsql",
                    "Configuring SnowSQL",
                ),
                " topic. If more than one of these configurations define the same connection, "
                "this command overwrites the previously imported connection definition with "
                "the most recent one. To illustrate, assume the same ",
                code("[connections.example]"),
                " connection is defined with different parameters in ",
                code("/etc/snowsql.cnf"),
                " with ",
                code("username=user1"),
                ", and in ",
                code("<HOME_DIR>/.snowsql/config"),
                " with ",
                code("username=user2"),
                " and ",
                code("password=<my-pwd>"),
                ". After you run the command, your ",
                ref("sf-cli"),
                " ",
                code("config.toml"),
                " file contains the ",
                code("[connections.example]"),
                " definition from the file with the higher precedence (",
                code("username=user2"),
                " and ",
                code("password=<my-pwd>"),
                ").",
            ),
            plain_text(
                "You can use the ",
                code("--snowsql-config-file"),
                " option to override this default behavior and import from one or more "
                "specific SnowSQL configuration files instead.",
            ),
            plain_text(
                "The ",
                code("snow helpers import-snowsql-connections"),
                " command also imports the default connection from SnowSQL, which is not a "
                "named connection. It is defined directly in the ",
                code("[connections]"),
                " section of the configuration file. Because ",
                ref("sf-cli"),
                " requires all connections to be named, the command defines a connection "
                "named ",
                code("[default]"),
                ". If you want to use another name for the default connection, you can "
                "specify it with the ",
                code("--default-connection-name"),
                " option.",
            ),
            plain_text(
                "If a SnowSQL connection matches the name of an existing ",
                ref("sf-cli"),
                " connection, the command prompt asks whether you want to overwrite the "
                "existing connection or skip importing that SnowSQL connection.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "The following example imports SnowSQL connections from the standard "
                    "configuration file locations. As the command processes the SnowSQL "
                    "configuration files, it shows the progress and prompts for "
                    "confirmation when a connection with the same name is already defined "
                    "in the ",
                    ref("sf-cli"),
                    " ",
                    code("config.toml"),
                    " file.",
                ),
                command="snow helpers import-snowsql-connections",
                output=(
                    "SnowSQL config file [/etc/snowsql.cnf] does not exist. Skipping.\n"
                    "SnowSQL config file [/etc/snowflake/snowsql.cnf] does not exist. Skipping.\n"
                    "SnowSQL config file [/usr/local/etc/snowsql.cnf] does not exist. Skipping.\n"
                    "Trying to read connections from [/Users/<user>/.snowsql.cnf].\n"
                    "Reading SnowSQL's connection configuration [connections.connection1] from [/Users/<user>/.snowsql.cnf]\n"
                    "Trying to read connections from [/Users/<user>/.snowsql/config].\n"
                    "Reading SnowSQL's default connection configuration from [/Users/<user>/.snowsql/config]\n"
                    "Reading SnowSQL's connection configuration [connections.connection1] from [/Users/<user>/.snowsql/config]\n"
                    "Reading SnowSQL's connection configuration [connections.connection2] from [/Users/<user>/.snowsql/config]\n"
                    "Connection 'connection1' already exists in Snowflake CLI, do you want to use SnowSQL definition and override existing connection in Snowflake CLI? [y/N]: Y\n"
                    "Connection 'connection2' already exists in Snowflake CLI, do you want to use SnowSQL definition and override existing connection in Snowflake CLI? [y/N]: n\n"
                    "Connection 'default' already exists in Snowflake CLI, do you want to use SnowSQL definition and override existing connection in Snowflake CLI? [y/N]: n\n"
                    "Saving [connection1] connection in Snowflake CLI's config.\n"
                    "Connections successfully imported from SnowSQL to Snowflake CLI."
                ),
            ),
        ),
    ),
)
def import_snowsql_connections(
    custom_snowsql_config_files: Optional[List[Path]] = typer.Option(
        None,
        "--snowsql-config-file",
        help="Specifies file paths to custom SnowSQL configuration. The option can be used multiple times to specify more than 1 file.",
        dir_okay=False,
        exists=True,
    ),
    default_cli_connection_name: str = typer.Option(
        "default",
        "--default-connection-name",
        help="Specifies the name which will be given in Snowflake CLI to the default connection imported from SnowSQL.",
    ),
    **options,
) -> CommandResult:
    """Import your existing connections from your SnowSQL configuration."""

    snowsql_config_files: list[Path] = custom_snowsql_config_files or [
        Path("/etc/snowsql.cnf"),
        Path("/etc/snowflake/snowsql.cnf"),
        Path("/usr/local/etc/snowsql.cnf"),
        Path.home() / Path(".snowsql.cnf"),
        Path.home() / Path(".snowsql/config"),
    ]
    snowsql_config_secure_paths: list[SecurePath] = [
        SecurePath(p) for p in snowsql_config_files
    ]

    all_imported_connections = _read_all_connections_from_snowsql(
        default_cli_connection_name, snowsql_config_secure_paths
    )
    _validate_and_save_connections_imported_from_snowsql(
        default_cli_connection_name, all_imported_connections
    )
    return MessageResult(
        "Connections successfully imported from SnowSQL to Snowflake CLI."
    )


def _read_all_connections_from_snowsql(
    default_cli_connection_name: str, snowsql_config_files: List[SecurePath]
) -> dict[str, dict]:
    import configparser

    imported_default_connection: dict[str, Any] = {}
    imported_named_connections: dict[str, dict] = {}

    for file in snowsql_config_files:
        if not file.exists():
            cli_console.step(
                f"SnowSQL config file [{str(file.path)}] does not exist. Skipping."
            )
            continue

        cli_console.step(f"Trying to read connections from [{str(file.path)}].")
        snowsql_config = configparser.ConfigParser()
        snowsql_config.read(file.path)

        if "connections" in snowsql_config and snowsql_config.items("connections"):
            cli_console.step(
                f"Reading SnowSQL's default connection configuration from [{str(file.path)}]"
            )
            snowsql_default_connection = snowsql_config.items("connections")
            imported_default_connection.update(
                _convert_connection_from_snowsql_config_section(
                    snowsql_default_connection
                )
            )

        other_snowsql_connection_section_names = [
            section_name
            for section_name in snowsql_config.sections()
            if section_name.startswith("connections.")
        ]
        for snowsql_connection_section_name in other_snowsql_connection_section_names:
            cli_console.step(
                f"Reading SnowSQL's connection configuration [{snowsql_connection_section_name}] from [{str(file.path)}]"
            )
            snowsql_named_connection = snowsql_config.items(
                snowsql_connection_section_name
            )
            if not snowsql_named_connection:
                cli_console.step(
                    f"Empty connection configuration [{snowsql_connection_section_name}] in [{str(file.path)}]. Skipping."
                )
                continue

            connection_name = snowsql_connection_section_name.removeprefix(
                "connections."
            )
            imported_named_conenction = _convert_connection_from_snowsql_config_section(
                snowsql_named_connection
            )
            if connection_name in imported_named_connections:
                imported_named_connections[connection_name].update(
                    imported_named_conenction
                )
            else:
                imported_named_connections[connection_name] = imported_named_conenction

    def imported_default_connection_as_named_connection():
        name = _validate_imported_default_connection_name(
            default_cli_connection_name, imported_named_connections
        )
        return {name: imported_default_connection}

    named_default_connection = (
        imported_default_connection_as_named_connection()
        if imported_default_connection
        else {}
    )

    return imported_named_connections | named_default_connection


def _validate_imported_default_connection_name(
    name_candidate: str, other_snowsql_connections: dict[str, dict]
) -> str:
    if name_candidate in other_snowsql_connections:
        new_name_candidate = typer.prompt(
            f"Chosen default connection name '{name_candidate}' is already taken by other connection being imported from SnowSQL. Please choose a different name for your default connection"
        )
        return _validate_imported_default_connection_name(
            new_name_candidate, other_snowsql_connections
        )
    else:
        return name_candidate


def _convert_connection_from_snowsql_config_section(
    snowsql_connection: list[tuple[str, Any]],
) -> dict[str, Any]:
    from ast import literal_eval

    key_names_replacements = {
        "accountname": "account",
        "username": "user",
        "databasename": "database",
        "dbname": "database",
        "schemaname": "schema",
        "warehousename": "warehouse",
        "rolename": "role",
        "private_key_path": "private_key_file",
    }

    def parse_value(value: Any):
        try:
            parsed_value = literal_eval(value)
        except Exception:
            parsed_value = value
        return parsed_value

    cli_connection: dict[str, Any] = {}
    for key, value in snowsql_connection:
        cli_key = key_names_replacements.get(key, key)
        cli_value = parse_value(value)
        cli_connection[cli_key] = cli_value
    return cli_connection


def _validate_and_save_connections_imported_from_snowsql(
    default_cli_connection_name: str, all_imported_connections: dict[str, Any]
):
    existing_cli_connection_names: set[str] = set(get_all_connections().keys())
    imported_connections_to_save: dict[str, Any] = {}
    for (
        imported_connection_name,
        imported_connection,
    ) in all_imported_connections.items():
        if imported_connection_name in existing_cli_connection_names:
            override_cli_connection = typer.confirm(
                f"Connection '{imported_connection_name}' already exists in Snowflake CLI, do you want to use SnowSQL definition and override existing connection in Snowflake CLI?"
            )
            if not override_cli_connection:
                continue
        imported_connections_to_save[imported_connection_name] = imported_connection

    for name, connection in imported_connections_to_save.items():
        cli_console.step(f"Saving [{name}] connection in Snowflake CLI's config.")
        add_connection_to_proper_file(name, ConnectionConfig.from_dict(connection))

    if default_cli_connection_name in imported_connections_to_save:
        cli_console.step(
            f"Setting [{default_cli_connection_name}] connection as Snowflake CLI's default connection."
        )
        set_config_value(
            path=["default_connection_name"],
            value=default_cli_connection_name,
        )


@app.command(
    name="check-snowsql-env-vars",
    requires_connection=False,
    docs=CommandDocs(
        related=_HELPERS_RELATED,
        usage_notes=(
            plain_text(
                "This command helps you migrate from SnowSQL to ",
                ref("sf-cli"),
                " by identifying your SnowSQL environment variables and mapping them to "
                "the corresponding ",
                ref("sf-cli"),
                " environment variables. It displays information with suggested changes "
                "and links to documentation.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "This example assumes a user has defined the following environment "
                    "variables:\n\n"
                    "- ",
                    code("SNOWSQL_USER"),
                    ": Username for the connection.\n" "- ",
                    code("SNOWSQL_ROLE"),
                    ": Role for the connection.\n" "- ",
                    code("SNOWSQL_UNUSED"),
                    ": Variable not used in ",
                    ref("sf-cli"),
                    ".",
                ),
                command="snow helpers check-snowsql-env-vars",
                output=(
                    "+--------------------------------------------------------------------------------------------------------------------------------------------+\n"
                    "| Found        | Suggested      | Additional info                                                                                            |\n"
                    "|--------------+----------------+------------------------------------------------------------------------------------------------------------|\n"
                    "| SNOWSQL_USER | SNOWFLAKE_USER | https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environme |\n"
                    "|              |                | nt-variables-for-snowflake-credentials                                                                     |\n"
                    "| SNOWSQL_ROLE | SNOWFLAKE_ROLE | https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environme |\n"
                    "|              |                | nt-variables-for-snowflake-credentials                                                                     |\n"
                    "+--------------------------------------------------------------------------------------------------------------------------------------------+\n"
                    "\n"
                    "+----------------------------------------------+\n"
                    "| Found          | Suggested | Additional info |\n"
                    "|----------------+-----------+-----------------|\n"
                    "| SNOWSQL_UNUSED | n/a       | Unused variable |\n"
                    "+----------------------------------------------+\n"
                    "\n"
                    "Found 3 SnowSQL environment variables, 2 with replacements, 1 unused."
                ),
            ),
        ),
    ),
)
def check_snowsql_env_vars(**options):
    """Check if there are any SnowSQL environment variables set."""

    env_vars = os.environ.copy()
    discovered, unused, summary = check_env_vars(env_vars)

    results = []
    if discovered:
        results.append(CollectionResult(discovered))
    if unused:
        results.append(CollectionResult(unused))

    results.append(MessageResult(summary))
    return MultipleResults(results)


@app.command(
    name="show-config-sources",
    requires_connection=False,
    hidden=os.environ.get(ALTERNATIVE_CONFIG_ENV_VAR, "").lower()
    not in ("1", "true", "yes", "on"),
)
def show_config_sources(
    key: Optional[str] = typer.Argument(
        None,
        help="Specific configuration key to show resolution for (e.g., 'account', 'user'). If not provided, shows summary for all keys.",
    ),
    connection: Optional[str] = typer.Option(
        None,
        "--connection",
        "-c",
        help="Filter output to show only configuration for a specific connection name.",
    ),
    show_details: bool = typer.Option(
        False,
        "--show-details",
        "-d",
        help="Show detailed resolution chains for all sources consulted.",
    ),
    **options,
) -> CommandResult:
    """
    Show where configuration values come from.

    This command displays the configuration resolution process, showing which
    source (CLI arguments, environment variables, or config files) provided
    each configuration value. Useful for debugging configuration issues.

    Note: This command requires the enhanced configuration system to be enabled.
    Set SNOWFLAKE_CLI_CONFIG_V2_ENABLED=true to enable it.
    """
    from snowflake.cli.api.config_ng import (
        is_resolution_logging_available,
    )
    from snowflake.cli.api.config_ng.resolution_logger import (
        get_configuration_explanation_results,
    )

    if not is_resolution_logging_available():
        return MessageResult(
            f"⚠️  Configuration resolution logging is not available.\n\n"
            f"To enable it, set the environment variable:\n"
            f"    export {ALTERNATIVE_CONFIG_ENV_VAR}=true\n\n"
            f"Then run this command again to see where configuration values come from."
        )

    return get_configuration_explanation_results(
        key=key, verbose=show_details, connection=connection
    )


# Enum of supported project definition versions, derived from the single source
# of truth in the project schemas module so the CLI surface cannot drift from it.
ProjectDefinitionVersion = Enum(  # type: ignore[misc]
    "ProjectDefinitionVersion",
    {f"V{version.replace('.', '_')}": version for version in get_version_map()},
    type=str,
)
_DEFAULT_DEFINITION_VERSION = ProjectDefinitionVersion("2")


def _accepted_version_scalars(version: str) -> list[Any]:
    """Scalar forms a user may write for ``definition_version`` in YAML.

    YAML leaves ``definition_version: 2`` as an int and ``1.1`` as a float, while
    quoting produces a string; the CLI accepts all of these. Pinning the schema to
    these forms lets an editor flag a version that does not match the schema in use.
    """
    numeric: Any = float(version) if "." in version else int(version)
    return [version, numeric]


def _build_project_definition_schema(version: str) -> dict[str, Any]:
    model = get_version_map()[version]
    schema = model.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["title"] = f"Snowflake CLI project definition v{version}"
    schema["description"] = (
        "JSON Schema for Snowflake CLI project definition files (snowflake.yml) at "
        f"definition_version {version}. Generated from the Snowflake CLI pydantic "
        "models: it captures structural validation (field names, types, required "
        "keys) but not every semantic check the CLI performs at load/deploy time."
    )
    # Pin definition_version so each schema self-identifies; otherwise the v2
    # schema would happily accept `definition_version: 1`, etc.
    version_property = schema.get("properties", {}).get("definition_version")
    if version_property is not None:
        version_property.pop("anyOf", None)
        version_property["enum"] = _accepted_version_scalars(version)
    return schema


@app.command(
    name="generate-project-schema",
    requires_connection=False,
    docs=CommandDocs(
        related=_HELPERS_RELATED,
        usage_notes=(
            plain_text(
                "The generated schema describes the structure of the ",
                code("snowflake.yml"),
                " project definition file for the selected definition version. Use the ",
                code("--definition-version"),
                " option to choose the version (",
                code("1"),
                ", ",
                code("1.1"),
                ", or ",
                code("2"),
                "; the default is ",
                code("2"),
                "), and the ",
                code("--output-file"),
                " (or ",
                code("-o"),
                ") option to write the schema to a file instead of printing it to "
                "standard output.",
            ),
            plain_text(
                "Because the schema is derived from the CLI's own models, it validates "
                "the structure of the file, for example unknown keys, incorrect types, "
                "and missing required fields. Some cross-field and semantic checks are "
                "applied only when the project is loaded or deployed, so a file that "
                "matches the schema can still fail at deploy time.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Print the schema for the default (version 2) project definition to "
                    "standard output:"
                ),
                command="snow helpers generate-project-schema",
            ),
            Example(
                description=plain_text(
                    "Write the schema to a file that your editor or CI pipeline can "
                    "reference:"
                ),
                command=(
                    "snow helpers generate-project-schema --output-file "
                    "snowflake-schema.json"
                ),
            ),
            Example(
                description=plain_text(
                    "Generate the schema for a version 1.1 project definition:"
                ),
                command="snow helpers generate-project-schema --definition-version 1.1",
            ),
        ),
    ),
)
def generate_project_schema(
    version: ProjectDefinitionVersion = typer.Option(  # type: ignore[valid-type]
        _DEFAULT_DEFINITION_VERSION,
        "--definition-version",
        help="Project definition version to generate the schema for.",
    ),
    output_file: Optional[Path] = typer.Option(
        None,
        "--output-file",
        "-o",
        help="Write the JSON Schema to this file. When omitted, schema is printed to stdout.",
        dir_okay=False,
        writable=True,
    ),
    **options,
) -> CommandResult:
    """Generate a JSON Schema for the Snowflake CLI project definition file (snowflake.yml)."""
    schema = _build_project_definition_schema(version.value)

    if output_file is not None:
        if not output_file.parent.exists():
            raise click.ClickException(
                f"Directory '{output_file.parent}' does not exist."
            )
        payload = json.dumps(schema, indent=2, sort_keys=True)
        SecurePath(output_file).write_text(payload + "\n")
        return MessageResult(f"Project definition schema written to {output_file}.")

    # Under a structured output format, return the schema as an object so the
    # command emits the JSON Schema document itself rather than a stringified
    # blob nested under a "message" key.
    if get_cli_context().output_format.is_json:
        return ObjectResult(schema)

    return MessageResult(json.dumps(schema, indent=2, sort_keys=True))


@app.command(
    name="check-version",
    requires_connection=False,
    docs=CommandDocs(
        related=_HELPERS_RELATED,
        usage_notes=(
            plain_text(
                code("snow helpers check-version"),
                " doesn't require a Snowflake connection. It checks locally cached "
                "version information by default. Use ",
                code("--refresh"),
                " to query PyPI and Homebrew directly, bypassing the cache.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Check whether a newer version is available using the local cache:"
                ),
                command="snow helpers check-version",
            ),
            Example(
                description=plain_text(
                    "Query PyPI and Homebrew directly for the latest version:"
                ),
                command="snow helpers check-version --refresh",
            ),
        ),
    ),
)
def check_version(
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Query PyPI and Homebrew for the latest version instead of using the local cache.",
    ),
    **options,
) -> CommandResult:
    """Check whether a newer version of the Snowflake CLI is available."""
    # This command is the explicit, on-demand version check, so mute the passive
    # upgrade banner for this run to avoid duplicating its own output.
    suppress_new_version_banner()
    info = get_version_info(force_refresh=refresh)
    if info.latest_version is None:
        raise CliError(
            "Could not determine the latest Snowflake CLI version. "
            "Check your network connection and try again. "
            "Re-run with --debug to see the underlying error."
        )
    record_version_check_displayed()
    return ObjectResult(asdict(info))


@app.command(
    name="detect-encoding",
    requires_connection=False,
    docs=CommandDocs(
        related=_HELPERS_RELATED,
        usage_notes=(
            plain_text(
                "Use ",
                code("snow helpers detect-encoding"),
                " to inspect the text encoding ",
                ref("sf-cli"),
                " uses in the current environment and to diagnose encoding warnings. "
                "The command reports the encodings applied to reading and writing project "
                "files, decoding subprocess output, and writing output to standard output, "
                "and it highlights settings that can corrupt files when projects are shared "
                "across platforms, for example between Windows and macOS or Linux.",
            ),
            plain_text(
                "To change these encodings, configure the ",
                code("[cli.encoding]"),
                " section of ",
                code("config.toml"),
                " or set the corresponding ",
                code("SNOWFLAKE_CLI_ENCODING_*"),
                " environment variables. For more information, see ",
                link("/developer-guide/snowflake-cli/connecting/configure-cli"),
                ".",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Show the encoding configuration for the current environment:"
                ),
                command="snow helpers detect-encoding",
            ),
            Example(
                description=plain_text(
                    "Emit the encoding details as JSON for scripting:"
                ),
                command="snow helpers detect-encoding --format json",
            ),
        ),
    ),
)
def detect_encoding(**options) -> CommandResult:
    """Show the encoding configuration for the current environment."""
    return MessageResult(get_encoding_diagnostics())
