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
import os
from pathlib import Path
from typing import Any, List, Optional

import typer
import yaml
from click import ClickException
from snowflake.cli._plugins.helpers.snowsl_vars_reader import check_env_vars
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.config import (
    ConnectionConfig,
    add_connection_to_proper_file,
    get_all_connections,
    set_config_value,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    MultipleResults,
)
from snowflake.cli.api.project.definition_conversion import (
    convert_project_definition_to_v2,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

app = SnowTyperFactory(
    name="helpers",
    help="Helper commands.",
)


@app.command()
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
            raise ClickException(
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
    with open("snowflake.yml", "w") as file:
        yaml.dump(
            pd_v2.model_dump(
                exclude_unset=True, exclude_none=True, mode="json", by_alias=True
            ),
            file,
            sort_keys=False,
            width=float("inf"),  # Don't break lines
        )
    return MessageResult("Project definition migrated to version 2.")


@app.command(name="import-snowsql-connections", requires_connection=False)
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


@app.command(name="check-snowsql-env-vars", requires_connection=False)
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
