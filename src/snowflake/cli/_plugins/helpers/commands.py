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
from pathlib import Path
from typing import Any, List, Optional

import typer
import yaml
from click import ClickException
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.config import (
    ConnectionConfig,
    add_connection_to_proper_file,
    get_all_connections,
    set_config_value,
)
from snowflake.cli.api.output.types import CommandResult, MessageResult
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
    migrate_local_yml: (bool | None) = typer.Option(
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
            log.debug(
                "SnowSQL config file [%s] does not exist. Skipping.", str(file.path)
            )
            continue

        log.debug("Trying to read connections from [%s].", str(file.path))
        snowsql_config = configparser.ConfigParser()
        snowsql_config.read(file.path)

        if "connections" in snowsql_config and snowsql_config.items("connections"):
            log.debug(
                "Reading SnowSQL's default connection configuration from [%s]",
                str(file.path),
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
            log.debug(
                "Reading SnowSQL's connection configuration [%s] from [%s]",
                snowsql_connection_section_name,
                str(file.path),
            )
            snowsql_named_connection = snowsql_config.items(
                snowsql_connection_section_name
            )
            if not snowsql_named_connection:
                log.debug(
                    "Empty connection configuration [%s] in [%s]. Skipping.",
                    snowsql_connection_section_name,
                    str(file.path),
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

    named_default_connection = (
        {default_cli_connection_name: imported_default_connection}
        if imported_default_connection
        else {}
    )
    if (
        named_default_connection
        and default_cli_connection_name in imported_named_connections
    ):
        raise ClickException(
            f"Default connection name [{default_cli_connection_name}] conflicts with the name of one of connections from SnowSQL. Please specify a different name for your default connection."
        )

    return imported_named_connections | named_default_connection


def _convert_connection_from_snowsql_config_section(
    snowsql_connection: list[tuple[str, Any]]
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
    all_imported_connection_names: set[str] = set(all_imported_connections.keys())
    conflicting_connection_names = (
        existing_cli_connection_names & all_imported_connection_names
    )
    if conflicting_connection_names:
        raise ClickException(
            f"Cannot import connections from SnowSQL because some of them conflict with existing CLI's connections. Please remove or rename the following connections: {', '.join(conflicting_connection_names)}."
        )

    for name, connection in all_imported_connections.items():
        log.debug("Saving [%s] connection in Snowflake CLI's config.", name)
        add_connection_to_proper_file(name, ConnectionConfig.from_dict(connection))

    if default_cli_connection_name in all_imported_connections:
        log.debug(
            "Setting [%s] connection as Snowflake CLI's default connection.",
            default_cli_connection_name,
        )
        set_config_value(
            section=None,
            key="default_connection_name",
            value=default_cli_connection_name,
        )
