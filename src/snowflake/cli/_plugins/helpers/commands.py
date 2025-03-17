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
from typing import List, Optional

import typer
import yaml
from click import ClickException
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.project.definition_conversion import (
    convert_project_definition_to_v2,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.secure_path import SecurePath

from .conn_import_helpers import (
    read_all_connections_from_snowsql,
    validate_and_save_connections_imported_from_snowsql,
)

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

    all_imported_connections = read_all_connections_from_snowsql(
        default_cli_connection_name, snowsql_config_secure_paths
    )
    validate_and_save_connections_imported_from_snowsql(
        default_cli_connection_name, all_imported_connections
    )
    return MessageResult(
        "Connections successfully imported from SnowSQL to Snowflake CLI."
    )


@app.command(name="check-snowsql-env-vars", requires_connection=False)
def check_snowsql_env_vars(**options):
    """Check if there are any SnowSQL environment variables set."""
    # from snowflake.cli.api.config import get_snowsql_env_vars
    #
    # snowsql_env_vars = get_snowsql_env_vars()
    # if not snowsql_env_vars:
    #     return MessageResult("No SnowSQL environment variables found.")
    known_snowsql_env_vars = [
        "SNOWSQL_ACCOUNT",
        "SNOWSQL_USER",
        "SNOWSQL_PASSWORD",
        "SNOWSQL_ROLE",
        "SNOWSQL_WAREHOUSE",
        "SNOWSQL_DATABASE",
        "SNOWSQL_SCHEMA",
    ]
    snowsql_env_vars = []
    return MessageResult(
        f"Found {len(snowsql_env_vars)} SnowSQL environment variables."
    )
