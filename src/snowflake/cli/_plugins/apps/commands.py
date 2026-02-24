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

import time
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional

import typer
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.project.util import get_env_username
from snowflake.cli.api.sql_execution import SqlExecutionMixin

app = SnowTyperFactory(
    name="apps",
    help="Manages Snowflake Apps.",
    is_hidden=FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_disabled,
)

DEFINITION_FILENAME = "snowflake.yml"
SNOW_APP_ENTITY_TYPE = "snow-app"

# Feature flags
IS_PERSONAL_DB_SUPPORTED = False  # Will be enabled in the future

# Default resource names for Snow Apps
SYSTEM_COMPUTE_POOL = "SYSTEM_COMPUTE_POOL_CPU"
SNOW_APPS_COMPUTE_POOL = "SNOW_APPS_COMPUTE_POOL"
DEFAULT_EXTERNAL_ACCESS = "SNOW_APPS_DEFAULT_EXTERNAL_ACCESS"
DEFAULT_ARTIFACT_REPOSITORY = "SNOW_APPS_DEFAULT_ARTIFACT_REPOSITORY"


def _check_feature_enabled():
    if FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_disabled():
        raise CliError("This feature is not available yet.")


def _print_messages_with_delay(messages: List[str], delay_seconds: float = 0.75):
    """Print messages with a delay between each one."""
    for message in messages:
        cli_console.step(message)
        time.sleep(delay_seconds)


def _object_exists(object_type: str, name: str) -> bool:
    """Check if an object exists in Snowflake."""
    try:
        return ObjectManager().object_exists(
            object_type=object_type, fqn=FQN.from_string(name)
        )
    except Exception:
        return False


def _get_compute_pool() -> Optional[str]:
    """
    Get the compute pool to use for Snow Apps.

    Checks in order:
    1. SYSTEM_COMPUTE_POOL_CPU
    2. SNOW_APPS_COMPUTE_POOL

    Returns None if neither exists.
    """
    if _object_exists("compute-pool", SYSTEM_COMPUTE_POOL):
        return SYSTEM_COMPUTE_POOL
    if _object_exists("compute-pool", SNOW_APPS_COMPUTE_POOL):
        return SNOW_APPS_COMPUTE_POOL
    return None


def _get_external_access(app_id: str) -> Optional[str]:
    """
    Get the external access integration to use for Snow Apps.

    Checks in order:
    1. SNOW_APPS_DEFAULT_EXTERNAL_ACCESS
    2. SNOW_APPS_<APP_ID>_EXTERNAL_ACCESS

    Returns None if neither exists.
    """
    if _object_exists("external-access-integration", DEFAULT_EXTERNAL_ACCESS):
        return DEFAULT_EXTERNAL_ACCESS

    app_specific_eai = f"SNOW_APPS_{app_id.upper()}_EXTERNAL_ACCESS"
    if _object_exists("external-access-integration", app_specific_eai):
        return app_specific_eai

    return None


def _get_snow_app_entities() -> Dict[str, Any]:
    """Get all snow-app entities from the project definition."""
    ctx = get_cli_context()
    project_def = ctx.project_definition

    if project_def is None:
        raise CliError(f"No {DEFINITION_FILENAME} found. Run 'snow apps init' first.")

    # Get entities with type "snow-app"
    snow_apps = {}
    if hasattr(project_def, "entities"):
        for entity_id, entity in project_def.entities.items():
            if getattr(entity, "type", None) == SNOW_APP_ENTITY_TYPE:
                snow_apps[entity_id] = entity

    return snow_apps


def _resolve_entity_id(entity_id: Optional[str]) -> str:
    """
    Resolve the entity_id from the argument or project definition.

    If entity_id is provided, use it. Otherwise, if there's exactly one
    snow-app entity in the project, use that. Otherwise, raise an error.
    """
    if entity_id:
        return entity_id

    snow_apps = _get_snow_app_entities()

    if len(snow_apps) == 0:
        raise CliError(
            f"No snow-app entities found in {DEFINITION_FILENAME}. "
            "Add a snow-app entity or run 'snow apps init' first."
        )
    elif len(snow_apps) == 1:
        return list(snow_apps.keys())[0]
    else:
        entity_ids = ", ".join(snow_apps.keys())
        raise CliError(
            f"Multiple snow-app entities found: {entity_ids}. "
            "Please specify --entity-id to select one."
        )


def _get_entity(entity_id: str) -> Any:
    """Get the snow-app entity by ID."""
    snow_apps = _get_snow_app_entities()
    if entity_id not in snow_apps:
        raise CliError(f"Entity '{entity_id}' not found in {DEFINITION_FILENAME}.")
    return snow_apps[entity_id]


class SnowAppManager(SqlExecutionMixin):
    """Manager for Snow App operations."""

    def create_schema_if_not_exists(self, database: str, schema: str) -> None:
        """Create schema if it doesn't exist."""
        fqn = f"{database}.{schema}"
        self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {fqn}")

    def stage_exists(self, stage_fqn: str) -> bool:
        """Check if a stage exists."""
        try:
            self.execute_query(f"DESCRIBE STAGE {stage_fqn}")
            return True
        except Exception:
            return False

    def clear_stage(self, stage_fqn: str) -> None:
        """Clear all files from a stage."""
        self.execute_query(f"REMOVE @{stage_fqn}")

    def create_stage(
        self, stage_fqn: str, encryption_type: str = "SNOWFLAKE_SSE"
    ) -> None:
        """Create a stage if it doesn't exist."""
        self.execute_query(
            f"CREATE STAGE IF NOT EXISTS {stage_fqn} ENCRYPTION = (TYPE = '{encryption_type}')"
        )


def _generate_snowflake_yml(
    app_id: str,
    warehouse: Optional[str],
) -> str:
    """Generate snowflake.yml content for a Snow App project."""

    username = get_env_username().upper()

    # Database: use personal DB if supported, otherwise use connection database
    if IS_PERSONAL_DB_SUPPORTED:
        database = f"USER${username}"
    else:
        database = "<% ctx.connection.database %>"

    # Schema: SNOW_APP_<APP_ID>_<USERNAME>
    schema = f"SNOW_APP_{app_id.upper()}_{username}"

    # Stage: <APP_ID>_CODE_STAGE
    code_stage = f"{app_id.upper()}_CODE_STAGE"

    # Compute pool: check for existing pools
    compute_pool = _get_compute_pool()
    if compute_pool:
        compute_pool_yaml = f"""build_compute_pool:
              name: {compute_pool}
            service_compute_pool:
              name: {compute_pool}"""
    else:
        compute_pool_yaml = """build_compute_pool: null
            service_compute_pool: null"""

    # Build EAI: check for existing integrations
    build_eai = _get_external_access(app_id)
    if build_eai:
        build_eai_yaml = f"""build_eai:
              name: {build_eai}"""
    else:
        build_eai_yaml = "build_eai: null"

    # TODO: Check if artifact repository exists
    artifact_repository = DEFAULT_ARTIFACT_REPOSITORY

    return dedent(
        f"""\
        definition_version: "2"

        entities:
          {app_id}:
            type: snow-app
            identifier:
              name: {app_id.upper()}
              database: {database}
              schema: {schema}
            meta:
              title: {app_id}
              description: null
              icon: null
            artifacts:
              - src: app/*
                dest: ./

            query_warehouse: {warehouse or "<% ctx.connection.warehouse %>"}
            {compute_pool_yaml}
            {build_eai_yaml}
            service_eai: null
            artifact_repository:
              name: {artifact_repository}
            code_stage:
              name: {code_stage}
        """
    )


@app.command(requires_connection=True)
def init(
    app_name: str = typer.Option(
        ...,
        "--app-name",
        help="Name of the Snowflake App to initialize.",
    ),
    **options,
) -> CommandResult:
    """
    Initializes a snowflake.yml file for a Snowflake App project.
    """
    _check_feature_enabled()

    project_file = Path.cwd() / DEFINITION_FILENAME
    if project_file.exists():
        return MessageResult(
            f"{DEFINITION_FILENAME} already exists. Skipping initialization."
        )

    # Get connection context for username and warehouse
    ctx = get_cli_context()
    warehouse = ctx.connection_context.warehouse

    project_file.write_text(_generate_snowflake_yml(app_name, warehouse))
    return MessageResult(f"Initialized Snowflake App project in {DEFINITION_FILENAME}.")


@app.command(requires_connection=True)
def create(**options) -> CommandResult:
    """
    Creates a Snowflake App.
    """
    _check_feature_enabled()
    return MessageResult("snow apps create")


@app.command(requires_connection=True)
def build(
    entity_id: Optional[str] = typer.Option(
        None,
        "--entity-id",
        help="ID of the snow-app entity to build. Required if multiple snow-app entities exist.",
    ),
    **options,
) -> CommandResult:
    """
    Builds a Snowflake App.

    If --entity-id is not specified and the project contains exactly one snow-app
    entity, that entity will be used automatically.
    """
    _check_feature_enabled()
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    # Get entity configuration
    identifier = getattr(entity, "identifier", {})
    database = getattr(identifier, "database", None) or "<default_db>"
    # Note: schema_ is the field name (schema is a reserved Pydantic method)
    schema = (
        getattr(identifier, "schema_", None) or f"SNOW_APP_{resolved_entity_id.upper()}"
    )

    code_stage_config = getattr(entity, "code_stage", None)
    if code_stage_config:
        stage_name = (
            getattr(code_stage_config, "name", None)
            or f"{resolved_entity_id.upper()}_CODE_STAGE"
        )
        encryption_type = getattr(code_stage_config, "encryption_type", "SNOWFLAKE_SSE")
    else:
        stage_name = f"{resolved_entity_id.upper()}_CODE_STAGE"
        encryption_type = "SNOWFLAKE_SSE"

    # Get artifacts configuration
    artifacts = getattr(entity, "artifacts", [])

    # Build fully qualified stage name
    stage_fqn = f"{database}.{schema}.{stage_name}"

    manager = SnowAppManager()
    stage_manager = StageManager()

    # Step 1: Create schema if it doesn't exist
    cli_console.step(f"Creating schema {schema} if it doesn't exist")
    manager.create_schema_if_not_exists(database, schema)

    # Step 2: Clear or create stage
    if manager.stage_exists(stage_fqn):
        cli_console.step(f"Clearing existing stage @{stage_fqn}")
        manager.clear_stage(stage_fqn)
    else:
        cli_console.step(f"Creating stage @{stage_fqn}")
        manager.create_stage(stage_fqn, encryption_type)

    # Step 3: Upload artifact files to stage
    cli_console.step("Uploading source files to stage")

    project_root = get_cli_context().project_root
    for artifact in artifacts:
        src = (
            getattr(artifact, "src", None)
            if hasattr(artifact, "src")
            else artifact.get("src")
            if isinstance(artifact, dict)
            else None
        )
        if src:
            # Handle glob patterns (e.g., "app/*")
            src_path = project_root / src.rstrip("/*")
            if src_path.exists():
                cli_console.step(f"Uploading {src_path} to @{stage_fqn}")
                stage_manager.put(
                    local_path=src_path,
                    stage_path=f"@{stage_fqn}",
                    overwrite=True,
                    auto_compress=False,
                )

    # Fake status messages for job service execution (placeholder for future implementation)
    _print_messages_with_delay(
        [
            "Dropping service if exists...",
            "Executing job service...",
            "Status: PENDING",
            "Status: PENDING",
            "Status: RUNNING",
            "Status: DONE",
        ]
    )

    return MessageResult("Build complete.")


@app.command(requires_connection=True)
def deploy(
    entity_id: Optional[str] = typer.Option(
        None,
        "--entity-id",
        help="ID of the snow-app entity to deploy. Required if multiple snow-app entities exist.",
    ),
    **options,
) -> CommandResult:
    """
    Deploys a Snowflake App.

    If --entity-id is not specified and the project contains exactly one snow-app
    entity, that entity will be used automatically.
    """
    _check_feature_enabled()
    resolved_entity_id = _resolve_entity_id(entity_id)

    _print_messages_with_delay(
        [
            "Updating service...",
            "Resuming service...",
            "Status: PENDING",
            "Status: PENDING",
            "Status: READY",
        ]
    )

    return MessageResult(
        f"App ready at https://{resolved_entity_id}.snowflakecomputing.app/"
    )
