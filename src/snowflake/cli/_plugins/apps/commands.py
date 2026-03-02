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

import json
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
SNOW_APPS_COMPUTE_POOL = "SNOW_APPS_DEFAULT_COMPUTE_POOL"
DEFAULT_EXTERNAL_ACCESS = "SNOW_APPS_DEFAULT_EXTERNAL_ACCESS"
DEFAULT_ARTIFACT_REPOSITORY = "SNOW_APPS_DEFAULT_ARTIFACT_REPOSITORY"
# TODO: Replace with artifact_repository from entity config once supported
DEFAULT_IMAGE_REPOSITORY = "SNOW_APPS_DEFAULT_IMAGE_REPOSITORY"


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
    # TODO: Enable when SYSTEM_COMPUTE_POOL is supported for SPCS
    if False and _object_exists("compute-pool", SYSTEM_COMPUTE_POOL):
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

    def drop_service_if_exists(self, service_fqn: str) -> None:
        """Drop a service if it exists."""
        self.execute_query(f"DROP SERVICE IF EXISTS {service_fqn}")

    def get_image_repo_url(self, repo_name: str) -> str:
        """Get the image repository URL and convert to local registry."""
        from snowflake.cli.api.project.util import (
            identifier_to_show_like_pattern,
            unquote_identifier,
        )
        from snowflake.connector.cursor import DictCursor

        show_obj_query = (
            f"show image repositories like {identifier_to_show_like_pattern(repo_name)}"
        )
        cursor = self.execute_query(show_obj_query, cursor_class=DictCursor)

        if cursor.rowcount is None or cursor.rowcount == 0:
            raise CliError(f"Image repository '{repo_name}' not found")

        unqualified_name = unquote_identifier(repo_name)
        rows = cursor.fetchall()
        row = next(
            (r for r in rows if r["name"].upper() == unqualified_name.upper()),
            None,
        )
        if not row:
            # Fall back to the first row if name matching fails
            row = rows[0] if rows else None
        if not row:
            raise CliError(f"Image repository '{repo_name}' not found")

        # Get repository_url from the result
        repo_url = row["repository_url"]

        # Prod:
        # repo_url = "pm-nax-consumer.registry.snowflakecomputing.com/gbloom_test_db/snow_app_my_test_app_gbloom/snow_apps_default_image_repository"

        # Convert to local registry URL (replace .registry-dev. or .registry. with .registry-local.)
        local_url = repo_url.replace(".registry-dev.", ".registry-local.")
        local_url = local_url.replace(".registry.", ".registry-local.")
        local_url = local_url.replace(".awsuswest2qa6.", ".")
        local_url = local_url.replace(".awsuswest2qa3.", ".")

        return local_url

    def execute_build_job(
        self,
        job_service_name: str,
        compute_pool: str,
        code_stage: str,
        image_repo_url: str,
        app_id: str,
        external_access_integration: Optional[str] = None,
    ) -> None:
        """Execute a build job service."""
        spec = f"""spec:
  containers:
  - name: main
    image: "/snowflake/images/snowflake_images/sf-image-build:0.0.1"
    env:
      IMAGE_REGISTRY_URL: "{image_repo_url}"
      IMAGE_NAME: "{app_id.lower()}"
      IMAGE_TAG: "latest"
      BUILD_CONTEXT: "/app"
    volumeMounts:
      - name: code-volume
        mountPath: /app
  volumes:
  - name: code-volume
    source: "@{code_stage}"
    uid: 65532"""

        query_lines = [
            f"EXECUTE JOB SERVICE IN COMPUTE POOL {compute_pool}",
            f"NAME = {job_service_name}",
            "ASYNC = TRUE",
            f"FROM SPECIFICATION $${spec}$$",
        ]

        if external_access_integration:
            query_lines.insert(
                3, f"EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration})"
            )

        query = "\n".join(query_lines)
        self.execute_query(query)

    def get_build_status(self, database: str, schema: str, job_name: str) -> str:
        """
        Get the status of the build job service.

        Returns:
            - "IDLE" if the job service doesn't exist
            - The actual status from SHOW SERVICES (e.g., "PENDING", "RUNNING", "DONE", "FAILED")
        """
        schema_fqn = f"{database}.{schema}"
        self.execute_query(f"SHOW SERVICES IN SCHEMA {schema_fqn}")

        # Query the result to find the job status
        result = self.execute_query(
            f'SELECT COUNT(*), MAX("status") '
            f"FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) "
            f"WHERE \"name\" = '{job_name}'"
        )
        row = result.fetchone()

        if not row or row[0] == 0:
            return "IDLE"

        return row[1]

    def create_service(
        self,
        service_name: str,
        compute_pool: str,
        query_warehouse: str,
        app_comment: Optional[str] = None,
    ) -> None:
        """Create a service with a placeholder spec (suspended by default)."""
        spec = """spec:
  containers:
    - name: main
      image: "/snowflake/images/snowflake_images/sf-image-build:0.0.1"
      command:
        - sleep
        - infinity
  endpoints:
    - name: app-endpoint
      port: 3000
      public: true
serviceRoles:
  - name: viewer
    endpoints:
      - app-endpoint"""

        query = (
            f"CREATE SERVICE IF NOT EXISTS {service_name}\n"
            f"IN COMPUTE POOL {compute_pool}\n"
            f"FROM SPECIFICATION $${spec}$$\n"
            f"QUERY_WAREHOUSE = {query_warehouse}"
        )
        self.execute_query(query)

        if app_comment:
            escaped_comment = app_comment.replace("'", "''")
            self.execute_query(
                f"ALTER SERVICE {service_name} SET COMMENT = '{escaped_comment}'"
            )

        # Suspend the service after creation (deploy will resume it)
        self.execute_query(f"ALTER SERVICE {service_name} SUSPEND")

    def alter_service_spec(
        self,
        service_name: str,
        image_url: str,
    ) -> None:
        """Alter a service with the built image spec."""
        spec = f"""spec:
  containers:
    - name: main
      image: "{image_url}"
  endpoints:
    - name: app-endpoint
      port: 3000
      public: true
serviceRoles:
  - name: viewer
    endpoints:
      - app-endpoint"""

        query = f"ALTER SERVICE {service_name}\n" f"FROM SPECIFICATION $${spec}$$"
        self.execute_query(query)

    def resume_service(self, service_name: str) -> None:
        """Resume a suspended service."""
        self.execute_query(f"ALTER SERVICE {service_name} RESUME")

    def get_service_status(self, database: str, schema: str, service_name: str) -> str:
        """
        Get the status of a service.

        Returns:
            - "IDLE" if the service doesn't exist
            - The actual status from SHOW SERVICES (e.g., "PENDING", "READY", "SUSPENDED", "FAILED")
        """
        schema_fqn = f"{database}.{schema}"
        self.execute_query(f"SHOW SERVICES IN SCHEMA {schema_fqn}")

        result = self.execute_query(
            f'SELECT COUNT(*), MAX("status") '
            f"FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) "
            f"WHERE \"name\" = '{service_name}'"
        )
        row = result.fetchone()

        if not row or row[0] == 0:
            return "IDLE"

        return row[1]

    def get_service_endpoint_url(
        self, service_fqn: str, endpoint_name: str = "app-endpoint"
    ) -> Optional[str]:
        """Get the ingress URL for a service endpoint."""
        self.execute_query(f"SHOW ENDPOINTS IN SERVICE {service_fqn}")

        result = self.execute_query(
            f'SELECT "ingress_url" '
            f"FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) "
            f"WHERE \"name\" = '{endpoint_name}'"
        )
        row = result.fetchone()

        if row:
            return row[0]
        return None


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
        compute_pool_yaml = f"""build_compute_pool:
              name: null
            service_compute_pool:
              name: null"""

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
def deploy(
    entity_id: Optional[str] = typer.Option(
        None,
        "--entity-id",
        help="ID of the snow-app entity to deploy. Required if multiple snow-app entities exist.",
    ),
    **options,
) -> CommandResult:
    """
    Builds and deploys a Snowflake App.

    Uploads source artifacts, builds a container image, creates (or updates)
    a service, and waits for it to become ready.

    If --entity-id is not specified and the project contains exactly one snow-app
    entity, that entity will be used automatically.
    """
    _check_feature_enabled()
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    # ── Extract entity configuration ──────────────────────────────────
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

    artifacts = getattr(entity, "artifacts", [])

    build_compute_pool_config = getattr(entity, "build_compute_pool", None)
    build_compute_pool = (
        getattr(build_compute_pool_config, "name", None)
        if build_compute_pool_config
        else None
    )

    build_eai_config = getattr(entity, "build_eai", None)
    build_eai = getattr(build_eai_config, "name", None) if build_eai_config else None

    service_compute_pool_config = getattr(entity, "service_compute_pool", None)
    service_compute_pool = (
        getattr(service_compute_pool_config, "name", None)
        if service_compute_pool_config
        else None
    )

    query_warehouse = getattr(entity, "query_warehouse", None)

    meta = getattr(entity, "meta", None)
    app_title = getattr(meta, "title", None) if meta else None
    app_description = getattr(meta, "description", None) if meta else None

    # TODO: Replace with artifact_repository from entity config once supported
    image_repository = DEFAULT_IMAGE_REPOSITORY

    # ── Validate required configuration ───────────────────────────────
    if not build_compute_pool:
        raise CliError(
            "build_compute_pool is required for deploy. "
            "Please configure it in snowflake.yml."
        )

    if not service_compute_pool:
        raise CliError(
            "service_compute_pool is required for deploy. "
            "Please configure it in snowflake.yml."
        )

    if not query_warehouse:
        raise CliError(
            "query_warehouse is required for deploy. "
            "Please configure it in snowflake.yml."
        )

    # ── Derived names ─────────────────────────────────────────────────
    stage_fqn = f"{database}.{schema}.{stage_name}"
    build_job_service_name = f"{resolved_entity_id.upper()}_BUILD_JOB"
    build_job_name = f"{database}.{schema}.{build_job_service_name}"
    service_name_short = f"{resolved_entity_id.upper()}_SERVICE"
    service_fqn = f"{database}.{schema}.{service_name_short}"

    manager = SnowAppManager()
    stage_manager = StageManager()

    # ── Build phase ───────────────────────────────────────────────────

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

    # Step 4: Get image repository URL
    cli_console.step(f"Getting image repository URL for {image_repository}")
    image_repo_url = manager.get_image_repo_url(image_repository)

    # Step 5: Drop existing build job if present
    cli_console.step(f"Dropping service if exists: {build_job_name}")
    manager.drop_service_if_exists(build_job_name)

    # Step 6: Execute build job service
    cli_console.step(f"Executing build job service: {build_job_name}")
    manager.execute_build_job(
        job_service_name=build_job_name,
        compute_pool=build_compute_pool,
        code_stage=stage_fqn,
        image_repo_url=image_repo_url,
        app_id=resolved_entity_id,
        external_access_integration=build_eai,
    )

    # Step 7: Poll for build completion
    cli_console.step("Waiting for build to complete...")
    while True:
        time.sleep(5)
        status = manager.get_build_status(database, schema, build_job_service_name)
        cli_console.step(f"Build status: {status}")

        if status == "DONE":
            break
        elif status == "FAILED":
            raise CliError(f"Build failed. Check service logs: {build_job_name}")
        elif status == "IDLE":
            raise CliError("Build job service not found. It may have failed to start.")
        elif status not in ("PENDING", "RUNNING"):
            cli_console.step(f"Unknown status: {status}")
            break

    # ── Deploy phase ──────────────────────────────────────────────────

    # Construct the image path from the repo URL
    # image_repo_url is a full registry URL like "host/db/schema/repo_name"
    # Extract the path portion (everything after the host) for the service spec
    repo_path = "/" + "/".join(image_repo_url.split("/")[1:])
    image_url = f"{repo_path}/{resolved_entity_id.lower()}:latest"

    # Build app comment with metadata
    comment_data = {"appId": resolved_entity_id.upper()}
    if app_title:
        comment_data["appName"] = app_title
    if app_description:
        comment_data["appDescription"] = app_description
    app_comment = json.dumps(comment_data)

    # Step 8: Create service if it doesn't exist
    cli_console.step(f"Creating service {service_fqn} if it doesn't exist")
    manager.create_service(
        service_name=service_fqn,
        compute_pool=service_compute_pool,
        query_warehouse=query_warehouse,
        app_comment=app_comment,
    )

    # Step 9: Alter service with built image
    cli_console.step(f"Updating service with image: {image_url}")
    manager.alter_service_spec(
        service_name=service_fqn,
        image_url=image_url,
    )

    # Step 10: Resume service
    cli_console.step("Resuming service")
    manager.resume_service(service_fqn)

    # Step 11: Poll until service is RUNNING
    cli_console.step("Waiting for service to be ready...")
    while True:
        time.sleep(5)
        status = manager.get_service_status(database, schema, service_name_short)
        cli_console.step(f"Service status: {status}")

        if status == "RUNNING":
            break
        elif status == "FAILED":
            raise CliError(f"Service failed. Check service logs: {service_fqn}")
        elif status == "IDLE":
            raise CliError(f"Service not found: {service_fqn}")
        elif status not in ("PENDING", "SUSPENDING", "SUSPENDED"):
            cli_console.step(f"Unknown status: {status}")
            break

    # Step 12: Get endpoint URL
    cli_console.step("Getting endpoint URL")
    endpoint_url = manager.get_service_endpoint_url(service_fqn)

    if endpoint_url:
        return MessageResult(f"App ready at https://{endpoint_url}")
    else:
        return MessageResult(
            f"App deployed but endpoint URL not yet available. "
            f'Check with: snow sql -q "SHOW ENDPOINTS IN SERVICE {service_fqn}"'
        )
