# Copyright (c) 2026 Snowflake Inc.
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

import time
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Set

from snowflake.cli._plugins.apps.snowflake_app_entity_model import DEFAULT_APP_PORT

if TYPE_CHECKING:
    from snowflake.cli._plugins.apps.snowflake_app_entity_model import (
        SnowflakeAppEntityModel,
    )
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin

DEFINITION_FILENAME = "snowflake.yml"
SNOWFLAKE_APP_ENTITY_TYPE = "snowflake-app"

# Default resource names for Snowflake Apps
SNOW_APPS_COMPUTE_POOL = "SNOW_APPS_DEFAULT_COMPUTE_POOL"
DEFAULT_EXTERNAL_ACCESS = "SNOW_APPS_DEFAULT_EXTERNAL_ACCESS"
# TODO: Replace with artifact_repository from entity config once supported
DEFAULT_IMAGE_REPOSITORY = "SNOW_APPS_DEFAULT_IMAGE_REPOSITORY"


def _check_feature_enabled():
    if FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_disabled():
        raise CliError("This feature is not available yet.")


def _poll_until(
    poll_fn: Callable[[], str],
    *,
    done_states: Set[str],
    error_states: Set[str],
    known_pending_states: Set[str],
    max_attempts: int = 240,
    interval_seconds: int = 5,
    timeout_message: str = "Operation timed out.",
) -> str:
    """Poll *poll_fn* until the returned status is in *done_states*.

    Raises ``CliError`` for statuses in *error_states* or on timeout.
    Returns the final status on success.
    """
    for _attempt in range(max_attempts):
        time.sleep(interval_seconds)
        status = poll_fn()
        cli_console.step(f"Status: {status}")

        if status in done_states:
            return status
        if status in error_states:
            raise CliError(f"{timeout_message} (status={status})")
        if status not in known_pending_states:
            cli_console.step(f"Unknown status: {status}")
            return status

    raise CliError(
        f"{timeout_message} "
        f"(timed out after {max_attempts * interval_seconds // 60} minutes)"
    )


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
    Get the compute pool to use for Snowflake Apps.

    Returns SNOW_APPS_DEFAULT_COMPUTE_POOL if it exists, otherwise None.
    """
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


def _get_snowflake_app_entities() -> Dict[str, Any]:
    """Get all snowflake-app entities from the project definition."""
    ctx = get_cli_context()
    project_def = ctx.project_definition

    if project_def is None:
        raise CliError(f"No {DEFINITION_FILENAME} found. Run 'snow apps init' first.")

    # Get entities with type "snowflake-app"
    snowflake_apps = {}
    if hasattr(project_def, "entities"):
        for entity_id, entity in project_def.entities.items():
            if getattr(entity, "type", None) == SNOWFLAKE_APP_ENTITY_TYPE:
                snowflake_apps[entity_id] = entity

    return snowflake_apps


def _resolve_entity_id(entity_id: Optional[str]) -> str:
    """
    Resolve the entity_id from the argument or project definition.

    If entity_id is provided, use it. Otherwise, if there's exactly one
    snowflake-app entity in the project, use that. Otherwise, raise an error.
    """
    if entity_id:
        return entity_id

    snowflake_apps = _get_snowflake_app_entities()

    if len(snowflake_apps) == 0:
        raise CliError(
            f"No snowflake-app entities found in {DEFINITION_FILENAME}. "
            "Add a snowflake-app entity or run 'snow apps init' first."
        )
    elif len(snowflake_apps) == 1:
        return list(snowflake_apps.keys())[0]
    else:
        entity_ids = ", ".join(snowflake_apps.keys())
        raise CliError(
            f"Multiple snowflake-app entities found: {entity_ids}. "
            "Please specify --entity-id to select one."
        )


def _get_entity(entity_id: str) -> SnowflakeAppEntityModel:
    """Get the snowflake-app entity by ID."""
    from snowflake.cli._plugins.apps.snowflake_app_entity_model import (
        SnowflakeAppEntityModel,
    )

    snowflake_apps = _get_snowflake_app_entities()
    if entity_id not in snowflake_apps:
        raise CliError(f"Entity '{entity_id}' not found in {DEFINITION_FILENAME}.")
    entity = snowflake_apps[entity_id]
    assert isinstance(entity, SnowflakeAppEntityModel)
    return entity


class SnowflakeAppManager(SqlExecutionMixin):
    """Manager for Snowflake App operations."""

    def create_schema_if_not_exists(self, database: str, schema: str) -> None:
        """Create schema if it doesn't exist."""
        fqn = f'"{database}"."{schema}"'
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
        app_port: int = DEFAULT_APP_PORT,
        app_comment: Optional[str] = None,
    ) -> None:
        """Create a service with a placeholder spec (suspended by default)."""
        spec = f"""spec:
  containers:
    - name: main
      image: "/snowflake/images/snowflake_images/sf-image-build:0.0.1"
      command:
        - sleep
        - infinity
  endpoints:
    - name: app-endpoint
      port: {app_port}
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
        app_port: int = DEFAULT_APP_PORT,
    ) -> None:
        """Alter a service with the built image spec."""
        spec = f"""spec:
  containers:
    - name: main
      image: "{image_url}"
  endpoints:
    - name: app-endpoint
      port: {app_port}
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
