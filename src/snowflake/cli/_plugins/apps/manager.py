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
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Set, TypeVar

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


T = TypeVar("T")


def _poll_until(
    poll_fn: Callable[[], T],
    *,
    done_states: Optional[Set[str]] = None,
    error_states: Optional[Set[str]] = None,
    known_pending_states: Optional[Set[str]] = None,
    is_done: Optional[Callable[[T], bool]] = None,
    is_error: Optional[Callable[[T], bool]] = None,
    format_status: Callable[[T], str] = str,
    max_attempts: int = 240,
    interval_seconds: int = 5,
    timeout_message: str = "Operation timed out.",
) -> T:
    """Poll *poll_fn* until the result satisfies a done condition.

    Two modes are supported:

    **State-set mode** (default when *done_states* is provided):
        Compare the returned string against *done_states*, *error_states*,
        and *known_pending_states* sets.

    **Predicate mode** (when *is_done* is provided):
        Call *is_done(result)* each iteration.  Optionally supply *is_error*
        to detect error values.

    Raises ``CliError`` on error or timeout.  Returns the final value on
    success.
    """
    for _attempt in range(max_attempts):
        time.sleep(interval_seconds)
        result = poll_fn()
        cli_console.step(f"Status: {format_status(result)}")

        if is_done is not None:
            # ── Predicate mode ────────────────────────────────────
            if is_done(result):
                return result
            if is_error is not None and is_error(result):
                raise CliError(f"{timeout_message} (status={format_status(result)})")
        else:
            # ── State-set mode (original behaviour) ───────────────
            if done_states and result in done_states:
                return result
            if error_states and result in error_states:
                raise CliError(f"{timeout_message} (status={result})")
            if known_pending_states is not None and result not in known_pending_states:
                cli_console.step(f"Unknown status: {result}")
                return result

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
        schema_fqn = FQN(database=None, schema=database, name=schema)
        self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn.sql_identifier}")

    def stage_exists(self, stage_fqn: FQN) -> bool:
        """Check if a stage exists."""
        try:
            self.execute_query(f"DESCRIBE STAGE {stage_fqn.sql_identifier}")
            return True
        except Exception:
            return False

    def clear_stage(self, stage_fqn: FQN) -> None:
        """Clear all files from a stage."""
        self.execute_query(f"REMOVE @{stage_fqn.identifier}")

    def create_stage(
        self, stage_fqn: FQN, encryption_type: str = "SNOWFLAKE_SSE"
    ) -> None:
        """Create a stage if it doesn't exist."""
        self.execute_query(
            f"CREATE STAGE IF NOT EXISTS {stage_fqn.sql_identifier} ENCRYPTION = (TYPE = '{encryption_type}')"
        )

    def drop_service_if_exists(self, service_fqn: FQN) -> None:
        """Drop a service if it exists."""
        self.execute_query(f"DROP SERVICE IF EXISTS {service_fqn.sql_identifier}")

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
        job_service_name: FQN,
        compute_pool: str,
        code_stage: FQN,
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
    source: "@{code_stage.identifier}"
    uid: 65532"""

        query_lines = [
            f"EXECUTE JOB SERVICE IN COMPUTE POOL {compute_pool}",
            f"NAME = {job_service_name.sql_identifier}",
            "ASYNC = TRUE",
            f"FROM SPECIFICATION $${spec}$$",
        ]

        if external_access_integration:
            query_lines.insert(
                3, f"EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration})"
            )

        query = "\n".join(query_lines)
        self.execute_query(query)

    def get_build_status(self, job_fqn: FQN) -> str:
        """
        Get the status of the build job service.

        Returns:
            - "IDLE" if the job service doesn't exist
            - The actual status from SHOW SERVICES (e.g., "PENDING", "RUNNING", "DONE", "FAILED")
        """
        self.execute_query(f"SHOW SERVICES IN SCHEMA {job_fqn.prefix}")

        # Query the result to find the job status
        result = self.execute_query(
            f'SELECT COUNT(*), MAX("status") '
            f"FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) "
            f"WHERE \"name\" = '{job_fqn.name}'"
        )
        row = result.fetchone()

        if not row or row[0] == 0:
            return "IDLE"

        return row[1]

    def create_service(
        self,
        service_name: FQN,
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
            f"CREATE SERVICE IF NOT EXISTS {service_name.sql_identifier}\n"
            f"IN COMPUTE POOL {compute_pool}\n"
            f"FROM SPECIFICATION $${spec}$$\n"
            f"QUERY_WAREHOUSE = {query_warehouse}"
        )
        self.execute_query(query)

        if app_comment:
            escaped_comment = app_comment.replace("'", "''")
            self.execute_query(
                f"ALTER SERVICE {service_name.sql_identifier} SET COMMENT = '{escaped_comment}'"
            )

        # Suspend the service after creation (deploy will resume it)
        self.execute_query(f"ALTER SERVICE {service_name.sql_identifier} SUSPEND")

    def alter_service_spec(
        self,
        service_name: FQN,
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

        query = (
            f"ALTER SERVICE {service_name.sql_identifier}\n"
            f"FROM SPECIFICATION $${spec}$$"
        )
        self.execute_query(query)

    def resume_service(self, service_name: FQN) -> None:
        """Resume a suspended service."""
        self.execute_query(f"ALTER SERVICE {service_name.sql_identifier} RESUME")

    def get_service_status(self, service_fqn: FQN) -> str:
        """
        Get the status of a service.

        Returns:
            - "IDLE" if the service doesn't exist
            - The actual status from SHOW SERVICES (e.g., "PENDING", "READY", "SUSPENDED", "FAILED")
        """
        self.execute_query(f"SHOW SERVICES IN SCHEMA {service_fqn.prefix}")

        result = self.execute_query(
            f'SELECT COUNT(*), MAX("status") '
            f"FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) "
            f"WHERE \"name\" = '{service_fqn.name}'"
        )
        row = result.fetchone()

        if not row or row[0] == 0:
            return "IDLE"

        return row[1]

    def get_service_endpoint_url(
        self, service_fqn: FQN, endpoint_name: str = "app-endpoint"
    ) -> Optional[str]:
        """Get the ingress URL for a service endpoint."""
        self.execute_query(f"SHOW ENDPOINTS IN SERVICE {service_fqn.identifier}")

        result = self.execute_query(
            f'SELECT "ingress_url" '
            f"FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) "
            f"WHERE \"name\" = '{endpoint_name}'"
        )
        row = result.fetchone()

        if row:
            return row[0]
        return None
