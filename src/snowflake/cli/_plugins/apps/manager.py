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

import json
import logging
import re
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Set, TypeVar

from snowflake.cli._plugins.apps.snowflake_app_entity_model import DEFAULT_APP_PORT

if TYPE_CHECKING:
    from snowflake.cli._plugins.apps.snowflake_app_entity_model import (
        SnowflakeAppEntityModel,
    )
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.artifacts.utils import bundle_artifacts
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.project_paths import ProjectPaths
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import DictCursor

log = logging.getLogger(__name__)

log = logging.getLogger(__name__)

DEFINITION_FILENAME = "snowflake.yml"
SNOWFLAKE_APP_ENTITY_TYPE = "snowflake-app"
# TODO: Update to "app" after migration from __app
_APP_COMMAND_NAME = "__app"

# Default resource names for Snowflake Apps
SNOW_APPS_COMPUTE_POOL = "SNOW_APPS_DEFAULT_COMPUTE_POOL"
DEFAULT_EXTERNAL_ACCESS = "SNOW_APPS_DEFAULT_EXTERNAL_ACCESS"
DEFAULT_IMAGE_REPOSITORY = "IMAGE_REPO"
DEFAULT_IMAGE_REPO_DATABASE = "APPS"
DEFAULT_IMAGE_REPO_SCHEMA = "PUBLIC"

APP_DEFAULTS_TABLE = "APPS.PUBLIC.SNOW_APP_DEFAULTS"
APP_DEFAULTS_INTEGRATION = "snowflake-apps"

_BUILD_IMAGE = "/snowflake/images/snowflake_images/sf-image-build:0.0.1"
_SERVICE_PLACEHOLDER_IMAGE = "/snowflake/images/snowflake_images/sf-image-build:0.0.1"

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


def _resolve_deploy_defaults(
    entity: "SnowflakeAppEntityModel",
    manager: "SnowflakeAppManager",
) -> Dict[str, Optional[str]]:
    """Resolve deploy defaults using a four-tier precedence:

    1. Values explicitly set in ``snowflake.yml`` (highest priority)
    2. Values from the current connection context
    3. Values from the ``APP_DEFAULTS_TABLE`` config table
    4. Built-in defaults (object-existence checks, lowest priority)

    Returns a dict with keys ``query_warehouse``, ``build_compute_pool``,
    ``service_compute_pool``, ``build_eai``, ``image_repository``,
    ``image_repo_database``, ``image_repo_schema``, ``database``, and
    ``schema``.  Any of them may still be ``None`` if no source provides
    a value.
    """

    # ── 1. snowflake.yml values ───────────────────────────────────────
    fqn = entity.fqn
    app_name = fqn.name
    yml_vals: Dict[str, Optional[str]] = {
        "query_warehouse": entity.query_warehouse,
        "build_compute_pool": (
            entity.build_compute_pool.name if entity.build_compute_pool else None
        ),
        "service_compute_pool": (
            entity.service_compute_pool.name if entity.service_compute_pool else None
        ),
        "build_eai": entity.build_eai.name if entity.build_eai else None,
        "image_repository": (
            entity.image_repository.name if entity.image_repository else None
        ),
        "image_repo_database": (
            entity.image_repository.database if entity.image_repository else None
        ),
        "image_repo_schema": (
            entity.image_repository.schema_ if entity.image_repository else None
        ),
        "database": fqn.database,
        "schema": fqn.schema,
    }

    # ── 2. Current connection values ──────────────────────────────────
    ctx = get_cli_context()
    conn = ctx.connection_context
    conn_vals: Dict[str, Optional[str]] = {
        "query_warehouse": conn.warehouse,
        "database": conn.database,
        "schema": conn.schema,
    }

    # ── 3. Config-table values ────────────────────────────────────────
    table_vals: Dict[str, Optional[str]] = {}
    role = manager.current_role()
    if role:
        raw = manager.fetch_config_table_defaults(role)
        if raw:
            cli_console.step(
                f"Loaded config-table defaults for role {role}: "
                + ", ".join(f"{k}={v}" for k, v in raw.items())
            )
        table_vals = {
            "query_warehouse": raw.get("warehouse"),
            "build_compute_pool": raw.get("compute_pool"),
            "service_compute_pool": raw.get("compute_pool"),
            "build_eai": raw.get("eai"),
            "image_repository": raw.get("image_repository"),
            "image_repo_database": raw.get("image_repo_database"),
            "image_repo_schema": raw.get("image_repo_schema"),
            "database": raw.get("database"),
            "schema": raw.get("schema"),
        }

    # ── 4. Built-in defaults ──────────────────────────────────────────
    builtin_vals: Dict[str, Optional[str]] = {
        "build_compute_pool": _get_compute_pool(),
        "service_compute_pool": _get_compute_pool(),
        "build_eai": _get_external_access(app_name),
        "image_repository": DEFAULT_IMAGE_REPOSITORY,
    }

    # ── Merge (first non-None wins) ──────────────────────────────────
    all_keys = set(yml_vals) | set(conn_vals) | set(table_vals) | set(builtin_vals)
    resolved: Dict[str, Optional[str]] = {}
    for key in all_keys:
        for source in (yml_vals, conn_vals, table_vals, builtin_vals):
            val = source.get(key)
            if val is not None:
                resolved[key] = val
                break
        else:
            resolved[key] = None

    # Default image repo lives in TEMP.APPS; user-specified repos without
    # explicit db/schema will fall back to the entity db/schema in the caller.
    if resolved["image_repository"] == DEFAULT_IMAGE_REPOSITORY:
        if not resolved.get("image_repo_database"):
            resolved["image_repo_database"] = DEFAULT_IMAGE_REPO_DATABASE
        if not resolved.get("image_repo_schema"):
            resolved["image_repo_schema"] = DEFAULT_IMAGE_REPO_SCHEMA

    return resolved


def _get_snowflake_app_entities() -> Dict[str, Any]:
    """Get all snowflake-app entities from the project definition."""
    ctx = get_cli_context()
    project_def = ctx.project_definition

    if project_def is None:
        raise CliError(
            f"No {DEFINITION_FILENAME} found. "
            f"Run 'snow {_APP_COMMAND_NAME} setup' first."
        )

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
            f"Add a snowflake-app entity or run 'snow {_APP_COMMAND_NAME} setup' first."
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


def perform_bundle(
    resolved_entity_id: str,
    entity: "SnowflakeAppEntityModel",
) -> ProjectPaths:
    """Bundle source artifacts for a snowflake-app entity.

    Resolves glob patterns and src/dest mappings defined in the entity's
    ``artifacts`` list and copies (or symlinks) the matched files into a
    temporary *bundle root* directory under ``<project_root>/output/bundle``.

    This function is the shared implementation behind both
    ``snow <__app> bundle`` and the bundling step of ``snow <__app> deploy``.

    Returns the :class:`ProjectPaths` instance so callers can inspect or
    upload the bundle root, and are responsible for cleanup via
    ``project_paths.clean_up_output()`` when finished.
    """
    artifacts = entity.artifacts

    project_root = get_cli_context().project_root
    project_paths = ProjectPaths(project_root=project_root)
    project_paths.remove_up_bundle_root()
    SecurePath(project_paths.bundle_root).mkdir(parents=True, exist_ok=True)

    cli_console.step(f"Bundling source files for '{resolved_entity_id}'")
    bundle_artifacts(project_paths, artifacts)

    return project_paths


_EXPOSE_SIMPLE_RE = re.compile(
    r"^\s*EXPOSE\s+(\d+)(?:/(?:tcp|udp))?\s*$", re.IGNORECASE
)
_EXPOSE_ANY_RE = re.compile(r"^\s*EXPOSE\s+", re.IGNORECASE)

# Sentinel returned when a Dockerfile contains an EXPOSE directive that uses
# unsupported syntax (multi-port or range).  Callers should check for this
# value explicitly rather than treating it as a valid port number.
EXPOSE_UNSUPPORTED_SYNTAX: int = 0


def _find_dockerfile_expose_port(bundle_root: Path) -> Optional[int]:
    """Parse the Dockerfile in *bundle_root* and return the first EXPOSEd port.

    Returns ``None`` when no ``Dockerfile`` exists or it contains no EXPOSE
    directive.  Returns :data:`EXPOSE_UNSUPPORTED_SYNTAX` (``0``) when an
    EXPOSE line is present but uses multi-port (``EXPOSE 3000 8080``) or
    range (``EXPOSE 3000-3005``) syntax which is not supported.

    Only simple ``EXPOSE <number>`` lines are recognised (the ``/tcp`` and
    ``/udp`` suffixes are stripped).
    """
    dockerfile = bundle_root / "Dockerfile"
    if not dockerfile.exists():
        return None

    lines = dockerfile.read_text().splitlines()
    for line in lines:
        m = _EXPOSE_SIMPLE_RE.match(line)
        if m:
            return int(m.group(1))

    for line in lines:
        if _EXPOSE_ANY_RE.match(line):
            return EXPOSE_UNSUPPORTED_SYNTAX

    return None


def _build_job_spec(
    image_repo_url: str,
    app_id: str,
    code_stage: FQN,
    build_image: Optional[str] = None,
) -> str:
    """Return the SPCS YAML spec for the image-build job service."""
    image = build_image or _BUILD_IMAGE
    return f"""spec:
  containers:
  - name: main
    image: "{image}"
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


def _service_spec(
    image_url: str,
    app_port: int = DEFAULT_APP_PORT,
    command: Optional[list] = None,
    execute_as_caller: bool = False,
) -> str:
    """Return the SPCS YAML spec for the application service."""
    command_block = ""
    if command:
        items = "\n".join(f"        - {c}" for c in command)
        command_block = f"\n      command:\n{items}"

    capabilities_block = ""
    if execute_as_caller:
        capabilities_block = """
capabilities:
  securityContext:
    executeAsCaller: true"""

    return f"""spec:
  containers:
    - name: main
      image: "{image_url}"{command_block}
  endpoints:
    - name: app-endpoint
      port: {app_port}
      public: true{capabilities_block}
serviceRoles:
  - name: viewer
    endpoints:
      - app-endpoint"""


class SnowflakeAppManager(SqlExecutionMixin):
    """Manager for Snowflake App operations."""

    def database_exists(self, database: str) -> bool:
        """Return True if *database* exists and is visible to the current role."""
        from snowflake.cli.api.project.util import to_string_literal

        cursor = self.execute_query(
            f"SHOW DATABASES LIKE {to_string_literal(database)}",
            cursor_class=DictCursor,
        )
        return cursor.fetchone() is not None

    def schema_exists(self, database: str, schema: str) -> bool:
        """Return True if *schema* exists in *database*."""
        from snowflake.cli.api.project.util import to_string_literal

        cursor = self.execute_query(
            f"SHOW SCHEMAS LIKE {to_string_literal(schema)}"
            f" IN DATABASE IDENTIFIER({to_string_literal(database)})",
            cursor_class=DictCursor,
        )
        return cursor.fetchone() is not None

    def role_has_schema_privilege(self, database: str, schema: str) -> bool:
        """Return True if the current role owns *database.schema*.

        Checks the ``owner`` column from ``SHOW SCHEMAS`` first, then falls
        back to ``SHOW GRANTS ON SCHEMA`` looking for an OWNERSHIP grant.
        """
        from snowflake.cli.api.project.util import to_string_literal

        role = self.current_role()
        if not role:
            return False

        cursor = self.execute_query(
            f"SHOW SCHEMAS LIKE {to_string_literal(schema)}"
            f" IN DATABASE IDENTIFIER({to_string_literal(database)})",
            cursor_class=DictCursor,
        )
        row = cursor.fetchone()
        if row and row.get("owner", "").upper() == role.upper():
            return True

        schema_fqn = FQN(database=None, schema=database, name=schema)
        cursor = self.execute_query(
            f"SHOW GRANTS ON SCHEMA {schema_fqn.sql_identifier}",
            cursor_class=DictCursor,
        )
        for grant_row in cursor:
            if (
                grant_row.get("grantee_name", "").upper() == role.upper()
                and grant_row.get("privilege") == "OWNERSHIP"
            ):
                return True
        return False

    def role_has_bind_service_endpoint(self, role: str) -> bool:
        """Return True if *role* has the account-level BIND SERVICE ENDPOINT privilege."""
        from snowflake.cli.api.project.util import to_string_literal

        safe_role = to_string_literal(role)
        cursor = self.execute_query(
            f"SHOW GRANTS TO ROLE IDENTIFIER({safe_role})", cursor_class=DictCursor
        )
        for row in cursor:
            if (
                row.get("privilege") == "BIND SERVICE ENDPOINT"
                and row.get("granted_on") == "ACCOUNT"
            ):
                return True
        return False

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

    def get_image_repo_url(
        self,
        repo_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> str:
        """Get the image repository URL and convert to local registry."""
        from snowflake.cli.api.project.util import (
            identifier_to_show_like_pattern,
            unquote_identifier,
        )

        show_obj_query = (
            f"show image repositories like {identifier_to_show_like_pattern(repo_name)}"
        )
        if database and schema:
            schema_fqn = FQN(database=None, schema=database, name=schema)
            show_obj_query += f" in schema {schema_fqn.sql_identifier}"
        elif database:
            show_obj_query += f" in database IDENTIFIER('{database}')"
        elif schema:
            raise CliError(
                "image_repository.schema requires image_repository.database to be set."
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
        build_image: Optional[str] = None,
    ) -> None:
        """Execute a build job service."""
        spec = _build_job_spec(image_repo_url, app_id, code_stage, build_image)

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
        cursor = self.execute_query(
            f"SHOW SERVICES IN SCHEMA {job_fqn.prefix}",
            cursor_class=DictCursor,
        )
        for row in cursor:
            if row["name"].upper() == job_fqn.name.upper():
                return row["status"]

        return "IDLE"

    def create_service(
        self,
        service_name: FQN,
        compute_pool: str,
        query_warehouse: str,
        app_port: int = DEFAULT_APP_PORT,
        app_comment: Optional[str] = None,
        execute_as_caller: bool = False,
    ) -> None:
        """Create a service with a placeholder spec (suspended by default)."""
        spec = _service_spec(
            _SERVICE_PLACEHOLDER_IMAGE,
            app_port,
            command=["sleep", "infinity"],
            execute_as_caller=execute_as_caller,
        )

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
        execute_as_caller: bool = False,
    ) -> None:
        """Alter a service with the built image spec."""
        spec = _service_spec(image_url, app_port, execute_as_caller=execute_as_caller)

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
        cursor = self.execute_query(
            f"SHOW SERVICES IN SCHEMA {service_fqn.prefix}",
            cursor_class=DictCursor,
        )
        for row in cursor:
            if row["name"].upper() == service_fqn.name.upper():
                return row["status"]

        return "IDLE"

    def get_service_logs(self, service_fqn: FQN, last: int = 500) -> str:
        """Fetch recent log output from an application service."""
        cursor = self.execute_query(
            f"CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS('{service_fqn.identifier}', {last})"
        )
        row = cursor.fetchone()
        return row[0] if row else ""

    def get_service_endpoint_url(
        self, service_fqn: FQN, endpoint_name: str = "app-endpoint"
    ) -> Optional[str]:
        """Get the ingress URL for a service endpoint."""
        cursor = self.execute_query(
            f"SHOW ENDPOINTS IN SERVICE {service_fqn.identifier}",
            cursor_class=DictCursor,
        )
        for row in cursor:
            if row["name"].upper() == endpoint_name.upper():
                url = row["ingress_url"]
                if (
                    url
                    and not url.startswith(("http://", "https://"))
                    and "provisioning in progress" not in url.lower()
                ):
                    url = f"https://{url}"
                return url
        return None

    def fetch_config_table_defaults(
        self, role: str, integration: str = APP_DEFAULTS_INTEGRATION
    ) -> Dict[str, str]:
        """Fetch defaults from the APP_DEFAULTS_TABLE for the given role.

        Returns a dict that may contain keys such as ``warehouse``,
        ``compute_pool``, ``eai``, ``database``, ``schema``.  Returns an empty
        dict when the table does not exist, the role lacks permissions, the
        query returns no rows, or any other error occurs.
        """
        from snowflake.cli.api.project.util import to_string_literal

        try:
            safe_integration = to_string_literal(integration)
            safe_role = to_string_literal(role.upper())

            cursor = self.execute_query(
                f"SELECT defaults FROM {APP_DEFAULTS_TABLE} "
                f"WHERE integration = {safe_integration} AND role = {safe_role} "
                f"ORDER BY updated_at DESC LIMIT 1",
                cursor_class=DictCursor,
            )
            row = cursor.fetchone()
            if row is None:
                return {}

            raw = row.get("DEFAULTS") or row.get("defaults")
            if raw is None:
                return {}

            defaults = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(defaults, dict):
                return {}

            return {k: str(v) for k, v in defaults.items() if v is not None}
        except Exception:
            log.debug(
                "Could not read %s (table may not "
                "exist or role lacks permissions) – skipping config-table defaults.",
                APP_DEFAULTS_TABLE,
                exc_info=True,
            )
            return {}

    @contextmanager
    def _use_database_and_schema(self, database: str, schema: str):
        """Temporarily set session database and schema, restoring previous values on exit."""
        prev_db = self.execute_query("SELECT CURRENT_DATABASE()").fetchone()[0]
        prev_schema = self.execute_query("SELECT CURRENT_SCHEMA()").fetchone()[0]
        self.execute_query(f"USE DATABASE {database}")
        self.execute_query(f"USE SCHEMA {schema}")
        try:
            yield
        finally:
            if prev_db:
                self.execute_query(f"USE DATABASE {prev_db}")
                if prev_schema:
                    self.execute_query(f"USE SCHEMA {prev_schema}")

    @staticmethod
    def _build_artifact_repo_config(
        query_warehouse: Optional[str] = None,
        build_eai: Optional[str] = None,
    ) -> str:
        """Build the JSON config blob accepted by the artifact-repo system functions."""
        cfg: Dict[str, Any] = {}
        if query_warehouse:
            cfg["query_warehouse"] = query_warehouse
        if build_eai:
            cfg["external_access_integrations"] = [build_eai]
        return json.dumps(cfg)

    @staticmethod
    def _build_artifact_repo_config(
        query_warehouse: Optional[str] = None,
        build_eai: Optional[str] = None,
    ) -> str:
        """Build the JSON config blob accepted by the artifact-repo system functions."""
        cfg: Dict[str, Any] = {}
        if query_warehouse:
            cfg["query_warehouse"] = query_warehouse
        if build_eai:
            cfg["external_access_integrations"] = [build_eai]
        return json.dumps(cfg)

    def build_app_artifact_repo(
        self,
        stage_fqn: FQN,
        artifact_repo_fqn: str,
        app_id: str,
        compute_pool: str,
        database: str,
        schema: str,
        runtime_image: str,
        query_warehouse: Optional[str] = None,
        build_eai: Optional[str] = None,
        project_type: str = "nodejs",
    ) -> str:
        """Build an app using SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO."""
        from snowflake.cli.api.project.util import to_string_literal

        with self._use_database_and_schema(database, schema):
            config = self._build_artifact_repo_config(query_warehouse, build_eai)
            query = (
                f"SELECT SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO("
                f"'@{stage_fqn.identifier}', "
                f"{to_string_literal(artifact_repo_fqn)}, "
                f"{to_string_literal(app_id)}, "
                f"{to_string_literal(compute_pool)}, "
                f"{to_string_literal(runtime_image)}, "
                f"{to_string_literal(project_type)}, "
                f"{to_string_literal(config)}"
                f")"
            )
            cursor = self.execute_query(query)
            row = cursor.fetchone()
            return row[0] if row else ""

    def run_app_artifact_repo(
        self,
        artifact_repo_fqn: str,
        app_id: str,
        version: str,
        service_name: str,
        compute_pool: str,
        database: str,
        schema: str,
        runtime_image: str,
        query_warehouse: Optional[str] = None,
        build_eai: Optional[str] = None,
    ) -> str:
        """Deploy an app using SYSTEM$SPCS_TEST_RUN_APP_ARTIFACT_REPO."""
        from snowflake.cli.api.project.util import to_string_literal

        with self._use_database_and_schema(database, schema):
            config = self._build_artifact_repo_config(query_warehouse, build_eai)
            query = (
                f"SELECT SYSTEM$SPCS_TEST_RUN_APP_ARTIFACT_REPO("
                f"{to_string_literal(artifact_repo_fqn)}, "
                f"{to_string_literal(app_id)}, "
                f"{to_string_literal(version)}, "
                f"{to_string_literal(service_name)}, "
                f"{to_string_literal(compute_pool)}, "
                f"{to_string_literal(runtime_image)}, "
                f"{to_string_literal(config)}"
                f")"
            )
            cursor = self.execute_query(query)
            row = cursor.fetchone()
            return row[0] if row else ""
