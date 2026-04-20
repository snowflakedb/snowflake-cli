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

from snowflake.cli._plugins.apps.generate import IS_PERSONAL_DB_SUPPORTED

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
from snowflake.connector.errors import ProgrammingError

log = logging.getLogger(__name__)

DEFINITION_FILENAME = "snowflake.yml"
SNOWFLAKE_APP_ENTITY_TYPE = "snowflake-app"
# TODO: Update to "app" after migration from __app
_APP_COMMAND_NAME = "__app"


# Mapping from SHOW PARAMETERS result names to internal resolution keys.
_SNOW_APPS_PARAM_MAP = {
    "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE": "query_warehouse",
    "DEFAULT_SNOWFLAKE_APPS_BUILD_COMPUTE_POOL": "build_compute_pool",
    "DEFAULT_SNOWFLAKE_APPS_SERVICE_COMPUTE_POOL": "service_compute_pool",
    "DEFAULT_SNOWFLAKE_APPS_BUILD_EXTERNAL_ACCESS_INTEGRATION": "build_eai",
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE": "database",
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA": "schema",
}

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


def _resolve_deploy_defaults(
    entity: "SnowflakeAppEntityModel",
    manager: "SnowflakeAppManager",
    app_name: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Resolve deploy defaults using a four-tier precedence:

    1. Values explicitly set in ``snowflake.yml`` (highest priority)
    2. SnowApps parameters (``SHOW PARAMETERS LIKE 'DEFAULT_SNOWFLAKE_APPS_%' IN USER``)
    3. Built-in defaults (personal DB for database, ``<app-id>_REPO`` for artifact repository)
    4. Current session values (lowest priority)

    Returns a dict with keys ``query_warehouse``, ``build_compute_pool``,
    ``service_compute_pool``, ``build_eai``, ``artifact_repository``,
    ``artifact_repo_database``, ``artifact_repo_schema``, ``database``,
    and ``schema``.  Any of them may still be ``None`` if no source
    provides a value.
    """

    # ── 1. snowflake.yml values ───────────────────────────────────────
    fqn = entity.fqn
    if app_name is None:
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
        "artifact_repository": (
            entity.artifact_repository.name if entity.artifact_repository else None
        ),
        "artifact_repo_database": (
            entity.artifact_repository.database if entity.artifact_repository else None
        ),
        "artifact_repo_schema": (
            entity.artifact_repository.schema_ if entity.artifact_repository else None
        ),
        "database": fqn.database,
        "schema": fqn.schema,
    }

    # ── 2. SnowApps parameters (user-level) ──────────────────────────
    param_vals: Dict[str, Optional[str]] = {}
    raw_params = manager.fetch_snow_apps_parameters()
    if raw_params:
        cli_console.step(
            "Loaded SnowApps parameters: "
            + ", ".join(f"{k}={v}" for k, v in raw_params.items())
        )
        param_vals = dict(raw_params)

    # ── 3. Built-in defaults ────────────────────────────────────────────
    from snowflake.cli.api.project.util import get_env_username

    default_vals: Dict[str, Optional[str]] = {
        "artifact_repository": f"{app_name}_REPO",
    }
    if IS_PERSONAL_DB_SUPPORTED:
        default_vals["database"] = f"USER${get_env_username().upper()}"

    # ── 4. Current session values ─────────────────────────────────────
    ctx = get_cli_context()
    conn = ctx.connection_context
    curr_session_vals: Dict[str, Optional[str]] = {
        "query_warehouse": conn.warehouse,
        "database": conn.database,
        "schema": conn.schema,
    }

    # ── Merge (first non-None wins) ──────────────────────────────────
    all_keys = (
        set(yml_vals) | set(param_vals) | set(default_vals) | set(curr_session_vals)
    )
    resolved: Dict[str, Optional[str]] = {}
    for key in all_keys:
        for source in (
            yml_vals,
            param_vals,
            default_vals,
            curr_session_vals,
        ):
            val = source.get(key)
            if val is not None:
                resolved[key] = val
                break
        else:
            resolved[key] = None

    # Artifact repo db/schema default to the resolved database/schema.
    if not resolved.get("artifact_repo_database"):
        resolved["artifact_repo_database"] = resolved.get("database")
    if not resolved.get("artifact_repo_schema"):
        resolved["artifact_repo_schema"] = resolved.get("schema")

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


class SnowflakeAppManager(SqlExecutionMixin):
    """Manager for Snowflake App operations.

    NOTE: DDL-building methods (create_app_service, build_app_artifact_repo, …)
    interpolate bare ``str`` arguments such as *compute_pool*,
    *query_warehouse*, and EAI names directly into SQL without identifier
    quoting.  This is safe as long as callers pass simple unquoted
    identifiers, but it will break for names containing spaces or special
    characters.  If that ever becomes a requirement, wrap them with
    ``FQN.from_string(name).sql_identifier`` or
    ``IDENTIFIER(to_string_literal(name))`` for consistency with the
    ``FQN``-based parameters that already use ``.sql_identifier``.
    """

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

    def drop_service_if_exists(self, service_fqn: FQN) -> None:
        """Drop a service if it exists."""
        self.execute_query(f"DROP SERVICE IF EXISTS {service_fqn.sql_identifier}")

    def drop_app_service_if_exists(self, service_fqn: FQN) -> None:
        """Drop an application service if it exists."""
        self.execute_query(
            f"DROP APPLICATION SERVICE IF EXISTS {service_fqn.sql_identifier}"
        )

    def drop_stage_if_exists(self, stage_fqn: FQN) -> None:
        """Drop a stage if it exists."""
        self.execute_query(f"DROP STAGE IF EXISTS {stage_fqn.sql_identifier}")

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

    def fetch_snow_apps_parameters(self) -> Dict[str, str]:
        """Fetch SnowApps default parameters for the current user.

        Runs ``SHOW PARAMETERS LIKE 'DEFAULT_SNOWFLAKE_APPS_%' IN USER``
        and returns a dict whose keys match the internal resolution names
        (``query_warehouse``, ``build_compute_pool``, etc.).

        Empty-string parameter values are treated as "not set" and omitted.
        Returns an empty dict on any error (e.g. insufficient privileges).
        """
        try:
            cursor = self.execute_query(
                "SHOW PARAMETERS LIKE 'DEFAULT_SNOWFLAKE_APPS_%' IN USER",
                cursor_class=DictCursor,
            )
            result: Dict[str, str] = {}
            for row in cursor:
                param_name = (row.get("key") or row.get("KEY") or "").upper()
                param_value = row.get("value") or row.get("VALUE") or ""
                mapped_key = _SNOW_APPS_PARAM_MAP.get(param_name)
                if mapped_key and param_value:
                    result[mapped_key] = param_value
            return result
        except ProgrammingError:
            log.warning(
                "Could not fetch SnowApps user parameters – skipping.",
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
        build_eai: Optional[str] = None,
    ) -> str:
        """Build the JSON config blob accepted by the artifact-repo system functions."""
        cfg: Dict[str, Any] = {}
        if build_eai:
            cfg["external_access_integrations"] = [build_eai]
        return json.dumps(cfg)

    def artifact_repo_exists(self, database: str, schema: str, repo_name: str) -> bool:
        """Return True if the artifact repository already exists."""
        from snowflake.cli.api.project.util import (
            identifier_to_show_like_pattern,
            unquote_identifier,
        )

        schema_fqn = FQN(database=None, schema=database, name=schema)
        cursor = self.execute_query(
            f"SHOW ARTIFACT REPOSITORIES LIKE {identifier_to_show_like_pattern(repo_name)}"
            f" IN SCHEMA {schema_fqn.sql_identifier}",
            cursor_class=DictCursor,
        )
        unqualified = unquote_identifier(repo_name).upper()
        return any(row["name"].upper() == unqualified for row in cursor)

    def create_artifact_repo(self, database: str, schema: str, repo_name: str) -> None:
        """Create an artifact repository.

        Uses IF NOT EXISTS so concurrent invocations (e.g. parallel CI
        jobs) don't race on the CREATE after both pass the existence check.
        """
        fqn = FQN(database=database, schema=schema, name=repo_name)
        self.execute_query(
            f"CREATE ARTIFACT REPOSITORY IF NOT EXISTS {fqn.sql_identifier} TYPE=APPLICATION"
        )

    def build_app_artifact_repo(
        self,
        stage_fqn: FQN,
        artifact_repo_fqn: str,
        app_id: str,
        compute_pool: Optional[str],
        database: str,
        schema: str,
        runtime_image: str = "",
        build_eai: Optional[str] = None,
        project_type: str = "nodejs",
    ) -> str:
        """Build an app using SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO."""
        from snowflake.cli.api.project.util import to_string_literal

        with self._use_database_and_schema(database, schema):
            config = self._build_artifact_repo_config(build_eai)
            query = (
                f"SELECT SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO("
                f"'@{stage_fqn.identifier}', "
                f"{to_string_literal(artifact_repo_fqn)}, "
                f"{to_string_literal(app_id)}, "
                f"{to_string_literal(compute_pool or '')}, "
                f"{to_string_literal(runtime_image)}, "
                f"{to_string_literal(project_type)}, "
                f"{to_string_literal(config)}"
                f")"
            )
            cursor = self.execute_query(query)
            row = cursor.fetchone()
            return row[0] if row else ""

    def create_app_service(
        self,
        service_fqn: FQN,
        artifact_repo_fqn: str,
        package_name: str,
        compute_pool: Optional[str] = None,
        version: Optional[str] = None,
        query_warehouse: Optional[str] = None,
        external_access_integrations: Optional[list[str]] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Create an application service from an artifact repository package."""
        parts = [
            f"CREATE APPLICATION SERVICE {service_fqn.identifier}",
            f"FROM ARTIFACT REPOSITORY {artifact_repo_fqn} PACKAGE {package_name}",
        ]
        if version:
            parts.append(f"VERSION {version}")
        if compute_pool:
            parts.append(f"IN COMPUTE POOL {compute_pool}")
        if external_access_integrations:
            eai_list = ", ".join(external_access_integrations)
            parts.append(f"EXTERNAL_ACCESS_INTEGRATIONS = ({eai_list})")
        if query_warehouse:
            parts.append(f"QUERY_WAREHOUSE = {query_warehouse}")
        if comment:
            escaped = comment.replace("'", "''")
            parts.append(f"COMMENT = '{escaped}'")

        query = "\n".join(parts)
        self.execute_query(query)

    def upgrade_app_service(
        self,
        service_fqn: FQN,
        version: Optional[str] = None,
    ) -> None:
        """Upgrade an existing application service to a new version."""
        query = f"ALTER APPLICATION SERVICE {service_fqn.identifier} UPGRADE"
        if version:
            query += f"\nTO VERSION {version}"
        self.execute_query(query)

    def describe_app_service(self, service_fqn: FQN) -> Dict[str, Any]:
        """Run ``DESCRIBE APPLICATION SERVICE`` and return a case-insensitive
        dict of the first result row.

        The Snowflake DictCursor may return column names in any case. This
        method normalises every key to lowercase so callers can reliably use
        ``result["url"]`` or ``result["is_upgrading"]``.

        Returns an empty dict when the DESCRIBE returns no rows.
        """
        cursor = self.execute_query(
            f"DESCRIBE APPLICATION SERVICE {service_fqn.identifier}",
            cursor_class=DictCursor,
        )
        row = cursor.fetchone()
        if row is None:
            return {}
        normalised = {k.lower(): v for k, v in row.items()}
        log.debug("DESCRIBE APPLICATION SERVICE %s: %s", service_fqn, normalised)
        return normalised

    def get_app_service_logs(self, service_name: str) -> str:
        """Get logs for an application service."""
        from snowflake.cli.api.project.util import to_string_literal

        cursor = self.execute_query(
            f"CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS({to_string_literal(service_name)})"
        )
        row = cursor.fetchone()
        return row[0] if row else ""
