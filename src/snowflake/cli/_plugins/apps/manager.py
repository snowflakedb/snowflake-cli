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

import glob
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from contextvars import copy_context
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
)

DEFAULT_PERSONAL_SCHEMA = "PUBLIC"
# Shared workspace name used when ``snow app setup`` resolves the destination
# database from the user's personal database. All of the user's apps live as
# subdirectories under this single workspace.
DEFAULT_PERSONAL_WORKSPACE_NAME = "SNOWFLAKE_APPS"
WORKSPACE_LIVE_VERSION_PATH = "versions/live"

# Snowflake assigns every user a *personal database* named ``USER$<username>``.
# Personal databases do not support stages, so app code destined for one must
# be uploaded to a workspace instead. The ``USER$`` prefix is system-assigned
# and always upper case; the username portion's case is preserved by Snowflake
# and is irrelevant to this check.
PERSONAL_DATABASE_PREFIX = "USER$"


def is_personal_database(database: Optional[str]) -> bool:
    """Return ``True`` when *database* is a Snowflake personal database (PDB).

    Personal databases are named ``USER$<username>`` and do not support
    stages, so the Snowflake App Runtime flow must upload code to a workspace
    rather than a stage whenever the resolved destination is one of them.

    The check tolerates quoted identifiers (e.g.
    ``"USER$first.last@domain.com"``) by stripping the surrounding quotes
    before matching the system-assigned ``USER$`` prefix.
    """
    if not database:
        return False
    name = identifier_to_str(database.strip())
    return name.upper().startswith(PERSONAL_DATABASE_PREFIX)


# Snowsight admin-setup docs, surfaced when the account-configured destination
# database/schema is not accessible to the current role so the user knows where
# to ask their administrator for access.
ACCOUNT_ADMIN_SETUP_URL = (
    "https://docs.snowflake.com/en/developer-guide/snowflake-app-runtime/"
    "account-admin-setup#after-setup"
)

# Placeholder object name used when probing destination privileges with
# EXPLAIN_PRIVILEGES. The privileges required to create these objects depend on
# the destination database/schema, not on the (not-yet-created) object name, so
# a fixed placeholder is sufficient.
PRIVILEGE_CHECK_OBJECT_NAME = "SNOWFLAKE_CLI_PRIVILEGE_CHECK"

if TYPE_CHECKING:
    from snowflake.cli._plugins.apps.snowflake_app_entity_model import (
        SnowflakeAppEntityModel,
    )
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.artifacts.utils import symlink_or_copy
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.project_paths import ProjectPaths
from snowflake.cli.api.project.util import identifier_to_str, to_identifier
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.stage_path import StagePath
from snowflake.cli.api.utils.path_utils import resolve_without_follow
from snowflake.cli.api.utils.tty import is_tty_interactive
from snowflake.connector.cursor import DictCursor
from snowflake.connector.errors import ProgrammingError

log = logging.getLogger(__name__)

# Characters allowed in a ``file://`` URI without wrapping it in a quoted
# string literal. Mirrors the stage manager's equivalent so workspace PUT
# statements escape local paths identically.
_UNQUOTED_FILE_URI_REGEX = r"[\w/*?\-.=&{}$#[\]\"\\!@%^+:]+"


def _local_path_to_file_uri(local_path: str) -> str:
    """Return a ``file://`` URI for *local_path*, ready to embed in a PUT.

    *local_path* must use the platform's native separators (e.g. backslashes
    on Windows); do not pass a ``Path.as_posix()`` string, as Snowflake's
    file-URI parser expects native Windows paths and a forward-slash drive
    path such as ``file://C:/...`` is rejected on Windows (connector error
    253006, ER_FILE_NOT_EXISTS).

    The returned value is either a bare URI (when it contains only characters
    allowed unquoted) or a single-quoted string literal. When quoting is
    required, backslashes are doubled because Snowflake's file-URI parser
    treats ``\\`` as an escape prefix even inside a string literal.

    Glob metacharacters in *local_path* are escaped with :func:`glob.escape`.
    The connector expands every PUT source through ``glob.glob`` before
    uploading, so an unescaped literal path containing ``*``, ``?`` or ``[``
    (e.g. a Next.js dynamic-route directory such as ``[id]`` or ``[...slug]``)
    is interpreted as a pattern: it silently matches nothing — raising
    connector error 253006 (``File doesn't exist``) — or, when a same-named
    sibling happens to match, resolves to a directory (``Not a file but a
    directory``). Escaping makes the connector match the file literally.
    """
    from snowflake.cli.api.project.util import to_string_literal

    uri = f"file://{glob.escape(local_path)}"
    if re.fullmatch(_UNQUOTED_FILE_URI_REGEX, uri):
        return uri
    return to_string_literal(uri.replace("\\", "\\\\"))


def app_fqn(
    *,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    name: str,
) -> FQN:
    """Build an :class:`FQN` with each component pre-quoted when needed.

    Snowflake App Runtime entities frequently target *personal databases*
    whose names contain characters illegal in unquoted identifiers — e.g.
    ``USER$first.last@domain.com``. ``FQN.identifier`` (and via it
    ``sql_identifier`` / ``prefix``) joins the components with literal
    dots, so without per-component quoting the server parses the result
    as several dot-separated identifiers and fails with ``invalid
    identifier`` / ``syntax error``.

    Routing each component through :func:`to_identifier` at construction
    time stores the already-quoted form on the FQN, so every downstream
    ``fqn.identifier`` / ``fqn.sql_identifier`` / ``fqn.prefix`` access
    produces valid SQL with zero changes to the SQL emission methods.
    :func:`to_identifier` is a no-op for names that are already valid
    (quoted or unquoted), so plain identifiers like ``DB.SCHEMA.OBJ`` are
    unchanged.

    The shared ``FQN`` API in :mod:`snowflake.cli.api.identifiers` is left
    untouched — this fix is scoped to the snowflake-app plugin.
    """
    return FQN(
        database=to_identifier(str(database)) if database else None,
        schema=to_identifier(str(schema)) if schema else None,
        name=to_identifier(str(name)),
    )


DEFINITION_FILENAME = "snowflake.yml"
SNOWFLAKE_APP_ENTITY_TYPE = "snowflake-app"

# Maximum number of files uploaded concurrently during the code-upload phase.
# Each file is sent with its own ``PUT``; the Snowflake connector permits a
# single connection to be shared across threads (DB API 2.0 threadsafety
# level 2), so several PUTs can run at once to hide per-statement round-trip
# latency. Capped to avoid overwhelming the connection or the local machine.
MAX_PARALLEL_UPLOADS = 5


# Mapping from SHOW PARAMETERS result names to internal resolution keys.
#
# Compute pools are intentionally absent: app services always run on
# server-managed compute pools, so the ``DEFAULT_SNOWFLAKE_APPS_*_COMPUTE_POOL``
# account parameters are no longer fetched. Compute pools are only honored when
# set explicitly in an existing ``snowflake.yml``.
_SNOW_APPS_PARAM_MAP = {
    "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE": "query_warehouse",
    "DEFAULT_SNOWFLAKE_APPS_BUILD_EXTERNAL_ACCESS_INTEGRATION": "build_eai",
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE": "database",
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA": "schema",
}

# Artifact-repo build jobs run as SPCS job services. The container/instance to
# read logs from is resolved at runtime via ``SHOW SERVICE CONTAINERS IN
# SERVICE``; when a service exposes multiple containers the one named ``builder``
# is preferred.
BUILD_JOB_CONTAINER_NAME = "builder"

T = TypeVar("T")


def _ts() -> str:
    """Return the current local time as ``HH:MM:SS`` for polling message prefixes."""
    return time.strftime("%H:%M:%S")


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
    on_poll: Optional[Callable[[], None]] = None,
) -> T:
    """Poll *poll_fn* until the result satisfies a done condition.

    Two modes are supported:

    **State-set mode** (default when *done_states* is provided):
        Compare the returned string against *done_states*, *error_states*,
        and *known_pending_states* sets.

    **Predicate mode** (when *is_done* is provided):
        Call *is_done(result)* each iteration.  Optionally supply *is_error*
        to detect error values.

    If *on_poll* is provided it is called every second between status
    checks, so log output streams continuously rather than in bursts
    every *interval_seconds*.  Exceptions from *on_poll* are logged and
    swallowed so they never interrupt the polling loop.

    Raises ``CliError`` on error or timeout.  Returns the final value on
    success.
    """

    def _failure_message_from_timeout_message(message: str) -> str:
        """Convert timeout-style wording into failure wording for terminal error states."""
        return re.sub(r"\btimed out\b", "failed", message, count=1, flags=re.IGNORECASE)

    for _attempt in range(max_attempts):
        if on_poll is not None:
            for _ in range(interval_seconds):
                time.sleep(1)
                try:
                    on_poll()
                except Exception:
                    log.debug("on_poll callback failed", exc_info=True)
        else:
            time.sleep(interval_seconds)

        result = poll_fn()
        cli_console.step(f"[{_ts()}] Status: {format_status(result)}")

        if is_done is not None:
            # ── Predicate mode ────────────────────────────────────
            if is_done(result):
                return result
            if is_error is not None and is_error(result):
                raise CliError(
                    f"{_failure_message_from_timeout_message(timeout_message)} "
                    f"(status={format_status(result)})"
                )
        else:
            # ── State-set mode (original behaviour) ───────────────
            if done_states and result in done_states:
                return result
            if error_states and result in error_states:
                raise CliError(
                    f"{_failure_message_from_timeout_message(timeout_message)} "
                    f"(status={result})"
                )
            if known_pending_states is not None and result not in known_pending_states:
                raise CliError(f"{timeout_message} (unexpected status={result})")

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


def _flatten_missing_privileges(node: Any) -> list[Dict[str, str]]:
    """Flatten an ``EXPLAIN_PRIVILEGES`` JSON tree into a flat list of the
    permission (leaf) nodes it reports.

    The tree is composed of permission nodes (``{"privilege", "objectType",
    "objectName"}``), ``allOf`` / ``oneOf`` group nodes, and the terminal
    decision node ``{"authorized": true}``. With ``missing_only => true`` an
    ``authorized`` node means nothing is missing, so it contributes nothing.
    """
    if not isinstance(node, dict):
        return []
    if node.get("authorized") is True:
        return []
    if any(key in node for key in ("privilege", "objectType", "objectName")):
        return [node]
    results: list[Dict[str, str]] = []
    for group_key in ("allOf", "oneOf"):
        for child in node.get(group_key, []) or []:
            results.extend(_flatten_missing_privileges(child))
    return results


def _deploy_privilege_check_statements(database: str, schema: str) -> list[str]:
    """Build the representative DDL ``snow app deploy`` issues against the
    destination *database*/*schema*, for privilege probing via
    ``EXPLAIN_PRIVILEGES``.

    We probe two statements: ``CREATE STAGE`` and ``CREATE ARTIFACT
    REPOSITORY``. Object names are placeholders — the required privileges depend
    on the destination database and schema, not the final object name — and are
    emitted as plain (per-component quoted) dotted identifiers rather than
    ``IDENTIFIER(...)`` so the analyzer can resolve them. Together these two
    require ``USAGE`` on the database and ``CREATE`` on the schema, the
    privileges that distinguish an accessible destination from an inaccessible
    one.

    Limitation: this is not the full set of statements ``snow app deploy`` runs.
    The others cannot be probed because they reference objects that do not exist
    at check time (artifact repository, package, build/app service, stage
    contents) — ``EXPLAIN_PRIVILEGES`` rejects those with "requires access on all
    objects" regardless of grants — or they need only ``USAGE`` already implied
    by the two probes (``SHOW`` / ``USE``), or they belong to the workspace
    upload flow used only for the personal-database default rather than the
    account-configured destination checked here.
    """
    placeholder = app_fqn(
        database=database, schema=schema, name=PRIVILEGE_CHECK_OBJECT_NAME
    ).identifier
    return [
        f"CREATE STAGE {placeholder} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')",
        f"CREATE ARTIFACT REPOSITORY {placeholder} TYPE=APPLICATION",
    ]


def _format_missing_privileges(nodes: list[Dict[str, str]]) -> list[str]:
    """Render missing-privilege nodes as de-duplicated, terminal-safe strings."""
    formatted: list[str] = []
    for node in nodes:
        privilege = (node.get("privilege") or "").strip()
        object_type = (node.get("objectType") or "").strip()
        object_name = (node.get("objectName") or "").strip()
        privilege_label = privilege if privilege else "any privilege"
        target = " ".join(part for part in (object_type, object_name) if part)
        description = (
            f"{privilege_label} on {sanitize_for_terminal(target)}"
            if target
            else privilege_label
        )
        if description not in formatted:
            formatted.append(description)
    return formatted


def _filter_accessible_remote_defaults(
    manager: "SnowflakeAppManager",
    params: Dict[str, str],
) -> Dict[str, str]:
    """Drop the account-configured destination database/schema when the current
    role lacks the privileges to deploy there.

    The destination database and schema can be configured at the account level
    by an administrator (``DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE`` /
    ``DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA``), but the role running the CLI
    may not have been granted the privileges needed to build and deploy there.
    Deploying against such a destination fails late with an opaque error, so we
    probe up front: every representative statement ``snow app deploy`` runs is
    analyzed with ``EXPLAIN_PRIVILEGES(… , missing_only => true, for_role =>
    <current role>)``.

    When the role is missing privileges (or no statement can be analyzed at all,
    which means the destination cannot even be resolved), the destination is
    removed from *params* and a warning lists the missing grants, so resolution
    falls back to the user's personal database — exactly as if no account
    defaults were configured. Statements that individually fail to analyze while
    others succeed are ignored, so an unsupported statement never diverts a user
    who otherwise has access.

    Returns a copy of *params* with the destination keys removed, or the
    original dict unchanged when the destination is usable or unset.
    """
    database = params.get("database")
    if not database:
        return params
    schema = params.get("schema") or DEFAULT_PERSONAL_SCHEMA

    role = manager.current_role()
    cli_console.step(
        "Checking deploy privileges on the account-configured destination "
        f"{sanitize_for_terminal(database)}.{sanitize_for_terminal(schema)}..."
    )
    statements = _deploy_privilege_check_statements(database, schema)
    log.info(
        "Probing deploy privileges as role %r on %r statement(s).",
        role,
        len(statements),
    )

    # The check fails if any probe statement reports missing privileges *or*
    # raises. With the reduced statement set (which references only the
    # destination database/schema) a ``ProgrammingError`` means the role cannot
    # analyze/resolve the destination — e.g. ``EXPLAIN_PRIVILEGES`` rejects it
    # with "requires access on all objects" — which is itself a failure, not a
    # condition to skip.
    missing: list[Dict[str, str]] = []
    check_failed = False
    for statement in statements:
        try:
            statement_missing = manager.get_missing_privileges(statement, role)
        except Exception as exc:
            check_failed = True
            log.info(
                "Privilege check: failed to analyze statement: %s (%s)",
                statement,
                exc,
            )
            log.debug(
                "EXPLAIN_PRIVILEGES error detail for: %s", statement, exc_info=True
            )
            continue
        if statement_missing:
            check_failed = True
            log.info(
                "Privilege check: missing %s for: %s",
                _format_missing_privileges(statement_missing),
                statement,
            )
            missing.extend(statement_missing)
        else:
            log.info("Privilege check: OK for: %s", statement)

    if not check_failed:
        log.info(
            "Privilege check passed: role %r has the privileges to deploy to " "%r.%r.",
            role,
            database,
            schema,
        )
        return params

    # Prefer the specific missing privileges when EXPLAIN_PRIVILEGES returned
    # them; otherwise the failure came from an analysis error, which means the
    # role cannot resolve the destination at all.
    missing_descriptions = _format_missing_privileges(missing) or [
        f"access to database '{sanitize_for_terminal(database)}'"
    ]

    log.info(
        "Privilege check failed: role %r is missing %s on %r.%r; "
        "falling back to the personal database.",
        role,
        missing_descriptions,
        database,
        schema,
    )

    role_label = f" '{sanitize_for_terminal(role)}'" if role else ""
    cli_console.warning(
        f"Your current role{role_label} is missing privileges required to "
        "deploy to the account-configured Snowflake App Runtime destination "
        f"'{sanitize_for_terminal(database)}.{sanitize_for_terminal(schema)}'. "
        "Falling back to your personal database. Ask your account administrator "
        f"to grant access: {ACCOUNT_ADMIN_SETUP_URL}"
    )
    filtered = dict(params)
    filtered.pop("database", None)
    filtered.pop("schema", None)
    return filtered


def _expand_personal_database(
    database: Optional[str], manager: "SnowflakeAppManager"
) -> Optional[str]:
    """Expand bare ``USER$`` to the full PDB name via a server-side query.

    ``FQN.using_context()`` already handles this when the username is present
    in the connection config (e.g. username/password auth).  This fallback
    covers auth methods (SSO, browser, keypair without user field) where
    ``connection_context.user`` is ``None``, by running
    ``SELECT 'USER$' || CURRENT_USER()``.
    """
    if database and identifier_to_str(database.strip()).upper() == PERSONAL_DATABASE_PREFIX:
        return manager.get_personal_database() or database
    return database


def _resolve_deploy_defaults(
    entity: "SnowflakeAppEntityModel",
    manager: "SnowflakeAppManager",
    app_name: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Resolve deploy defaults using a four-tier precedence:

    1. Values explicitly set in ``snowflake.yml`` (highest priority)
    2. Snowflake App Runtime parameters (``SHOW PARAMETERS LIKE 'DEFAULT_SNOWFLAKE_APPS_%' IN USER``)
    3. Built-in defaults (personal DB for database, ``<app-id>_REPO`` for artifact repository)
    4. Current session values (lowest priority)

    Returns a dict with keys ``query_warehouse``, ``build_compute_pool``,
    ``service_compute_pool``, ``build_eai``, ``service_eai``,
    ``artifact_repository``,
    ``artifact_repo_database``, ``artifact_repo_schema``, ``database``,
    and ``schema``.  Any of them may still be ``None`` if no source
    provides a value.

    ``build_compute_pool`` and ``service_compute_pool`` are resolved only
    from ``snowflake.yml`` (tier 1): app services otherwise run on
    server-managed compute pools, so the account parameters and built-in
    defaults never supply them.
    """

    # ── 1. snowflake.yml values ───────────────────────────────────────
    fqn = entity.fqn
    fqn.using_context()
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
        "service_eai": entity.service_eai.name if entity.service_eai else None,
        "artifact_repository": (
            entity.artifact_repository.name if entity.artifact_repository else None
        ),
        "artifact_repo_database": (
            entity.artifact_repository.database if entity.artifact_repository else None
        ),
        "artifact_repo_schema": (
            entity.artifact_repository.schema_ if entity.artifact_repository else None
        ),
        "database": _expand_personal_database(fqn.database, manager),
        "schema": fqn.schema,
    }

    # ── 2. Snowflake App Runtime parameters (user-level) ─────────────
    param_vals: Dict[str, Optional[str]] = {}
    cli_console.step("Fetching Snowflake App Runtime account parameters...")
    raw_params = manager.fetch_snow_apps_parameters()
    if raw_params:
        cli_console.step(
            "Loaded Snowflake App Runtime parameters: "
            + ", ".join(f"{k}={v}" for k, v in raw_params.items())
        )
        # Drop the account-configured destination database/schema when the
        # current role cannot access them so resolution falls back to the
        # personal database below.
        raw_params = _filter_accessible_remote_defaults(manager, raw_params)
        param_vals = dict(raw_params)

    # ── 3. Built-in defaults ────────────────────────────────────────────
    default_vals: Dict[str, Optional[str]] = {
        "artifact_repository": f"{app_name}_REPO",
    }
    cli_console.step("Checking whether a personal database exists...")
    personal_db = manager.get_personal_database()
    if personal_db:
        default_vals["database"] = personal_db
        default_vals["schema"] = DEFAULT_PERSONAL_SCHEMA

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
        raise CliError(f"No {DEFINITION_FILENAME} found. Run 'snow app setup' first.")

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
            f"Add a snowflake-app entity or run 'snow app setup' first."
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
    ``snow app bundle`` and the bundling step of ``snow app deploy`` for
    ``snowflake-app`` entities.

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
    _bundle_app_artifacts(project_paths, artifacts)

    return project_paths


def _bundle_app_artifacts(project_paths: ProjectPaths, artifacts) -> BundleMap:
    """Bundle snowflake-app artifacts while excluding the active bundle root subtree."""
    bundle_root = resolve_without_follow(project_paths.bundle_root)
    bundle_map = BundleMap(
        project_root=project_paths.project_root,
        deploy_root=project_paths.bundle_root,
    )
    for artifact in artifacts:
        bundle_map.add(artifact)

    def _exclude_bundle_root_sources(src: Path, _dest: Path) -> bool:
        resolved_src = resolve_without_follow(src)
        return resolved_src != bundle_root and bundle_root not in resolved_src.parents

    for absolute_src, absolute_dest in bundle_map.all_mappings(
        absolute=True,
        expand_directories=True,
        predicate=_exclude_bundle_root_sources,
    ):
        if absolute_src.is_file():
            symlink_or_copy(
                absolute_src,
                absolute_dest,
                deploy_root=project_paths.bundle_root,
                project_root=project_paths.project_root,
            )
    return bundle_map


class SnowflakeAppManager(SqlExecutionMixin):
    """Manager for Snowflake App Runtime operations.

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

    def __init__(self, *args, interactive: Optional[bool] = None, **kwargs):
        super().__init__(*args, **kwargs)
        # Whether to show the query spinner. ``None`` defers to TTY detection;
        # callers (e.g. ``snow app deploy``) pass the resolved
        # ``--interactive`` / ``--no-interactive`` flag.
        self._interactive = interactive
        # Set while uploads run concurrently. The per-query spinner uses a Rich
        # live display, of which only one may be active at a time, so concurrent
        # PUTs must not each open their own spinner.
        self._suppress_query_spinner = False

    @property
    def _is_interactive(self) -> bool:
        if self._interactive is not None:
            return self._interactive
        return is_tty_interactive()

    def execute_query(self, query: str, **kwargs):
        """Execute a Snowflake query with CLI spinner feedback.

        The spinner is only shown when running interactively. This honors the
        ``--interactive`` / ``--no-interactive`` flag passed by the command and
        falls back to TTY detection when the flag is not set, so the spinner is
        skipped for non-interactive runs (``--no-interactive``, piped/redirected
        output, CI, etc.) where its control characters would pollute captured
        output.

        The spinner is also skipped while ``_suppress_query_spinner`` is set
        (during concurrent uploads) because Rich permits only one live display
        at a time.
        """
        if self._suppress_query_spinner or not self._is_interactive:
            return super().execute_query(query, **kwargs)
        with cli_console.spinner() as spinner:
            spinner.add_task(description="", total=None)
            return super().execute_query(query, **kwargs)

    def get_personal_database(self) -> Optional[str]:
        """Return the personal database name for the current user.

        Runs ``SELECT 'USER$' || CURRENT_USER() AS personal_database`` and
        returns the result.  Returns ``None`` when the query fails or the
        current user is not set (e.g. in unauthenticated contexts).

        The case returned by ``CURRENT_USER()`` is preserved verbatim:
        Snowflake folds unquoted usernames to upper case at creation,
        but users created as quoted identifiers (e.g.
        ``"first.last@domain.com"``) keep their original case, and so do
        their personal databases (``USER$first.last@domain.com``). Since
        :func:`app_fqn` later wraps this value in a case-sensitive quoted
        identifier, normalizing case here would silently target the
        wrong database for those users.
        """
        try:
            cursor = self.execute_query(
                "SELECT 'USER$' || CURRENT_USER() AS personal_database"
            )
            row = cursor.fetchone()
            if row and row[0] and not row[0].endswith("$"):
                return str(row[0])
        except Exception:
            log.warning("Could not resolve personal database.", exc_info=True)
        return None

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

    def current_role(self) -> Optional[str]:
        """Return the active role name, or ``None`` when it cannot be resolved."""
        try:
            cursor = self.execute_query("SELECT CURRENT_ROLE()")
            row = cursor.fetchone()
            if row and row[0]:
                return str(row[0])
        except Exception:
            log.warning("Could not resolve current role.", exc_info=True)
        return None

    def get_missing_privileges(
        self, statement: str, role: Optional[str] = None
    ) -> list[Dict[str, str]]:
        """Return the privileges *role* is missing to run *statement*.

        Calls ``EXPLAIN_PRIVILEGES(statement => …, missing_only => true
        [, for_role => …])`` and flattens the returned JSON tree into a list
        of permission dicts (``{"privilege", "objectType", "objectName"}``).

        Returns an empty list when no privileges are missing (the server
        responds with ``{"authorized": true}``). Propagates ``ProgrammingError``
        when the statement cannot be analyzed — e.g. the current role cannot
        resolve a referenced object, which itself signals missing access.
        """
        from snowflake.cli.api.project.util import to_string_literal

        args = [
            f"statement => {to_string_literal(statement)}",
            "missing_only => true",
        ]
        if role:
            args.append(f"for_role => {to_string_literal(role)}")
        cursor = self.execute_query(f"CALL EXPLAIN_PRIVILEGES({', '.join(args)})")
        row = cursor.fetchone()
        if not row or not row[0]:
            return []
        try:
            payload = json.loads(row[0])
        except (TypeError, ValueError):
            log.debug("Could not parse EXPLAIN_PRIVILEGES output: %r", row[0])
            return []
        return _flatten_missing_privileges(payload)

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

    def create_workspace(self, workspace_fqn: FQN) -> None:
        """Create a workspace if needed and ensure it has a live version."""
        self.execute_query(
            f"CREATE WORKSPACE IF NOT EXISTS {workspace_fqn.sql_identifier}"
        )
        self.ensure_workspace_live_version(workspace_fqn)

    def ensure_workspace_live_version(self, workspace_fqn: FQN) -> None:
        """Ensure the workspace has a writable live version."""
        try:
            # TODO: switch to "ADD LIVE VERSION IF NOT EXISTS FROM LAST"
            # when Snowflake workspaces support that syntax.
            self.execute_query(
                f"ALTER WORKSPACE {workspace_fqn.sql_identifier} "
                f"ADD LIVE VERSION FROM LAST"
            )
        except ProgrammingError as e:
            error_text = str(e)
            if getattr(e, "errno", None) == 99106 or (
                "099106" in error_text and "42710" in error_text
            ):
                return
            raise

    def commit_workspace_live_version(self, workspace_fqn: FQN) -> None:
        """Commit the current workspace live version."""
        self.execute_query(f"ALTER WORKSPACE {workspace_fqn.sql_identifier} COMMIT")

    def clear_workspace(self, workspace_fqn: FQN) -> None:
        """Remove all files from the workspace's live version."""
        self.execute_query(
            f"REMOVE snow://workspace/{workspace_fqn.identifier}"
            f"/{WORKSPACE_LIVE_VERSION_PATH}/"
        )

    def drop_workspace_if_exists(self, workspace_fqn: FQN) -> None:
        """Drop a workspace if it exists."""
        self.execute_query(f"DROP WORKSPACE IF EXISTS {workspace_fqn.sql_identifier}")

    def workspace_uri(self, workspace_fqn: FQN) -> str:
        """Return the ``snow://workspace/...`` URI pointing at the live version."""
        return (
            f"snow://workspace/{workspace_fqn.identifier}"
            f"/{WORKSPACE_LIVE_VERSION_PATH}"
        )

    def workspace_last_uri(self, workspace_fqn: FQN) -> str:
        """Return the ``snow://workspace/...`` URI pointing at the last committed version."""
        return f"snow://workspace/{workspace_fqn.identifier}" f"/versions/last"

    def workspace_subdirectory_uri(
        self, workspace_fqn: FQN, directory_name: str
    ) -> str:
        """Return a workspace URI under the live version for *directory_name*."""
        normalized_directory = directory_name.strip("/")
        return f"{self.workspace_uri(workspace_fqn)}/{normalized_directory}"

    def workspace_last_subdirectory_uri(
        self, workspace_fqn: FQN, directory_name: str
    ) -> str:
        """Return a workspace URI under the last committed version for *directory_name*."""
        normalized_directory = directory_name.strip("/")
        return f"{self.workspace_last_uri(workspace_fqn)}/{normalized_directory}"

    def clear_workspace_subdirectory(
        self, workspace_fqn: FQN, directory_name: str
    ) -> None:
        """Remove all files from a subdirectory under the workspace live version."""
        self.execute_query(
            f"REMOVE {self.workspace_subdirectory_uri(workspace_fqn, directory_name)}/"
        )

    def _run_uploads(
        self, uploads: List[Tuple[str, Dict[str, str]]]
    ) -> Iterator[Dict[str, str]]:
        """Run a batch of ``PUT`` statements, up to :data:`MAX_PARALLEL_UPLOADS`
        at a time, yielding each file's result dict as its upload completes.

        *uploads* is a list of ``(put_sql, result)`` pairs.  Each ``PUT`` is
        executed on its own cursor; the Snowflake connector allows a single
        connection to be shared across threads (DB API 2.0 threadsafety level
        2), so running several at once hides per-statement round-trip latency.
        The per-query spinner is suppressed for the duration because Rich
        permits only one live display at a time and concurrent spinners would
        corrupt the terminal; callers still stream progress from the yielded
        results.

        The CLI context (used to resolve the shared connection, among other
        things) lives in a :class:`~contextvars.ContextVar`, which is *not*
        inherited by ``ThreadPoolExecutor`` worker threads.  Each ``PUT`` is
        therefore run inside a per-task :func:`~contextvars.copy_context`
        snapshot of the current context, so workers resolve the same connection
        the main thread would.  A fresh copy per task is required: a single
        ``Context`` cannot be entered by two threads at once.

        Results are yielded in completion order.  The first worker error is
        re-raised after the pool shuts down so a failed upload surfaces to the
        caller.
        """
        if not uploads:
            return
        previous_suppress = self._suppress_query_spinner
        self._suppress_query_spinner = True
        try:
            with ThreadPoolExecutor(max_workers=MAX_PARALLEL_UPLOADS) as executor:
                future_to_result = {}
                for put_sql, result in uploads:
                    ctx = copy_context()
                    future = executor.submit(ctx.run, self.execute_query, put_sql)
                    future_to_result[future] = result
                for future in as_completed(future_to_result):
                    # Propagate the first failure; remaining futures are
                    # cancelled/awaited by the context manager on exit.
                    future.result()
                    yield future_to_result[future]
        finally:
            self._suppress_query_spinner = previous_suppress

    def upload_to_workspace(
        self,
        local_root: Path,
        workspace_fqn: FQN,
        target_subdirectory: Optional[str] = None,
        overwrite: bool = True,
    ) -> Iterator[Dict[str, str]]:
        """Recursively upload *local_root*'s contents into the workspace's live version.

        Each file under *local_root* is uploaded with a single ``PUT``
        statement, preserving its relative directory structure under
        ``snow://workspace/<ws>/versions/live/``.  Files are uploaded
        one-at-a-time (rather than via ``PUT <dir>/*``) because the glob
        form also matches subdirectories, and the Snowflake PUT endpoint
        rejects directories with ``253006: Not a file but a directory``.
        Up to :data:`MAX_PARALLEL_UPLOADS` files are uploaded concurrently.
        Each uploaded file is yielded as a dict with ``source`` and
        ``target`` keys (in completion order) so callers can display progress.
        """
        base_uri = self.workspace_uri(workspace_fqn)
        if target_subdirectory:
            base_uri = self.workspace_subdirectory_uri(
                workspace_fqn, target_subdirectory
            )
        local_root = local_root.resolve()
        overwrite_str = str(overwrite).lower()
        from snowflake.cli.api.project.util import to_string_literal

        uploads: List[Tuple[str, Dict[str, str]]] = []
        for path in sorted(local_root.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(local_root)
            rel_dir = rel.parent
            dest_dir = (
                f"{base_uri}/{rel_dir.as_posix()}/"
                if rel_dir != Path(".")
                else f"{base_uri}/"
            )
            # Build the local file URI from the *native* path (not as_posix):
            # Snowflake's file-URI parser rejects forward-slash Windows drive
            # paths like ``file://C:/...`` (raising connector error 253006,
            # ER_FILE_NOT_EXISTS). ``local_path_to_file_uri`` returns a value
            # ready to embed directly, so it must not be re-quoted.
            local_uri = _local_path_to_file_uri(str(path.resolve()))
            put_sql = (
                f"PUT {local_uri} {to_string_literal(dest_dir)} "
                f"auto_compress=false overwrite={overwrite_str}"
            )
            uploads.append(
                (put_sql, {"source": str(rel), "target": f"{dest_dir}{path.name}"})
            )

        yield from self._run_uploads(uploads)

    def upload_to_stage(
        self,
        local_root: Path,
        stage_fqn: FQN,
        overwrite: bool = True,
    ) -> Iterator[Dict[str, str]]:
        """Recursively upload *local_root*'s contents into a stage.

        Each file under *local_root* is uploaded with its own ``PUT``
        statement, preserving the relative directory structure under
        ``@<stage>``.  Files are uploaded one-at-a-time (rather than via
        ``PUT <dir>/*``) because the glob form also matches subdirectories,
        and the Snowflake PUT endpoint rejects directories with ``253006:
        Not a file but a directory``.  This mirrors :meth:`upload_to_workspace`
        and, unlike a recursive ``PUT`` of the bundle root, does not mutate
        the local bundle while uploading.

        Up to :data:`MAX_PARALLEL_UPLOADS` files are uploaded concurrently.
        Each uploaded file is yielded as a dict with ``source`` and
        ``target`` keys (in completion order) so callers can display progress.
        """
        local_root = local_root.resolve()
        base_path = StagePath.from_stage_str(f"@{stage_fqn.identifier}")
        overwrite_str = str(overwrite).lower()
        uploads: List[Tuple[str, Dict[str, str]]] = []
        for path in sorted(local_root.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(local_root)
            rel_dir = rel.parent
            dest_path = (
                base_path / rel_dir.as_posix() if rel_dir != Path(".") else base_path
            )
            # Build the local file URI from the *native* path (not as_posix):
            # Snowflake's file-URI parser rejects forward-slash Windows drive
            # paths like ``file://C:/...``. ``_local_path_to_file_uri`` returns
            # a value ready to embed directly, so it must not be re-quoted.
            local_uri = _local_path_to_file_uri(str(path.resolve()))
            put_sql = (
                f"PUT {local_uri} {dest_path.path_for_sql()} "
                f"auto_compress=false overwrite={overwrite_str}"
            )
            uploads.append(
                (
                    put_sql,
                    {
                        "source": str(rel),
                        "target": f"{dest_path.absolute_path()}/{path.name}",
                    },
                )
            )

        yield from self._run_uploads(uploads)

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

    def resolve_application_service_url_from_describe(
        self, desc: Dict[str, Any]
    ) -> Optional[str]:
        """Return a browser-ready URL from :meth:`describe_app_service` output.

        Returns *None* when the row is empty, the service is upgrading, the URL
        is missing, or the URL is still the *provisioning* placeholder. Otherwise
        returns the ``url`` value with an ``https://`` prefix when needed.
        """
        if not desc:
            return None
        if str(desc.get("is_upgrading", "")).lower() in ("true", "1", "yes"):
            return None
        url = (desc.get("url") or "").strip()
        if not url or "provisioning in progress" in url.lower():
            return None
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
        return url

    def get_service_endpoint_url(self, service_fqn: FQN) -> Optional[str]:
        """Get the public URL for an application service.

        Uses ``DESCRIBE APPLICATION SERVICE`` (same source as the deploy wait
        loop): the ``url`` column from the describe result.
        """
        desc = self.describe_app_service(service_fqn)
        return self.resolve_application_service_url_from_describe(desc)

    def fetch_snow_apps_parameters(self) -> Dict[str, str]:
        """Fetch Snowflake App Runtime default parameters for the current user.

        Runs ``SHOW PARAMETERS LIKE 'DEFAULT_SNOWFLAKE_APPS_%' IN USER``
        and returns a dict whose keys match the internal resolution names
        (``query_warehouse``, ``build_eai``, etc.). Compute pool parameters
        are intentionally ignored — app services run on server-managed
        compute pools.

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
                # Skip parameters at the system-default level. Snowflake
                # returns an empty string for ``level`` when a parameter has
                # never been explicitly set at the account or user level;
                # a non-empty ``value`` in that case is merely the built-in
                # default (e.g. ``SYSTEM_COMPUTE_POOL_CPU``) and should not
                # be treated as an admin-configured value.
                param_level = row.get("level") or row.get("LEVEL") or ""
                mapped_key = _SNOW_APPS_PARAM_MAP.get(param_name)
                if mapped_key and param_value and param_level:
                    result[mapped_key] = param_value
            return result
        except ProgrammingError:
            log.warning(
                "Could not fetch Snowflake App Runtime user parameters – skipping.",
                exc_info=True,
            )
            return {}

    @contextmanager
    def _use_database_and_schema(self, database: str, schema: str):
        """Temporarily set session database and schema, restoring previous values on exit.

        Names that contain characters illegal in unquoted identifiers
        (e.g. personal databases like ``USER$first.last@domain.com``) are
        wrapped in double quotes via :func:`to_identifier`. The previous
        values returned by ``CURRENT_DATABASE()`` / ``CURRENT_SCHEMA()``
        are also routed through ``to_identifier`` since they come back
        as raw, unquoted strings.
        """
        prev_db = self.execute_query("SELECT CURRENT_DATABASE()").fetchone()[0]
        prev_schema = self.execute_query("SELECT CURRENT_SCHEMA()").fetchone()[0]
        self.execute_query(f"USE DATABASE {to_identifier(database)}")
        self.execute_query(f"USE SCHEMA {to_identifier(schema)}")
        try:
            yield
        finally:
            if prev_db:
                self.execute_query(f"USE DATABASE {to_identifier(prev_db)}")
                if prev_schema:
                    self.execute_query(f"USE SCHEMA {to_identifier(prev_schema)}")

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
            to_identifier,
            unquote_identifier,
        )

        schema_identifier = f"{to_identifier(database)}.{to_identifier(schema)}"
        cursor = self.execute_query(
            f"SHOW ARTIFACT REPOSITORIES LIKE {identifier_to_show_like_pattern(repo_name)}"
            f" IN SCHEMA {schema_identifier}",
            cursor_class=DictCursor,
        )
        unqualified = unquote_identifier(repo_name).upper()
        return any(row["name"].upper() == unqualified for row in cursor)

    def create_artifact_repo(self, database: str, schema: str, repo_name: str) -> None:
        """Create an artifact repository.

        Uses IF NOT EXISTS so concurrent invocations (e.g. parallel CI
        jobs) don't race on the CREATE after both pass the existence check.
        """
        fqn = app_fqn(database=database, schema=schema, name=repo_name)
        self.execute_query(
            f"CREATE ARTIFACT REPOSITORY IF NOT EXISTS {fqn.sql_identifier} TYPE=APPLICATION"
        )

    def build_app_artifact_repo(
        self,
        stage_fqn: Optional[FQN] = None,
        artifact_repo_fqn: str = "",
        app_id: str = "",
        compute_pool: Optional[str] = None,
        database: str = "",
        schema: str = "",
        runtime_image: str = "",
        build_eai: Optional[str] = None,
        project_type: str = "",
        source_uri: Optional[str] = None,
    ) -> str:
        """Build an app using SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO.

        The build source is specified by either *stage_fqn* (legacy stage
        flow) or *source_uri* (e.g. a ``snow://workspace/...`` URI for the
        workspace flow).  Exactly one of the two must be provided.
        """
        from snowflake.cli.api.project.util import to_string_literal

        if source_uri is None:
            if stage_fqn is None:
                raise ValueError("Either stage_fqn or source_uri must be provided")
            source_uri = f"@{stage_fqn.identifier}"

        if not artifact_repo_fqn.strip():
            raise ValueError("artifact_repo_fqn must be a non-empty string")
        if not app_id.strip():
            raise ValueError("app_id must be a non-empty string")

        with self._use_database_and_schema(database, schema):
            config = self._build_artifact_repo_config(build_eai)
            log.info(
                "Calling SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO with arguments:\n"
                "  source_uri=%r\n"
                "  artifact_repo_fqn=%r\n"
                "  app_id=%r\n"
                "  compute_pool=%r\n"
                "  runtime_image=%r\n"
                "  project_type=%r\n"
                "  config=%s\n"
                "  (session database=%r, schema=%r)",
                source_uri,
                artifact_repo_fqn,
                app_id,
                compute_pool or "",
                runtime_image,
                project_type,
                config,
                database,
                schema,
            )
            query = (
                f"SELECT SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO("
                f"{to_string_literal(source_uri)}, "
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

    def is_application_service(self, service_fqn: FQN) -> bool:
        """Return True when settings should use the ``app-service`` URL segment.

        Detection order:
        1) If ``DESCRIBE APPLICATION SERVICE`` returns a row, treat as application
           service.
        2) Otherwise, if a legacy ``SERVICE`` object exists with the same FQN,
           treat as legacy service.
        3) If type checks fail (errors/unknown), default to application service.
        """
        try:
            if self.describe_app_service(service_fqn):
                return True
        except ProgrammingError:
            log.debug(
                "DESCRIBE APPLICATION SERVICE failed for %s",
                service_fqn,
                exc_info=True,
            )

        if _object_exists("service", service_fqn.identifier):
            return False

        return True

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

    def _resolve_build_job_container(
        self, build_job_fqn: FQN
    ) -> Optional[tuple[str, str]]:
        """Resolve the ``(instance_id, container_name)`` for a build job.

        Runs ``SHOW SERVICE CONTAINERS IN SERVICE`` (the build job is an SPCS
        job service) and returns the coordinates of the container to read logs
        from. When the service exposes more than one container, a warning lists
        them and the container named :data:`BUILD_JOB_CONTAINER_NAME` is
        preferred, falling back to the first one.

        Returns ``None`` when no running container is reported yet (e.g. the
        service is still ``PENDING``); such results are not cached so a later
        poll can retry. Successful resolutions are cached per build job so the
        ``SHOW`` query and any warning happen only once.
        """
        cache: Dict[str, tuple[str, str]] = self.__dict__.setdefault(
            "_build_job_container_cache", {}
        )
        cache_key = build_job_fqn.identifier
        if cache_key in cache:
            return cache[cache_key]

        cursor = self.execute_query(
            f"SHOW SERVICE CONTAINERS IN SERVICE {build_job_fqn.identifier}",
            cursor_class=DictCursor,
        )
        rows = [{k.lower(): v for k, v in row.items()} for row in cursor]

        # Surface the raw result in verbose mode (INFO) to aid debugging.
        log.info(
            "SHOW SERVICE CONTAINERS IN SERVICE %s returned %d row(s):",
            sanitize_for_terminal(build_job_fqn.identifier),
            len(rows),
        )
        for row in rows:
            log.info("  %s", sanitize_for_terminal(str(row)))

        containers: list[tuple[str, str]] = []
        for row in rows:
            container_name = row.get("container_name")
            instance_id = row.get("instance_id")
            # A SUSPENDED/PENDING service reports NULL container fields.
            if container_name is None or instance_id is None:
                continue
            containers.append((str(instance_id), str(container_name)))

        if not containers:
            return None

        if len(containers) > 1:
            listed = ", ".join(sanitize_for_terminal(name) for _, name in containers)
            cli_console.warning(
                f"Build job {sanitize_for_terminal(build_job_fqn.identifier)} "
                f"has multiple containers: {listed}. Using "
                f"'{BUILD_JOB_CONTAINER_NAME}' if present, otherwise the first."
            )

        resolved = next(
            (
                (instance_id, name)
                for instance_id, name in containers
                if name == BUILD_JOB_CONTAINER_NAME
            ),
            containers[0],
        )
        cache[cache_key] = resolved
        return resolved

    def get_build_job_logs(self, build_job_fqn: FQN, last: int = 500) -> list[str]:
        """Fetch build logs for an artifact-repo build job.

        Uses ``SYSTEM$GET_SERVICE_LOGS`` — the same mechanism that backs the
        application logs surfaced by ``snow app events`` — rather than the build
        job's ``SPCS_GET_LOGS`` table function. The build job's container and
        instance are resolved at runtime via ``SHOW SERVICE CONTAINERS IN
        SERVICE`` (see :meth:`_resolve_build_job_container`).
        """
        from snowflake.cli.api.project.util import to_string_literal

        resolved = self._resolve_build_job_container(build_job_fqn)
        if resolved is None:
            return []
        instance_id, container_name = resolved

        cursor = self.execute_query(
            f"CALL SYSTEM$GET_SERVICE_LOGS("
            f"{to_string_literal(build_job_fqn.identifier)}, "
            f"{to_string_literal(instance_id)}, "
            f"{to_string_literal(container_name)}, "
            f"{last})"
        )
        row = cursor.fetchone()
        if not row or not row[0]:
            return []
        return [line for line in str(row[0]).splitlines() if line]
