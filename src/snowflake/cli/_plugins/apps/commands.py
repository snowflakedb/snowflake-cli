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

"""Snowflake App Runtime (``snowflake-app``) implementation functions.

These functions are called from the unified ``snow app`` command group in
``_plugins/nativeapp/commands.py`` when the detected flow is
:class:`~snowflake.cli._plugins.nativeapp.v2_conversions.compat.AppFlow.SNOWFLAKE_APP`.

They are plain Python functions (no Typer decorators) so they can be
dispatched to from the unified handlers without CLI-framework coupling.
"""

import functools
import json
import logging
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Literal, NamedTuple, Optional

import typer
from click import ClickException
from snowflake.cli._plugins.apps.generate import _generate_snowflake_yml
from snowflake.cli._plugins.apps.manager import (
    DEFAULT_PERSONAL_SCHEMA,
    DEFAULT_PERSONAL_WORKSPACE_NAME,
    DEFINITION_FILENAME,
    SnowflakeAppManager,
    _get_entity,
    _poll_until,
    _resolve_deploy_defaults,
    _resolve_entity_id,
    _ts,
    app_fqn,
    is_personal_database,
    perform_bundle,
)
from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.config import (
    get_connection_dict,
    get_default_connection_name,
    get_file_io_encoding,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CommandResult,
    EmptyResult,
    MessageResult,
    ObjectResult,
)
from snowflake.cli.api.project.util import identifier_for_url
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.connector.errors import ProgrammingError

if TYPE_CHECKING:
    from snowflake.cli._plugins.apps.snowflake_app_entity_model import (
        SnowflakeAppEntityModel,
    )

log = logging.getLogger(__name__)

# Default number of log lines returned by ``snow app events`` for the
# Snowflake App Runtime flow. The unified command accepts ``--last`` with a ``None``
# default; each flow applies its own default when the user does not provide
# a value (Native App uses ``-1``, Snowflake App Runtime uses this constant).
DEFAULT_SNOWFLAKE_APP_EVENTS_LAST = 500

# Telemetry counter recording how many files were uploaded during the
# upload phase of a deploy.
FILES_UPLOADED_COUNTER = "snowflake_app.upload.files_uploaded"

# ── Source provenance labels ──────────────────────────────────────────
SOURCE_USER_INPUT = "user input"
SOURCE_ACCOUNT_PARAM = "account parameter"
SOURCE_CURRENT_SESSION = "current session"
SOURCE_DEFAULT = "default"
SOURCE_MISSING = "missing"


def _ensure_utf8_output() -> None:
    """Force UTF-8 on ``stdout``/``stderr`` so non-ASCII output cannot crash.

    On Windows the default console encoding is a legacy code page (e.g. cp1252),
    not UTF-8. Snowflake App Runtime commands render dynamic free-text tables —
    ``events`` prints arbitrary application log text (frequently emoji, box-
    drawing, or accented characters) and ``setup --dry-run`` prints the plan
    preview. Writing a character outside the code page makes the table renderer
    raise an uncaught ``UnicodeEncodeError`` *after* the command already did its
    real work (logs fetched / plan computed), aborting with a non-zero exit and
    no useful message.

    Reconfiguring the streams to UTF-8 with ``errors="replace"`` keeps the
    output printable everywhere. macOS/Linux already default to UTF-8, so this
    is effectively a no-op there. Streams that cannot be reconfigured (already
    wrapped or redirected, e.g. a test harness buffer) are left untouched.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            # Non-reconfigurable stream (e.g. already wrapped / redirected).
            pass


def _utf8_output(func: Callable[..., CommandResult]) -> Callable[..., CommandResult]:
    """Force UTF-8 stdout/stderr before ``func`` produces any output.

    Applied to the Snowflake App Runtime command entry points so their result
    tables render non-ASCII text without an uncaught ``UnicodeEncodeError`` on
    Windows. See :func:`_ensure_utf8_output`.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> CommandResult:
        _ensure_utf8_output()
        return func(*args, **kwargs)

    return wrapper


_CodeStorageType = Literal["workspace", "stage"]


class _CodeStorage(NamedTuple):
    """Resolved code-storage backend for an app deploy/teardown.

    ``type`` selects between the ``"workspace"`` and ``"stage"`` flows.
    ``name`` plus the optional database/schema overrides identify the backing
    object; ``encryption_type`` applies only to the stage flow.
    """

    type: _CodeStorageType  # noqa: A003
    name: str
    database_override: Optional[str]
    schema_override: Optional[str]
    encryption_type: str


def _resolve_code_storage(
    entity: "SnowflakeAppEntityModel",
    *,
    database: Optional[str],
    schema: Optional[str],
    app_name: str,
) -> _CodeStorage:
    """Decide whether app code is uploaded to a workspace or a stage.

    Personal databases (``USER$<user>``) do not support stages, so any app
    whose *resolved* destination database is a personal database must use a
    workspace — regardless of what (if anything) ``snowflake.yml`` configured.
    This both honors explicit configuration for non-personal destinations and
    repairs project files that predate personal-database detection (a
    ``code_stage`` pointing at a personal database, or no code-storage block at
    all) by transparently routing them through the shared
    ``SNOWFLAKE_APPS`` workspace.

    Resolution order:

    1. Explicit ``code_workspace`` → workspace, as configured.
    2. Explicit ``code_stage`` → stage, as configured. When the destination is
       a personal database a warning is emitted (stages are generally
       unsupported there), but the user's explicit choice is still honored.
    3. Neither configured → workspace when the destination is a personal
       database, otherwise a stage named ``<app>_CODE``.
    """
    destination_is_personal = is_personal_database(database)

    if entity.code_workspace is not None:
        return _CodeStorage(
            type="workspace",
            name=entity.code_workspace.name,
            database_override=entity.code_workspace.database,
            schema_override=entity.code_workspace.schema_,
            encryption_type="SNOWFLAKE_SSE",  # unused in workspace flow
        )

    if entity.code_stage is not None:
        if destination_is_personal:
            cli_console.warning(
                f"code_stage '{sanitize_for_terminal(entity.code_stage.name)}' "
                "is configured, but the resolved destination database "
                f"'{sanitize_for_terminal(str(database))}' is a personal "
                "database, which generally does not support stages. Honoring "
                "the configured stage; the deploy may fail if stages are not "
                "supported there."
            )
        return _CodeStorage(
            type="stage",
            name=entity.code_stage.name,
            database_override=entity.code_stage.database,
            schema_override=entity.code_stage.schema_,
            encryption_type=entity.code_stage.encryption_type or "SNOWFLAKE_SSE",
        )

    # Neither code_workspace nor code_stage configured: pick the backend that
    # the destination supports.
    if destination_is_personal:
        return _CodeStorage(
            type="workspace",
            name=DEFAULT_PERSONAL_WORKSPACE_NAME,
            database_override=None,
            schema_override=None,
            encryption_type="SNOWFLAKE_SSE",
        )
    return _CodeStorage(
        type="stage",
        name=f"{app_name}_CODE",
        database_override=None,
        schema_override=None,
        encryption_type="SNOWFLAKE_SSE",
    )


@_utf8_output
def snowflake_app_setup(
    app_name: Optional[str],
    dry_run: bool,
    compute_pool: Optional[str],
    build_eai: Optional[str],
) -> CommandResult:
    """Initialize a ``snowflake.yml`` for a Snowflake App Runtime project.

    See the ``snow app setup`` command in
    :mod:`snowflake.cli._plugins.nativeapp.commands` for the CLI surface.
    """
    ctx = get_cli_context()
    metrics = ctx.metrics

    def _run() -> CommandResult:
        with metrics.span("snowflake_app.setup"):
            resolved_app_name = app_name
            if resolved_app_name is None:
                derived_app_name = Path.cwd().name
                # For implicit names, normalize directory strings into a valid
                # identifier by mapping common separators to "_" and stripping
                # all other disallowed characters.
                resolved_app_name = re.sub(
                    r"[^a-zA-Z0-9_]",
                    "",
                    derived_app_name.replace(" ", "_").replace("-", "_"),
                )

            if not resolved_app_name:
                raise ClickException(
                    "Could not derive app name from the current directory. "
                    "Please provide --app-name."
                )

            if not re.fullmatch(r"[a-zA-Z0-9_]+", resolved_app_name):
                raise ClickException(
                    f"Invalid app name '{resolved_app_name}'. "
                    "Only letters, digits, and underscores are allowed."
                )
            # snowflake.yml is a CLI-owned manifest that the ``snow app`` commands read
            # back with the same encoding policy (see _app_group_callback): an explicit
            # cli.encoding.file_io setting wins, otherwise UTF-8. Writing it the same
            # way keeps the round-trip consistent regardless of the host code page, even
            # when the generated content (e.g. a non-Latin app title) is non-ASCII.
            encoding = get_file_io_encoding() or "utf-8"
            project_file = Path.cwd() / DEFINITION_FILENAME
            if not dry_run and project_file.exists():
                return MessageResult(
                    f"{DEFINITION_FILENAME} already exists. Skipping initialization."
                )

            connection_name = (
                ctx.connection_context.connection_name or get_default_connection_name()
            )
            conn_config = get_connection_dict(connection_name)

            manager = SnowflakeAppManager()
            with metrics.span("snowflake_app.setup.resolve_defaults"):
                # ``SYSTEM$GET_APPLICATION_SERVICE_DEFAULTS()`` resolves the
                # ``DEFAULT_SNOWFLAKE_APPS_*`` parameters and drops any
                # account-configured destination the current role cannot access
                # server-side. On accounts where that function is not yet
                # available, ``fetch_app_service_defaults`` transparently falls
                # back to the legacy ``SHOW PARAMETERS`` + ``EXPLAIN_PRIVILEGES``
                # flow, so the resolution below is unaffected either way. The
                # fetch span nests under this ``resolve_defaults`` span, which it
                # reads from the metrics span stack.
                params = manager.fetch_app_service_defaults()

            def _resolve(
                user_input=None,
                account_param=None,
                default_value=None,
                current_session=None,
            ):
                """Return (value, source) using a fixed resolution order.

                Resolution: user_input > account_param > default_value > current_session.
                """
                if user_input is not None:
                    return user_input, SOURCE_USER_INPUT
                if account_param is not None:
                    return account_param, SOURCE_ACCOUNT_PARAM
                if default_value is not None:
                    return default_value, SOURCE_DEFAULT
                if current_session is not None:
                    return current_session, SOURCE_CURRENT_SESSION
                return None, SOURCE_MISSING

            # ── Pre-compute current session values ─────────────────────────────
            conn = ctx.connection_context
            # ``conn.warehouse/database/schema`` are only non-None when the user
            # explicitly passed the corresponding connection-override flag on the
            # command line (e.g. ``--warehouse MY_WH``).  Values from the
            # connection config file come through ``conn_config`` instead.
            cli_wh = getattr(conn, "warehouse", None) or None
            cli_db = getattr(conn, "database", None) or None
            cli_schema = getattr(conn, "schema", None) or None

            # A user-supplied database must be paired with an explicit schema: schema
            # resolution would otherwise fall back to an account parameter or the
            # personal-database default, silently placing the app in a schema that does
            # not belong to the requested database.
            if cli_db and not cli_schema:
                raise CliError(
                    "--schema is required when --database is specified. "
                    "Provide --schema to select the schema within the requested database."
                )

            session_wh = conn_config.get("warehouse") or None
            session_db = conn_config.get("database") or None
            session_schema = conn_config.get("schema") or None

            with metrics.span("snowflake_app.setup.get_personal_database"):
                personal_db = manager.get_personal_database()
            personal_schema = DEFAULT_PERSONAL_SCHEMA if personal_db else None

            # ── Resolve each field ────────────────────────────────────────────
            resolved = {
                "database": _resolve(
                    user_input=cli_db,
                    account_param=params.get("database"),
                    default_value=personal_db,
                    current_session=session_db,
                ),
                # TODO: Support per-app schema (e.g. APPS.APP_<app_id>) instead of
                # a single shared schema for all apps.
                "schema": _resolve(
                    user_input=cli_schema,
                    account_param=params.get("schema"),
                    default_value=personal_schema,
                    current_session=session_schema,
                ),
                "warehouse": _resolve(
                    user_input=cli_wh,
                    account_param=params.get("query_warehouse"),
                    current_session=session_wh,
                ),
                # Compute pools are intentionally not resolved or written: app
                # services always run on server-managed compute pools, so
                # ``snow app setup`` never configures ``build_compute_pool`` /
                # ``service_compute_pool``. The (hidden) ``--compute-pool`` flag is
                # accepted for backward compatibility but no longer has any effect.
                # TODO: Remove --build-eai argument once the builder service no longer
                # requires an external access integration.
                "build_eai": _resolve(
                    user_input=build_eai,
                    account_param=params.get("build_eai"),
                ),
            }

            # ── Validate required values ─────────────────────────────────────
            if not resolved["database"][0]:
                raise ClickException(
                    "Missing database. Provide --database, set the DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE account parameter, or check your connection."
                )
            if not resolved["schema"][0]:
                raise ClickException(
                    "Missing schema. Provide --schema, set the DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA account parameter, or check your connection."
                )
            if not resolved["warehouse"][0]:
                raise ClickException(
                    "Missing warehouse. Provide --warehouse, set the DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE account parameter, or check your connection."
                )

            resolved_values = {k: v[0] for k, v in resolved.items()}

            if not dry_run:
                # Use a workspace whenever the destination is a personal database:
                # either it was resolved from the built-in personal-DB default tier,
                # or it arrived via an account parameter / the current session but is
                # still a ``USER$<user>`` personal database. Personal databases do not
                # support stages, so emitting ``code_stage`` for one would produce a
                # ``snowflake.yml`` that always fails at deploy time.
                use_workspace = resolved["database"][
                    1
                ] == SOURCE_DEFAULT or is_personal_database(resolved_values["database"])
                with metrics.span("snowflake_app.setup.write_manifest"):
                    project_file.write_text(
                        _generate_snowflake_yml(
                            resolved_app_name,
                            resolved_values,
                            use_workspace=use_workspace,
                        ),
                        encoding=encoding,
                    )

            is_json = get_cli_context().output_format.is_json
            if is_json:
                return ObjectResult({"success": not dry_run, **resolved_values})

            if dry_run:
                cli_console.step("Dry run — resolved configuration:")
            else:
                cli_console.step(
                    f"Initialized Snowflake App Runtime project in {DEFINITION_FILENAME}."
                )
            for key, (value, source) in resolved.items():
                # Skip optional fields that could not be resolved (e.g. ``build_eai``
                # when no value was provided and no account parameter is set).
                # Emitting ``build_eai: None  (missing)`` is noisy and implies the
                # field is required when it is not.
                if value is None and source == SOURCE_MISSING:
                    continue
                cli_console.step(f"  {key}: {value}  ({source})")
            return EmptyResult()

    try:
        return _run()
    except ClickException as exc:
        # A dry run is a non-committal preview, so a failed setup should not
        # break callers that gate on the exit code (e.g. CI). Neutralize the
        # exit code but re-raise so the error is rendered by the exact same
        # path as a normal failure.
        if dry_run:
            exc.exit_code = 0
        raise


@_utf8_output
def snowflake_app_bundle(entity_id: Optional[str]) -> CommandResult:
    """Bundle a Snowflake App Runtime by resolving artifacts defined in ``snowflake.yml``."""
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    project_paths = perform_bundle(resolved_entity_id, entity)
    return MessageResult(f"Bundle generated at {project_paths.bundle_root}")


@_utf8_output
def snowflake_app_validate(entity_id: Optional[str]) -> CommandResult:
    """Validate a local Snowflake App Runtime project."""
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    # ── Validate database and schema ──────────────────────────────────
    fqn = entity.fqn
    database = fqn.database
    schema = fqn.schema

    manager = SnowflakeAppManager()
    metrics = get_cli_context().metrics

    if database:
        with metrics.span("snowflake_app.validate.check_database"):
            if not manager.database_exists(database):
                raise CliError(
                    f"Database '{database}' does not exist or is not accessible."
                )
        if schema:
            with metrics.span("snowflake_app.validate.check_schema"):
                if not manager.schema_exists(database, schema):
                    raise CliError(
                        f"Schema '{database}.{schema}' does not exist "
                        f"or is not accessible."
                    )

    # ── Validate project can bundle artifacts ─────────────────────────
    project_paths = None
    try:
        with metrics.span("snowflake_app.validate.bundle"):
            project_paths = perform_bundle(resolved_entity_id, entity)
    finally:
        if project_paths is not None:
            project_paths.clean_up_output()
    return MessageResult("Valid Snowflake App Runtime project.")


def _wait_for_service_endpoint(
    manager: SnowflakeAppManager,
    service_fqn: FQN,
    metrics,
) -> str:
    """Poll an application service until it exposes a browser-ready URL.

    Unlike the default ``open`` path, this tolerates a service that does not
    exist yet: ``DESCRIBE APPLICATION SERVICE`` raises a ``ProgrammingError``
    while the service is still being created, which is treated as "not ready
    yet" so the loop keeps waiting instead of failing. Returns the resolved
    URL once available; raises ``CliError`` if the service reports FAILED or
    the wait times out.
    """

    def _describe() -> dict:
        try:
            return manager.describe_app_service(service_fqn)
        except ProgrammingError:
            # Service not created yet (or not visible) — keep polling.
            return {}

    def _url_is_ready(desc: dict) -> bool:
        return manager.resolve_application_service_url_from_describe(desc) is not None

    def _svc_has_failed(desc: dict) -> bool:
        return desc.get("status", "").upper() == "FAILED"

    def _format_status(desc: dict) -> str:
        if not desc:
            return "waiting for service to be created..."
        url = desc.get("url")
        if url:
            return sanitize_for_terminal(url)
        status = (desc.get("status") or "").strip()
        if status:
            return sanitize_for_terminal(status)
        return "url not yet available"

    # Fast path: return immediately if the endpoint is already available so a
    # ready app does not incur an extra polling interval of latency.
    initial = _describe()
    ready_url = manager.resolve_application_service_url_from_describe(initial)
    if ready_url:
        return ready_url

    cli_console.step(
        f"[{_ts()}] Waiting for application service "
        f"'{service_fqn.identifier}' to be ready..."
    )
    with metrics.span("snowflake_app.open.wait_for_endpoint"):
        desc = _poll_until(
            poll_fn=_describe,
            is_done=_url_is_ready,
            is_error=_svc_has_failed,
            format_status=_format_status,
            timeout_message=(
                "Timed out waiting for application service "
                f"'{service_fqn.identifier}' to become ready. "
                "Check application service state and logs:\n"
                f"  DESCRIBE APPLICATION SERVICE {service_fqn.identifier}\n"
                f"  CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS('{service_fqn.identifier}')"
            ),
        )
    url = manager.resolve_application_service_url_from_describe(desc)
    if not url:
        raise CliError(
            "Application service URL is not available. "
            f"Check: DESCRIBE APPLICATION SERVICE {service_fqn.identifier}"
        )
    return url


@_utf8_output
def snowflake_app_open(
    entity_id: Optional[str],
    print_only: bool,
    settings: bool,
    watch: bool = False,
) -> CommandResult:
    """Open a deployed Snowflake App Runtime (or its settings page) in the browser."""
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    fqn = entity.fqn
    ctx = get_cli_context()
    metrics = ctx.metrics

    db = fqn.database or ctx.connection_context.database
    schema = fqn.schema or ctx.connection_context.schema

    if not db or not schema:
        missing = [k for k, v in {"database": db, "schema": schema}.items() if not v]
        raise CliError(
            f"Cannot resolve {' or '.join(missing)} for the app. "
            "Set them in snowflake.yml or in your connection configuration."
        )

    if settings:
        with metrics.span("snowflake_app.open.resolve_settings_url"):
            app_id = (
                f"{identifier_for_url(db)}"
                f".{identifier_for_url(schema)}"
                f".{identifier_for_url(fqn.name)}"
            )
            url = make_snowsight_url(
                ctx.connection, f"#/apps/app-service/{app_id}/details"
            )
    else:
        service_fqn = app_fqn(database=db, schema=schema, name=fqn.name)

        manager = SnowflakeAppManager()
        if watch:
            # In watch mode the service may not exist yet — poll until it is
            # created and its endpoint is ready rather than failing.
            url = _wait_for_service_endpoint(manager, service_fqn, metrics)
        else:
            try:
                with metrics.span("snowflake_app.open.resolve_endpoint"):
                    url = manager.get_service_endpoint_url(service_fqn)
                    if not url:
                        raise CliError(
                            f"No endpoint URL found for service {service_fqn}. "
                            f"Is the app deployed? Run 'snow app deploy' first."
                        )
            except ProgrammingError as err:
                raise CliError(
                    f"Could not resolve endpoint URL for service {service_fqn.identifier}. "
                    "This may indicate missing privileges on the target schema or application service."
                ) from err

    if not print_only:
        typer.launch(url)
    return MessageResult(url)


@_utf8_output
def snowflake_app_events(
    entity_id: Optional[str],
    last: Optional[int],
) -> CommandResult:
    """Fetch recent log events from a deployed Snowflake App Runtime."""
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    fqn = entity.fqn
    # Rebuild to a 3-part name; entity FQN may carry extra fields (e.g. prefix)
    service_fqn = app_fqn(database=fqn.database, schema=fqn.schema, name=fqn.name)

    effective_last = last if last is not None else DEFAULT_SNOWFLAKE_APP_EVENTS_LAST

    manager = SnowflakeAppManager()
    metrics = get_cli_context().metrics
    try:
        with metrics.span("snowflake_app.events.fetch_logs"):
            logs = manager.get_service_logs(service_fqn, last=effective_last)
    except ProgrammingError:
        raise ClickException(
            f"Could not retrieve logs for '{service_fqn.identifier}'. "
            "Verify that the app is deployed and the service is running. "
            "If the service exists, this can also happen when the active role cannot read application service logs."
        )
    return MessageResult(logs)


def _make_build_log_streamer(
    manager: SnowflakeAppManager, build_job_fqn: FQN
) -> Callable[[], None]:
    """Return an ``on_poll`` callback that streams new build log lines.

    Lines are emitted at INFO level so they only appear when the user
    runs the deploy with ``--verbose`` (or ``--debug``).  The callback
    keeps a running count of lines already shown and only emits the
    delta on each invocation.  Failures fetching logs are swallowed so
    they never interrupt the surrounding polling loop.
    """
    seen_count = 0

    def _stream() -> None:
        nonlocal seen_count
        try:
            logs = manager.get_build_job_logs(build_job_fqn)
        except Exception:
            log.debug("Failed to fetch build logs", exc_info=True)
            return
        new_lines = logs[seen_count:]
        for line in new_lines:
            log.info(line)
        seen_count = len(logs)

    return _stream


def _log_service_logs(manager: SnowflakeAppManager, service_fqn: FQN) -> None:
    """Fetch service logs and emit them at INFO level.

    INFO-level output only appears when the user runs the deploy with
    ``--verbose`` (or ``--debug``). Failures fetching logs are swallowed so the
    original deployment error remains the primary failure signal.
    """
    try:
        logs = manager.get_service_logs(service_fqn)
    except Exception:
        log.debug("Failed to fetch application service logs", exc_info=True)
        return
    for line in logs.splitlines():
        log.info(line)


@_utf8_output
def snowflake_app_deploy(
    entity_id: Optional[str],
    upload_only: bool,
    build_only: bool,
    promote_only: bool,
    interactive: Optional[bool] = None,
) -> CommandResult:
    """Build and deploy a Snowflake App Runtime through upload, build, and deploy phases."""
    phase_flags = sum((upload_only, build_only, promote_only))
    if phase_flags > 1:
        raise ClickException(
            "Only one of --upload-only, --build-only, or --promote-only "
            "may be specified."
        )

    run_upload = not build_only and not promote_only
    run_build = not upload_only and not promote_only
    run_deploy = not upload_only and not build_only
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    # ── Extract entity configuration ──────────────────────────────────
    fqn = entity.fqn
    app_name = fqn.name

    ctx = get_cli_context()
    metrics = ctx.metrics
    conn = ctx.connection_context
    database = fqn.database or conn.database
    schema = fqn.schema or conn.schema

    query_warehouse = entity.query_warehouse

    app_title = entity.meta.title if entity.meta else None
    app_description = entity.meta.description if entity.meta else None
    app_icon = entity.meta.icon if entity.meta else None

    # ── Resolve defaults (snowflake.yml > account parameters > built-in) ──
    manager = SnowflakeAppManager(interactive=interactive)
    with metrics.span("snowflake_app.deploy.resolve_defaults"):
        defaults = _resolve_deploy_defaults(entity, manager, app_name=app_name)

    database = defaults["database"]
    schema = defaults["schema"]
    build_compute_pool = defaults["build_compute_pool"]
    service_compute_pool = defaults["service_compute_pool"]
    query_warehouse = defaults["query_warehouse"]
    build_eai = defaults["build_eai"]
    # ``service_eai`` is optional; when omitted, continue using ``build_eai``
    # for the deployed application service to preserve existing projects.
    service_eai = defaults.get("service_eai") or build_eai

    # ── Resolve code storage backend ──────────────────────────────────
    # ``code_stage`` and ``code_workspace`` are mutually exclusive (enforced
    # by the entity model). The backend is chosen here — after the destination
    # database is resolved — because a personal database does not support
    # stages and must always use a workspace, even when ``snowflake.yml``
    # specifies a stage or omits code storage entirely.
    storage = _resolve_code_storage(
        entity, database=database, schema=schema, app_name=app_name
    )
    use_workspace = storage.type == "workspace"
    storage_name = storage.name
    storage_db_override = storage.database_override
    storage_schema_override = storage.schema_override
    encryption_type = storage.encryption_type

    # Compute pools resolved from ``snowflake.yml`` or the
    # ``DEFAULT_SNOWFLAKE_APPS_*_COMPUTE_POOL`` account parameters are passed
    # through to the server: forwarded as the 4th argument to
    # ``SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO`` and emitted as
    # ``IN COMPUTE POOL`` in ``CREATE APPLICATION SERVICE``. When neither
    # source provides a value the server allocates the pools itself.
    ar_name = defaults["artifact_repository"]
    ar_database = defaults["artifact_repo_database"]
    ar_schema = defaults["artifact_repo_schema"]
    artifact_repo_fqn_str = app_fqn(
        database=ar_database, schema=ar_schema, name=ar_name
    ).identifier

    # ── Derived names ─────────────────────────────────────────────────
    # If the code storage was defined as a fully-qualified identifier
    # (e.g. ``DB.SCHEMA.NAME``) use its components; otherwise fall back
    # to the app's resolved database/schema for backwards-compatibility
    # with entities that configure ``code_stage``/``code_workspace`` as a
    # bare name.
    storage_fqn = app_fqn(
        database=storage_db_override or database,
        schema=storage_schema_override or schema,
        name=storage_name,
    )
    service_fqn = app_fqn(database=database, schema=schema, name=app_name)
    workspace_source_uri = manager.workspace_subdirectory_uri(storage_fqn, app_name)

    # Tracks whether this invocation created the code stage, so it can be
    # dropped once the build has consumed it (see the build phase below).
    stage_created = False

    # ── Upload phase ──────────────────────────────────────────────────

    if run_upload:
        with metrics.span("snowflake_app.bundle"):
            project_paths = perform_bundle(resolved_entity_id, entity)
        try:
            with metrics.span("snowflake_app.upload"):
                if use_workspace:
                    with metrics.span("snowflake_app.upload.prepare_workspace"):
                        action = "create workspace"
                        required_privilege = "CREATE WORKSPACE on the schema"
                        try:
                            cli_console.step(f"Creating workspace {storage_fqn}")
                            manager.create_workspace(storage_fqn)
                            action = "clear workspace files"
                            required_privilege = "WRITE on the workspace"
                            cli_console.step(
                                f"Clearing existing workspace files in {workspace_source_uri}/"
                            )
                            manager.clear_workspace_subdirectory(storage_fqn, app_name)
                        except ProgrammingError as e:
                            role = manager.current_role()
                            role_clause = f"role '{role}'" if role else "your role"
                            raise CliError(
                                f"Failed to {action} '{storage_fqn.identifier}': {e}. "
                                f"Verify that {role_clause} has the required "
                                f"privileges (USAGE on the database and schema, "
                                f"and {required_privilege})."
                            ) from e
                    with metrics.span("snowflake_app.upload.push_workspace_files"):
                        cli_console.step(
                            f"Uploading bundled files to {workspace_source_uri}"
                        )
                        files_uploaded = 0
                        for result in manager.upload_to_workspace(
                            local_root=project_paths.bundle_root,
                            workspace_fqn=storage_fqn,
                            target_subdirectory=app_name,
                            overwrite=True,
                        ):
                            files_uploaded += 1
                            cli_console.step(
                                f"  Uploaded {result['source']} -> {result['target']}"
                            )
                        metrics.set_counter(FILES_UPLOADED_COUNTER, files_uploaded)
                    with metrics.span("snowflake_app.upload.commit_workspace"):
                        cli_console.step(
                            f"Committing workspace live version for {storage_fqn}"
                        )
                        manager.commit_workspace_live_version(storage_fqn)
                        cli_console.step(
                            f"Creating a fresh live version for {storage_fqn}"
                        )
                        manager.ensure_workspace_live_version(storage_fqn)
                else:
                    with metrics.span("snowflake_app.upload.prepare_stage"):
                        # Start the upload from an empty stage so files left
                        # over from a prior deploy never leak into the build.
                        # Clearing with REMOVE can leave stale chunks behind, so
                        # drop and recreate instead — but drop only when the
                        # stage already exists. A first deploy has nothing to
                        # drop, and issuing DROP STAGE there would demand
                        # OWNERSHIP the deploying role need not hold, so skipping
                        # it lets a role with only CREATE STAGE deploy.
                        try:
                            if manager.stage_exists(storage_fqn):
                                cli_console.step(f"Recreating stage @{storage_fqn}")
                                manager.drop_stage_if_exists(storage_fqn)
                            else:
                                cli_console.step(f"Creating stage @{storage_fqn}")
                            manager.create_stage(storage_fqn, encryption_type)
                            stage_created = True
                        except ProgrammingError as e:
                            role = manager.current_role()
                            role_clause = f"role '{role}'" if role else "your role"
                            raise CliError(
                                f"Failed to recreate stage '{storage_fqn.identifier}': {e}. "
                                f"Verify that {role_clause} has the required "
                                f"privileges (USAGE on the database and schema, "
                                f"OWNERSHIP on the stage, and CREATE STAGE on the schema)."
                            ) from e

                    with metrics.span("snowflake_app.upload.push_stage_files"):
                        cli_console.step(f"Uploading bundled files to @{storage_fqn}")
                        files_uploaded = 0
                        for result in manager.upload_to_stage(
                            local_root=project_paths.bundle_root,
                            stage_fqn=storage_fqn,
                            overwrite=True,
                        ):
                            files_uploaded += 1
                            cli_console.step(
                                f"  Uploaded {result['source']} -> {result['target']}"
                            )
                        metrics.set_counter(FILES_UPLOADED_COUNTER, files_uploaded)
        finally:
            project_paths.clean_up_output()

    if upload_only:
        if use_workspace:
            return MessageResult(f"Artifacts uploaded to {workspace_source_uri}")
        return MessageResult(f"Artifacts uploaded to @{storage_fqn}")

    # ── Build phase ───────────────────────────────────────────────────

    if run_build:
        with metrics.span("snowflake_app.build"):
            with metrics.span("snowflake_app.build.ensure_artifact_repo"):
                if not manager.artifact_repo_exists(
                    database=ar_database, schema=ar_schema, repo_name=ar_name
                ):
                    cli_console.step(
                        f"Creating artifact repository: {artifact_repo_fqn_str}"
                    )
                    manager.create_artifact_repo(
                        database=ar_database, schema=ar_schema, repo_name=ar_name
                    )

            with metrics.span("snowflake_app.build.submit"):
                cli_console.step("Building app using artifact repository...")
                project_type_override = getattr(entity, "spcs_test_project_type", None)
                build_kwargs: dict = dict(
                    artifact_repo_fqn=artifact_repo_fqn_str,
                    app_id=app_name,
                    compute_pool=build_compute_pool,
                    database=database,
                    schema=schema,
                    runtime_image=entity.runtime_image,
                    build_eai=build_eai,
                    project_type=(
                        project_type_override
                        if isinstance(project_type_override, str)
                        else ""
                    ),
                )
                if use_workspace:
                    build_kwargs[
                        "source_uri"
                    ] = manager.workspace_last_subdirectory_uri(storage_fqn, app_name)
                else:
                    build_kwargs["stage_fqn"] = storage_fqn
                build_result = manager.build_app_artifact_repo(**build_kwargs)
                cli_console.step(
                    f"SPCS_TEST_BUILD_APP_ARTIFACT_REPO output:\n{build_result}"
                )

                match = re.search(r"Build job submitted:\s*(\S+)", build_result)
                if not match:
                    raise CliError(
                        f"Could not parse build job name from output: {build_result}"
                    )
                artifact_build_job_fqn = FQN.from_string(match.group(1))
                cli_console.step(
                    f"[{_ts()}] Waiting for artifact repo build to complete: "
                    f"{artifact_build_job_fqn}..."
                )

            with metrics.span("snowflake_app.build.wait"):
                _poll_until(
                    poll_fn=lambda: manager.get_build_status(artifact_build_job_fqn),
                    done_states={"DONE"},
                    error_states={"FAILED", "IDLE"},
                    known_pending_states={"PENDING", "RUNNING"},
                    timeout_message=(
                        f"Artifact repo build timed out. Check build logs:\n"
                        f"  SELECT * FROM TABLE("
                        f"{artifact_build_job_fqn.identifier}!SPCS_GET_LOGS())"
                    ),
                    on_poll=_make_build_log_streamer(manager, artifact_build_job_fqn),
                )

            # The stage only holds the uploaded source that the artifact-repo
            # build consumes; once the build succeeds it is no longer needed.
            # Drop it only when this invocation created it, so a pre-existing
            # stage relied on by ``--build-only`` (which skips the upload phase)
            # is left untouched. This is best-effort cleanup: the build has
            # already succeeded, so a drop failure only leaves a harmless stage
            # behind and must not fail the deploy — warn and continue.
            if stage_created:
                with metrics.span("snowflake_app.build.drop_stage") as drop_span:
                    cli_console.step(
                        f"Dropping stage @{storage_fqn} now that the build is complete"
                    )
                    try:
                        manager.drop_stage_if_exists(storage_fqn)
                    except Exception as e:
                        # Record the failure on the span so these otherwise
                        # silent (warn-and-continue) cleanup errors stay
                        # observable in telemetry, then swallow it: the build
                        # already succeeded, so a stray stage must not fail the
                        # deploy.
                        log.debug(
                            "Failed to drop stage %s after build",
                            storage_fqn.identifier,
                            exc_info=True,
                        )
                        drop_span.finish(error=e)
                        cli_console.warning(
                            f"Could not drop stage '{sanitize_for_terminal(storage_fqn.identifier)}' "
                            f"after the build completed: {e}. The build succeeded; "
                            "you can remove the stage manually if desired."
                        )

    if build_only:
        return MessageResult("Build completed successfully.")

    # ── Deploy phase ──────────────────────────────────────────────────

    comment_data = {"appId": app_name}
    if app_title:
        comment_data["appName"] = app_title
    if app_description:
        comment_data["appDescription"] = app_description
    if app_icon:
        comment_data["appIcon"] = app_icon
    app_comment = json.dumps(comment_data)

    eai_list = [service_eai] if service_eai else None

    did_upgrade = False
    with metrics.span("snowflake_app.deploy_service"):
        cli_console.step("Creating application service...")
        try:
            with metrics.span("snowflake_app.deploy_service.create") as create_span:
                try:
                    manager.create_app_service(
                        service_fqn=service_fqn,
                        artifact_repo_fqn=artifact_repo_fqn_str,
                        package_name=app_name,
                        compute_pool=service_compute_pool,
                        version="LATEST",
                        query_warehouse=query_warehouse,
                        external_access_integrations=eai_list,
                        comment=app_comment,
                    )
                except ProgrammingError as e:
                    # "Already exists" is the expected re-deploy path: the
                    # outer handler dispatches to ALTER ... UPGRADE. Finish
                    # the Create span successfully so telemetry doesn't
                    # double-count every redeploy as a ProgrammingError on
                    # this span; the recovery is recorded by
                    # ``deploy_service.upgrade`` instead.
                    if e.errno == 2002 and "already exists" in str(e).lower():
                        create_span.finish()
                    raise
        except ProgrammingError as e:
            if e.errno == 2002 and "already exists" in str(e).lower():
                cli_console.step(
                    f"Application service {app_name} already exists. Upgrading..."
                )
                try:
                    with metrics.span("snowflake_app.deploy_service.upgrade"):
                        manager.upgrade_app_service(
                            service_fqn=service_fqn,
                            version="LATEST",
                        )
                except ProgrammingError as upgrade_error:
                    _log_service_logs(manager, service_fqn)
                    raise CliError(
                        "Deployment failed while upgrading application service "
                        f"'{service_fqn.identifier}': {upgrade_error}. "
                        "Verify privileges for ALTER APPLICATION SERVICE and access to referenced objects."
                    ) from upgrade_error
                did_upgrade = True
            else:
                _log_service_logs(manager, service_fqn)
                raise CliError(
                    "Deployment failed while creating application service "
                    f"'{service_fqn.identifier}': {e}. "
                    "Verify privileges for CREATE APPLICATION SERVICE plus USAGE on configured compute pools, warehouse, and external access integrations."
                ) from e

    def _svc_is_upgrading(d: dict) -> bool:
        return str(d.get("is_upgrading", "")).lower() in ("true", "1", "yes")

    def _svc_has_failed(d: dict) -> bool:
        return d.get("status", "").upper() == "FAILED"

    def _url_is_ready(d: dict) -> bool:
        return manager.resolve_application_service_url_from_describe(d) is not None

    try:
        with metrics.span("snowflake_app.endpoint_provision"):
            if did_upgrade:
                cli_console.step(f"[{_ts()}] Waiting for upgrade to complete...")
                with metrics.span("snowflake_app.endpoint_provision.wait_for_upgrade"):
                    desc = _poll_until(
                        poll_fn=lambda: manager.describe_app_service(service_fqn),
                        is_done=_url_is_ready,
                        is_error=_svc_has_failed,
                        format_status=lambda d: (
                            "upgrading" if _svc_is_upgrading(d) else "ready"
                        ),
                        timeout_message=(
                            f"Upgrade timed out. Check application service state and logs:\n"
                            f"  DESCRIBE APPLICATION SERVICE {service_fqn.identifier}\n"
                            f"  CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS('{service_fqn.identifier}')"
                        ),
                    )
            else:
                cli_console.step(
                    f"[{_ts()}] Waiting for application service endpoint..."
                )
                with metrics.span("snowflake_app.endpoint_provision.wait_for_endpoint"):
                    desc = _poll_until(
                        poll_fn=lambda: manager.describe_app_service(service_fqn),
                        is_done=_url_is_ready,
                        is_error=_svc_has_failed,
                        format_status=lambda d: d.get("url") or "url not yet available",
                        timeout_message=(
                            f"Application service deployment timed out. Check application service state and logs:\n"
                            f"  DESCRIBE APPLICATION SERVICE {service_fqn.identifier}\n"
                            f"  CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS('{service_fqn.identifier}')"
                        ),
                    )
    except CliError:
        try:
            if _svc_has_failed(manager.describe_app_service(service_fqn)):
                _log_service_logs(manager, service_fqn)
        except Exception:
            log.debug(
                "Failed to inspect application service after deploy error",
                exc_info=True,
            )
        raise

    endpoint_url = manager.resolve_application_service_url_from_describe(desc)
    if not endpoint_url:
        raise CliError(
            "Application service URL is not available after deploy. "
            f"Check: DESCRIBE APPLICATION SERVICE {service_fqn.identifier}"
        )
    return MessageResult(f"App ready at {endpoint_url}")


@_utf8_output
def snowflake_app_teardown(
    entity_id: Optional[str],
    force: bool,
) -> CommandResult:
    """Drop a deployed Snowflake App Runtime and its associated objects."""
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    fqn = entity.fqn
    manager = SnowflakeAppManager()
    metrics = get_cli_context().metrics
    with metrics.span("snowflake_app.teardown.resolve_defaults"):
        defaults = _resolve_deploy_defaults(entity, manager, app_name=fqn.name)

    db = defaults.get("database")
    schema = defaults.get("schema")

    if not db or not schema:
        missing = [k for k, v in {"database": db, "schema": schema}.items() if not v]
        raise CliError(
            f"Cannot resolve {' or '.join(missing)} for the app. "
            "Set them in snowflake.yml or in your connection configuration."
        )

    app_name = fqn.name
    service_fqn = app_fqn(database=db, schema=schema, name=app_name)

    # Mirror the deploy-time backend selection so a personal-database app is
    # torn down via its workspace rather than a (never-created) stage.
    storage = _resolve_code_storage(
        entity, database=db, schema=schema, app_name=app_name
    )
    use_workspace = storage.type == "workspace"
    storage_name = storage.name
    storage_db = storage.database_override or db
    storage_schema = storage.schema_override or schema

    storage_fqn = app_fqn(database=storage_db, schema=storage_schema, name=storage_name)

    def _app_service_still_exists() -> bool:
        try:
            return bool(manager.describe_app_service(service_fqn))
        except ProgrammingError:
            return False

    def _verify_service_drop() -> None:
        try:
            still_exists = _app_service_still_exists()
        except Exception as err:
            raise CliError(
                f"Could not verify application service {service_fqn.identifier} "
                f"was dropped: {err}"
            ) from err

        if still_exists:
            raise CliError(
                f"Failed to drop application service {service_fqn.identifier}. "
                f"Check: DESCRIBE APPLICATION SERVICE {service_fqn.identifier}"
            )

    if not force:
        # Wrap the interactive prompt in its own span so the time spent waiting
        # on the user is attributable and does not silently inflate the overall
        # command duration (which is otherwise unaccounted for by any span).
        with metrics.span("snowflake_app.teardown.confirm"):
            should_continue = typer.confirm(
                f"Are you sure you want to drop application service "
                f"{service_fqn.identifier} and its associated objects?"
            )
        if not should_continue:
            return MessageResult("Teardown cancelled.")

    cli_console.step(f"Dropping application service {service_fqn.identifier}")
    with metrics.span("snowflake_app.teardown.drop_service"):
        manager.drop_app_service_if_exists(service_fqn)
        _verify_service_drop()

    if use_workspace:
        # The workspace may be shared across apps (e.g. the default
        # ``SNOWFLAKE_APPS`` workspace), so we only clear this app's
        # subdirectory and leave the workspace itself in place.
        cli_console.step(
            f"Clearing workspace files for {app_name} in {storage_fqn.identifier}"
        )
        with metrics.span("snowflake_app.teardown.clear_workspace"):
            manager.clear_workspace_subdirectory(storage_fqn, app_name)
    else:
        cli_console.step(f"Dropping stage {storage_fqn.identifier}")
        with metrics.span("snowflake_app.teardown.drop_stage"):
            manager.drop_stage_if_exists(storage_fqn)

    return MessageResult(
        f"Successfully dropped application service {service_fqn.identifier}."
    )
