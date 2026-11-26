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
from contextlib import AbstractContextManager
from pathlib import Path
from types import SimpleNamespace
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterator,
    Literal,
    NamedTuple,
    NoReturn,
    Optional,
    cast,
)

import typer
from click import ClickException
from snowflake.cli._plugins.apps.app_yml import (
    APP_YML_FILENAME,
    PYTHON_REQUIRED_PHASES,
    RUNTIME_NODE,
    RUNTIME_PYTHON,
    AppYmlDefinition,
    AppYmlTarget,
    load_app_yml,
    resolve_target,
)
from snowflake.cli._plugins.apps.events import (
    SERVERLESS_EVENT_TABLE_UNAVAILABLE_ERRNOS,
    EventStream,
    MetricCategory,
    format_log_lines,
    parse_event_stream,
    parse_lifecycle_records,
    parse_metric_category,
    parse_metric_records,
    resolve_time_window,
)
from snowflake.cli._plugins.apps.generate import (
    _generate_app_yml,
    detect_python_project,
    hybrid_app_hint,
)
from snowflake.cli._plugins.apps.manager import (
    DEFAULT_PERSONAL_SCHEMA,
    DEFINITION_FILENAME,
    MANAGED_COMPUTE_POOL_COMPUTE_RESOURCE,
    PER_ACCOUNT_CERT_ISSUE_FUNCTION,
    SERVERLESS_COMPUTE_RESOURCE,
    PerAccountCertStatus,
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
from snowflake.cli._plugins.apps.templates import (
    TEMPLATES_REPOSITORY,
    TemplateRepository,
    readable_label,
    render_app_yml,
)
from snowflake.cli._plugins.apps.upload_errors import (
    UploadPhase,
    classify_upload_error,
)
from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli.api.cli_global_context import get_cli_context, span
from snowflake.cli.api.config import (
    get_connection_dict,
    get_default_connection_name,
    get_file_io_encoding,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.errno import INSUFFICIENT_PRIVILEGES
from snowflake.cli.api.exceptions import (
    CliArgumentError,
    CliError,
    IncompatibleParametersError,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.metrics import CLIMetricsSpan
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    EmptyResult,
    MessageResult,
    ObjectResult,
)
from snowflake.cli.api.project.util import identifier_for_url, unquote_identifier
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector.errors import OperationalError, ProgrammingError

if TYPE_CHECKING:
    from snowflake.cli._plugins.apps.snowflake_app_entity_model import (
        SnowflakeAppEntityModel,
    )

log = logging.getLogger(__name__)

# Telemetry span naming convention for the ``snow app`` commands:
#   * Every command entry point opens one root span ``snowflake_app.<command>``
#     (e.g. ``snowflake_app.deploy``) that wraps the whole command — including
#     target resolution — so total duration and command-level failures are
#     always attributable. ``setup`` opens it inline (see below); the others use
#     the ``@span`` decorator.
#   * A step that only belongs to one command nests under it as
#     ``snowflake_app.<command>.<step>`` (e.g. ``snowflake_app.setup.write_manifest``,
#     ``snowflake_app.deploy.resolve_defaults``).
#   * A pipeline phase shared by both deploy flows (``snowflake.yml`` and
#     ``app.yml``) keeps its own ``snowflake_app.<phase>`` namespace
#     (``bundle``, ``upload``, ``build``, ``deploy_service``,
#     ``endpoint_provision``) so it reads the same regardless of which flow ran
#     it; it still nests under the ``snowflake_app.deploy`` root at runtime.

# Telemetry counter recording how many files were uploaded during the
# upload phase of a deploy.
FILES_UPLOADED_COUNTER = "snowflake_app.upload.files_uploaded"

# Raised when ``--target`` is passed to a command in a project that is not
# driven by an ``app.yml`` (the ``snowflake.yml`` flow has no notion of
# targets). Shared by every command that accepts ``--target``.
_TARGET_REQUIRES_APP_YML = (
    "--target is only supported for Snowflake App Runtime projects that "
    "define deployment targets in app.yml (version 2)."
)

# Raised when ``SYSTEM$GET_APPLICATION_SERVICE_EVENT_TABLE_DATA`` cannot serve
# a serverless (CNG) app's data in this account yet.
_CNG_HEALTH_MONITORING_UNAVAILABLE = (
    "Health monitoring is not yet available for serverless apps in this "
    "account. Use snow app events without --type metric, --type lifecycle, "
    "--since, or --until to get live logs."
)


def _load_app_yml_for_command(target: Optional[str]) -> Optional[AppYmlDefinition]:
    """Return the ``app.yml`` that drives this project, or ``None``.

    Centralises the ``app.yml``-vs-``snowflake.yml`` routing shared by every
    ``snow app`` command that accepts ``--target``: when no ``app.yml`` (version
    2) drives the project, ``--target`` has no meaning and is rejected up front
    so the ``snowflake.yml`` fallback never has to consider it.
    """
    app_def = load_app_yml(get_cli_context().project_root)
    if app_def is None and target is not None:
        raise CliError(_TARGET_REQUIRES_APP_YML)
    return app_def


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

    ``temporary`` marks a backend the CLI provisions for the deploy and owns
    end to end: it is named ``<app>_CODE`` (an app-name prefix), created during
    the upload phase, and dropped once the build has consumed it. It is set only
    when neither ``code_stage`` nor ``code_workspace`` is configured, so an
    explicitly configured stage/workspace is always persisted (never dropped).
    Because the name is deterministic, a ``--build-only`` run that skips the
    upload can still find and drop the temporary backend a prior
    ``--upload-only`` created.
    """

    type: _CodeStorageType  # noqa: A003
    name: str
    database_override: Optional[str]
    schema_override: Optional[str]
    encryption_type: str
    temporary: bool = False


class _CodeStorageRef(NamedTuple):
    """A configured code-storage location, before backend selection.

    Normalizes the two config shapes — the structured ``snowflake.yml`` entity
    references (name + optional database/schema) and the ``app.yml`` FQN strings
    — into one form that :func:`_resolve_code_storage` reasons about.
    ``encryption_type`` applies only to a stage and may be ``None``.
    """

    name: str
    database: Optional[str]
    schema_: Optional[str]
    encryption_type: Optional[str] = None


def _ensure_app_derived_schema(
    manager: SnowflakeAppManager,
    *,
    database: Optional[str],
    schema: Optional[str],
    app_name: str,
) -> None:
    """Create a missing schema when its name was derived from the app name.

    App Space defaults give the CLI a database and derive one schema per app.
    Other schema names remain user-managed, preserving their existing behavior.
    """
    if (
        not database
        or not schema
        or unquote_identifier(schema) != unquote_identifier(app_name)
    ):
        return

    cli_console.step(
        "Creating app schema "
        f"{sanitize_for_terminal(database)}.{sanitize_for_terminal(schema)} "
        "if it does not exist"
    )
    try:
        manager.create_schema_if_not_exists(database, schema)
    except ProgrammingError as e:
        raise CliError(
            "Could not create app-derived schema "
            f"'{sanitize_for_terminal(database)}."
            f"{sanitize_for_terminal(schema)}': {e}. "
            f"Grant CREATE SCHEMA on database "
            f"'{sanitize_for_terminal(database)}' to the active role, "
            "or create the schema manually."
        ) from e


def _resolve_code_storage(
    *,
    code_workspace: Optional[_CodeStorageRef],
    code_stage: Optional[_CodeStorageRef],
    database: Optional[str],
    schema: Optional[str],
    app_name: str,
) -> _CodeStorage:
    """Decide whether app code is uploaded to a workspace or a stage.

    Personal databases (``USER$<user>``) do not support stages, so any app
    whose *resolved* destination database is a personal database must use a
    workspace — regardless of what (if anything) was configured. This both
    honors explicit configuration for non-personal destinations and repairs
    projects that predate personal-database detection (a ``code_stage`` pointing
    at a personal database) by transparently routing them through a workspace.
    Both the ``snowflake.yml`` and ``app.yml`` flows share this decision so
    their behavior stays aligned.

    Resolution order:

    1. Explicit ``code_workspace`` → workspace, as configured and *persisted*.
    2. Explicit ``code_stage`` → stage, as configured and *persisted*. A warning
       is emitted only when the stage itself resolves into a personal database
       (stages are generally unsupported there); a stage in a standard database
       is fine even when the service is deployed to a personal database, since
       the two are located independently.
    3. Neither configured → a *temporary* backend the CLI owns end to end: a
       ``<app>_CODE`` stage for a regular database, or a ``<app>_CODE``
       workspace when the destination is a personal database (stages are
       unsupported there). A temporary backend is created during the upload
       phase and dropped once the build has consumed it (see
       :func:`_upload_and_build_app`). To persist code storage across deploys,
       configure ``code_stage`` / ``code_workspace`` explicitly.
    """
    destination_is_personal = is_personal_database(database)

    if code_workspace is not None:
        return _CodeStorage(
            type="workspace",
            name=code_workspace.name,
            database_override=code_workspace.database,
            schema_override=code_workspace.schema_,
            encryption_type="SNOWFLAKE_SSE",  # unused in workspace flow
        )

    if code_stage is not None:
        # The stage may live in its own database (a fully-qualified
        # ``code_stage``); it only fails when *that* database is personal, not
        # merely because the service's destination is.
        stage_database = code_stage.database or database
        if is_personal_database(stage_database):
            cli_console.warning(
                f"code_stage '{sanitize_for_terminal(code_stage.name)}' "
                f"resolves to the personal database "
                f"'{sanitize_for_terminal(str(stage_database))}', which "
                "generally does not support stages. Honoring the configured "
                "stage; the deploy may fail. Consider a stage in a standard "
                "database or a workspace (code_workspace) instead."
            )
        return _CodeStorage(
            type="stage",
            name=code_stage.name,
            database_override=code_stage.database,
            schema_override=code_stage.schema_,
            encryption_type=code_stage.encryption_type or "SNOWFLAKE_SSE",
        )

    # Neither code_workspace nor code_stage configured: provision a temporary
    # ``<app>_CODE`` backend the CLI owns for this deploy and drops after the
    # build. A personal database has to use a workspace (stages are unsupported
    # there); every other destination uses a stage, which is the lighter-weight
    # default. The name is deterministic so a ``--build-only`` run can find (and
    # drop) the backend a prior ``--upload-only`` created.
    if destination_is_personal:
        return _CodeStorage(
            type="workspace",
            name=f"{app_name}_CODE",
            database_override=None,
            schema_override=None,
            encryption_type="SNOWFLAKE_SSE",
            temporary=True,
        )
    return _CodeStorage(
        type="stage",
        name=f"{app_name}_CODE",
        database_override=None,
        schema_override=None,
        encryption_type="SNOWFLAKE_SSE",
        temporary=True,
    )


def _entity_code_storage(
    entity: "SnowflakeAppEntityModel",
    *,
    database: Optional[str],
    schema: Optional[str],
    app_name: str,
) -> _CodeStorage:
    """Resolve the code-storage backend for a ``snowflake.yml`` entity."""
    cw = entity.code_workspace
    cs = entity.code_stage
    return _resolve_code_storage(
        code_workspace=(
            _CodeStorageRef(cw.name, cw.database, cw.schema_)
            if cw is not None
            else None
        ),
        code_stage=(
            _CodeStorageRef(cs.name, cs.database, cs.schema_, cs.encryption_type)
            if cs is not None
            else None
        ),
        database=database,
        schema=schema,
        app_name=app_name,
    )


def _app_yml_storage_ref(value: Optional[str]) -> Optional[_CodeStorageRef]:
    """Parse an ``app.yml`` code-storage FQN string into a :class:`_CodeStorageRef`.

    Accepts a bare name or a ``DB.SCHEMA.NAME`` identifier; missing components
    stay ``None`` so :func:`_resolve_code_storage` can fall back to the app's
    resolved database/schema.
    """
    if not value:
        return None
    parsed = FQN.from_string(value)
    return _CodeStorageRef(
        name=parsed.name, database=parsed.database, schema_=parsed.schema
    )


def _app_yml_code_storage(
    *,
    code_stage: Optional[str],
    code_workspace: Optional[str],
    database: Optional[str],
    schema: Optional[str],
    app_name: str,
) -> _CodeStorage:
    """Resolve the code-storage backend for an ``app.yml`` project.

    ``code_stage`` / ``code_workspace`` are overridable per target, so they are
    taken from the resolved (merged) target. When neither is set the CLI
    provisions a temporary ``<app>_CODE`` backend for the deploy and drops it
    after the build (see :func:`_resolve_code_storage`).
    """
    return _resolve_code_storage(
        code_workspace=_app_yml_storage_ref(code_workspace),
        code_stage=_app_yml_storage_ref(code_stage),
        database=database,
        schema=schema,
        app_name=app_name,
    )


def _storage_fqn(storage: _CodeStorage, *, database: str, schema: str) -> FQN:
    """Build the FQN of a resolved code-storage object.

    A fully-qualified ``code_stage``/``code_workspace`` supplies its own
    database/schema; otherwise the app's resolved database/schema are used.
    """
    return app_fqn(
        database=storage.database_override or database,
        schema=storage.schema_override or schema,
        name=storage.name,
    )


def _failing_upload_file(exc: BaseException) -> Optional[str]:
    """Return the local file an upload error names, if it names one.

    Files are uploaded concurrently, so the exception that surfaces is not tied
    to any yielded result. A local filesystem error identifies its own file; a
    connector error does not, and its message text has to speak for itself.
    """
    filename = getattr(exc, "filename", None)
    return str(filename) if filename else None


def _stream_uploads(
    manager: SnowflakeAppManager,
    uploads: Iterator[dict],
    *,
    phase: UploadPhase,
    target: str,
    metrics,
    reraise: tuple[type[BaseException], ...] = (),
) -> None:
    """Report upload progress, and explain a failure in context.

    ``OperationalError`` matters as much as ``ProgrammingError`` here: the
    connector raises it for a failed file transfer, so an ``except
    ProgrammingError`` alone lets those through as a raw traceback.

    Exceptions listed in *reraise* are left untouched for a caller that can
    recover from them; wrapping those would silently disable that recovery.
    """
    files_uploaded = 0
    try:
        for result in uploads:
            files_uploaded += 1
            cli_console.step(f"  Uploaded {result['source']} -> {result['target']}")
    except (ProgrammingError, OperationalError, OSError) as e:
        if isinstance(e, reraise):
            raise
        raise classify_upload_error(
            e,
            phase=phase,
            target=target,
            role=manager.current_role(),
            source_file=_failing_upload_file(e),
            files_uploaded=files_uploaded,
        ) from e
    finally:
        # Recorded on the failure path too, so telemetry shows how far the
        # upload got before it broke.
        metrics.set_counter(FILES_UPLOADED_COUNTER, files_uploaded)


def _upload_via_workspace(
    manager: SnowflakeAppManager,
    *,
    workspace_fqn: FQN,
    app_name: str,
    database: Optional[str],
    project_paths,
    metrics,
) -> None:
    """Prepare and upload the workspace code-storage backend.

    On a failure the behaviour depends on the destination: a personal database
    cannot fall back to a stage, so an actionable :class:`UploadError` is
    raised; a regular database re-raises the raw ``ProgrammingError`` so the
    caller can fall back to the stage flow.
    """
    workspace_source_uri = manager.workspace_subdirectory_uri(workspace_fqn, app_name)
    with metrics.span("snowflake_app.upload.prepare_workspace"):
        # Tracked so the error names the statement that actually failed and
        # the privilege that statement needs, rather than every privilege the
        # phase might want.
        action = "create workspace"
        required_privilege = "CREATE WORKSPACE on the schema"
        try:
            cli_console.step(f"Creating workspace {workspace_fqn}")
            manager.create_workspace(workspace_fqn)
            action = "clear workspace files"
            required_privilege = "WRITE on the workspace"
            cli_console.step(
                f"Clearing existing workspace files in {workspace_source_uri}/"
            )
            manager.clear_workspace_subdirectory(workspace_fqn, app_name)
        except ProgrammingError as e:
            # Regular databases can fall back to a stage, so let the raw error
            # propagate for the caller to handle. Wrapping it here would
            # silently disable that fallback. Personal databases have no such
            # fallback (stages are unsupported), so surface an actionable
            # error instead.
            if not is_personal_database(database):
                raise
            raise classify_upload_error(
                e,
                phase=UploadPhase.PREPARE_WORKSPACE,
                target=workspace_fqn.identifier,
                action=action,
                required_privilege=required_privilege,
                role=manager.current_role(),
                database=workspace_fqn.database,
                schema=workspace_fqn.schema,
            ) from e
    with metrics.span("snowflake_app.upload.push_workspace_files"):
        cli_console.step(f"Uploading bundled files to {workspace_source_uri}")
        _stream_uploads(
            manager,
            manager.upload_to_workspace(
                local_root=project_paths.bundle_root,
                workspace_fqn=workspace_fqn,
                target_subdirectory=app_name,
                overwrite=True,
            ),
            phase=UploadPhase.PUSH_WORKSPACE_FILES,
            target=workspace_source_uri,
            metrics=metrics,
            # A regular database can still fall back to a stage, so a SQL
            # error has to stay raw for the caller to catch. Anything else it
            # cannot recover from, so those are wrapped.
            reraise=() if is_personal_database(database) else (ProgrammingError,),
        )


def _create_stage_if_permitted(
    manager: SnowflakeAppManager, stage_fqn: FQN, encryption: str
) -> Optional[ProgrammingError]:
    """Create *stage_fqn*, returning the refusal if the role is not allowed to.

    ``CREATE STAGE IF NOT EXISTS`` is a no-op when the stage is already there,
    but it still needs CREATE STAGE on the schema, so it doubles as a probe:
    it answers "could this stage be created again?" before anything drops it.

    Only an insufficient-privileges failure is returned, because that is the
    one the caller has an answer for; anything else is a real error.
    """
    try:
        manager.create_stage(stage_fqn, encryption)
        return None
    except ProgrammingError as e:
        if getattr(e, "errno", None) != INSUFFICIENT_PRIVILEGES:
            raise
        return e


def _drop_stage_for_recreate(manager: SnowflakeAppManager, stage_fqn: FQN) -> bool:
    """Drop *stage_fqn* so it can be recreated empty; False if not permitted.

    ``DROP STAGE`` is documented as requiring OWNERSHIP. Only an
    insufficient-privileges failure is read as "not permitted", because that is
    the one the caller has a fallback for; anything else is a real error and
    propagates.
    """
    try:
        manager.drop_stage_if_exists(stage_fqn)
        return True
    except ProgrammingError as e:
        if getattr(e, "errno", None) != INSUFFICIENT_PRIVILEGES:
            raise
        log.debug(
            "Not permitted to drop stage %s; clearing its contents instead.",
            stage_fqn.identifier,
            exc_info=True,
        )
        return False


def _warn_stage_cleared_not_recreated(stage_fqn: FQN, reason: str) -> None:
    """Say that the stage was emptied in place, and what that costs."""
    cli_console.warning(
        f"Clearing stage @{sanitize_for_terminal(stage_fqn.identifier)} "
        f"instead of recreating it, because {reason}. Files deleted from the "
        "project since the last deploy may survive on the stage. Grant the "
        "deploying role OWNERSHIP on the stage and CREATE STAGE on the schema "
        "to have it recreated cleanly."
    )


def _empty_stage_for_upload(
    manager: SnowflakeAppManager, stage_fqn: FQN, encryption: str
) -> bool:
    """Present an empty stage for the upload, and say whether it was created.

    The upload has to start from an empty stage so files left over from an
    earlier deploy never leak into the build. Dropping and recreating is the
    only way to guarantee that, but it needs OWNERSHIP on the stage and CREATE
    STAGE on the schema, and a deploying role often holds neither — such a role
    could never redeploy at all.

    So the approach follows what the role can do: drop and recreate when it can
    do both, and otherwise clear the stage with ``REMOVE``, which needs only
    WRITE. The weaker guarantee ``REMOVE`` gives is bounded, because every
    ``PUT`` uses ``overwrite=true``: a file still in the project is always
    replaced by its current version, and only a file deleted from the project
    since the last deploy can survive. That path warns.

    The order matters. ``CREATE STAGE IF NOT EXISTS`` runs first because it is
    the one statement that is safe to attempt either way — a no-op when the
    stage exists — so it reports whether the role could recreate the stage
    while the stage is still there to fall back on. Dropping first would leave
    a role that can drop but not create with no stage at all and no way to get
    one back.

    Each statement needs a different privilege, so the action and the privilege
    are tracked as the block progresses and a failure names only the one that
    actually applied.
    """
    action = "look up stage"
    required_privilege = "USAGE on the schema"
    try:
        stage_existed = manager.stage_exists(stage_fqn)
        cli_console.step(
            f"Recreating stage @{stage_fqn}"
            if stage_existed
            else f"Creating stage @{stage_fqn}"
        )

        action = "create stage"
        required_privilege = "CREATE STAGE on the schema"
        cannot_create = _create_stage_if_permitted(manager, stage_fqn, encryption)
        if not stage_existed:
            if cannot_create:
                raise cannot_create
            return True

        if cannot_create is None:
            action = "drop stage"
            required_privilege = "OWNERSHIP on the stage"
            if _drop_stage_for_recreate(manager, stage_fqn):
                action = "create stage"
                required_privilege = "CREATE STAGE on the schema"
                manager.create_stage(stage_fqn, encryption)
                return True

        _warn_stage_cleared_not_recreated(
            stage_fqn,
            "the deploying role cannot create it again"
            if cannot_create
            else "the deploying role cannot drop it",
        )
        action = "clear stage"
        required_privilege = "WRITE on the stage"
        manager.remove_stage_contents(stage_fqn)
        return False
    except ProgrammingError as e:
        raise classify_upload_error(
            e,
            phase=UploadPhase.PREPARE_STAGE,
            target=stage_fqn.identifier,
            action=action,
            required_privilege=required_privilege,
            role=manager.current_role(),
            database=stage_fqn.database,
            schema=stage_fqn.schema,
            encryption_type=encryption,
        ) from e


def _upload_via_stage(
    manager: SnowflakeAppManager,
    *,
    stage_fqn: FQN,
    encryption: str,
    project_paths,
    metrics,
) -> bool:
    """Prepare and upload the stage code-storage backend.

    Returns whether this invocation created the stage, so the caller knows
    whether it may drop it once the build has consumed it.
    """
    with metrics.span("snowflake_app.upload.prepare_stage"):
        stage_created = _empty_stage_for_upload(manager, stage_fqn, encryption)

    with metrics.span("snowflake_app.upload.push_stage_files"):
        cli_console.step(f"Uploading bundled files to @{stage_fqn}")
        _stream_uploads(
            manager,
            manager.upload_to_stage(
                local_root=project_paths.bundle_root,
                stage_fqn=stage_fqn,
                overwrite=True,
            ),
            phase=UploadPhase.PUSH_STAGE_FILES,
            target=f"@{stage_fqn.identifier}",
            metrics=metrics,
        )

    return stage_created


def _upload_app_code(
    manager: SnowflakeAppManager,
    *,
    storage: _CodeStorage,
    storage_fqn: FQN,
    app_name: str,
    database: str,
    schema: str,
    project_paths,
    metrics,
) -> tuple[bool, FQN, bool]:
    """Upload bundled source to the selected code-storage backend.

    Shared by the ``snowflake.yml`` and ``app.yml`` deploy flows so both pick
    and use the backend identically. Returns ``(use_workspace, storage_fqn,
    stage_created)``; ``storage_fqn`` may change when a regular-database
    workspace upload fails and the flow falls back to a ``<app>_CODE`` stage,
    and ``stage_created`` records whether this invocation created a stage (so
    the caller can drop it once the build has consumed it). A stage that was
    cleared rather than recreated — because the role does not own it — is not
    reported as created, since dropping it would fail for the same reason.
    """
    use_workspace = storage.type == "workspace"
    encryption_type = storage.encryption_type
    stage_created = False
    with metrics.span("snowflake_app.upload"):
        if not use_workspace:
            stage_created = _upload_via_stage(
                manager,
                stage_fqn=storage_fqn,
                encryption=encryption_type,
                project_paths=project_paths,
                metrics=metrics,
            )
        elif is_personal_database(database):
            # Personal databases must use a workspace; there is no stage to fall
            # back to, so workspace failures surface as an actionable privilege
            # error from the helper.
            _upload_via_workspace(
                manager,
                workspace_fqn=storage_fqn,
                app_name=app_name,
                database=database,
                project_paths=project_paths,
                metrics=metrics,
            )
        else:
            try:
                _upload_via_workspace(
                    manager,
                    workspace_fqn=storage_fqn,
                    app_name=app_name,
                    database=database,
                    project_paths=project_paths,
                    metrics=metrics,
                )
            except ProgrammingError as e:
                # The workspace backend is unusable for this role/destination.
                # Fall back to the stage flow so a role that cannot create/use a
                # workspace can still deploy: upload to a ``<app>_CODE`` stage,
                # let the build consume it, and drop it afterwards.
                cli_console.warning(
                    f"Could not use a workspace for code storage in "
                    f"'{sanitize_for_terminal(storage_fqn.identifier)}': {e}. "
                    "Falling back to a stage."
                )
                use_workspace = False
                storage_fqn = app_fqn(
                    database=database,
                    schema=schema,
                    name=f"{app_name}_CODE",
                )
                stage_created = _upload_via_stage(
                    manager,
                    stage_fqn=storage_fqn,
                    encryption="SNOWFLAKE_SSE",
                    project_paths=project_paths,
                    metrics=metrics,
                )
    return use_workspace, storage_fqn, stage_created


def _drop_stage_after_build(
    manager: SnowflakeAppManager, storage_fqn: FQN, metrics
) -> None:
    """Drop a code stage once the artifact-repo build has consumed it.

    Best-effort cleanup: the build has already succeeded, so a drop failure
    only leaves a harmless stage behind and must not fail the deploy — record
    it on the span for observability, warn, and continue.
    """
    with metrics.span("snowflake_app.build.drop_stage") as drop_span:
        cli_console.step(
            f"Dropping stage @{storage_fqn} now that the build is complete"
        )
        try:
            manager.drop_stage_if_exists(storage_fqn)
        except Exception as e:
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


def _drop_workspace_after_build(
    manager: SnowflakeAppManager, storage_fqn: FQN, metrics
) -> None:
    """Drop a temporary code workspace once the build has consumed it.

    Best-effort cleanup: the build has already succeeded, so a drop failure
    only leaves a harmless workspace behind and must not fail the deploy —
    record it on the span for observability, warn, and continue.
    """
    with metrics.span("snowflake_app.build.drop_workspace") as drop_span:
        cli_console.step(
            f"Dropping workspace {storage_fqn} now that the build is complete"
        )
        try:
            manager.drop_workspace_if_exists(storage_fqn)
        except Exception as e:
            log.debug(
                "Failed to drop workspace %s after build",
                storage_fqn.identifier,
                exc_info=True,
            )
            drop_span.finish(error=e)
            cli_console.warning(
                f"Could not drop workspace "
                f"'{sanitize_for_terminal(storage_fqn.identifier)}' after the "
                f"build completed: {e}. The build succeeded; you can remove the "
                "workspace manually if desired."
            )


def _teardown_app_code(
    manager: SnowflakeAppManager,
    *,
    storage: _CodeStorage,
    storage_fqn: FQN,
    app_name: str,
    metrics,
) -> None:
    """Clean up an app's code storage during teardown.

    Mirrors the deploy-time backend selection so a personal-database app is
    torn down via its workspace rather than a (never-created) stage. A
    temporary backend (one the CLI provisioned itself) is dropped outright — a
    workspace as well as a stage — since the CLI owns it end to end. An
    explicitly configured workspace may be shared across apps, so only this
    app's subdirectory is cleared; a stage is dropped outright.
    """
    if storage.type == "workspace":
        if storage.temporary:
            cli_console.step(f"Dropping workspace {storage_fqn.identifier}")
            with metrics.span("snowflake_app.teardown.drop_workspace"):
                manager.drop_workspace_if_exists(storage_fqn)
        else:
            cli_console.step(
                f"Clearing workspace files for {app_name} in {storage_fqn.identifier}"
            )
            with metrics.span("snowflake_app.teardown.clear_workspace"):
                manager.clear_workspace_subdirectory(storage_fqn, app_name)
    else:
        cli_console.step(f"Dropping stage {storage_fqn.identifier}")
        with metrics.span("snowflake_app.teardown.drop_stage"):
            manager.drop_stage_if_exists(storage_fqn)


_PACKAGE_VERSION_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_$]{0,254}")


class _UploadBuildResult(NamedTuple):
    """Outcome of the shared upload and build pipeline.

    ``result`` is set when the command should stop (``--upload-only`` or
    ``--build-only``). ``version`` is the package version this build published,
    when ``SHOW VERSIONS`` could identify one.
    """

    result: Optional[CommandResult] = None
    version: Optional[str] = None


class _AppMessage(MessageResult):
    """A terminal sentence plus fields for ``--format JSON``.

    Defined here so the extra fields stay in the Snowflake App Runtime plugin.
    The shared JSON encoder already prints :meth:`MessageResult.result`.
    """

    def __init__(self, message: str, **details: Any):
        super().__init__(message)
        self._details = {
            key: value for key, value in details.items() if value is not None
        }

    @property
    def result(self):
        if not self._details:
            return {"message": self.message}
        return {"message": self.message, **self._details}


def _app_result(message: str, **details: Any) -> MessageResult:
    """A sentence for the terminal, plus the same facts for ``--format JSON``."""
    return _AppMessage(message, **details)


def _validate_package_version(version: str) -> str:
    """Return *version* as an unquoted identifier, or reject it.

    Package versions are names such as ``VERSION$1``. ``LATEST`` is the alias
    the deploy phase already uses. The value is interpolated into
    ``VERSION <name>``, so anything that is not an unquoted identifier is
    rejected before it can reach SQL.
    """
    candidate = version.strip()
    if not _PACKAGE_VERSION_RE.fullmatch(candidate):
        raise CliArgumentError(
            f"Invalid package version '{version}'. "
            "Use a version name from SHOW VERSIONS IN ARTIFACT REPOSITORY, "
            "such as VERSION$1, or LATEST."
        )
    return candidate.upper()


def _package_version_for_deploy(
    version: Optional[str], promote_only: bool
) -> Optional[str]:
    """Validate ``--version`` and require ``--promote-only``.

    A deploy that builds publishes a new version and promotes that version.
    ``--version`` selects an existing version, which is only meaningful when
    the upload and build phases are skipped.
    """
    if version is None:
        return None
    if not str(version).strip():
        raise CliArgumentError(
            "--version requires a package version name, such as VERSION$1."
        )
    if not promote_only:
        raise CliArgumentError(
            "--version can only be used with --promote-only. "
            "A deploy that uploads or builds publishes a new package version. "
            "To run an existing version, use "
            "snow app deploy --promote-only --version <version>."
        )
    return _validate_package_version(str(version))


def _read_package_versions(
    manager: SnowflakeAppManager, artifact_repo_fqn: str, package_name: str
) -> Optional[list]:
    """Return published version rows, or ``None`` when they cannot be read.

    A missing package (the first build) and a mock that does not implement
    the query both come back as ``None``. Callers treat that as "unknown",
    not as an empty package.
    """
    try:
        rows = manager.list_package_versions(artifact_repo_fqn, package_name)
    except ProgrammingError:
        log.debug(
            "Could not list versions of package %s in %s.",
            package_name,
            artifact_repo_fqn,
            exc_info=True,
        )
        return None
    if not isinstance(rows, list):
        return None
    return rows


def _row_version(row: dict) -> Optional[str]:
    value = row.get("version")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _created_on_key(row: dict) -> str:
    value = row.get("created_on")
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _same_build_job(reported: str, job_identifier: str) -> bool:
    """True when *reported* names the same build job as *job_identifier*."""

    def _norm(value: str) -> str:
        return value.strip().strip('"').upper()

    reported_name = _norm(reported)
    job_name = _norm(job_identifier)
    if not reported_name or not job_name:
        return False
    return (
        reported_name == job_name
        or reported_name.endswith("." + job_name)
        or job_name.endswith("." + reported_name)
    )


def _version_built_by_job(
    versions: Optional[list], job_identifier: str, lookup: Callable[[str], object]
) -> Optional[str]:
    """Return the version whose build job is *job_identifier*.

    ``None`` means the lookup could not be completed (the system function
    failed) or no listed version was produced by that job. Callers then fall
    back to the before/after version diff.
    """
    if not versions:
        return None
    for row in versions:
        if not isinstance(row, dict):
            continue
        version = _row_version(row)
        if not version:
            continue
        try:
            reported = lookup(version)
        except ProgrammingError:
            log.debug(
                "Could not resolve the build job for package version %s.",
                version,
                exc_info=True,
            )
            return None
        if isinstance(reported, str) and _same_build_job(reported, job_identifier):
            return version
    return None


def _newly_published_version(
    before: Optional[list], after: Optional[list]
) -> Optional[str]:
    """Return the version present in *after* but not in *before*.

    When several rows are new, the newest ``created_on`` wins. ``None`` for
    *before* means the baseline could not be read; the newest row in *after*
    is then the best available name for the version this build published.
    """
    if not after:
        return None
    known = set()
    for row in before or []:
        if isinstance(row, dict):
            name = _row_version(row)
            if name:
                known.add(name.upper())
    created = []
    for row in after:
        if not isinstance(row, dict):
            continue
        name = _row_version(row)
        if name and name.upper() not in known:
            created.append(row)
    if not created:
        return None
    created.sort(key=_created_on_key, reverse=True)
    return _row_version(created[0])


def _service_source(description: dict) -> dict:
    """Return the ``source`` object from ``DESCRIBE APPLICATION SERVICE``.

    The column may arrive as a JSON string. An empty or unreadable source is
    ``{}``.
    """
    if not isinstance(description, dict):
        return {}
    source = description.get("source")
    if isinstance(source, str):
        text = source.strip()
        if not text:
            return {}
        try:
            source = json.loads(text)
        except json.JSONDecodeError:
            log.debug("Could not parse application service source: %s", text)
            return {}
    if not isinstance(source, dict):
        return {}
    return source


def _source_value(description: dict, field: str) -> Optional[str]:
    """Read one case-insensitive field from the service ``source`` object."""
    wanted = field.lower()
    for key, value in _service_source(description).items():
        if str(key).lower() != wanted or value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _live_version_from_describe(description: dict) -> Optional[str]:
    """Read the deployed package version from ``DESCRIBE APPLICATION SERVICE``.

    The ``source`` column is a JSON object with ``version`` (and may arrive
    as a string). A missing source, or one that is not JSON, means the live
    version is unknown, not that the deploy failed.
    """
    return _source_value(description, "version")


# GS rejects a serverless deploy that misses its health check with this code
# (shown as ``094547``). The failed version does not take effect, and the app
# stays offline until a later deploy passes. ``DESCRIBE`` may still name the
# previous version; that is a server bug, so it is not reported as live.
_HEALTH_CHECK_ERRNO = 94547


def _is_health_check_failure(exc: BaseException) -> bool:
    """True when *exc* is the serverless health-check timeout."""
    text = str(exc).lower()
    if "did not become healthy" in text or "094547" in text:
        return True
    errno = getattr(exc, "errno", None)
    return errno == _HEALTH_CHECK_ERRNO


def _fail_deploy(
    message: str, *, cause: Optional[BaseException] = None, **fields: Any
) -> NoReturn:
    """Fail a deploy, and print the same facts as JSON when requested.

    The sentence stays on stderr via :class:`CliError`. ``--format JSON``
    also writes one object to stdout so a workflow can read ``version`` and
    ``url`` without parsing that sentence. ``status`` is ``failed`` so the
    object is not a successful deploy.
    """
    output_format = getattr(get_cli_context(), "output_format", None)
    if isinstance(output_format, OutputFormat) and output_format.is_json:
        from snowflake.cli._app.printing import print_result

        print_result(_app_result(message, status="failed", phase="deploy", **fields))
    raise CliError(message) from cause


def _raise_health_check_failure(
    manager: SnowflakeAppManager,
    service_fqn: FQN,
    exc: BaseException,
    *,
    expected_version: Optional[str],
) -> NoReturn:
    """Fail a deploy the server rejected for a missed health check.

    The failed version does not take effect, and the app stays offline until
    a later deploy passes. ``DESCRIBE`` is used only for the URL. Its version
    is not reported: today it can still name the previous version, and that
    is a server bug.
    """
    url = None
    try:
        description = manager.describe_app_service(service_fqn)
        url = manager.resolve_application_service_url_from_describe(description)
    except Exception:
        log.debug(
            "Could not read the app URL after a health-check failure.",
            exc_info=True,
        )
    if expected_version and expected_version.upper() != "LATEST":
        message = (
            f"Version {expected_version} failed its health check and didn't "
            f"take effect: {exc}."
        )
    else:
        message = (
            "The new version failed its health check and didn't take effect: " f"{exc}."
        )
    message += (
        " The app is offline until a deploy passes. Deploy a fixed version, "
        "or run `snow app deploy --promote-only --version <previous>`."
    )
    if url:
        message += f" The app URL is {url}."
    attempted = (
        expected_version
        if expected_version and expected_version.upper() != "LATEST"
        else None
    )
    _fail_deploy(
        message,
        cause=exc,
        version=attempted,
        url=url,
        service=service_fqn.identifier,
    )


def _expected_live_version(
    package_version: Optional[str], built_version: Optional[str]
) -> Optional[str]:
    """The concrete version a successful deploy should leave running.

    ``--version`` wins. Otherwise the version this command's build published.
    ``LATEST`` carries no concrete name.
    """
    if package_version and package_version.upper() != "LATEST":
        return package_version
    return built_version


def _build_completed_result(
    *,
    package: str,
    artifact_repository: str,
    version: Optional[str],
    versions_read: bool,
) -> MessageResult:
    if version:
        message = (
            "Build completed successfully. "
            f"Published package {package} version {version} "
            f"to artifact repository {artifact_repository}."
        )
    elif versions_read:
        message = (
            "Build completed successfully. "
            f"No new package version was listed for package {package} "
            f"in artifact repository {artifact_repository}."
        )
    else:
        message = (
            "Build completed successfully. "
            "The package version could not be read from artifact repository "
            f"{artifact_repository}. List versions with: "
            "SHOW VERSIONS IN ARTIFACT REPOSITORY "
            f"{artifact_repository} FOR PACKAGE {package}."
        )
    return _app_result(
        message,
        phase="build",
        package=package,
        artifact_repository=artifact_repository,
        version=version,
    )


def _deploy_ready_result(
    *,
    url: str,
    service: str,
    description: dict,
    package: Optional[str] = None,
    artifact_repository: Optional[str] = None,
    expected_version: Optional[str] = None,
    target: Optional[str] = None,
) -> MessageResult:
    """Report the URL and the version ``DESCRIBE`` shows after a successful deploy.

    A health-check rejection is raised from the deploy statement (``094547``)
    before this runs. A successful statement is reported as ready even when
    ``DESCRIBE`` still names an older version.
    """
    live = _live_version_from_describe(description)
    alias = _source_value(description, "alias")
    message = f"App ready at {url}."
    if live:
        message += f" Application service {service} is serving version {live}."
        if (
            expected_version
            and expected_version.upper() != "LATEST"
            and alias
            and alias.upper() == "LATEST"
        ):
            message += " The service source still records alias LATEST."
    elif expected_version:
        message += (
            f" Application service {service} was updated to version "
            f"{expected_version}."
        )
    return _app_result(
        message,
        phase="deploy",
        url=url,
        service=service,
        package=package,
        artifact_repository=artifact_repository,
        version=expected_version or live,
        live_version=live,
        alias=alias,
        target=target,
    )


def _log_text(logs: str, service: str) -> str:
    """Return log text, or a sentence when there is nothing to show."""
    if isinstance(logs, str) and logs.strip():
        return logs
    return f"No log lines for application service {service}."


def _upload_and_build_app(
    manager: SnowflakeAppManager,
    *,
    storage: _CodeStorage,
    storage_fqn: FQN,
    app_id: str,
    database: str,
    schema: str,
    artifact_repo_fqn: str,
    artifact_repo_database: Optional[str],
    artifact_repo_schema: Optional[str],
    artifact_repo_name: str,
    build_eai: Optional[str],
    build_job_location: Optional[str],
    bundle: Callable[[], Any],
    run_upload: bool,
    run_build: bool,
    upload_only: bool,
    build_only: bool,
    metrics,
    extra_build_kwargs: Optional[dict] = None,
) -> _UploadBuildResult:
    """Run the shared upload + build pipeline for both deploy entrypoints.

    Bundles source (via the *bundle* callable), uploads it to the resolved
    code-storage backend, and builds the artifact package. Only the surrounding
    configuration and the final deploy step differ between the ``snowflake.yml``
    and ``app.yml`` flows; this pipeline is identical, so both call it.

    ``app_id`` is the code/package identifier used for the workspace
    subdirectory, the stage name, and the build's ``app_id`` (the entity's app
    name or the ``app.yml`` package name). ``build_job_location`` is the
    optional ``<database>.<schema>`` the builder runs the build job in (only the
    ``app.yml`` flow sets it; ``None`` keeps the default PDB behaviour).
    ``extra_build_kwargs`` carries flow-specific build arguments (the entity
    flow forwards ``compute_pool``, ``runtime_image`` and ``project_type``).

    ``result`` is set for ``--upload-only`` / ``--build-only``. Otherwise the
    caller continues into its own deploy phase. ``version`` is the package
    version this build published, when one could be read.
    """
    use_workspace = storage.type == "workspace"
    stage_created = False
    published_version: Optional[str] = None
    versions_after: Optional[list] = None

    # ── Upload phase ──────────────────────────────────────────────────
    if run_upload:
        with metrics.span("snowflake_app.bundle"):
            project_paths = bundle()
        try:
            use_workspace, storage_fqn, stage_created = _upload_app_code(
                manager,
                storage=storage,
                storage_fqn=storage_fqn,
                app_name=app_id,
                database=database,
                schema=schema,
                project_paths=project_paths,
                metrics=metrics,
            )
        finally:
            project_paths.clean_up_output()

    if upload_only:
        if use_workspace:
            location = manager.workspace_subdirectory_uri(storage_fqn, app_id)
        else:
            location = f"@{storage_fqn}"
        return _UploadBuildResult(
            result=_app_result(
                f"Artifacts uploaded to {location}.",
                phase="upload",
                location=location,
                package=app_id,
            )
        )

    # ── Build phase ───────────────────────────────────────────────────
    if run_build:
        versions_before = _read_package_versions(manager, artifact_repo_fqn, app_id)
        with metrics.span("snowflake_app.build"):
            with metrics.span("snowflake_app.build.ensure_artifact_repo"):
                if not manager.artifact_repo_exists(
                    database=artifact_repo_database,
                    schema=artifact_repo_schema,
                    repo_name=artifact_repo_name,
                ):
                    cli_console.step(
                        f"Creating artifact repository: {artifact_repo_fqn}"
                    )
                    manager.create_artifact_repo(
                        database=artifact_repo_database,
                        schema=artifact_repo_schema,
                        repo_name=artifact_repo_name,
                    )

            with metrics.span("snowflake_app.build.submit"):
                cli_console.step("Building app using artifact repository...")
                build_kwargs: dict = dict(
                    artifact_repo_fqn=artifact_repo_fqn,
                    app_id=app_id,
                    database=database,
                    schema=schema,
                    build_eai=build_eai,
                    build_job_location=build_job_location,
                    **(extra_build_kwargs or {}),
                )
                if use_workspace:
                    build_kwargs["source_uri"] = manager.workspace_subdirectory_uri(
                        storage_fqn, app_id
                    )
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

            # Code storage only holds the uploaded source that the artifact-repo
            # build consumes; once the build succeeds it is no longer needed.
            #
            # A temporary backend (one the CLI provisioned because neither
            # ``code_stage`` nor ``code_workspace`` was configured) is always
            # dropped here, even on a ``--build-only`` run that skipped the
            # upload: its ``<app>_CODE`` name is deterministic, so the build
            # phase finds and drops whatever a prior ``--upload-only`` created.
            # An explicitly configured stage is instead dropped only when this
            # invocation created it, so a persisted stage relied on by
            # ``--build-only`` is left untouched; a configured workspace is
            # never dropped here.
            if storage.temporary:
                if use_workspace:
                    _drop_workspace_after_build(manager, storage_fqn, metrics)
                else:
                    _drop_stage_after_build(manager, storage_fqn, metrics)
            elif stage_created:
                _drop_stage_after_build(manager, storage_fqn, metrics)
        versions_after = _read_package_versions(manager, artifact_repo_fqn, app_id)
        published_version = _version_built_by_job(
            versions_after,
            artifact_build_job_fqn.identifier,
            lambda version: manager.get_version_build_job(
                artifact_repo_fqn, app_id, version
            ),
        )
        if published_version is None:
            published_version = _newly_published_version(
                versions_before, versions_after
            )

    if build_only:
        return _UploadBuildResult(
            result=_build_completed_result(
                package=app_id,
                artifact_repository=artifact_repo_fqn,
                version=published_version,
                versions_read=versions_after is not None,
            ),
            version=published_version,
        )

    return _UploadBuildResult(version=published_version)


def _app_service_exists(manager: SnowflakeAppManager, service_fqn: FQN) -> bool:
    """Return True if an application service already uses *service_fqn*.

    Setup only warns about an existing app, so a failed lookup reports none
    instead of failing setup. The lookup fails, for example, on an App Space
    schema that the first deploy has yet to create.
    """
    try:
        return manager.app_service_exists(service_fqn)
    except ProgrammingError:
        log.debug(
            "Could not check whether application service %s exists.",
            service_fqn,
            exc_info=True,
        )
        return False


def _resolve_app_name(
    app_name: Optional[str], directory_name: str, directory_description: str
) -> str:
    """Return *app_name*, or one derived from *directory_name*, after validating it."""
    resolved_app_name = app_name
    if resolved_app_name is None:
        # For implicit names, normalize directory strings into a valid
        # identifier by mapping common separators to "_" and stripping
        # all other disallowed characters.
        resolved_app_name = re.sub(
            r"[^a-zA-Z0-9_]",
            "",
            directory_name.replace(" ", "_").replace("-", "_"),
        )

    if not resolved_app_name:
        raise CliArgumentError(
            f"Could not derive app name from {directory_description}. "
            "Please provide --app-name."
        )

    if not re.fullmatch(r"[a-zA-Z0-9_]+", resolved_app_name):
        raise CliArgumentError(
            f"Invalid app name '{sanitize_for_terminal(resolved_app_name)}'. "
            "Only letters, digits, and underscores are allowed."
        )
    return resolved_app_name


def _resolve_deploy_location(
    manager: SnowflakeAppManager,
    app_name: str,
    build_eai: Optional[str],
    defaults_span: AbstractContextManager[CLIMetricsSpan],
    personal_database_span: AbstractContextManager[CLIMetricsSpan],
) -> dict[str, tuple[Optional[str], str]]:
    """Resolve where the app named *app_name* deploys, and where each value came from.

    Returns ``(value, source)`` for ``database``, ``schema``, ``warehouse``, and
    ``build_eai``, and raises when any of the first three can't be resolved.
    *defaults_span* and *personal_database_span* time the two account lookups, so
    each command reports them under its own span names.
    """
    ctx = get_cli_context()
    connection_name = (
        ctx.connection_context.connection_name or get_default_connection_name()
    )
    conn_config = get_connection_dict(connection_name)

    with defaults_span:
        params = manager.fetch_app_service_defaults(app_name)

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

    with personal_database_span:
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
        raise CliError(
            "Missing database. Provide --database, set the DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE account parameter, or check your connection."
        )
    if not resolved["schema"][0]:
        raise CliError(
            "Missing schema. Provide --schema, set the DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA account parameter, or check your connection."
        )
    if not resolved["warehouse"][0]:
        raise CliError(
            "Missing warehouse. Provide --warehouse, set the DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE account parameter, or check your connection."
        )
    return resolved


@_utf8_output
def snowflake_app_setup(
    app_name: Optional[str],
    dry_run: bool,
    compute_pool: Optional[str],
    build_eai: Optional[str],
    capabilities: bool = False,
) -> CommandResult:
    """Initialize a Snowflake App Runtime project manifest.

    Writes an ``app.yml`` — the v2 manifest the ``snow app`` commands read
    instead of ``snowflake.yml`` — so new projects are always initialized as v2.
    An already-initialized project is left untouched: initialization is skipped
    when either manifest is present. See the ``snow app setup`` command in
    :mod:`snowflake.cli._plugins.nativeapp.commands` for the CLI surface.

    With *capabilities*, nothing is written; see
    :func:`_report_account_capabilities`.
    """
    if capabilities:
        return _report_account_capabilities(app_name, dry_run, compute_pool, build_eai)

    ctx = get_cli_context()
    metrics = ctx.metrics

    def _run() -> CommandResult:
        with metrics.span("snowflake_app.setup"):
            resolved_app_name = _resolve_app_name(
                app_name, Path.cwd().name, "the current directory"
            )
            # app.yml is a CLI-owned manifest that the ``snow app`` commands read
            # back with the same encoding policy (see _app_group_callback): an explicit
            # cli.encoding.file_io setting wins, otherwise UTF-8. Writing it the same
            # way keeps the round-trip consistent regardless of the host code page, even
            # when the generated content (e.g. a non-Latin app title) is non-ASCII.
            encoding = get_file_io_encoding() or "utf-8"
            project_file = Path.cwd() / APP_YML_FILENAME
            if not dry_run:
                # A snowflake.yml project is already initialized, and an app.yml
                # would silently take precedence over it at deploy time, so leave
                # the project alone rather than migrating it behind the user's back.
                for existing in (APP_YML_FILENAME, DEFINITION_FILENAME):
                    if (Path.cwd() / existing).exists():
                        return _app_result(
                            f"{existing} already exists. Skipping initialization.",
                            file=existing,
                        )

            manager = SnowflakeAppManager()
            resolved = _resolve_deploy_location(
                manager,
                resolved_app_name,
                build_eai,
                defaults_span=metrics.span("snowflake_app.setup.resolve_defaults"),
                personal_database_span=metrics.span(
                    "snowflake_app.setup.get_personal_database"
                ),
            )

            resolved_values = {k: v[0] for k, v in resolved.items()}

            # ── Report the code-storage backend ──────────────────────────────
            # ``app.yml`` no longer bakes the backend into the manifest:
            # ``code_stage`` / ``code_workspace`` are omitted and the backend is
            # provisioned *temporarily* at deploy time — a ``<app>_CODE`` stage
            # for a regular database, or a ``<app>_CODE`` workspace when the
            # destination is a personal database (stages are unsupported there).
            # It is created for the deploy and dropped once the build consumes
            # it. Configure ``code_stage`` / ``code_workspace`` in ``app.yml`` to
            # persist code storage across deploys instead. Nothing is probed or
            # persisted here; the value below is informational only.
            destination_db = resolved_values["database"]
            use_workspace = is_personal_database(destination_db)
            code_storage = "temporary workspace" if use_workspace else "temporary stage"

            # ── Check for an app that already uses this name ─────────────────
            # A deploy replaces a service that has the same name. Only warn:
            # deploying over an existing app on purpose is allowed.
            service_fqn = app_fqn(
                database=destination_db,
                schema=resolved_values["schema"],
                # Must match the name _generate_app_yml writes.
                name=resolved_app_name.upper(),
            )
            with metrics.span("snowflake_app.setup.check_existing_app_service"):
                app_exists = _app_service_exists(manager, service_fqn)

            python_project = detect_python_project(Path.cwd())

            if not dry_run:
                with metrics.span("snowflake_app.setup.write_manifest"):
                    project_file.write_text(
                        _generate_app_yml(
                            resolved_app_name,
                            resolved_values,
                            use_workspace=use_workspace,
                            python_project=python_project,
                        ),
                        encoding=encoding,
                    )

            is_json = get_cli_context().output_format.is_json
            if is_json:
                return ObjectResult(
                    {
                        "message": (
                            "Dry run. Resolved the Snowflake App Runtime "
                            "project configuration without writing app.yml."
                            if dry_run
                            else (
                                "Initialized Snowflake App Runtime project "
                                f"in {APP_YML_FILENAME}."
                            )
                        ),
                        "file": APP_YML_FILENAME,
                        "success": not dry_run,
                        "code_storage": code_storage,
                        "app_exists": app_exists,
                        "runtime": python_project.runtime if python_project else "node",
                        "streamlit": bool(
                            python_project
                            and python_project.runtime == "python"
                            and python_project.streamlit
                        ),
                        **resolved_values,
                    }
                )

            if dry_run:
                cli_console.step("Dry run — resolved configuration:")
            else:
                cli_console.step(
                    f"Initialized Snowflake App Runtime project in {APP_YML_FILENAME}."
                )
            for key, (value, source) in resolved.items():
                # Skip optional fields that could not be resolved (e.g. ``build_eai``
                # when no value was provided and no account parameter is set).
                # Emitting ``build_eai: None  (missing)`` is noisy and implies the
                # field is required when it is not.
                if value is None and source == SOURCE_MISSING:
                    continue
                cli_console.step(f"  {key}: {value}  ({source})")
            cli_console.step(f"  code storage: {code_storage}")
            if python_project is not None and python_project.node:
                cli_console.warning(hybrid_app_hint(python_project.streamlit))
            elif python_project is not None:
                kind = "Streamlit " if python_project.streamlit else ""
                cli_console.step(
                    f"  runtime: {python_project.runtime}  "
                    f"(detected {kind}Python project)"
                )
                cli_console.warning(
                    "Python apps have no default commands. Declare `run` in "
                    f"{APP_YML_FILENAME} before deploying."
                )
            if app_exists:
                rename_hint = (
                    "pass a different --app-name"
                    if dry_run
                    else f"change name in {APP_YML_FILENAME}"
                )
                cli_console.warning(
                    "Application service "
                    f"{sanitize_for_terminal(service_fqn.identifier)} already "
                    "exists. Deploying this project will replace that app's code "
                    f"and settings. To create a separate app, {rename_hint}."
                )
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


def _report_account_capabilities(
    app_name: Optional[str],
    dry_run: bool,
    compute_pool: Optional[str],
    build_eai: Optional[str],
) -> CommandResult:
    """Report whether the account supports each Snowflake App Runtime capability.

    Backs ``snow app setup --capabilities``. It writes no ``app.yml``, so the
    setup options that only shape that file are rejected.
    """
    metrics = get_cli_context().metrics
    with metrics.span("snowflake_app.setup"):
        setup_options = {
            "--app-name": app_name,
            "--dry-run": True if dry_run else None,
            "--compute-pool": compute_pool,
            "--build-eai": build_eai,
        }
        set_options = [
            name for name, value in setup_options.items() if value is not None
        ]
        if set_options:
            raise IncompatibleParametersError(["--capabilities", *set_options])

        with metrics.span("snowflake_app.setup.fetch_capabilities"):
            capabilities = SnowflakeAppManager().fetch_account_capabilities()
        return ObjectResult(capabilities)


@_utf8_output
@span("cmd.app.templates.fetch")
def snowflake_app_templates(ref: Optional[str] = None) -> CommandResult:
    """List the Snowflake App Runtime project templates.

    Reads the template catalog at *ref*, or at the templates release tag. See
    the ``snow app templates`` command in
    :mod:`snowflake.cli._plugins.nativeapp.commands` for the CLI surface.
    """
    with TemplateRepository(ref) as repository:
        source = repository.load()
    if get_cli_context().output_format.is_json:
        return CollectionResult(
            template.model_dump(exclude={"path", "digest", "files"})
            for template in source.templates
        )
    return CollectionResult(
        {
            "id": template.id,
            "title": template.title,
            "runtime": template.runtime,
            "default": template.default,
            "summary": template.summary,
        }
        for template in source.templates
    )


@_utf8_output
def snowflake_app_init(
    path: str,
    template_id: Optional[str],
    app_name: Optional[str],
    ref: Optional[str] = None,
) -> CommandResult:
    """Create a Snowflake App Runtime project from a template.

    The project is assembled in a temporary directory: the template's files at
    *ref* (or at the templates release tag), each checked against the catalog,
    and its ``app.yml`` filled in from the account's deployment defaults. Only
    then is it copied to *path*, which must not exist or be an empty directory.
    See the ``snow app init`` command in
    :mod:`snowflake.cli._plugins.nativeapp.commands` for the CLI surface.
    """
    destination = SecurePath(path)
    if destination.exists() and (
        not destination.is_dir() or any(destination.iterdir())
    ):
        raise CliError(
            f"{sanitize_for_terminal(path)} already exists and is not an empty "
            "directory. Choose a new directory for the project."
        )
    directory_name = destination.path.resolve().name
    resolved_app_name = _resolve_app_name(
        app_name, directory_name, "the project directory"
    )
    name = resolved_app_name.upper()
    metrics = get_cli_context().metrics

    with TemplateRepository(ref) as repository:
        manager = SnowflakeAppManager()
        # Telemetry goes out over the Snowflake connection, and the CLI opens one
        # just for telemetry only when logging in is silent. Connecting before
        # reading the templates reports a GitHub failure for every user.
        resolved = _resolve_deploy_location(
            manager,
            resolved_app_name,
            None,
            defaults_span=metrics.span("cmd.app.init.resolve.defaults"),
            personal_database_span=metrics.span(
                "cmd.app.init.resolve.personal_database"
            ),
        )
        values = {key: value for key, (value, _source) in resolved.items()}
        source = repository.load()
        template = source.get(template_id)
        with SecurePath.temporary_directory() as tmp:
            project = tmp / "project"
            project.mkdir()
            with metrics.span("cmd.app.init.fetch.template"):
                repository.download(source, template, project)
            app_yml = project / APP_YML_FILENAME
            rendered = render_app_yml(
                app_yml.read_text(DEFAULT_SIZE_LIMIT_MB, encoding="utf-8"),
                template=template,
                ref=source.ref,
                commit=source.commit,
                name=name,
                label=readable_label(app_name or directory_name),
                database=cast(str, values["database"]),
                schema=cast(str, values["schema"]),
                warehouse=cast(str, values["warehouse"]),
                build_eai=values["build_eai"],
            )
            with app_yml.open("wb") as fd:
                fd.write(rendered.encode("utf-8"))
            destination.mkdir(parents=True, exist_ok=True)
            for child in project.iterdir():
                child.copy(destination.path / child.name)

    service_fqn = app_fqn(
        database=cast(str, values["database"]),
        schema=cast(str, values["schema"]),
        name=name,
    )
    with metrics.span("cmd.app.init.validate.existing_app_service"):
        app_exists = _app_service_exists(manager, service_fqn)
    missing_capabilities: list[str] = []
    if template.requires:
        with metrics.span("cmd.app.init.fetch.capabilities"):
            capabilities = manager.fetch_account_capabilities()
        # A capability this CLI doesn't know can't be checked, so it isn't reported.
        missing_capabilities = [
            capability
            for capability in template.requires
            if capabilities.get(capability) is False
        ]

    if get_cli_context().output_format.is_json:
        return ObjectResult(
            {
                "path": str(destination.path),
                "template": template.id,
                "ref": source.ref,
                "commit": source.commit,
                "digest": template.digest,
                "name": name,
                **values,
                "app_exists": app_exists,
                "missing_capabilities": missing_capabilities,
            }
        )

    cli_console.step(
        f"Created {sanitize_for_terminal(path)} from the {template.id} template "
        f"({TEMPLATES_REPOSITORY} {source.ref}, commit {source.commit[:12]})."
    )
    cli_console.step(f"  name: {name}")
    for key, (value, value_source) in resolved.items():
        if value is None and value_source == SOURCE_MISSING:
            continue
        cli_console.step(f"  {key}: {sanitize_for_terminal(value)}  ({value_source})")
    if missing_capabilities:
        cli_console.warning(
            f"The {template.id} template needs "
            f"{', '.join(missing_capabilities)}, which this account doesn't "
            "report as enabled. The app can't deploy until they are."
        )
    if app_exists:
        cli_console.warning(
            "Application service "
            f"{sanitize_for_terminal(service_fqn.identifier)} already exists. "
            "Deploying this project will replace that app's code and settings. "
            f"To create a separate app, change name in {APP_YML_FILENAME}."
        )
    return MessageResult(
        f"Read {APP_YML_FILENAME} and README.md in {sanitize_for_terminal(path)}, "
        "then run `snow app deploy` there to deploy the app."
    )


@_utf8_output
@span("snowflake_app.bundle")
def snowflake_app_bundle(entity_id: Optional[str]) -> CommandResult:
    """Bundle a Snowflake App Runtime project's source artifacts.

    The whole project root is bundled (minus the ``app.yml`` ``ignore`` globs)
    when an ``app.yml`` drives the project, otherwise the ``snowflake.yml``
    entity's artifacts are used. Bundling is target-independent (all targets
    share the same source), so no ``--target`` is needed.
    """
    app_def = load_app_yml(get_cli_context().project_root)
    if app_def is not None:
        # Bundling is target-independent, so it uses only the baseline. ``name``
        # may be defined solely per target (validated at deploy time), so fall
        # back to ``package_name`` then a generic id — the bundle id only names
        # the local output directory.
        baseline_name = FQN.from_string(app_def.name).name if app_def.name else None
        bundle_id = app_def.package_name or baseline_name or "app"
        project_paths = perform_bundle(
            bundle_id,
            SimpleNamespace(artifacts=app_def.bundle_artifacts),
        )
        return _app_result(
            f"Bundle generated at {project_paths.bundle_root}.",
            bundle_root=str(project_paths.bundle_root),
        )

    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    project_paths = perform_bundle(resolved_entity_id, entity)
    return _app_result(
        f"Bundle generated at {project_paths.bundle_root}.",
        bundle_root=str(project_paths.bundle_root),
    )


@_utf8_output
@span("snowflake_app.validate")
def snowflake_app_validate(
    entity_id: Optional[str], target: Optional[str] = None
) -> CommandResult:
    """Validate a local Snowflake App Runtime project.

    Uses ``app.yml`` (the ``--target`` target) when present, otherwise the
    ``snowflake.yml`` entity.
    """
    manager, database, schema, bundle = _resolve_validate_target(entity_id, target)
    metrics = get_cli_context().metrics

    # ── Validate database and schema ──────────────────────────────────
    # ``snowflake.yml`` projects may leave the destination unset for offline,
    # bundle-only validation; the checks below run only when a database is
    # resolved. An ``app.yml`` target always resolves a database and schema.
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
            project_paths = bundle()
    finally:
        if project_paths is not None:
            project_paths.clean_up_output()
    if database and schema:
        message = (
            "Valid Snowflake App Runtime project. "
            f"Database '{database}' and schema '{database}.{schema}' are "
            "accessible, and the project source bundles."
        )
    elif database:
        message = (
            "Valid Snowflake App Runtime project. "
            f"Database '{database}' is accessible, and the project source bundles."
        )
    else:
        message = "Valid Snowflake App Runtime project. The project source bundles."
    return _app_result(message, database=database, schema=schema)


def _resolve_validate_target(
    entity_id: Optional[str],
    target: Optional[str],
) -> tuple[SnowflakeAppManager, Optional[str], Optional[str], Callable[[], Any]]:
    """Resolve ``(manager, database, schema, bundle)`` for ``snow app validate``.

    Routes ``app.yml`` (the ``--target`` target) and ``snowflake.yml`` through
    one path so the command body is shared. ``bundle`` is a source-specific
    callable that produces the project bundle when invoked. For a
    ``snowflake.yml`` project the destination may be unset (offline bundle-only
    validation), so the database/schema can be ``None``.
    """
    app_def = _load_app_yml_for_command(target)
    if app_def is not None:
        _ensure_python_phases_declared(app_def)
        manager = SnowflakeAppManager()
        dep = _resolve_app_yml_target(app_def, target, manager=manager)
        return (
            manager,
            dep.database,
            dep.schema,
            lambda: perform_bundle(
                dep.package_name,
                SimpleNamespace(artifacts=dep.target.bundle_artifacts),
            ),
        )

    manager = SnowflakeAppManager()
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)
    fqn = entity.fqn
    if fqn.database:
        # Only touch the connection when there is a destination to validate;
        # bundle-only validation stays offline. Resolution happens in place on
        # the shared entity.fqn (also expands USER$ → USER$<user>); downstream
        # re-reads of entity.fqn (e.g. perform_bundle) see the resolved value.
        fqn.using_context()
    return (
        manager,
        fqn.database,
        fqn.schema,
        lambda: perform_bundle(resolved_entity_id, entity),
    )


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
@span("snowflake_app.open")
def snowflake_app_open(
    entity_id: Optional[str],
    print_only: bool,
    settings: bool,
    watch: bool = False,
    target: Optional[str] = None,
) -> CommandResult:
    """Open a deployed Snowflake App Runtime (or its settings page) in the browser.

    Resolves the application service from ``app.yml`` (the ``--target`` target)
    when present, otherwise from the ``snowflake.yml`` entity.
    """
    svc = _resolve_command_service(entity_id, target)
    if not svc.database or not svc.schema:
        missing = [
            k
            for k, v in {"database": svc.database, "schema": svc.schema}.items()
            if not v
        ]
        raise CliError(
            f"Cannot resolve {' or '.join(missing)} for the app. "
            "Set them in app.yml, snowflake.yml, or your connection configuration."
        )

    return _open_app_service(
        svc.manager,
        service_fqn=svc.service_fqn,
        database=svc.database,
        schema=svc.schema,
        name=svc.name,
        print_only=print_only,
        settings=settings,
        watch=watch,
    )


@_utf8_output
@span("snowflake_app.events")
def snowflake_app_events(
    entity_id: Optional[str],
    last: Optional[int],
    *,
    event_type: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    metric: Optional[str] = None,
    raw: bool = False,
    target: Optional[str] = None,
    instance: Optional[int] = None,
) -> CommandResult:
    """Fetch logs, metrics, or lifecycle events from a deployed Snowflake App Runtime.

    The bare command (``--type log`` with no window) tails the live container's
    logs, unchanged. Supplying a ``--since`` / ``--until`` window switches logs
    to the historical event table. ``--type metric`` and ``--type lifecycle``
    are always sourced from the event table and default to the last hour when no
    window is given.

    The application service is resolved from ``app.yml`` (the ``--target``
    target) when present, otherwise from the ``snowflake.yml`` entity.
    """
    stream = parse_event_stream(event_type)
    category = parse_metric_category(metric)
    if category is not None and stream is not EventStream.METRIC:
        raise CliError("--metric can only be used with --type metric.")
    if raw and stream is not EventStream.METRIC:
        raise CliError("--raw can only be used with --type metric.")
    if instance is not None and instance < 0:
        raise CliError(
            "--instance must be a non-negative integer (0-based service instance index)."
        )
    if instance is not None and (stream is not EventStream.LOG or since or until):
        raise CliError(
            "--instance can only be used with the live log tail (--type log with no --since/--until)."
        )

    svc = _resolve_command_service(entity_id, target)
    return _emit_app_events(
        svc.manager,
        svc.service_fqn,
        last,
        stream=stream,
        category=category,
        since=since,
        until=until,
        raw=raw,
        instance=instance,
    )


def _emit_app_events(
    manager: SnowflakeAppManager,
    service_fqn: FQN,
    last: Optional[int],
    *,
    stream: EventStream,
    category: Optional[MetricCategory],
    since: Optional[str],
    until: Optional[str],
    raw: bool,
    instance: Optional[int] = None,
) -> CommandResult:
    """Fetch and render one observability stream for an application service.

    Shared by the ``app.yml`` and ``snowflake.yml`` event flows once the
    service FQN has been resolved.

    The server decides whether a serverless (CNG) app's event table can be
    read in this account; when it cannot, metric, lifecycle, and windowed-log
    requests raise :class:`CliError` pointing at the live log tail.
    """
    metrics = get_cli_context().metrics

    # Logs with no window keep the legacy live-container tail.
    if stream is EventStream.LOG and not since and not until:
        try:
            with metrics.span("snowflake_app.events.fetch_logs"):
                logs = manager.get_service_logs(
                    service_fqn, last=last, instance_id=instance
                )
        except ProgrammingError as err:
            hint = (
                f" Instance {instance} may not exist — verify the index is valid and the service is running."
                if instance is not None
                else " Verify that the app is deployed and the service is running."
                " If the service exists, this can also happen when the active role cannot read application service logs."
            )
            raise CliError(
                f"Could not retrieve logs for '{service_fqn.identifier}': {err.msg}.{hint}"
            ) from err
        return _app_result(
            _log_text(logs, service_fqn.identifier),
            service=service_fqn.identifier,
            stream="log",
        )

    # Everything else (windowed logs, metrics, lifecycle) reads the event table.
    # ``--last`` caps the number of records returned (newest first); the manager
    # transparently pages past the inline cap when more are requested. ``None``
    # means "no explicit cap" — return whatever the inline call yields.
    start_time, end_time = resolve_time_window(since, until)
    try:
        with metrics.span(f"snowflake_app.events.fetch_{stream.value}"):
            payload = manager.get_event_table_data(
                service_fqn,
                stream.event_table_type,
                start_time,
                end_time,
                limit=last,
            )
    except ProgrammingError as err:
        if err.errno in SERVERLESS_EVENT_TABLE_UNAVAILABLE_ERRNOS:
            raise CliError(_CNG_HEALTH_MONITORING_UNAVAILABLE) from err
        raise CliError(
            f"Could not retrieve {stream.value} data for '{service_fqn.identifier}'. "
            "Verify that the app is deployed and that the active role can read "
            "the application service's event table. Recently emitted telemetry "
            "may not appear immediately due to ingestion lag."
        ) from err

    if stream is EventStream.METRIC:
        records = parse_metric_records(payload, category=category, raw_values=raw)
        return CollectionResult(records[:last] if last is not None else records)
    if stream is EventStream.LIFECYCLE:
        records = parse_lifecycle_records(payload)
        return CollectionResult(records[:last] if last is not None else records)
    # Logs render oldest-first; keep the newest ``last`` lines when capped.
    # ``lines[-0:]`` would return the whole log, so guard the zero case.
    lines = format_log_lines(payload).splitlines()
    if last is not None:
        lines = lines[-last:] if last > 0 else []
    return _app_result(
        _log_text("\n".join(lines), service_fqn.identifier),
        service=service_fqn.identifier,
        stream="log",
    )


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


def _is_cng_compute_resource(compute_resource: Optional[str]) -> bool:
    """Return ``True`` for the serverless app-service backend."""
    return (compute_resource or "").upper() == SERVERLESS_COMPUTE_RESOURCE


def _effective_compute_resource(
    manager: SnowflakeAppManager,
    compute_resource: Optional[str],
) -> Optional[str]:
    """Return *compute_resource*, raising when the account disallows ``SERVERLESS``."""
    if not _is_cng_compute_resource(compute_resource):
        return compute_resource
    if manager.serverless_account_parameters_enabled():
        return compute_resource
    raise CliError("compute_resource SERVERLESS is not enabled for this account.")


def _ensure_python_phases_declared(app_def: AppYmlDefinition) -> None:
    """Raise when an app whose runtime includes python omits a phase it must declare.

    Checked locally, before anything is uploaded: the builder runs a Python
    app's declared commands and infers none. A ``node`` and ``python`` runtime
    that omits ``install`` or ``build`` only gets a warning, since neither is
    required, but no npm command runs without them.
    """
    missing = app_def.missing_python_phases()
    if missing:
        raise CliError(
            f"{APP_YML_FILENAME} has a runtime that includes `python` but does not declare "
            f"{', '.join(f'`{name}`' for name in missing)}. A Python app gets no "
            f"default commands: declare "
            f"{', '.join(f'`{name}`' for name in PYTHON_REQUIRED_PHASES)} in "
            f"{APP_YML_FILENAME} (`install` and `build` are optional)."
        )
    if {RUNTIME_NODE, RUNTIME_PYTHON} <= set(app_def.runtime.split(",")):
        undeclared = [
            f"`{name}`"
            for name in ("install", "build")
            if getattr(app_def, name) is None or not getattr(app_def, name).commands
        ]
        if undeclared:
            cli_console.warning(
                "`runtime` includes node and python, so no npm defaults apply. "
                f"Declare {' and '.join(undeclared)} in {APP_YML_FILENAME}, or "
                f"{'none will run' if len(undeclared) > 1 else 'it will not run'}."
            )


def _ensure_python_runtime_enabled(
    manager: SnowflakeAppManager,
    app_def: AppYmlDefinition,
    compute_resource: Optional[str],
) -> None:
    """Raise when *app_def* uses the Python runtime on a compute resource the account does not allow.

    The app uses the Python runtime when its ``app.yml`` runtime includes python
    (:attr:`AppYmlDefinition.is_python_runtime`).

    An unset *compute_resource* is checked as ``MANAGED_COMPUTE_POOL``, the
    backend the server uses by default.
    """
    if not app_def.is_python_runtime:
        return
    effective = compute_resource or MANAGED_COMPUTE_POOL_COMPUTE_RESOURCE
    if manager.python_runtime_enabled(effective):
        return
    raise CliError(
        f"The Python runtime is not enabled for compute_resource {effective} in "
        f"this account. This app's runtime in {APP_YML_FILENAME} includes `python`. Deploy to a "
        f"compute_resource that allows the Python runtime, or contact Snowflake "
        f"support to enable it."
    )


def _ensure_cng_url_cert_ready(
    manager: SnowflakeAppManager, *, skip: bool, required: bool
) -> None:
    """Ensure the account's per-account URL certificate is being provisioned.

    CNG (serverless) apps serve from per-account URLs backed by a per-account
    TLS certificate whose issuance can take up to ~3 hours — far too long to
    happen inside ``CREATE APPLICATION SERVICE`` — so this probes for it up front
    (never polling) via a client-side TLS probe. When the certificate is absent,
    issuance is started automatically and a full deploy stops so it can be
    re-run once provisioning completes.

    The caller gates this on the app being CNG.

    ``skip`` (``--skip-certs-check``) opts out: no probe and no issuance, and
    the deploy continues.

    ``required`` says whether a missing certificate is fatal for the current
    phase: only the deploy phase creates the service, so only it passes
    ``required=True``. ``--upload-only`` / ``--build-only`` still start issuance
    but only warn, so the user can upload/build now and promote once issuance
    completes. An inconclusive probe (``UNKNOWN``) or an underivable host never
    blocks and does not start issuance — a false negative must not prevent a
    deploy or kick off provisioning for a certificate that may already exist.
    """
    if skip:
        log.debug("Skipping per-account URL certificate check (--skip-certs-check).")
        return

    probe_host = manager.per_account_cert_probe_host()
    if probe_host is None:
        log.debug(
            "Skipping per-account URL certificate pre-check: could not derive "
            "the account's app host."
        )
        return

    cli_console.step("Checking per-account URL certificate...")
    status = manager.per_account_cert_status_for_host(probe_host)
    if status is PerAccountCertStatus.PROVISIONED:
        return

    if status is PerAccountCertStatus.UNKNOWN:
        cli_console.warning(
            "Could not verify the account's per-account URL certificate "
            "(the ingress was unreachable or untrusted — e.g. PrivateLink, a "
            "proxy, or a custom CA). Continuing the deploy. If the app URL shows "
            "a browser TLS warning, provision the certificate by running:\n"
            f"  SELECT {PER_ACCOUNT_CERT_ISSUE_FUNCTION}();"
        )
        return

    # NOT_PROVISIONED: start issuance. A full deploy cannot create the service
    # until the certificate is being served.
    cli_console.step("Starting per-account URL certificate provisioning...")
    manager.issue_per_account_url_cert()
    message = (
        "This account does not yet have a per-account URL certificate, "
        "which serverless apps require. Provisioning has been started "
        f"for you via {PER_ACCOUNT_CERT_ISSUE_FUNCTION}(). This can take up "
        "to 3 hours. Re-run 'snow app deploy' once provisioning completes."
    )

    if required:
        raise CliError(message)
    # Upload/build phase: the service is not being created yet, so don't block —
    # surface the same guidance as a warning and let the phase proceed.
    cli_console.warning(message)


def _warn_if_cng_url_cert_missing(manager: SnowflakeAppManager, url: str) -> None:
    """Warn (never fail) when the per-account URL certificate is missing.

    Used by ``snow app open``: launching the browser at a CNG app URL without a
    provisioned certificate shows a TLS warning. The app already exists, so this
    probes the resolved *url*'s host directly (the exact certificate the browser
    would see). Unlike the deploy pre-check — which knows the app is CNG from its
    resolved ``compute_resource`` — ``open`` does not resolve the entity, so it
    leans on ``per_account_cert_status_for_url`` returning ``UNKNOWN`` for
    non-per-account hosts (e.g. SPCS ``snowflakecomputing.app``), so an SPCS
    app's TLS state is never misattributed to a per-account cert. Any probe error
    is swallowed so ``open`` never breaks on this advisory.
    """
    try:
        status = manager.per_account_cert_status_for_url(url)
    except Exception:
        log.debug("per-account URL certificate check failed", exc_info=True)
        return
    if status is not PerAccountCertStatus.NOT_PROVISIONED:
        return
    cli_console.warning(
        "This account does not yet have a per-account URL certificate, so the "
        "app URL may show a browser TLS warning. Start provisioning by "
        f"running:\n  SELECT {PER_ACCOUNT_CERT_ISSUE_FUNCTION}();\n"
        "This can take up to 3 hours."
    )


# ── app.yml (targets) deploy path ─────────────────────────────────────


def _resolve_app_yml_database(
    manager: SnowflakeAppManager,
    target_database: Optional[str],
) -> Optional[str]:
    """Resolve a target's destination database.

    Uses the target's ``database`` (required in ``app.yml``; there is no
    connection fallback). The ``USER$`` shorthand (as written in ``app.yml``) is
    expanded to the caller's personal database ``USER$<user>``.
    """
    database = target_database
    if database and database.strip().upper() == "USER$":
        personal = manager.get_personal_database()
        if not personal:
            raise CliError(
                "Target requests the personal database (USER$) but it could "
                "not be resolved for the current user."
            )
        return personal
    return database


def _active_connection_account() -> Optional[str]:
    """Best-effort account identifier of the active connection (no SQL).

    Prefers an explicit ``--account`` override, otherwise the configured
    ``account`` of the named connection. Returns ``None`` when it cannot be
    determined cheaply so a target's ``account`` check never false-warns.
    """
    conn = get_cli_context().connection_context
    if conn.account:
        return conn.account
    if not conn.connection_name:
        return None
    try:
        return get_connection_dict(conn.connection_name).get("account")
    except Exception:
        return None


def _warn_on_target_account_mismatch(
    target_name: Optional[str], target_account: Optional[str]
) -> None:
    """Warn when the resolved ``account`` differs from the active connection.

    Per-target account binding (cross-account dev/prod) is a later milestone;
    the active connection is always used. Only warns when both accounts are
    known and differ, so a matching or undeterminable account stays quiet.
    """
    if not target_account:
        return
    active = _active_connection_account()
    if active and active.strip().lower() != target_account.strip().lower():
        source = f"Target '{target_name}'" if target_name else "app.yml"
        cli_console.warning(
            f"{source} declares account "
            f"'{sanitize_for_terminal(target_account)}', but per-target account "
            "binding is not yet supported; using the active connection "
            f"(account '{sanitize_for_terminal(active)}')."
        )


def _split_app_yml_object(
    value: Optional[str],
    *,
    database: Optional[str],
    schema: Optional[str],
    default_name: str,
) -> tuple[Optional[str], Optional[str], str]:
    """Return ``(database, schema, name)`` for a code stage / artifact repo.

    Accepts either a bare name or a ``DB.SCHEMA.NAME`` identifier; missing
    components fall back to the resolved app database/schema. When *value* is
    empty the *default_name* is used against the app's database/schema.
    """
    if value:
        parsed = FQN.from_string(value)
        return (parsed.database or database, parsed.schema or schema, parsed.name)
    return (database, schema, default_name)


class _AppYmlDeployment(NamedTuple):
    """A single ``app.yml`` target resolved into the objects commands act on.

    Resolving a target once yields everything the ``snow app`` commands need to
    operate on it: the application-service FQN (``open`` / ``events`` /
    ``teardown``), the resolved code-storage backend and its FQN (``deploy`` /
    ``teardown``), the raw database/schema/service name (Snowsight URLs), the
    package name (``deploy``), and the raw target for building a service
    specification (``deploy``). This keeps target resolution in one place
    instead of being re-derived by each command.
    """

    target_name: Optional[str]
    package_name: str
    database: str
    schema: str
    service_name: str
    service_fqn: FQN
    storage: _CodeStorage
    storage_fqn: FQN
    target: AppYmlTarget


def _resolve_app_yml_target(
    app_def: AppYmlDefinition,
    target: Optional[str],
    *,
    manager: SnowflakeAppManager,
) -> _AppYmlDeployment:
    """Resolve one ``app.yml`` target against the active connection.

    Selects the target (explicit ``--target`` or ``default_target``) and derives
    everything the commands act on. ``name`` is required and anchors the rest: a
    fully-qualified ``name`` overrides ``database`` / ``schema`` (a bare name
    inherits them), ``package_name`` defaults to the bare ``name`` and
    ``artifact_repo`` / ``code_stage`` default to ``<name>_REPO`` / ``<name>_CODE``.
    The ``USER$`` personal-database shorthand is expanded. The code-storage
    backend (workspace or stage) is chosen the same way as the ``snowflake.yml``
    flow.
    """
    resolved_target_name, tgt = resolve_target(app_def, target)

    # ``name`` may be a fully-qualified identifier, in which case its db/schema
    # override the separate fields; a bare name inherits them. There is no
    # connection fallback for database/schema.
    database = _resolve_app_yml_database(manager, tgt.database)
    schema = tgt.schema_
    database, schema, service_name = _split_app_yml_object(
        tgt.name, database=database, schema=schema, default_name=tgt.name
    )
    # ``name`` / ``database`` / ``schema`` / ``query_warehouse`` may be set at
    # the top level (baseline), on the selected target, or a mix; db/schema may
    # also come from a fully-qualified ``name``. The *resolved* target must
    # define all four (there is no connection fallback). Enforcing it here — on
    # the merged target — is what lets values live in either scope.
    if not service_name or not database or not schema or not tgt.query_warehouse:
        missing = [
            field
            for field, value in (
                ("name", service_name),
                ("database", database),
                ("schema", schema),
                ("query_warehouse", tgt.query_warehouse),
            )
            if not value
        ]
        where = (
            f"target '{resolved_target_name}'"
            if resolved_target_name
            else "the app.yml baseline"
        )
        raise CliError(
            f"Missing required field(s) in {where}: {', '.join(missing)}. "
            "Set them at the top level of app.yml or on the selected target."
        )

    # ``package_name`` and the artifact-repo / code-stage names all default off
    # the (bare) service name when unset.
    package_name = tgt.package_name or service_name

    _warn_on_target_account_mismatch(resolved_target_name, tgt.account)

    # Backend chosen the same way as the snowflake.yml flow (see
    # _resolve_code_storage); code storage comes from the merged target. With
    # neither backend configured the CLI provisions a temporary ``<app>_CODE``
    # backend for the deploy and drops it after the build.
    storage = _app_yml_code_storage(
        code_stage=tgt.code_stage,
        code_workspace=tgt.code_workspace,
        database=database,
        schema=schema,
        app_name=service_name,
    )
    storage_fqn = _storage_fqn(storage, database=database, schema=schema)

    return _AppYmlDeployment(
        target_name=resolved_target_name,
        package_name=package_name,
        database=database,
        schema=schema,
        service_name=service_name,
        service_fqn=app_fqn(database=database, schema=schema, name=service_name),
        storage=storage,
        storage_fqn=storage_fqn,
        target=tgt,
    )


class _ResolvedService(NamedTuple):
    """An application service addressed by a read/observe command.

    The common result of resolving a command's target from either ``app.yml``
    (the ``--target`` target) or the ``snowflake.yml`` entity, so ``open`` and
    ``events`` share one resolution path instead of each re-deriving it.
    """

    manager: SnowflakeAppManager
    service_fqn: FQN
    database: Optional[str]
    schema: Optional[str]
    name: str


def _resolve_command_service(
    entity_id: Optional[str],
    target: Optional[str],
) -> _ResolvedService:
    """Resolve the application service for a read/observe command (``open`` /
    ``events``).

    Prefers ``app.yml`` (selecting the ``--target`` target); otherwise falls
    back to the ``snowflake.yml`` entity, in which case ``--target`` is not
    valid. Missing ``snowflake.yml`` location values use the same App Space
    defaults as deploy.
    """
    manager = SnowflakeAppManager()
    app_def = _load_app_yml_for_command(target)
    if app_def is not None:
        dep = _resolve_app_yml_target(app_def, target, manager=manager)
        return _ResolvedService(
            manager,
            dep.service_fqn,
            dep.database,
            dep.schema,
            dep.service_name,
        )

    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)
    # Resolve db/schema from the active connection in place on the shared
    # entity.fqn (also expands USER$ → USER$<user>).
    fqn = entity.fqn
    fqn.using_context()
    conn = get_cli_context().connection_context
    database = fqn.database or conn.database
    schema = fqn.schema or conn.schema
    if not database or not schema:
        defaults = manager.fetch_app_service_defaults(fqn.name)
        database = database or defaults.get("database")
        schema = schema or defaults.get("schema")
    # Rebuild to a 3-part name; entity FQN may carry extra fields (e.g. prefix).
    service_fqn = app_fqn(database=database, schema=schema, name=fqn.name)
    return _ResolvedService(manager, service_fqn, database, schema, fqn.name)


def _open_app_service(
    manager: SnowflakeAppManager,
    *,
    service_fqn: FQN,
    database: Optional[str],
    schema: Optional[str],
    name: str,
    print_only: bool,
    settings: bool,
    watch: bool,
) -> CommandResult:
    """Resolve and (optionally) launch an application service's browser URL.

    Shared by the ``app.yml`` and ``snowflake.yml`` open flows once the
    service and its destination database/schema/name have been resolved.
    """
    ctx = get_cli_context()
    metrics = ctx.metrics

    if settings:
        with metrics.span("snowflake_app.open.resolve_settings_url"):
            app_id = (
                f"{identifier_for_url(database)}"
                f".{identifier_for_url(schema)}"
                f".{identifier_for_url(name)}"
            )
            url = make_snowsight_url(
                ctx.connection, f"#/apps/app-service/{app_id}/details"
            )
    elif watch:
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

    if not settings:
        # CNG apps serve from per-account URLs; warn (never fail) when the
        # per-account certificate is not yet provisioned so the user knows why
        # the browser may show a TLS warning.
        _warn_if_cng_url_cert_missing(manager, url)

    if not print_only:
        typer.launch(url)
    return _app_result(url, url=url, service=service_fqn.identifier)


def _confirm_drop_and_verify(
    manager: SnowflakeAppManager,
    service_fqn: FQN,
    *,
    force: bool,
    metrics,
) -> Optional[CommandResult]:
    """Confirm (unless ``force``), drop, and verify an application service.

    Returns a "cancelled" :class:`MessageResult` when the user declines the
    prompt, otherwise ``None`` after the service has been dropped and verified.
    Shared by the ``app.yml`` and ``snowflake.yml`` teardown flows.
    """
    if not force:
        # Wrap the interactive prompt in its own span so the time spent waiting
        # on the user is attributable and does not silently inflate the overall
        # command duration.
        with metrics.span("snowflake_app.teardown.confirm"):
            should_continue = typer.confirm(
                f"Are you sure you want to drop application service "
                f"{service_fqn.identifier} and its associated objects?"
            )
        if not should_continue:
            return _app_result(
                "Teardown cancelled.",
                service=service_fqn.identifier,
            )

    cli_console.step(f"Dropping application service {service_fqn.identifier}")
    with metrics.span("snowflake_app.teardown.drop_service"):
        manager.drop_app_service_if_exists(service_fqn)
        try:
            still_exists = bool(manager.describe_app_service(service_fqn))
        except ProgrammingError:
            still_exists = False
        except Exception as err:  # noqa: BLE001
            raise CliError(
                f"Could not verify application service {service_fqn.identifier} "
                f"was dropped: {err}"
            ) from err
        if still_exists:
            raise CliError(
                f"Failed to drop application service {service_fqn.identifier}. "
                f"Check: DESCRIBE APPLICATION SERVICE {service_fqn.identifier}"
            )
    return None


class _TeardownTarget(NamedTuple):
    """The application service and code storage a teardown must drop.

    The common result of resolving a teardown's target from either ``app.yml``
    or the ``snowflake.yml`` entity, so ``snow app teardown`` has a single body.
    """

    manager: SnowflakeAppManager
    service_fqn: FQN
    storage: _CodeStorage
    storage_fqn: FQN
    code_app_name: str


def _resolve_teardown_target(
    entity_id: Optional[str],
    target: Optional[str],
) -> _TeardownTarget:
    """Resolve the service and code storage for ``snow app teardown``.

    Uses ``app.yml`` (the ``--target`` target) when present, otherwise the
    ``snowflake.yml`` entity — resolved through the same deploy-defaults path as
    ``snow app deploy`` so teardown addresses exactly what deploy created.
    """
    manager = SnowflakeAppManager()
    app_def = _load_app_yml_for_command(target)
    if app_def is not None:
        dep = _resolve_app_yml_target(app_def, target, manager=manager)
        return _TeardownTarget(
            manager,
            dep.service_fqn,
            dep.storage,
            dep.storage_fqn,
            dep.package_name,
        )

    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)
    fqn = entity.fqn
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
    # Mirror the deploy-time backend selection so a personal-database app is
    # torn down via its workspace rather than a (never-created) stage.
    storage = _entity_code_storage(
        entity, database=db, schema=schema, app_name=app_name
    )
    storage_fqn = _storage_fqn(storage, database=db, schema=schema)
    return _TeardownTarget(
        manager,
        app_fqn(database=db, schema=schema, name=app_name),
        storage,
        storage_fqn,
        app_name,
    )


def _wait_for_app_yml_endpoint(
    manager: SnowflakeAppManager,
    service_fqn: FQN,
    metrics,
) -> tuple[str, dict]:
    """Poll a just-applied application service until its endpoint is ready.

    Returns the browser URL and the ``DESCRIBE`` row that produced it, so the
    caller can read the version the service is serving.
    """

    def _url_is_ready(d: dict) -> bool:
        return manager.resolve_application_service_url_from_describe(d) is not None

    def _svc_has_failed(d: dict) -> bool:
        return d.get("status", "").upper() == "FAILED"

    cli_console.step(f"[{_ts()}] Waiting for application service endpoint...")
    try:
        with metrics.span("snowflake_app.endpoint_provision"):
            desc = _poll_until(
                poll_fn=lambda: manager.describe_app_service(service_fqn),
                is_done=_url_is_ready,
                is_error=_svc_has_failed,
                format_status=lambda d: d.get("url") or "url not yet available",
                timeout_message=(
                    "Application service deployment timed out. Check application "
                    "service state and logs:\n"
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

    url = manager.resolve_application_service_url_from_describe(desc)
    if not url:
        raise CliError(
            "Application service URL is not available after deploy. "
            f"Check: DESCRIBE APPLICATION SERVICE {service_fqn.identifier}"
        )
    return url, desc


def _deploy_from_app_yml(
    app_def: AppYmlDefinition,
    *,
    target: Optional[str],
    upload_only: bool,
    build_only: bool,
    promote_only: bool,
    interactive: Optional[bool],
    skip_certs_check: bool = False,
    package_version: Optional[str] = None,
) -> CommandResult:
    """Deploy a single ``app.yml`` target through upload, build, and deploy.

    The upload and build phases mirror the ``snowflake.yml`` flow (bundle →
    stage → artifact-repo build). The deploy phase differs: instead of the
    ``CREATE`` / ``ALTER ... UPGRADE`` pair, the target's per-environment
    configuration is applied declaratively via ``CREATE OR ALTER APPLICATION
    SERVICE`` with an inline ``SPECIFICATION`` (see
    :meth:`SnowflakeAppManager.build_service_specification`).

    The phase flags select a single phase: ``upload_only`` stops after the
    upload, ``build_only`` runs only the build (assumes source is already
    uploaded), and ``promote_only`` skips upload and build and applies the
    deploy phase against the already-built package. ``package_version``
    selects that package version; ``None`` promotes ``VERSION LATEST``.
    """
    _ensure_python_phases_declared(app_def)

    ctx = get_cli_context()
    metrics = ctx.metrics
    manager = SnowflakeAppManager(interactive=interactive)

    dep = _resolve_app_yml_target(app_def, target, manager=manager)
    package_name = dep.package_name
    tgt = dep.target
    database = dep.database
    schema = dep.schema
    service_fqn = dep.service_fqn

    service_label = sanitize_for_terminal(dep.service_name)
    if dep.target_name:
        cli_console.step(
            f"Deploying target '{sanitize_for_terminal(dep.target_name)}' "
            f"(application service {service_label})."
        )
    else:
        cli_console.step(f"Deploying application service {service_label}.")

    _ensure_python_runtime_enabled(manager, app_def, tgt.compute_resource)

    _ensure_app_derived_schema(
        manager,
        database=database,
        schema=schema,
        app_name=dep.service_name,
    )

    sql_version = package_version or "LATEST"
    if promote_only:
        if package_version and package_version != "LATEST":
            cli_console.step(
                f"Promoting version {package_version} (skipping upload and build)."
            )
        else:
            cli_console.step(
                "Promoting the latest built package (skipping upload and build)."
            )

    # ``compute_resource`` selects the serverless or SPCS backend and is
    # write-once. ``SERVERLESS`` is applied only when the account feature
    # parameter allows it. When it is unset the server defaults the backend.
    compute_resource = _effective_compute_resource(manager, tgt.compute_resource)

    # Probe for the per-account URL certificate up front (see
    # _ensure_cng_url_cert_ready): it needs no built artifact, and issuance is
    # far too slow to happen inside CREATE OR ALTER APPLICATION SERVICE.
    # A missing certificate starts provisioning unless --skip-certs-check.
    if _is_cng_compute_resource(compute_resource):
        with metrics.span("snowflake_app.deploy.cng_cert_precheck"):
            _ensure_cng_url_cert_ready(
                manager,
                skip=skip_certs_check,
                required=not upload_only and not build_only,
            )

    # The artifact repository and builder EAI are package-build fields that a
    # target may override, so they are read from the resolved (merged) target.
    ar_db, ar_schema, ar_name = _split_app_yml_object(
        tgt.artifact_repo,
        database=database,
        schema=schema,
        default_name=f"{dep.service_name}_REPO",
    )
    artifact_repo_fqn_str = app_fqn(
        database=ar_db, schema=ar_schema, name=ar_name
    ).identifier

    # ── Shared upload + build pipeline ────────────────────────────────
    uploaded = _upload_and_build_app(
        manager,
        storage=dep.storage,
        storage_fqn=dep.storage_fqn,
        app_id=package_name,
        database=database,
        schema=schema,
        artifact_repo_fqn=artifact_repo_fqn_str,
        artifact_repo_database=ar_db,
        artifact_repo_schema=ar_schema,
        artifact_repo_name=ar_name,
        build_eai=tgt.build_eai,
        build_job_location=tgt.build_job_location,
        bundle=lambda: perform_bundle(
            dep.package_name,
            SimpleNamespace(artifacts=tgt.bundle_artifacts),
        ),
        run_upload=not build_only and not promote_only,
        run_build=not upload_only and not promote_only,
        upload_only=upload_only,
        build_only=build_only,
        metrics=metrics,
    )
    if uploaded.result is not None:
        return uploaded.result

    # ── Deploy phase (declarative CREATE OR ALTER + inline SPECIFICATION) ──
    # ``url_prefix``, ``health_check``, and ``postgres_instances`` are
    # serverless-only fields, so they are only emitted when ``compute_resource``
    # is ``SERVERLESS``. Whether the server accepts ``postgres_instances`` is
    # decided by the account parameter ENABLE_APP_SERVICE_CNG_POSTGRES.
    is_cng = _is_cng_compute_resource(compute_resource)
    if tgt.postgres_instances and not is_cng:
        raise CliError(
            "postgres_instances is only valid when compute_resource is SERVERLESS."
        )
    specification = manager.build_service_specification(
        tgt,
        database=database,
        schema=schema,
        include_url_prefix=is_cng,
        include_health_check=is_cng,
        include_postgres_instances=is_cng,
    )
    with metrics.span("snowflake_app.deploy_service"):
        cli_console.step(f"Applying application service {service_fqn.identifier}...")
        try:
            with metrics.span("snowflake_app.deploy_service.create_or_alter"):
                manager.create_or_alter_app_service(
                    service_fqn=service_fqn,
                    artifact_repo_fqn=artifact_repo_fqn_str,
                    package_name=package_name,
                    specification=specification,
                    version=sql_version,
                    compute_resource=compute_resource,
                )
        except ProgrammingError as e:
            if _is_health_check_failure(e):
                _raise_health_check_failure(
                    manager,
                    service_fqn,
                    e,
                    expected_version=_expected_live_version(
                        package_version, uploaded.version
                    ),
                )
            _log_service_logs(manager, service_fqn)
            raise CliError(
                "Deployment failed while applying application service "
                f"'{service_fqn.identifier}': {e}. Verify privileges for "
                "CREATE OR ALTER APPLICATION SERVICE plus USAGE on the "
                "warehouse, secrets, and external access integrations "
                "referenced by the target."
            ) from e

    endpoint_url, description = _wait_for_app_yml_endpoint(
        manager, service_fqn, metrics
    )
    return _deploy_ready_result(
        url=endpoint_url,
        service=service_fqn.identifier,
        description=description,
        package=package_name,
        artifact_repository=artifact_repo_fqn_str,
        expected_version=_expected_live_version(package_version, uploaded.version),
        target=dep.target_name,
    )


@_utf8_output
@span("snowflake_app.deploy")
def snowflake_app_deploy(
    entity_id: Optional[str],
    upload_only: bool,
    build_only: bool,
    promote_only: bool,
    interactive: Optional[bool] = None,
    skip_certs_check: bool = False,
    target: Optional[str] = None,
    version: Optional[str] = None,
) -> CommandResult:
    """Build and deploy a Snowflake App Runtime through upload, build, and deploy phases.

    When an ``app.yml`` with ``version >= 2`` is present in the project root the
    deploy resolves the requested ``--target`` (or ``default_target``) from it
    and deploys that target's application service via ``CREATE OR ALTER
    APPLICATION SERVICE`` with an inline ``SPECIFICATION`` — see
    :func:`_deploy_from_app_yml`. Otherwise the existing ``snowflake.yml`` flow
    is used.
    """
    phase_flags = sum((upload_only, build_only, promote_only))
    if phase_flags > 1:
        raise ClickException(
            "Only one of --upload-only, --build-only, or --promote-only "
            "may be specified."
        )
    package_version = _package_version_for_deploy(version, promote_only)

    app_def = _load_app_yml_for_command(target)
    if app_def is not None:
        return _deploy_from_app_yml(
            app_def,
            target=target,
            upload_only=upload_only,
            build_only=build_only,
            promote_only=promote_only,
            interactive=interactive,
            skip_certs_check=skip_certs_check,
            package_version=package_version,
        )

    run_upload = not build_only and not promote_only
    run_build = not upload_only and not promote_only
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    # ── Extract entity configuration ──────────────────────────────────
    fqn = entity.fqn
    app_name = fqn.name

    ctx = get_cli_context()
    metrics = ctx.metrics

    app_title = entity.meta.title if entity.meta else None
    app_description = entity.meta.description if entity.meta else None
    app_icon = entity.meta.icon if entity.meta else None

    # ── Resolve defaults (snowflake.yml > account parameters > built-in) ──
    manager = SnowflakeAppManager(interactive=interactive)
    with metrics.span("snowflake_app.deploy.resolve_defaults"):
        defaults = _resolve_deploy_defaults(entity, manager, app_name=app_name)

    database = defaults["database"]
    schema = defaults["schema"]
    _ensure_app_derived_schema(
        manager,
        database=database,
        schema=schema,
        app_name=app_name,
    )
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
    storage = _entity_code_storage(
        entity, database=database, schema=schema, app_name=app_name
    )

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
    storage_fqn = _storage_fqn(storage, database=database, schema=schema)
    service_fqn = app_fqn(database=database, schema=schema, name=app_name)

    # ── Shared upload + build pipeline ────────────────────────────────
    # Compute pools, the runtime image and the project-type override are
    # entity-only build arguments; everything else is shared with the app.yml
    # flow. ``promote_only`` clears both run flags, so the pipeline is a no-op
    # and control falls straight through to the deploy phase.
    project_type_override = getattr(entity, "spcs_test_project_type", None)
    sql_version = package_version or "LATEST"
    uploaded = _upload_and_build_app(
        manager,
        storage=storage,
        storage_fqn=storage_fqn,
        app_id=app_name,
        database=database,
        schema=schema,
        artifact_repo_fqn=artifact_repo_fqn_str,
        artifact_repo_database=ar_database,
        artifact_repo_schema=ar_schema,
        artifact_repo_name=ar_name,
        build_eai=build_eai,
        # ``build_job_location`` is an app.yml v2-only field; the snowflake.yml
        # entity flow leaves it unset so the builder uses the default (PDB).
        build_job_location=None,
        bundle=lambda: perform_bundle(resolved_entity_id, entity),
        run_upload=run_upload,
        run_build=run_build,
        upload_only=upload_only,
        build_only=build_only,
        metrics=metrics,
        extra_build_kwargs=dict(
            compute_pool=build_compute_pool,
            runtime_image=entity.runtime_image,
            project_type=(
                project_type_override if isinstance(project_type_override, str) else ""
            ),
        ),
    )
    if uploaded.result is not None:
        return uploaded.result

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
                        version=sql_version,
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
                            version=sql_version,
                        )
                except ProgrammingError as upgrade_error:
                    if _is_health_check_failure(upgrade_error):
                        _raise_health_check_failure(
                            manager,
                            service_fqn,
                            upgrade_error,
                            expected_version=_expected_live_version(
                                package_version, uploaded.version
                            ),
                        )
                    _log_service_logs(manager, service_fqn)
                    raise CliError(
                        "Deployment failed while upgrading application service "
                        f"'{service_fqn.identifier}': {upgrade_error}. "
                        "Verify privileges for ALTER APPLICATION SERVICE and access to referenced objects."
                    ) from upgrade_error
                did_upgrade = True
            else:
                if _is_health_check_failure(e):
                    _raise_health_check_failure(
                        manager,
                        service_fqn,
                        e,
                        expected_version=_expected_live_version(
                            package_version, uploaded.version
                        ),
                    )
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
    return _deploy_ready_result(
        url=endpoint_url,
        service=service_fqn.identifier,
        description=desc,
        package=app_name,
        artifact_repository=artifact_repo_fqn_str,
        expected_version=_expected_live_version(package_version, uploaded.version),
    )


@_utf8_output
@span("snowflake_app.teardown")
def snowflake_app_teardown(
    entity_id: Optional[str],
    force: bool,
    target: Optional[str] = None,
) -> CommandResult:
    """Drop a deployed Snowflake App Runtime and its associated objects.

    Uses ``app.yml`` (the ``--target`` target) when present, otherwise the
    ``snowflake.yml`` entity.
    """
    metrics = get_cli_context().metrics
    dep = _resolve_teardown_target(entity_id, target)

    cancelled = _confirm_drop_and_verify(
        dep.manager, dep.service_fqn, force=force, metrics=metrics
    )
    if cancelled is not None:
        return cancelled

    # Clean up the code-storage backend the deploy would have used: a workspace
    # subdirectory (personal databases) or a stage. This tidies up source left
    # behind by an ``--upload-only`` run or an interrupted deploy.
    _teardown_app_code(
        dep.manager,
        storage=dep.storage,
        storage_fqn=dep.storage_fqn,
        app_name=dep.code_app_name,
        metrics=metrics,
    )

    return _app_result(
        f"Successfully dropped application service {dep.service_fqn.identifier}.",
        service=dep.service_fqn.identifier,
    )
