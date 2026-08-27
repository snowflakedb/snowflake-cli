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
from types import SimpleNamespace
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterator,
    Literal,
    NamedTuple,
    Optional,
)

import typer
from click import ClickException
from snowflake.cli._plugins.apps.app_yml import (
    APP_YML_FILENAME,
    AppYmlDefinition,
    AppYmlTarget,
    load_app_yml,
    resolve_target,
)
from snowflake.cli._plugins.apps.events import (
    EventStream,
    MetricCategory,
    format_log_lines,
    parse_event_stream,
    parse_lifecycle_records,
    parse_metric_category,
    parse_metric_records,
    resolve_time_window,
)
from snowflake.cli._plugins.apps.generate import _generate_app_yml
from snowflake.cli._plugins.apps.manager import (
    DEFAULT_PERSONAL_SCHEMA,
    DEFINITION_FILENAME,
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
from snowflake.cli.api.errno import INSUFFICIENT_PRIVILEGES
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    EmptyResult,
    MessageResult,
    ObjectResult,
)
from snowflake.cli.api.project.util import identifier_for_url
from snowflake.cli.api.sanitizers import sanitize_for_terminal
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
) -> Optional[CommandResult]:
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

    Returns a short-circuit :class:`CommandResult` for ``--upload-only`` /
    ``--build-only``, or ``None`` when the caller should proceed to its own
    (flow-specific) deploy phase.
    """
    use_workspace = storage.type == "workspace"
    stage_created = False

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
            return MessageResult(
                "Artifacts uploaded to "
                f"{manager.workspace_subdirectory_uri(storage_fqn, app_id)}"
            )
        return MessageResult(f"Artifacts uploaded to @{storage_fqn}")

    # ── Build phase ───────────────────────────────────────────────────
    if run_build:
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

    if build_only:
        return MessageResult("Build completed successfully.")

    return None


@_utf8_output
def snowflake_app_setup(
    app_name: Optional[str],
    dry_run: bool,
    compute_pool: Optional[str],
    build_eai: Optional[str],
) -> CommandResult:
    """Initialize a Snowflake App Runtime project manifest.

    Writes an ``app.yml`` — the v2 manifest the ``snow app`` commands read
    instead of ``snowflake.yml`` — so new projects are always initialized as v2.
    An already-initialized project is left untouched: initialization is skipped
    when either manifest is present. See the ``snow app setup`` command in
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
                        return MessageResult(
                            f"{existing} already exists. Skipping initialization."
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

            if not dry_run:
                with metrics.span("snowflake_app.setup.write_manifest"):
                    project_file.write_text(
                        _generate_app_yml(
                            resolved_app_name,
                            resolved_values,
                            use_workspace=use_workspace,
                        ),
                        encoding=encoding,
                    )

            is_json = get_cli_context().output_format.is_json
            if is_json:
                return ObjectResult(
                    {
                        "success": not dry_run,
                        "code_storage": code_storage,
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
        return MessageResult(f"Bundle generated at {project_paths.bundle_root}")

    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    project_paths = perform_bundle(resolved_entity_id, entity)
    return MessageResult(f"Bundle generated at {project_paths.bundle_root}")


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
    return MessageResult("Valid Snowflake App Runtime project.")


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
    manager = SnowflakeAppManager()
    app_def = _load_app_yml_for_command(target)
    if app_def is not None:
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
        return MessageResult(logs)

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
    return MessageResult("\n".join(lines))


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
    """Return ``True`` for the CNG (serverless) app-service backend."""
    return (compute_resource or "").upper() == SERVERLESS_COMPUTE_RESOURCE


def _ensure_cng_url_cert_ready(
    manager: SnowflakeAppManager, *, provision: bool, required: bool
) -> None:
    """Pre-check that the account's per-account URL certificate is in place.

    CNG (serverless) apps serve from per-account URLs backed by a per-account
    TLS certificate whose issuance can take up to ~3 hours — far too long to
    happen inside ``CREATE APPLICATION SERVICE`` — so this probes for it up front
    (never polling) via a client-side TLS probe.

    The caller gates this on the app being CNG, which also implies the feature
    flag is on (``compute_resource`` stays ``None`` while it is off), so the flag
    is not re-checked here.

    ``required`` says whether a missing certificate is fatal for the current
    phase: only the deploy phase creates the service, so only it passes
    ``required=True``. ``--upload-only`` / ``--build-only`` still probe (early,
    cheap diagnosis) but only warn, so the user can upload/build now and promote
    once issuance completes. An inconclusive probe (``UNKNOWN``) or an
    underivable host never blocks — a false negative must not prevent a deploy.
    """
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

    # NOT_PROVISIONED.
    if provision:
        cli_console.step("Starting per-account URL certificate provisioning...")
        manager.issue_per_account_url_cert()
        message = (
            "This account does not yet have a per-account URL certificate, "
            "which CNG (serverless) apps require. Provisioning has been started "
            f"for you via {PER_ACCOUNT_CERT_ISSUE_FUNCTION}(). This can take up "
            "to 3 hours. Re-run 'snow app deploy' once provisioning completes."
        )
    else:
        message = (
            "This account does not yet have a per-account URL certificate, which "
            "CNG (serverless) apps require. Start provisioning by running:\n"
            f"  SELECT {PER_ACCOUNT_CERT_ISSUE_FUNCTION}();\n"
            "Provisioning can take up to 3 hours. Re-run 'snow app deploy' once "
            "it completes, or re-run with '--provision-certs' to start it "
            "automatically."
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
    gates on the CNG feature flag (no CNG app can exist while it is off) and
    leans on ``per_account_cert_status_for_url`` returning ``UNKNOWN`` for
    non-per-account hosts (e.g. SPCS ``snowflakecomputing.app``), so an SPCS
    app's TLS state is never misattributed to a per-account cert. Any probe error
    is swallowed so ``open`` never breaks on this advisory.
    """
    if not FeatureFlag.ENABLE_APP_SERVICE_COMPUTE_RESOURCE.is_enabled():
        return
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
    valid. Deploy and teardown need the fuller deploy-defaults resolution
    (compute pools, artifact repo, code storage) and resolve their target
    separately.
    """
    manager = SnowflakeAppManager()
    app_def = _load_app_yml_for_command(target)
    if app_def is not None:
        dep = _resolve_app_yml_target(app_def, target, manager=manager)
        return _ResolvedService(
            manager, dep.service_fqn, dep.database, dep.schema, dep.service_name
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
    return MessageResult(url)


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
            return MessageResult("Teardown cancelled.")

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
    # Resolve db/schema from the active connection in place on the shared
    # entity.fqn (also expands USER$ → USER$<user>); downstream re-reads of
    # entity.fqn intentionally see the resolved value.
    fqn = entity.fqn
    fqn.using_context()
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
) -> str:
    """Poll a just-applied application service until its endpoint is ready."""

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
    return url


def _deploy_from_app_yml(
    app_def: AppYmlDefinition,
    *,
    target: Optional[str],
    upload_only: bool,
    build_only: bool,
    promote_only: bool,
    interactive: Optional[bool],
    provision_certs: bool = False,
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
    deploy phase against the already-built package (``VERSION LATEST``).
    """
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

    if promote_only:
        cli_console.step(
            "Promoting the latest built package (skipping upload and build)."
        )

    # ``compute_resource`` selects the CNG (serverless) or SPCS backend and is
    # write-once. CNG is not ready yet, so it is only honoured while the feature
    # flag is on; when off it is ignored and the server defaults the backend.
    compute_resource: Optional[str] = None
    if FeatureFlag.ENABLE_APP_SERVICE_COMPUTE_RESOURCE.is_enabled():
        compute_resource = tgt.compute_resource

    # Probe for the per-account URL certificate up front (see
    # _ensure_cng_url_cert_ready): it needs no built artifact, and issuance is
    # far too slow to happen inside CREATE OR ALTER APPLICATION SERVICE.
    if _is_cng_compute_resource(compute_resource):
        with metrics.span("snowflake_app.deploy.cng_cert_precheck"):
            _ensure_cng_url_cert_ready(
                manager,
                provision=provision_certs,
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
    result = _upload_and_build_app(
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
    if result is not None:
        return result

    # ── Deploy phase (declarative CREATE OR ALTER + inline SPECIFICATION) ──
    # ``url_prefix`` and ``health_check`` are CNG-only fields, so they are only
    # emitted on the CNG (serverless) path, which already requires the feature
    # flag (compute_resource stays None while it is off).
    is_cng = _is_cng_compute_resource(compute_resource)
    specification = manager.build_service_specification(
        tgt,
        database=database,
        schema=schema,
        include_url_prefix=is_cng,
        include_health_check=is_cng,
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
                    version="LATEST",
                    compute_resource=compute_resource,
                )
        except ProgrammingError as e:
            _log_service_logs(manager, service_fqn)
            raise CliError(
                "Deployment failed while applying application service "
                f"'{service_fqn.identifier}': {e}. Verify privileges for "
                "CREATE OR ALTER APPLICATION SERVICE plus USAGE on the "
                "warehouse, secrets, and external access integrations "
                "referenced by the target."
            ) from e

    endpoint_url = _wait_for_app_yml_endpoint(manager, service_fqn, metrics)
    return MessageResult(f"App ready at {endpoint_url}")


@_utf8_output
@span("snowflake_app.deploy")
def snowflake_app_deploy(
    entity_id: Optional[str],
    upload_only: bool,
    build_only: bool,
    promote_only: bool,
    interactive: Optional[bool] = None,
    provision_certs: bool = False,
    target: Optional[str] = None,
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

    app_def = _load_app_yml_for_command(target)
    if app_def is not None:
        return _deploy_from_app_yml(
            app_def,
            target=target,
            upload_only=upload_only,
            build_only=build_only,
            promote_only=promote_only,
            interactive=interactive,
            provision_certs=provision_certs,
        )

    run_upload = not build_only and not promote_only
    run_build = not upload_only and not promote_only
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    # ── Extract entity configuration ──────────────────────────────────
    # Resolve db/schema from the active connection in place on the shared
    # entity.fqn (also expands USER$ → USER$<user>); downstream re-reads of
    # entity.fqn (e.g. perform_bundle) intentionally see the resolved value.
    fqn = entity.fqn
    fqn.using_context()
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
    result = _upload_and_build_app(
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
    if result is not None:
        return result

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

    return MessageResult(
        f"Successfully dropped application service {dep.service_fqn.identifier}."
    )
