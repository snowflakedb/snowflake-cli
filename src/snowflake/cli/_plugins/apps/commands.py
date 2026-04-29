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

"""Snowflake Apps Deploy (``snowflake-app``) implementation functions.

These functions are called from the unified ``snow app`` command group in
``_plugins/nativeapp/commands.py`` when the detected flow is
:class:`~snowflake.cli._plugins.nativeapp.v2_conversions.compat.AppFlow.SNOWFLAKE_APP`.

They are plain Python functions (no Typer decorators) so they can be
dispatched to from the unified handlers without CLI-framework coupling.
"""

import json
import re
from pathlib import Path
from typing import Optional

import typer
from click import ClickException
from snowflake.cli._plugins.apps.generate import _generate_snowflake_yml
from snowflake.cli._plugins.apps.manager import (
    DEFAULT_PERSONAL_SCHEMA,
    DEFINITION_FILENAME,
    SnowflakeAppManager,
    _get_entity,
    _poll_until,
    _resolve_deploy_defaults,
    _resolve_entity_id,
    perform_bundle,
)
from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.config import get_connection_dict, get_default_connection_name
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
from snowflake.connector.errors import ProgrammingError

# Default number of log lines returned by ``snow app events`` for the
# Snowflake Apps Deploy flow. The unified command accepts ``--last`` with a ``None``
# default; each flow applies its own default when the user does not provide
# a value (Native App uses ``-1``, Snowflake Apps Deploy uses this constant).
DEFAULT_SNOWFLAKE_APP_EVENTS_LAST = 500

# ── Source provenance labels ──────────────────────────────────────────
SOURCE_USER_INPUT = "user input"
SOURCE_ACCOUNT_PARAM = "account parameter"
SOURCE_CURRENT_SESSION = "current session"
SOURCE_DEFAULT = "default"
SOURCE_MISSING = "missing"


def snowflake_app_setup(
    app_name: Optional[str],
    dry_run: bool,
    compute_pool: Optional[str],
    build_eai: Optional[str],
) -> CommandResult:
    """Initialize a ``snowflake.yml`` for a Snowflake Apps Deploy project.

    See the ``snow app setup`` command in
    :mod:`snowflake.cli._plugins.nativeapp.commands` for the CLI surface.
    """
    resolved_app_name = app_name
    if resolved_app_name is None:
        derived_app_name = Path.cwd().name
        # For implicit names, normalize directory strings into a valid
        # identifier by mapping common separators to "_" and stripping
        # all other disallowed characters.
        resolved_app_name = re.sub(
            r"[^a-zA-Z0-9_]", "", derived_app_name.replace(" ", "_").replace("-", "_")
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

    project_file = Path.cwd() / DEFINITION_FILENAME
    if not dry_run and project_file.exists():
        return MessageResult(
            f"{DEFINITION_FILENAME} already exists. Skipping initialization."
        )

    ctx = get_cli_context()
    connection_name = (
        ctx.connection_context.connection_name or get_default_connection_name()
    )
    conn_config = get_connection_dict(connection_name)

    manager = SnowflakeAppManager()
    metrics = ctx.metrics
    with metrics.span("snowflake_app.setup.resolve_defaults"):
        params = manager.fetch_snow_apps_parameters()

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
    session_wh = (
        getattr(conn, "warehouse", None) or conn_config.get("warehouse") or None
    )
    session_db = getattr(conn, "database", None) or conn_config.get("database") or None
    session_schema = getattr(conn, "schema", None) or conn_config.get("schema") or None

    personal_db = manager.get_personal_database()
    personal_schema = DEFAULT_PERSONAL_SCHEMA if personal_db else None

    # ── Resolve each field ────────────────────────────────────────────
    resolved = {
        "database": _resolve(
            account_param=params.get("database"),
            default_value=personal_db,
            current_session=session_db,
        ),
        # TODO: Support per-app schema (e.g. APPS.APP_<app_id>) instead of
        # a single shared schema for all apps.
        "schema": _resolve(
            account_param=params.get("schema"),
            default_value=personal_schema,
            current_session=session_schema,
        ),
        "warehouse": _resolve(
            account_param=params.get("query_warehouse"),
            current_session=session_wh,
        ),
        # TODO: Consider removing --compute-pool argument once services can run
        # in the system default compute pool (SYSTEM_COMPUTE_POOL_CPU).
        "build_compute_pool": _resolve(
            user_input=compute_pool,
            account_param=params.get("build_compute_pool"),
        ),
        "service_compute_pool": _resolve(
            user_input=compute_pool,
            account_param=params.get("service_compute_pool"),
        ),
        # TODO: Remove --build-eai argument once the builder service no longer
        # requires an external access integration.
        "build_eai": _resolve(
            user_input=build_eai,
            account_param=params.get("build_eai"),
        ),
    }

    # ── Validate required values ─────────────────────────────────────
    # TODO: database, warehouse, and schema cannot be passed as arguments
    # yet — they must come from account parameters or the current session.
    if not resolved["database"][0]:
        raise ClickException(
            "Missing database. Set the DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE account parameter or check your connection."
        )
    if not resolved["schema"][0]:
        raise ClickException(
            "Missing schema. Set the DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA account parameter or check your connection."
        )
    if not resolved["warehouse"][0]:
        raise ClickException(
            "Missing warehouse. Set the DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE account parameter or check your connection."
        )

    resolved_values = {k: v[0] for k, v in resolved.items()}

    if not dry_run:
        project_file.write_text(
            _generate_snowflake_yml(resolved_app_name, resolved_values)
        )

    is_json = get_cli_context().output_format.is_json
    if is_json:
        return ObjectResult({"success": not dry_run, **resolved_values})

    if dry_run:
        cli_console.step("Dry run — resolved configuration:")
    else:
        cli_console.step(
            f"Initialized Snowflake Apps Deploy project in {DEFINITION_FILENAME}."
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


def snowflake_app_bundle(entity_id: Optional[str]) -> CommandResult:
    """Bundle a Snowflake Apps Deploy by resolving artifacts defined in ``snowflake.yml``."""
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    project_paths = perform_bundle(resolved_entity_id, entity)
    return MessageResult(f"Bundle generated at {project_paths.bundle_root}")


def snowflake_app_validate(entity_id: Optional[str]) -> CommandResult:
    """Validate a local Snowflake Apps Deploy project."""
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    # ── Validate database and schema ──────────────────────────────────
    fqn = entity.fqn
    database = fqn.database
    schema = fqn.schema

    manager = SnowflakeAppManager()

    if database:
        if not manager.database_exists(database):
            raise CliError(
                f"Database '{database}' does not exist or is not accessible."
            )
        if schema:
            if not manager.schema_exists(database, schema):
                raise CliError(
                    f"Schema '{database}.{schema}' does not exist "
                    f"or is not accessible."
                )

    # ── Validate project can bundle artifacts ─────────────────────────
    project_paths = None
    try:
        project_paths = perform_bundle(resolved_entity_id, entity)
    finally:
        if project_paths is not None:
            project_paths.clean_up_output()
    return MessageResult("Valid Snowflake Apps Deploy project.")


def snowflake_app_open(
    entity_id: Optional[str],
    print_only: bool,
    settings: bool,
) -> CommandResult:
    """Open a deployed Snowflake Apps Deploy (or its settings page) in the browser."""
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    fqn = entity.fqn
    ctx = get_cli_context()

    db = fqn.database or ctx.connection_context.database
    schema = fqn.schema or ctx.connection_context.schema

    if not db or not schema:
        missing = [k for k, v in {"database": db, "schema": schema}.items() if not v]
        raise CliError(
            f"Cannot resolve {' or '.join(missing)} for the app. "
            "Set them in snowflake.yml or in your connection configuration."
        )

    if settings:
        app_id = (
            f"{identifier_for_url(db)}"
            f".{identifier_for_url(schema)}"
            f".{identifier_for_url(fqn.name)}"
        )
        url = make_snowsight_url(ctx.connection, f"#/apps/service/{app_id}/details")
    else:
        service_fqn = FQN(
            database=db,
            schema=schema,
            name=fqn.name,
        )

        manager = SnowflakeAppManager()
        url = manager.get_service_endpoint_url(service_fqn)

        if not url:
            raise CliError(
                f"No endpoint URL found for service {service_fqn}. "
                f"Is the app deployed? Run 'snow app deploy' first."
            )

    if not print_only:
        typer.launch(url)
    return MessageResult(url)


def snowflake_app_events(
    entity_id: Optional[str],
    last: Optional[int],
) -> CommandResult:
    """Fetch recent log events from a deployed Snowflake Apps Deploy."""
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    fqn = entity.fqn
    # Rebuild to a 3-part name; entity FQN may carry extra fields (e.g. prefix)
    service_fqn = FQN(database=fqn.database, schema=fqn.schema, name=fqn.name)

    effective_last = last if last is not None else DEFAULT_SNOWFLAKE_APP_EVENTS_LAST

    manager = SnowflakeAppManager()
    try:
        logs = manager.get_service_logs(service_fqn, last=effective_last)
    except ProgrammingError:
        raise ClickException(
            f"Could not retrieve logs for '{service_fqn.identifier}'. "
            "Verify that the app is deployed and the service is running."
        )
    return MessageResult(logs)


def snowflake_app_deploy(
    entity_id: Optional[str],
    upload_only: bool,
    build_only: bool,
    deploy_only: bool,
) -> CommandResult:
    """Build and deploy a Snowflake Apps Deploy through upload, build, and deploy phases."""
    phase_flags = sum((upload_only, build_only, deploy_only))
    if phase_flags > 1:
        raise ClickException(
            "Only one of --upload-only, --build-only, or --deploy-only "
            "may be specified."
        )

    run_upload = not build_only and not deploy_only
    run_build = not upload_only and not deploy_only
    run_deploy = not upload_only and not build_only
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    # ── Extract entity configuration ──────────────────────────────────
    fqn = entity.fqn
    app_name = fqn.name

    ctx = get_cli_context()
    conn = ctx.connection_context
    database = fqn.database or conn.database
    schema = fqn.schema or conn.schema

    # ── Resolve code storage backend ──────────────────────────────────
    # Workspace is the default; if both code_workspace and code_stage are
    # defined, workspace wins. code_stage remains available for users who
    # explicitly opt into the legacy stage flow.
    use_workspace = entity.code_workspace is not None or entity.code_stage is None
    if use_workspace:
        if entity.code_workspace:
            storage_name = entity.code_workspace.name
            storage_db_override = entity.code_workspace.database
            storage_schema_override = entity.code_workspace.schema_
        else:
            storage_name = f"{app_name}_CODE"
            storage_db_override = None
            storage_schema_override = None
        encryption_type = "SNOWFLAKE_SSE"  # unused in workspace flow
    else:
        storage_name = entity.code_stage.name
        storage_db_override = entity.code_stage.database
        storage_schema_override = entity.code_stage.schema_
        encryption_type = entity.code_stage.encryption_type or "SNOWFLAKE_SSE"

    build_compute_pool = (
        entity.build_compute_pool.name if entity.build_compute_pool else None
    )
    build_eai = entity.build_eai.name if entity.build_eai else None
    service_compute_pool = (
        entity.service_compute_pool.name if entity.service_compute_pool else None
    )
    query_warehouse = entity.query_warehouse

    app_title = entity.meta.title if entity.meta else None
    app_description = entity.meta.description if entity.meta else None
    app_icon = entity.meta.icon if entity.meta else None

    # ── Resolve defaults (snowflake.yml > account parameters > built-in) ──
    manager = SnowflakeAppManager()
    defaults = _resolve_deploy_defaults(entity, manager, app_name=app_name)

    database = defaults["database"]
    schema = defaults["schema"]
    build_compute_pool = defaults["build_compute_pool"]
    service_compute_pool = defaults["service_compute_pool"]
    query_warehouse = defaults["query_warehouse"]
    build_eai = defaults["build_eai"]

    ar_name = defaults["artifact_repository"]
    ar_database = defaults["artifact_repo_database"]
    ar_schema = defaults["artifact_repo_schema"]
    artifact_repo_fqn_str = f"{ar_database}.{ar_schema}.{ar_name}"

    # ── Derived names ─────────────────────────────────────────────────
    # If the code storage was defined as a fully-qualified identifier
    # (e.g. ``DB.SCHEMA.NAME``) use its components; otherwise fall back
    # to the app's resolved database/schema for backwards-compatibility
    # with entities that configure ``code_stage``/``code_workspace`` as a
    # bare name.
    storage_fqn = FQN(
        database=storage_db_override or database,
        schema=storage_schema_override or schema,
        name=storage_name,
    )
    service_fqn = FQN(database=database, schema=schema, name=app_name)
    workspace_source_uri = manager.workspace_subdirectory_uri(storage_fqn, app_name)

    stage_manager = StageManager()

    # ── Upload phase ──────────────────────────────────────────────────

    if run_upload:
        project_paths = perform_bundle(resolved_entity_id, entity)
        try:
            if use_workspace:
                cli_console.step(f"Creating workspace {storage_fqn}")
                manager.create_workspace(storage_fqn)
                cli_console.step(
                    f"Clearing existing workspace files in {workspace_source_uri}/"
                )
                manager.clear_workspace_subdirectory(storage_fqn, app_name)
                cli_console.step(f"Uploading bundled files to {workspace_source_uri}")
                for result in manager.upload_to_workspace(
                    local_root=project_paths.bundle_root,
                    workspace_fqn=storage_fqn,
                    target_subdirectory=app_name,
                    overwrite=True,
                ):
                    cli_console.step(
                        f"  Uploaded {result['source']} -> {result['target']}"
                    )
            else:
                if manager.stage_exists(storage_fqn):
                    cli_console.step(f"Clearing existing stage @{storage_fqn}")
                    manager.clear_stage(storage_fqn)
                else:
                    cli_console.step(f"Creating stage @{storage_fqn}")
                    manager.create_stage(storage_fqn, encryption_type)

                cli_console.step(f"Uploading bundled files to @{storage_fqn}")
                for result in stage_manager.put_recursive(
                    local_path=project_paths.bundle_root,
                    stage_path=f"@{storage_fqn}",
                    overwrite=True,
                    auto_compress=False,
                    temp_directory=project_paths.bundle_root,
                ):
                    cli_console.step(
                        f"  Uploaded {result['source']} -> {result['target']}"
                    )
        finally:
            project_paths.clean_up_output()

    if upload_only:
        if use_workspace:
            return MessageResult(f"Artifacts uploaded to {workspace_source_uri}")
        return MessageResult(f"Artifacts uploaded to @{storage_fqn}")

    # ── Build phase ───────────────────────────────────────────────────

    if run_build:
        if not manager.artifact_repo_exists(
            database=ar_database, schema=ar_schema, repo_name=ar_name
        ):
            cli_console.step(f"Creating artifact repository: {artifact_repo_fqn_str}")
            manager.create_artifact_repo(
                database=ar_database, schema=ar_schema, repo_name=ar_name
            )

        cli_console.step("Building app using artifact repository...")
        build_kwargs: dict = dict(
            artifact_repo_fqn=artifact_repo_fqn_str,
            app_id=app_name,
            compute_pool=build_compute_pool,
            database=database,
            schema=schema,
            runtime_image=entity.runtime_image,
            build_eai=build_eai,
        )
        if use_workspace:
            build_kwargs["source_uri"] = workspace_source_uri
        else:
            build_kwargs["stage_fqn"] = storage_fqn
        build_result = manager.build_app_artifact_repo(**build_kwargs)
        cli_console.step(f"SPCS_TEST_BUILD_APP_ARTIFACT_REPO output:\n{build_result}")

        match = re.search(r"Build job submitted:\s*(\S+)", build_result)
        if not match:
            raise CliError(
                f"Could not parse build job name from output: {build_result}"
            )
        artifact_build_job_fqn = FQN.from_string(match.group(1))
        cli_console.step(
            f"Waiting for artifact repo build to complete: "
            f"{artifact_build_job_fqn}..."
        )
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

    eai_list = [build_eai] if build_eai else None

    did_upgrade = False
    cli_console.step("Creating application service...")
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
        if e.errno == 2002 and "already exists" in str(e).lower():
            cli_console.step(
                f"Application service {app_name} already exists. Upgrading..."
            )
            manager.upgrade_app_service(
                service_fqn=service_fqn,
                version="LATEST",
            )
            did_upgrade = True
        else:
            raise

    def _svc_is_upgrading(d: dict) -> bool:
        return str(d.get("is_upgrading", "")).lower() in ("true", "1", "yes")

    def _svc_has_failed(d: dict) -> bool:
        return d.get("status", "").upper() == "FAILED"

    def _url_is_ready(d: dict) -> bool:
        url = d.get("url", "")
        return bool(url) and "provisioning in progress" not in url.lower()

    if did_upgrade:
        cli_console.step("Waiting for upgrade to complete...")
        desc = _poll_until(
            poll_fn=lambda: manager.describe_app_service(service_fqn),
            is_done=lambda d: not _svc_is_upgrading(d) and _url_is_ready(d),
            is_error=_svc_has_failed,
            format_status=lambda d: ("upgrading" if _svc_is_upgrading(d) else "ready"),
            timeout_message=(
                f"Upgrade timed out. Check logs:\n"
                f"  CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS('{app_name}')"
            ),
        )
    else:
        cli_console.step("Waiting for application service endpoint...")
        desc = _poll_until(
            poll_fn=lambda: manager.describe_app_service(service_fqn),
            is_done=_url_is_ready,
            is_error=_svc_has_failed,
            format_status=lambda d: d.get("url") or "url not yet available",
            timeout_message=(
                f"Endpoint provisioning timed out. "
                f"Check: DESCRIBE APPLICATION SERVICE {service_fqn.identifier}"
            ),
        )

    endpoint_url = desc.get("url", "")
    if endpoint_url and not endpoint_url.startswith(("http://", "https://")):
        endpoint_url = f"https://{endpoint_url}"
    return MessageResult(f"App ready at {endpoint_url}")


def snowflake_app_teardown(
    entity_id: Optional[str],
    force: bool,
) -> CommandResult:
    """Drop a deployed Snowflake Apps Deploy and its associated objects."""
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    fqn = entity.fqn
    manager = SnowflakeAppManager()
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
    service_fqn = FQN(database=db, schema=schema, name=app_name)
    use_artifact_repo = entity.artifact_repository is not None

    use_workspace = entity.code_workspace is not None or entity.code_stage is None
    if use_workspace:
        if entity.code_workspace:
            storage_name = entity.code_workspace.name
            storage_db = entity.code_workspace.database or db
            storage_schema = entity.code_workspace.schema_ or schema
        else:
            storage_name = f"{app_name}_CODE"
            storage_db, storage_schema = db, schema
    else:
        storage_name = entity.code_stage.name
        storage_db = entity.code_stage.database or db
        storage_schema = entity.code_stage.schema_ or schema

    storage_fqn = FQN(database=storage_db, schema=storage_schema, name=storage_name)
    build_job_fqn = FQN(database=db, schema=schema, name=f"{app_name}_BUILD_JOB")

    object_kind = "application service" if use_artifact_repo else "service"

    if not force:
        should_continue = typer.confirm(
            f"Are you sure you want to drop {object_kind} {service_fqn.identifier}"
            f" and its associated objects?"
        )
        if not should_continue:
            return MessageResult("Teardown cancelled.")

    cli_console.step(f"Dropping {object_kind} {service_fqn.identifier}")
    if use_artifact_repo:
        manager.drop_app_service_if_exists(service_fqn)
    else:
        manager.drop_service_if_exists(service_fqn)

    if use_workspace:
        cli_console.step(f"Dropping workspace {storage_fqn.identifier}")
        manager.drop_workspace_if_exists(storage_fqn)
    else:
        cli_console.step(f"Dropping stage {storage_fqn.identifier}")
        manager.drop_stage_if_exists(storage_fqn)

    if not use_artifact_repo:
        cli_console.step(f"Dropping build job service {build_job_fqn.identifier}")
        manager.drop_service_if_exists(build_job_fqn)

    return MessageResult(
        f"Successfully dropped {object_kind} {service_fqn.identifier}."
    )
