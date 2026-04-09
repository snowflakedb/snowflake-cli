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

import json
import re
from pathlib import Path
from typing import Optional

import typer
from click import ClickException
from snowflake.cli._plugins.apps.generate import (
    IS_PERSONAL_DB_SUPPORTED,
    _generate_snowflake_yml,
)
from snowflake.cli._plugins.apps.manager import (
    _APP_COMMAND_NAME,
    DEFINITION_FILENAME,
    EXPOSE_UNSUPPORTED_SYNTAX,
    SnowflakeAppManager,
    _find_dockerfile_expose_port,
    _get_entity,
    _poll_until,
    _resolve_deploy_defaults,
    _resolve_entity_id,
    perform_bundle,
)
from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.config import get_connection_dict, get_default_connection_name
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CommandResult,
    EmptyResult,
    MessageResult,
    ObjectResult,
)
from snowflake.cli.api.project.util import get_env_username, identifier_for_url
from snowflake.connector.errors import ProgrammingError

app = SnowTyperFactory(
    name=_APP_COMMAND_NAME,
    help="Manages Snowflake Apps.",
    is_hidden=FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_disabled,
)


@app.command("setup", requires_connection=True)
def setup(
    app_name: str = typer.Option(
        ...,
        "--app-name",
        help="Name of the Snowflake App to initialize.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Only print the resolved configuration values without writing snowflake.yml.",
    ),
    compute_pool: Optional[str] = typer.Option(
        None,
        "--compute-pool",
        help="Compute pool for building and running the app.",
    ),
    build_eai: Optional[str] = typer.Option(
        None,
        "--build-eai",
        help="External access integration used during the app build.",
    ),
    **options,
) -> CommandResult:
    """
    Initializes a snowflake.yml file for a Snowflake App project.
    """

    if not re.fullmatch(r"[a-zA-Z0-9_]+", app_name):
        raise ClickException(
            f"Invalid app name '{app_name}'. "
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
    config_table = {}
    role = manager.current_role()
    if role:
        config_table = manager.fetch_config_table_defaults(role)

    def _resolve(flag_val, config_key, conn_key=None, builtin=None):
        """Return (value, source) using: flag > account default > connection config > builtin."""
        if flag_val is not None:
            return flag_val, "flag"
        table_val = config_table.get(config_key)
        if table_val:
            return table_val, "account default"
        if conn_key is not None:
            # ctx.connection_context is only populated (without update_from_config) when
            # the user explicitly passed the flag on the CLI, so this signals "flag" provenance.
            ctx_val = getattr(ctx.connection_context, conn_key, None)
            if ctx_val:
                return ctx_val, "flag"
            conn_val = conn_config.get(conn_key)
            if conn_val:
                return conn_val, "connection config"
        if builtin is not None:
            return builtin, "default"
        return None, "missing"

    if IS_PERSONAL_DB_SUPPORTED:
        database_resolved = (f"USER${get_env_username().upper()}", "personal db")
    else:
        database_resolved = _resolve(None, "database", conn_key="database")

    resolved = {
        "database": database_resolved,
        "schema": _resolve(None, "schema", conn_key="schema"),
        "warehouse": _resolve(None, "warehouse", conn_key="warehouse"),
        "compute_pool": _resolve(compute_pool, "compute_pool"),
        "build_eai": _resolve(build_eai, "eai"),
    }

    img_repo = _resolve(None, "image_repository")
    if img_repo[0]:
        resolved["image_repository"] = img_repo

    # Validate: fail on ALL missing required values at once
    required_keys = ["database", "warehouse", "compute_pool", "build_eai"]
    missing = [key for key in required_keys if not resolved[key][0]]
    if missing:
        flag_map = {
            "database": "--database",
            "warehouse": "--warehouse",
            "compute_pool": "--compute-pool",
            "build_eai": "--build-eai",
        }
        flags = ", ".join(flag_map[k] for k in missing)
        raise ClickException(
            f"Missing required value(s): {', '.join(missing)}. "
            f"Pass them using: {flags}"
        )

    resolved_values = {k: v[0] for k, v in resolved.items()}

    if not dry_run:
        project_file.write_text(_generate_snowflake_yml(app_name, resolved_values))

    is_json = get_cli_context().output_format.is_json
    if is_json:
        return ObjectResult({"success": not dry_run, **resolved_values})

    if dry_run:
        cli_console.step("Dry run — resolved configuration:")
    else:
        cli_console.step(f"Initialized Snowflake App project in {DEFINITION_FILENAME}.")
    for key, (value, source) in resolved.items():
        cli_console.step(f"  {key}: {value}  ({source})")
    return EmptyResult()


@app.command()
def bundle(
    entity_id: Optional[str] = typer.Option(
        None,
        "--entity-id",
        help="ID of the snowflake-app entity to bundle. Required if multiple snowflake-app entities exist.",
    ),
    **options,
) -> CommandResult:
    """
    Bundles a Snowflake App by resolving artifacts defined in snowflake.yml.

    Copies (or symlinks) the matched source files into a local output directory
    (output/bundle) so you can inspect exactly what would be uploaded during deploy.
    """
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    project_paths = perform_bundle(resolved_entity_id, entity)
    return MessageResult(f"Bundle generated at {project_paths.bundle_root}")


@app.command(requires_connection=True)
def validate(
    entity_id: Optional[str] = typer.Option(
        None,
        "--entity-id",
        help="ID of the snowflake-app entity to validate. Required if multiple snowflake-app entities exist.",
    ),
    **options,
) -> CommandResult:
    """
    Validates a local Snowflake App project.

    Bundles the project, checks that a Dockerfile with an EXPOSE directive
    exists, and verifies that the current role has the BIND SERVICE ENDPOINT
    privilege required for deployment.
    """
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    warnings: list[str] = []

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

    # ── Validate bundle / Dockerfile ──────────────────────────────────
    project_paths = None
    try:
        project_paths = perform_bundle(resolved_entity_id, entity)

        # Validate Dockerfile has an EXPOSE directive
        bundle_root = project_paths.bundle_root
        dockerfile = bundle_root / "Dockerfile"
        if not dockerfile.exists():
            raise CliError(
                f"No Dockerfile found in bundled artifacts. "
                f"A Dockerfile is required for Snowflake App projects."
            )

        exposed_port = _find_dockerfile_expose_port(bundle_root)
        if exposed_port is None:
            warnings.append(
                "Dockerfile does not contain a recognized EXPOSE directive. "
                "The Dockerfile must expose a port for the app service."
            )
        elif exposed_port == EXPOSE_UNSUPPORTED_SYNTAX:
            warnings.append(
                "Could not determine the exposed port from the Dockerfile. "
                "Only simple 'EXPOSE <port>' is supported "
                "(multi-port and range syntax are not)."
            )
        elif exposed_port != entity.app_port:
            warnings.append(
                f"Dockerfile exposes port {exposed_port}, but the entity "
                f"'app_port' is configured as {entity.app_port}. "
                f"These should match for the service endpoint to work correctly."
            )
    finally:
        if project_paths is not None:
            project_paths.clean_up_output()

    for warning in warnings:
        cli_console.warning(warning)

    if warnings:
        return MessageResult(f"Validation passed with {len(warnings)} warning(s).")
    return MessageResult("Valid Snowflake App project.")


@app.command("open", requires_connection=True)
def open_app(
    entity_id: Optional[str] = typer.Option(
        None,
        "--entity-id",
        help="ID of the snowflake-app entity to open. Required if multiple snowflake-app entities exist.",
    ),
    print_only: bool = typer.Option(
        False,
        "--print-only",
        help="Print the app URL without opening it in the browser.",
    ),
    settings: bool = typer.Option(
        False,
        "--settings",
        help="Open the app settings page in Snowsight instead of the app itself.",
    ),
    **options,
) -> CommandResult:
    """
    Opens a deployed Snowflake App in the browser.

    Looks up the service endpoint URL for the app and opens it.  Use
    --print-only to print the URL without launching a browser.
    Use --settings to open the Snowsight settings page for the app.
    """
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
                f"Is the app deployed? Run 'snow {_APP_COMMAND_NAME} deploy' first."
            )

    if not print_only:
        typer.launch(url)
    return MessageResult(url)


@app.command(requires_connection=True)
def events(
    entity_id: Optional[str] = typer.Option(
        None,
        "--entity-id",
        help="ID of the snowflake-app entity. Required if multiple snowflake-app entities exist.",
    ),
    last: int = typer.Option(
        500,
        "--last",
        help="Number of log lines to retrieve. Default: 500. Note: output is capped at 100KB.",
    ),
    **options,
) -> CommandResult:
    """
    Fetches the recent log events from a deployed Snowflake App.
    Output is capped at 100KB regardless of the number of lines requested.
    """
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    fqn = entity.fqn
    # Rebuild to a 3-part name; entity FQN may carry extra fields (e.g. prefix)
    service_fqn = FQN(database=fqn.database, schema=fqn.schema, name=fqn.name)

    manager = SnowflakeAppManager()
    try:
        logs = manager.get_service_logs(service_fqn, last=last)
    except ProgrammingError:
        raise ClickException(
            f"Could not retrieve logs for '{service_fqn.identifier}'. "
            "Verify that the app is deployed and the service is running."
        )
    return MessageResult(logs)


@app.command(requires_connection=True)
def deploy(
    entity_id: Optional[str] = typer.Option(
        None,
        "--entity-id",
        help="ID of the snowflake-app entity to deploy. Required if multiple snowflake-app entities exist.",
    ),
    upload_only: bool = typer.Option(
        False,
        "--upload-only",
        help="Bundle and upload source artifacts to the stage, then stop. "
        "Skips the build and deploy phases.",
    ),
    build_only: bool = typer.Option(
        False,
        "--build-only",
        help="Run only the build phase (assumes artifacts have already been uploaded). "
        "Skips the upload and deploy phases.",
    ),
    deploy_only: bool = typer.Option(
        False,
        "--deploy-only",
        help="Run only the deploy phase (assumes the container image has already been built). "
        "Skips the upload and build phases.",
    ),
    **options,
) -> CommandResult:
    """
    Builds and deploys a Snowflake App.

    The deploy pipeline has three phases: upload, build, and deploy.
    By default all three phases run in sequence. Use --upload-only,
    --build-only, or --deploy-only to run a single phase.

    If --entity-id is not specified and the project contains exactly one snowflake-app
    entity, that entity will be used automatically.
    """
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

    if entity.code_stage:
        stage_name = entity.code_stage.name
        encryption_type = entity.code_stage.encryption_type or "SNOWFLAKE_SSE"
    else:
        stage_name = f"{app_name}_CODE_STAGE"
        encryption_type = "SNOWFLAKE_SSE"

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

    # ── Resolve defaults (snowflake.yml > config table > built-in) ────
    manager = SnowflakeAppManager()
    defaults = _resolve_deploy_defaults(entity, manager)

    database = defaults["database"]
    schema = defaults["schema"]
    build_compute_pool = defaults["build_compute_pool"]
    service_compute_pool = defaults["service_compute_pool"]
    query_warehouse = defaults["query_warehouse"]
    build_eai = defaults["build_eai"]

    image_repository = defaults["image_repository"]
    image_repo_database = defaults.get("image_repo_database") or database
    image_repo_schema = defaults.get("image_repo_schema") or schema

    use_artifact_repo = entity.artifact_repository is not None
    if use_artifact_repo:
        ar = entity.artifact_repository
        ar_database = ar.database or database
        ar_schema = ar.schema_ or schema
        artifact_repo_fqn_str = f"{ar_database}.{ar_schema}.{ar.name}"

    # ── Validate required configuration ───────────────────────────────
    if run_build and not build_compute_pool:
        raise CliError(
            "build_compute_pool is required for the build phase. "
            "Please configure it in snowflake.yml."
        )

    if run_deploy and not service_compute_pool:
        raise CliError(
            "service_compute_pool is required for the deploy phase. "
            "Please configure it in snowflake.yml."
        )

    if run_deploy and not use_artifact_repo and not query_warehouse:
        raise CliError(
            "query_warehouse is required for the deploy phase. "
            "Please configure it in snowflake.yml."
        )

    # ── Derived names ─────────────────────────────────────────────────
    stage_fqn = FQN(database=database, schema=schema, name=stage_name)
    build_job_service_name = f"{app_name}_BUILD_JOB"
    build_job_fqn = FQN(database=database, schema=schema, name=build_job_service_name)
    service_fqn = FQN(database=database, schema=schema, name=app_name)

    stage_manager = StageManager()
    image_repo_url = None

    if (run_build or run_deploy) and not use_artifact_repo:
        cli_console.step(f"Getting image repository URL for {image_repository}")
        image_repo_url = manager.get_image_repo_url(
            image_repository, database=image_repo_database, schema=image_repo_schema
        )

    # ── Upload phase ──────────────────────────────────────────────────

    if run_upload:
        if manager.stage_exists(stage_fqn):
            cli_console.step(f"Clearing existing stage @{stage_fqn}")
            manager.clear_stage(stage_fqn)
        else:
            cli_console.step(f"Creating stage @{stage_fqn}")
            manager.create_stage(stage_fqn, encryption_type)

        project_paths = perform_bundle(resolved_entity_id, entity)

        try:
            cli_console.step(f"Uploading bundled files to @{stage_fqn}")
            for result in stage_manager.put_recursive(
                local_path=project_paths.bundle_root,
                stage_path=f"@{stage_fqn}",
                overwrite=True,
                auto_compress=False,
                temp_directory=project_paths.bundle_root,
            ):
                cli_console.step(f"  Uploaded {result['source']} -> {result['target']}")
        finally:
            project_paths.clean_up_output()

    if upload_only:
        return MessageResult(f"Artifacts uploaded to @{stage_fqn}")

    # ── Build phase ───────────────────────────────────────────────────

    if run_build:
        if use_artifact_repo:
            cli_console.step("Building app using artifact repository...")
            build_result = manager.build_app_artifact_repo(
                stage_fqn=stage_fqn,
                artifact_repo_fqn=artifact_repo_fqn_str,
                app_id=app_name,
                compute_pool=build_compute_pool,
                database=database,
                schema=schema,
                runtime_image=entity.runtime_image,
                query_warehouse=query_warehouse,
                build_eai=build_eai,
            )
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
        else:
            cli_console.step(f"Dropping service if exists: {build_job_fqn}")
            manager.drop_service_if_exists(build_job_fqn)

            cli_console.step(f"Executing build job service: {build_job_fqn}")
            manager.execute_build_job(
                job_service_name=build_job_fqn,
                compute_pool=build_compute_pool,
                code_stage=stage_fqn,
                image_repo_url=image_repo_url,
                app_id=app_name,
                external_access_integration=build_eai,
                build_image=entity.build_image,
            )

            cli_console.step("Waiting for build to complete...")
            _poll_until(
                poll_fn=lambda: manager.get_build_status(build_job_fqn),
                done_states={"DONE"},
                error_states={"FAILED", "IDLE"},
                known_pending_states={"PENDING", "RUNNING"},
                timeout_message=f"Build timed out. Check service logs: {build_job_fqn}",
            )

    if build_only:
        return MessageResult("Build completed successfully.")

    # ── Deploy phase ──────────────────────────────────────────────────

    if use_artifact_repo:
        cli_console.step("Deploying app using artifact repository...")
        run_result = manager.run_app_artifact_repo(
            artifact_repo_fqn=artifact_repo_fqn_str,
            app_id=app_name,
            version="LATEST",
            service_name=app_name,
            compute_pool=service_compute_pool,
            database=database,
            schema=schema,
            runtime_image=entity.runtime_image,
            query_warehouse=query_warehouse,
            build_eai=build_eai,
        )
        cli_console.step(f"SPCS_TEST_RUN_APP_ARTIFACT_REPO output:\n{run_result}")
    else:
        assert image_repo_url is not None
        repo_path = "/" + "/".join(image_repo_url.split("/")[1:])
        image_url = f"{repo_path}/{app_name.lower()}:latest"

        comment_data = {"appId": app_name}
        if app_title:
            comment_data["appName"] = app_title
        if app_description:
            comment_data["appDescription"] = app_description
        if app_icon:
            comment_data["appIcon"] = app_icon
        app_comment = json.dumps(comment_data)

        cli_console.step(f"Creating service {service_fqn} if it doesn't exist")
        manager.create_service(
            service_name=service_fqn,
            compute_pool=service_compute_pool,
            query_warehouse=query_warehouse,
            app_comment=app_comment,
            execute_as_caller=entity.execute_as_caller,
        )

        cli_console.step(f"Updating service with image: {image_url}")
        manager.alter_service_spec(
            service_name=service_fqn,
            image_url=image_url,
            execute_as_caller=entity.execute_as_caller,
        )

        cli_console.step("Resuming service")
        manager.resume_service(service_fqn)

        cli_console.step("Waiting for service to be ready...")
        _poll_until(
            poll_fn=lambda: manager.get_service_status(service_fqn),
            done_states={"RUNNING"},
            error_states={"FAILED", "IDLE"},
            known_pending_states={"PENDING", "SUSPENDING", "SUSPENDED"},
            timeout_message=f"Service timed out. Check service status: {service_fqn}",
        )

    # ── Get endpoint URL ──────────────────────────────────────────────
    cli_console.step("Getting endpoint URL")
    ep_name = "web" if use_artifact_repo else "app-endpoint"
    endpoint_url = _poll_until(
        poll_fn=lambda: manager.get_service_endpoint_url(
            service_fqn, endpoint_name=ep_name
        ),
        is_done=lambda url: url is not None
        and "provisioning in progress" not in url.lower(),
        format_status=lambda url: url or "Endpoint URL not yet available",
        timeout_message=(
            f"Endpoint provisioning timed out. "
            f'Check with: snow sql -q "SHOW ENDPOINTS IN SERVICE {service_fqn}"'
        ),
    )
    return MessageResult(f"App ready at {endpoint_url}")
