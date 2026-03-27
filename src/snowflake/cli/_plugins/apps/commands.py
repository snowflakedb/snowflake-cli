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
from pathlib import Path
from typing import Optional

import typer
from snowflake.cli._plugins.apps.generate import _generate_snowflake_yml
from snowflake.cli._plugins.apps.manager import (
    _APP_COMMAND_NAME,
    APP_DEFAULTS_TABLE,
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
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import CommandResult, MessageResult

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
    **options,
) -> CommandResult:
    """
    Initializes a snowflake.yml file for a Snowflake App project.
    """

    project_file = Path.cwd() / DEFINITION_FILENAME
    if project_file.exists():
        return MessageResult(
            f"{DEFINITION_FILENAME} already exists. Skipping initialization."
        )

    # Get connection context for username and warehouse
    ctx = get_cli_context()
    ctx.connection_context.validate_and_complete()
    ctx.connection_context.update_from_config()
    warehouse = ctx.connection_context.warehouse
    database = ctx.connection_context.database

    project_file.write_text(_generate_snowflake_yml(app_name, warehouse, database))
    return MessageResult(f"Initialized Snowflake App project in {DEFINITION_FILENAME}.")


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

    # Check BIND SERVICE ENDPOINT privilege
    manager = SnowflakeAppManager()
    role = manager.current_role()
    if role and role.upper() != "ACCOUNTADMIN":
        if not manager.role_has_bind_service_endpoint(role):
            warnings.append(
                f"Role '{role}' does not have the BIND SERVICE ENDPOINT "
                f"privilege. This privilege is required to deploy a Snowflake App."
            )

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
    **options,
) -> CommandResult:
    """
    Opens a deployed Snowflake App in the browser.

    Looks up the service endpoint URL for the app and opens it.  Use
    --print-only to print the URL without launching a browser.
    """
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    fqn = entity.fqn
    service_fqn = FQN(database=fqn.database, schema=fqn.schema, name=fqn.name)

    manager = SnowflakeAppManager()
    endpoint_url = manager.get_service_endpoint_url(service_fqn)

    if not endpoint_url:
        raise CliError(
            f"No endpoint URL found for service {service_fqn}. "
            f"Is the app deployed? Run 'snow {_APP_COMMAND_NAME} deploy' first."
        )

    if not print_only:
        typer.launch(endpoint_url)
    return MessageResult(endpoint_url)


@app.command(requires_connection=True)
def deploy(
    entity_id: Optional[str] = typer.Option(
        None,
        "--entity-id",
        help="ID of the snowflake-app entity to deploy. Required if multiple snowflake-app entities exist.",
    ),
    skip_build: bool = typer.Option(
        False,
        "--skip-build",
        help="Skip the build phase and go straight to deploying the service. "
        "Assumes the container image has already been built.",
    ),
    **options,
) -> CommandResult:
    """
    Builds and deploys a Snowflake App.

    Uploads source artifacts, builds a container image, creates (or updates)
    a service, and waits for it to become ready.

    If --entity-id is not specified and the project contains exactly one snowflake-app
    entity, that entity will be used automatically.

    Use --skip-build to skip the build phase and redeploy using the existing
    container image (e.g. after a configuration-only change).
    """
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    # ── Extract entity configuration ──────────────────────────────────
    fqn = entity.fqn
    app_name = fqn.name

    if entity.code_stage:
        stage_name = entity.code_stage.name
        encryption_type = entity.code_stage.encryption_type or "SNOWFLAKE_SSE"
    else:
        stage_name = f"{app_name}_CODE_STAGE"
        encryption_type = "SNOWFLAKE_SSE"

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

    # ── Validate required configuration ───────────────────────────────
    if not skip_build and not build_compute_pool:
        raise CliError(
            "build_compute_pool is required for deploy. "
            f"Please configure it in snowflake.yml or {APP_DEFAULTS_TABLE}."
        )

    if not service_compute_pool:
        raise CliError(
            "service_compute_pool is required for deploy. "
            f"Please configure it in snowflake.yml or {APP_DEFAULTS_TABLE}."
        )

    if not query_warehouse:
        raise CliError(
            "query_warehouse is required for deploy. "
            f"Please configure it in snowflake.yml or {APP_DEFAULTS_TABLE}."
        )

    # ── Derived names ─────────────────────────────────────────────────
    stage_fqn = FQN(database=database, schema=schema, name=stage_name)
    build_job_service_name = f"{app_name}_BUILD_JOB"
    build_job_fqn = FQN(database=database, schema=schema, name=build_job_service_name)
    service_fqn = FQN(database=database, schema=schema, name=app_name)

    stage_manager = StageManager()

    # ── Resolve image repository URL (needed by both build and deploy) ─
    cli_console.step(f"Getting image repository URL for {image_repository}")
    image_repo_url = manager.get_image_repo_url(
        image_repository, database=image_repo_database, schema=image_repo_schema
    )

    # ── Build phase ───────────────────────────────────────────────────

    if skip_build:
        cli_console.step("Skipping build phase (--skip-build)")
    else:
        # Step 1: Create schema if it doesn't exist
        cli_console.step(f"Creating schema {schema} if it doesn't exist")
        manager.create_schema_if_not_exists(database, schema)

        # Step 2: Clear or create stage
        if manager.stage_exists(stage_fqn):
            cli_console.step(f"Clearing existing stage @{stage_fqn}")
            manager.clear_stage(stage_fqn)
        else:
            cli_console.step(f"Creating stage @{stage_fqn}")
            manager.create_stage(stage_fqn, encryption_type)

        # Step 3: Bundle and upload artifact files to stage
        #
        # We reuse perform_bundle (same logic as `snow __app bundle`) to resolve
        # glob patterns and src/dest mappings into a flat temporary directory,
        # then upload that directory recursively so nested folders are preserved
        # on the stage.
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

        # Step 4: Drop existing build job if present
        cli_console.step(f"Dropping service if exists: {build_job_fqn}")
        manager.drop_service_if_exists(build_job_fqn)

        # Step 5: Execute build job service
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

        # Step 6: Poll for build completion
        cli_console.step("Waiting for build to complete...")
        _poll_until(
            poll_fn=lambda: manager.get_build_status(build_job_fqn),
            done_states={"DONE"},
            error_states={"FAILED", "IDLE"},
            known_pending_states={"PENDING", "RUNNING"},
            timeout_message=f"Build timed out. Check service logs: {build_job_fqn}",
        )

    # ── Deploy phase ──────────────────────────────────────────────────

    # Construct the image path from the repo URL
    # image_repo_url is a full registry URL like "host/db/schema/repo_name"
    # Extract the path portion (everything after the host) for the service spec
    repo_path = "/" + "/".join(image_repo_url.split("/")[1:])
    image_url = f"{repo_path}/{app_name.lower()}:latest"

    # Build app comment with metadata
    comment_data = {"appId": app_name}
    if app_title:
        comment_data["appName"] = app_title
    if app_description:
        comment_data["appDescription"] = app_description
    if app_icon:
        comment_data["appIcon"] = app_icon
    app_comment = json.dumps(comment_data)

    # Step 7: Create service if it doesn't exist
    cli_console.step(f"Creating service {service_fqn} if it doesn't exist")
    manager.create_service(
        service_name=service_fqn,
        compute_pool=service_compute_pool,
        query_warehouse=query_warehouse,
        app_comment=app_comment,
        execute_as_caller=entity.execute_as_caller,
    )

    # Step 8: Alter service with built image
    cli_console.step(f"Updating service with image: {image_url}")
    manager.alter_service_spec(
        service_name=service_fqn,
        image_url=image_url,
        execute_as_caller=entity.execute_as_caller,
    )

    # Step 9: Resume service
    cli_console.step("Resuming service")
    manager.resume_service(service_fqn)

    # Step 10: Poll until service is RUNNING
    cli_console.step("Waiting for service to be ready...")
    _poll_until(
        poll_fn=lambda: manager.get_service_status(service_fqn),
        done_states={"RUNNING"},
        error_states={"FAILED", "IDLE"},
        known_pending_states={"PENDING", "SUSPENDING", "SUSPENDED"},
        timeout_message=f"Service timed out. Check service status: {service_fqn}",
    )

    # Step 11: Get endpoint URL (poll until provisioning completes)
    cli_console.step("Getting endpoint URL")
    endpoint_url = _poll_until(
        poll_fn=lambda: manager.get_service_endpoint_url(service_fqn),
        is_done=lambda url: url is not None
        and "provisioning in progress" not in url.lower(),
        format_status=lambda url: url or "Endpoint URL not yet available",
        timeout_message=(
            f"Endpoint provisioning timed out. "
            f'Check with: snow sql -q "SHOW ENDPOINTS IN SERVICE {service_fqn}"'
        ),
    )
    return MessageResult(f"App ready at {endpoint_url}")
