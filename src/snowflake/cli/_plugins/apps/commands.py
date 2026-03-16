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
    DEFAULT_IMAGE_REPOSITORY,
    DEFINITION_FILENAME,
    SnowflakeAppManager,
    _check_feature_enabled,
    _get_entity,
    _poll_until,
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
    name="apps",
    help="Manages Snowflake Apps.",
    is_hidden=FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_disabled,
)


@app.command(requires_connection=True)
def init(
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
    _check_feature_enabled()

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
    _check_feature_enabled()
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    project_paths = perform_bundle(resolved_entity_id, entity)
    return MessageResult(f"Bundle generated at {project_paths.bundle_root}")


@app.command(requires_connection=True)
def deploy(
    entity_id: Optional[str] = typer.Option(
        None,
        "--entity-id",
        help="ID of the snowflake-app entity to deploy. Required if multiple snowflake-app entities exist.",
    ),
    **options,
) -> CommandResult:
    """
    Builds and deploys a Snowflake App.

    Uploads source artifacts, builds a container image, creates (or updates)
    a service, and waits for it to become ready.

    If --entity-id is not specified and the project contains exactly one snowflake-app
    entity, that entity will be used automatically.
    """
    _check_feature_enabled()
    resolved_entity_id = _resolve_entity_id(entity_id)
    entity = _get_entity(resolved_entity_id)

    # ── Extract entity configuration ──────────────────────────────────
    # Use the model's .fqn property which handles both string and Identifier forms.
    fqn = entity.fqn
    database = fqn.database or "<default_db>"
    schema = fqn.schema or f"SNOW_APP_{resolved_entity_id.upper()}"

    if entity.code_stage:
        stage_name = entity.code_stage.name
        encryption_type = entity.code_stage.encryption_type or "SNOWFLAKE_SSE"
    else:
        stage_name = f"{resolved_entity_id.upper()}_CODE_STAGE"
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

    # TODO: Replace with artifact_repository from entity config once supported
    image_repository = DEFAULT_IMAGE_REPOSITORY

    # ── Validate required configuration ───────────────────────────────
    if not build_compute_pool:
        raise CliError(
            "build_compute_pool is required for deploy. "
            "Please configure it in snowflake.yml."
        )

    if not service_compute_pool:
        raise CliError(
            "service_compute_pool is required for deploy. "
            "Please configure it in snowflake.yml."
        )

    if not query_warehouse:
        raise CliError(
            "query_warehouse is required for deploy. "
            "Please configure it in snowflake.yml."
        )

    # ── Derived names ─────────────────────────────────────────────────
    stage_fqn = FQN(database=database, schema=schema, name=stage_name)
    build_job_service_name = f"{resolved_entity_id.upper()}_BUILD_JOB"
    build_job_fqn = FQN(database=database, schema=schema, name=build_job_service_name)
    service_name_short = resolved_entity_id.upper()
    service_fqn = FQN(database=database, schema=schema, name=service_name_short)

    manager = SnowflakeAppManager()
    stage_manager = StageManager()

    # ── Build phase ───────────────────────────────────────────────────

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
    # We reuse perform_bundle (same logic as `snow apps bundle`) to resolve
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

    # Step 4: Get image repository URL
    cli_console.step(f"Getting image repository URL for {image_repository}")
    image_repo_url = manager.get_image_repo_url(image_repository)

    # Step 5: Drop existing build job if present
    cli_console.step(f"Dropping service if exists: {build_job_fqn}")
    manager.drop_service_if_exists(build_job_fqn)

    # Step 6: Execute build job service
    cli_console.step(f"Executing build job service: {build_job_fqn}")
    manager.execute_build_job(
        job_service_name=build_job_fqn,
        compute_pool=build_compute_pool,
        code_stage=stage_fqn,
        image_repo_url=image_repo_url,
        app_id=resolved_entity_id,
        external_access_integration=build_eai,
    )

    # Step 7: Poll for build completion
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
    image_url = f"{repo_path}/{resolved_entity_id.lower()}:latest"

    # Build app comment with metadata
    comment_data = {"appId": resolved_entity_id.upper()}
    if app_title:
        comment_data["appName"] = app_title
    if app_description:
        comment_data["appDescription"] = app_description
    if app_icon:
        comment_data["appIcon"] = app_icon
    app_comment = json.dumps(comment_data)

    # Step 8: Create service if it doesn't exist
    cli_console.step(f"Creating service {service_fqn} if it doesn't exist")
    manager.create_service(
        service_name=service_fqn,
        compute_pool=service_compute_pool,
        query_warehouse=query_warehouse,
        app_comment=app_comment,
    )

    # Step 9: Alter service with built image
    cli_console.step(f"Updating service with image: {image_url}")
    manager.alter_service_spec(
        service_name=service_fqn,
        image_url=image_url,
    )

    # Step 10: Resume service
    cli_console.step("Resuming service")
    manager.resume_service(service_fqn)

    # Step 11: Poll until service is RUNNING
    cli_console.step("Waiting for service to be ready...")
    _poll_until(
        poll_fn=lambda: manager.get_service_status(service_fqn),
        done_states={"RUNNING"},
        error_states={"FAILED", "IDLE"},
        known_pending_states={"PENDING", "SUSPENDING", "SUSPENDED"},
        timeout_message=f"Service timed out. Check service status: {service_fqn}",
    )

    # Step 12: Get endpoint URL (poll until provisioning completes)
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
