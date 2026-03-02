# Copyright (c) 2024 Snowflake Inc.
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

from pathlib import Path
from textwrap import dedent
from typing import List, Optional

import typer
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.project.util import get_env_username
from snowflake.cli.api.sql_execution import SqlExecutionMixin

app = SnowTyperFactory(
    name="apps",
    help="Manages Snowflake Apps.",
    is_hidden=FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_disabled,
)

DEFINITION_FILENAME = "snowflake.yml"
BUILD_SPEC_FILENAME = "build-spec.yml"
SERVICE_SPEC_FILENAME = "service-spec.yml"

# Feature flags
IS_PERSONAL_DB_SUPPORTED = False  # Will be enabled in the future

# Default resource names for Snow Apps
SYSTEM_COMPUTE_POOL = "SYSTEM_COMPUTE_POOL_CPU"
SNOW_APPS_COMPUTE_POOL = "SNOW_APPS_DEFAULT_COMPUTE_POOL"
DEFAULT_EXTERNAL_ACCESS = "SNOW_APPS_DEFAULT_EXTERNAL_ACCESS"
DEFAULT_IMAGE_REPOSITORY = "SNOW_APPS_DEFAULT_IMAGE_REPOSITORY"


def _check_feature_enabled():
    if FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_disabled():
        raise CliError("This feature is not available yet.")


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
    Get the compute pool to use for Snow Apps.

    Checks in order:
    1. SYSTEM_COMPUTE_POOL_CPU
    2. SNOW_APPS_COMPUTE_POOL

    Returns None if neither exists.
    """
    # TODO: Enable when SYSTEM_COMPUTE_POOL is supported for SPCS
    if False and _object_exists("compute-pool", SYSTEM_COMPUTE_POOL):
        return SYSTEM_COMPUTE_POOL
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


def _get_image_repo_url(repo_name: str) -> Optional[str]:
    """Get the image repository URL. Returns None if not found."""
    from snowflake.cli.api.project.util import (
        identifier_to_show_like_pattern,
        unquote_identifier,
    )
    from snowflake.connector.cursor import DictCursor

    try:
        executor = SqlExecutionMixin()
        show_obj_query = (
            f"show image repositories like {identifier_to_show_like_pattern(repo_name)}"
        )
        cursor = executor.execute_query(show_obj_query, cursor_class=DictCursor)

        if cursor.rowcount is None or cursor.rowcount == 0:
            return None

        unqualified_name = unquote_identifier(repo_name)
        rows = cursor.fetchall()
        row = next(
            (r for r in rows if r["name"].upper() == unqualified_name.upper()),
            None,
        )
        if not row:
            row = rows[0] if rows else None
        if not row:
            return None

        return row["repository_url"]
    except Exception:
        return None


def _generate_snowflake_yml(
    app_id: str,
    database: str,
    schema: str,
    compute_pool: Optional[str],
    warehouse: Optional[str],
) -> str:
    """Generate snowflake.yml content with a service entity."""

    return dedent(
        f"""\
        definition_version: "2"

        entities:
          {app_id}_service:
            type: service
            identifier:
              name: {app_id.upper()}_SERVICE
              database: {database}
              schema: {schema}
            stage: {app_id.upper()}_SERVICE_STAGE
            compute_pool: {compute_pool or "null"}
            spec_file: {SERVICE_SPEC_FILENAME}
            query_warehouse: {warehouse or "null"}
            comment: '{{"appId": "{app_id.upper()}", "appName": "{app_id}"}}'
            artifacts:
              - src: {SERVICE_SPEC_FILENAME}
                dest: {SERVICE_SPEC_FILENAME}
        """
    )


def _generate_build_spec(
    app_id: str,
    database: str,
    schema: str,
    image_repo_url: Optional[str],
) -> str:
    """Generate build-spec.yml content for the image build job."""

    code_stage_fqn = f"@{database}.{schema}.{app_id.upper()}_CODE_STAGE"
    repo_url = image_repo_url or "<IMAGE_REPO_URL>"

    return dedent(
        f"""\
        spec:
          containers:
          - name: main
            image: "/snowflake/images/snowflake_images/sf-image-build:0.0.1"
            env:
              IMAGE_REGISTRY_URL: "{repo_url}"
              IMAGE_NAME: "{app_id.lower()}"
              IMAGE_TAG: "latest"
              BUILD_CONTEXT: "/app"
            volumeMounts:
              - name: code-volume
                mountPath: /app
          volumes:
          - name: code-volume
            source: "{code_stage_fqn}"
            uid: 65532
        """
    )


def _generate_service_spec(
    app_id: str,
    image_path: Optional[str],
) -> str:
    """Generate service-spec.yml content for the app service."""

    image = image_path or f"/<DATABASE>/<SCHEMA>/<IMAGE_REPO>/{app_id.lower()}:latest"

    return dedent(
        f"""\
        spec:
          containers:
            - name: main
              image: "{image}"
          endpoints:
            - name: app-endpoint
              port: 3000
              public: true
        serviceRoles:
          - name: viewer
            endpoints:
              - app-endpoint
        """
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
    Initializes project files for a Snowflake App.

    Generates snowflake.yml (service entity), build-spec.yml (image build job),
    and service-spec.yml (service definition). The generated files are used with
    existing snow spcs commands to build and deploy the app.
    """
    _check_feature_enabled()

    project_file = Path.cwd() / DEFINITION_FILENAME
    build_spec_file = Path.cwd() / BUILD_SPEC_FILENAME
    service_spec_file = Path.cwd() / SERVICE_SPEC_FILENAME

    if project_file.exists():
        return MessageResult(
            f"{DEFINITION_FILENAME} already exists. Skipping initialization."
        )

    # Get connection context
    ctx = get_cli_context()
    warehouse = ctx.connection_context.warehouse
    username = get_env_username().upper()

    # Database: use personal DB if supported, otherwise use connection database
    if IS_PERSONAL_DB_SUPPORTED:
        database = f"USER${username}"
    else:
        database = ctx.connection_context.database or "<% ctx.connection.database %>"

    # Schema: SNOW_APP_<APP_ID>_<USERNAME>
    schema = f"SNOW_APP_{app_name.upper()}_{username}"

    # Compute pool: check for existing pools
    compute_pool = _get_compute_pool()

    # Build EAI: check for existing integrations
    build_eai = _get_external_access(app_name)

    # Image repository: try to look up the URL
    image_repo_url = _get_image_repo_url(DEFAULT_IMAGE_REPOSITORY)

    # Compute image path for the service spec
    image_path = None
    if image_repo_url:
        # repo_url is like "host/db/schema/repo" — extract the path after the host
        parts = image_repo_url.split("/")
        if len(parts) > 1:
            repo_path = "/" + "/".join(parts[1:])
            image_path = f"{repo_path}/{app_name.lower()}:latest"

    # Write snowflake.yml
    project_file.write_text(
        _generate_snowflake_yml(
            app_id=app_name,
            database=database,
            schema=schema,
            compute_pool=compute_pool,
            warehouse=warehouse,
        )
    )
    cli_console.step(f"Created {DEFINITION_FILENAME}")

    # Write build-spec.yml
    build_spec_file.write_text(
        _generate_build_spec(
            app_id=app_name,
            database=database,
            schema=schema,
            image_repo_url=image_repo_url,
        )
    )
    cli_console.step(f"Created {BUILD_SPEC_FILENAME}")

    # Write service-spec.yml
    service_spec_file.write_text(
        _generate_service_spec(
            app_id=app_name,
            image_path=image_path,
        )
    )
    cli_console.step(f"Created {SERVICE_SPEC_FILENAME}")

    # Print summary of detected configuration
    messages: List[str] = [
        f"Initialized Snowflake App project for '{app_name}'.",
        f"  Database: {database}",
        f"  Schema: {schema}",
        f"  Warehouse: {warehouse or '<not set>'}",
        f"  Compute pool: {compute_pool or '<not set>'}",
        f"  Build EAI: {build_eai or '<not set>'}",
        f"  Image repo URL: {image_repo_url or '<not set>'}",
        "",
        "Next steps:",
        f"  1. Review and update {DEFINITION_FILENAME}, {BUILD_SPEC_FILENAME}, and {SERVICE_SPEC_FILENAME}",
        f'  2. Create the schema:    snow sql -q "CREATE SCHEMA IF NOT EXISTS {database}.{schema}"',
        f"  3. Create the code stage: snow stage create {database}.{schema}.{app_name.upper()}_CODE_STAGE",
        f"  4. Upload source code:   snow stage copy ./* @{database}.{schema}.{app_name.upper()}_CODE_STAGE --recursive --overwrite",
        f"  5. Build the image:      snow spcs service execute-job {app_name.upper()}_BUILD_JOB --compute-pool <POOL> --spec-path {BUILD_SPEC_FILENAME}",
        f"  6. Deploy the service:   snow spcs service deploy --entity-id={app_name}_service",
        f"  7. Check endpoints:      snow spcs service list-endpoints {app_name.upper()}_SERVICE",
    ]

    return MessageResult("\n".join(messages))
