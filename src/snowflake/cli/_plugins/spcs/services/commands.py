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

from __future__ import annotations

import itertools
import time
import uuid
from pathlib import Path
from typing import Generator, Iterable, List, Optional, cast

import typer
from snowflake.cli._plugins.object.command_aliases import (
    add_object_command_aliases,
    scope_option,
)
from snowflake.cli._plugins.object.common import CommentOption, Tag, TagOption
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli._plugins.spcs.common import (
    validate_and_set_instances,
)
from snowflake.cli._plugins.spcs.services.manager import ServiceManager
from snowflake.cli._plugins.spcs.services.service_entity_model import ServiceEntityModel
from snowflake.cli._plugins.spcs.services.service_project_paths import (
    ServiceProjectPaths,
)
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.flags import (
    IfNotExistsOption,
    OverrideableOption,
    entity_argument,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import (
    CliArgumentError,
    IncompatibleParametersError,
)
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    QueryJsonValueResult,
    QueryResult,
    SingleQueryResult,
    StreamResult,
)
from snowflake.cli.api.project.definition_helper import (
    get_entity_from_project_definition,
)
from snowflake.cli.api.project.util import is_valid_object_name
from snowflake.cli.api.stage_path import StagePath
from snowflake.connector.cursor import DictCursor
from snowflake.connector.errors import ProgrammingError

app = SnowTyperFactory(
    name="service",
    help="Manages Snowpark Container Services services.",
    short_help="Manages services.",
)

# Define common options
container_name_option = typer.Option(
    ...,
    "--container-name",
    help="Name of the container.",
    show_default=False,
)

instance_id_option = typer.Option(
    ...,
    "--instance-id",
    help="ID of the service instance, starting with 0.",
    show_default=False,
)

since_option = typer.Option(
    default="",
    help="Fetch events that are newer than this time ago, in Snowflake interval syntax.",
)

until_option = typer.Option(
    default="",
    help="Fetch events that are older than this time ago, in Snowflake interval syntax.",
)

show_all_columns_option = typer.Option(
    False,
    "--all",
    is_flag=True,
    help="Fetch all columns.",
)


def _service_name_callback(name: FQN) -> FQN:
    if not is_valid_object_name(name.identifier, max_depth=2, allow_quoted=False):
        raise CliArgumentError(
            f"'{name}' is not a valid service name. Note service names must be unquoted identifiers. The same constraint also applies to database and schema names where you create a service."
        )
    return name


ServiceNameArgument = identifier_argument(
    sf_object="service",
    example="my_service",
    callback=_service_name_callback,
)

SpecPathOption = typer.Option(
    ...,
    "--spec-path",
    help="Path to service specification file.",
    file_okay=True,
    dir_okay=False,
    exists=True,
    show_default=False,
)
DEFAULT_NUM_LINES = 500

_MIN_INSTANCES_HELP = "Minimum number of service instances to run."
MinInstancesOption = OverrideableOption(
    1, "--min-instances", help=_MIN_INSTANCES_HELP, min=1
)

_MAX_INSTANCES_HELP = "Maximum number of service instances to run."
MaxInstancesOption = OverrideableOption(
    None, "--max-instances", help=_MAX_INSTANCES_HELP, min=1
)

_QUERY_WAREHOUSE_HELP = "Warehouse to use if a service container connects to Snowflake to execute a query without explicitly specifying a warehouse to use."
QueryWarehouseOption = OverrideableOption(
    None,
    "--query-warehouse",
    help=_QUERY_WAREHOUSE_HELP,
)

_AUTO_RESUME_HELP = "The service will automatically resume when a service function or ingress is called."
AutoResumeOption = OverrideableOption(
    True,
    "--auto-resume/--no-auto-resume",
    help=_AUTO_RESUME_HELP,
)

_AUTO_SUSPEND_SECS_HELP = "Number of seconds of inactivity after which the service will be automatically suspended."
AutoSuspendSecsOption = OverrideableOption(
    None,
    "--auto-suspend-secs",
    help=_AUTO_SUSPEND_SECS_HELP,
    min=0,
)

_COMMENT_HELP = "Comment for the service."

add_object_command_aliases(
    app=app,
    object_type=ObjectType.SERVICE,
    name_argument=ServiceNameArgument,
    like_option=like_option(
        help_example='`list --like "my%"` lists all services that begin with “my”.'
    ),
    scope_option=scope_option(help_example="`list --in compute-pool my_pool`"),
)


@app.command(requires_connection=True)
def create(
    name: FQN = ServiceNameArgument,
    compute_pool: str = typer.Option(
        ...,
        "--compute-pool",
        help="Compute pool to run the service on.",
        show_default=False,
    ),
    spec_path: Path = SpecPathOption,
    min_instances: int = MinInstancesOption(),
    max_instances: Optional[int] = MaxInstancesOption(),
    auto_resume: bool = AutoResumeOption(),
    external_access_integrations: Optional[List[str]] = typer.Option(
        None,
        "--eai-name",
        help="Identifies external access integrations (EAI) that the service can access. This option may be specified multiple times for multiple EAIs.",
    ),
    query_warehouse: Optional[str] = QueryWarehouseOption(),
    tags: Optional[List[Tag]] = TagOption(help="Tag for the service."),
    comment: Optional[str] = CommentOption(help=_COMMENT_HELP),
    if_not_exists: bool = IfNotExistsOption(),
    **options,
) -> CommandResult:
    """
    Creates a new service in the current schema.
    """
    max_instances = validate_and_set_instances(
        min_instances, max_instances, "instances"
    )
    cursor = ServiceManager().create(
        service_name=name.identifier,
        min_instances=min_instances,
        max_instances=max_instances,
        compute_pool=compute_pool,
        spec_path=spec_path,
        external_access_integrations=external_access_integrations,
        auto_resume=auto_resume,
        query_warehouse=query_warehouse,
        tags=tags,
        comment=comment,
        if_not_exists=if_not_exists,
    )
    return SingleQueryResult(cursor)


@app.command(requires_connection=True)
@with_project_definition()
def deploy(
    entity_id: str = entity_argument("service"),
    upgrade: bool = typer.Option(
        False,
        "--upgrade",
        help="Updates the existing service. Can update min_instances, max_instances, query_warehouse, auto_resume, auto_suspend_secs, external_access_integrations and comment.",
    ),
    **options,
) -> CommandResult:
    """
    Deploys a service defined in the project definition file.
    """
    service: ServiceEntityModel = get_entity_from_project_definition(
        entity_type=ObjectType.SERVICE,
        entity_id=entity_id,
    )
    service_project_paths = ServiceProjectPaths(get_cli_context().project_root)
    max_instances = validate_and_set_instances(
        service.min_instances, service.max_instances, "instances"
    )
    cursor = ServiceManager().deploy(
        service_name=service.fqn.identifier,
        stage=service.stage,
        artifacts=service.artifacts,
        compute_pool=service.compute_pool,
        spec_path=service.spec_file,
        min_instances=service.min_instances,
        max_instances=max_instances,
        auto_resume=service.auto_resume,
        auto_suspend_secs=service.auto_suspend_secs,
        external_access_integrations=service.external_access_integrations,
        query_warehouse=service.query_warehouse,
        tags=service.tags,
        comment=service.comment,
        service_project_paths=service_project_paths,
        upgrade=upgrade,
    )
    return SingleQueryResult(cursor)


@app.command(requires_connection=True)
def execute_job(
    name: FQN = ServiceNameArgument,
    compute_pool: str = typer.Option(
        ...,
        "--compute-pool",
        help="Compute pool to run the job service on.",
        show_default=False,
    ),
    spec_path: Path = SpecPathOption,
    external_access_integrations: Optional[List[str]] = typer.Option(
        None,
        "--eai-name",
        help="Identifies external access integrations (EAI) that the job service can access. This option may be specified multiple times for multiple EAIs.",
    ),
    query_warehouse: Optional[str] = QueryWarehouseOption(),
    comment: Optional[str] = CommentOption(help=_COMMENT_HELP),
    async_mode: bool = typer.Option(
        False,
        "--async",
        help="Execute the job asynchronously without waiting for completion.",
        is_flag=True,
    ),
    replicas: Optional[int] = typer.Option(
        None,
        "--replicas",
        help="Number of job replicas to run.",
        min=1,
    ),
    **options,
) -> CommandResult:
    """
    Creates and executes a job service in the current schema.
    """
    cursor = ServiceManager().execute_job(
        job_service_name=name.identifier,
        compute_pool=compute_pool,
        spec_path=spec_path,
        external_access_integrations=external_access_integrations,
        query_warehouse=query_warehouse,
        comment=comment,
        async_mode=async_mode,
        replicas=replicas,
    )
    return SingleQueryResult(cursor)


@app.command(requires_connection=True, deprecated=True)
def status(name: FQN = ServiceNameArgument, **options) -> CommandResult:
    """
    Retrieves the status of a service. This command is deprecated and will be removed in a future release. Use `describe` instead to get service status and use `list-instances` and `list-containers` to get more detailed information about service instances and containers.
    """
    cursor = ServiceManager().status(service_name=name.identifier)
    return QueryJsonValueResult(cursor)


@app.command(requires_connection=True)
def logs(
    name: FQN = ServiceNameArgument,
    container_name: str = container_name_option,
    instance_id: str = instance_id_option,
    num_lines: int = typer.Option(
        DEFAULT_NUM_LINES, "--num-lines", help="Number of lines to retrieve."
    ),
    previous_logs: bool = typer.Option(
        False,
        "--previous-logs",
        help="Retrieve logs from the last terminated container.",
        is_flag=True,
    ),
    since_timestamp: Optional[str] = typer.Option(
        "", "--since", help="Start log retrieval from a specified UTC timestamp."
    ),
    include_timestamps: bool = typer.Option(
        False, "--include-timestamps", help="Include timestamps in logs.", is_flag=True
    ),
    follow: bool = typer.Option(
        False,
        "--follow",
        help="Stream logs in real-time.",
        is_flag=True,
        hidden=True,
    ),
    follow_interval: int = typer.Option(
        2,
        "--follow-interval",
        help="Set custom polling intervals for log streaming (--follow flag) in seconds.",
        hidden=True,
    ),
    **options,
):
    """
    Retrieves local logs from a service container.
    """
    if follow:
        if num_lines != DEFAULT_NUM_LINES:
            raise IncompatibleParametersError(["--follow", "--num-lines"])
        if previous_logs:
            raise IncompatibleParametersError(["--follow", "--previous-logs"])

    manager = ServiceManager()

    if follow:
        stream: Iterable[CommandResult] = (
            MessageResult(log_batch)
            for log_batch in manager.stream_logs(
                service_name=name.identifier,
                container_name=container_name,
                instance_id=instance_id,
                num_lines=num_lines,
                since_timestamp=since_timestamp,
                include_timestamps=include_timestamps,
                interval_seconds=follow_interval,
            )
        )
        stream = itertools.chain(stream, [MessageResult("")])
    else:
        stream = (
            MessageResult(log)
            for log in manager.logs(
                service_name=name.identifier,
                container_name=container_name,
                instance_id=instance_id,
                num_lines=num_lines,
                previous_logs=previous_logs,
                since_timestamp=since_timestamp,
                include_timestamps=include_timestamps,
            )
        )

    return StreamResult(cast(Generator[CommandResult, None, None], stream))


@app.command(
    requires_connection=True,
    is_enabled=FeatureFlag.ENABLE_SPCS_SERVICE_EVENTS.is_enabled,
)
def events(
    name: FQN = ServiceNameArgument,
    container_name: str = container_name_option,
    instance_id: str = instance_id_option,
    since: str = since_option,
    until: str = until_option,
    first: int = typer.Option(
        default=None,
        show_default=False,
        help="Fetch only the first N events. Cannot be used with --last.",
    ),
    last: int = typer.Option(
        default=None,
        show_default=False,
        help="Fetch only the last N events. Cannot be used with --first.",
    ),
    show_all_columns: bool = show_all_columns_option,
    **options,
):
    """
    Retrieve platform events for a service container.
    """

    if first is not None and last is not None:
        raise IncompatibleParametersError(["--first", "--last"])

    manager = ServiceManager()
    events = manager.get_events(
        service_name=name.identifier,
        container_name=container_name,
        instance_id=instance_id,
        since=since,
        until=until,
        first=first,
        last=last,
        show_all_columns=show_all_columns,
    )

    if not events:
        return MessageResult("No events found.")

    return CollectionResult(events)


@app.command(
    requires_connection=True,
)
def metrics(
    name: FQN = ServiceNameArgument,
    container_name: str = container_name_option,
    instance_id: str = instance_id_option,
    since: str = since_option,
    until: str = until_option,
    show_all_columns: bool = show_all_columns_option,
    **options,
):
    """
    Retrieve platform metrics for a service container.
    """

    manager = ServiceManager()
    if since or until:
        metrics = manager.get_all_metrics(
            service_name=name,
            container_name=container_name,
            instance_id=instance_id,
            since=since,
            until=until,
            show_all_columns=show_all_columns,
        )
    else:
        metrics = manager.get_latest_metrics(
            service_name=name,
            container_name=container_name,
            instance_id=instance_id,
            show_all_columns=show_all_columns,
        )

    if not metrics:
        return MessageResult("No metrics found.")

    return CollectionResult(metrics)


@app.command(requires_connection=True)
def upgrade(
    name: FQN = ServiceNameArgument,
    spec_path: Path = SpecPathOption,
    **options,
):
    """
    Updates an existing service with a new specification file.
    """
    return SingleQueryResult(
        ServiceManager().upgrade_spec(service_name=name.identifier, spec_path=spec_path)
    )


@app.command("list-endpoints", requires_connection=True)
def list_endpoints(name: FQN = ServiceNameArgument, **options):
    """
    Lists the endpoints in a service.
    """
    return QueryResult(ServiceManager().list_endpoints(service_name=name.identifier))


@app.command("list-instances", requires_connection=True)
def list_service_instances(name: FQN = ServiceNameArgument, **options) -> CommandResult:
    """
    Lists all service instances in a service.
    """
    return QueryResult(ServiceManager().list_instances(service_name=name.identifier))


@app.command("list-containers", requires_connection=True)
def list_service_containers(
    name: FQN = ServiceNameArgument, **options
) -> CommandResult:
    """
    Lists all service containers in a service.
    """
    return QueryResult(ServiceManager().list_containers(service_name=name.identifier))


@app.command("list-roles", requires_connection=True)
def list_service_roles(name: FQN = ServiceNameArgument, **options) -> CommandResult:
    """
    Lists all service roles in a service.
    """
    return QueryResult(ServiceManager().list_roles(service_name=name.identifier))


@app.command(requires_connection=True)
def suspend(name: FQN = ServiceNameArgument, **options) -> CommandResult:
    """
    Suspends the service, shutting down and deleting all its containers.
    """
    return SingleQueryResult(ServiceManager().suspend(name))


@app.command(requires_connection=True)
def resume(name: FQN = ServiceNameArgument, **options) -> CommandResult:
    """
    Resumes the service from a SUSPENDED state.
    """
    return SingleQueryResult(ServiceManager().resume(name))


@app.command("set", requires_connection=True)
def set_property(
    name: FQN = ServiceNameArgument,
    min_instances: Optional[int] = MinInstancesOption(default=None, show_default=False),
    max_instances: Optional[int] = MaxInstancesOption(show_default=False),
    query_warehouse: Optional[str] = QueryWarehouseOption(show_default=False),
    auto_resume: Optional[bool] = AutoResumeOption(default=None, show_default=False),
    auto_suspend_secs: Optional[int] = AutoSuspendSecsOption(show_default=False),
    external_access_integrations: Optional[List[str]] = typer.Option(
        None,
        "--eai-name",
        help="Identifies external access integrations (EAI) that the service can access. This option may be specified multiple times for multiple EAIs.",
    ),
    comment: Optional[str] = CommentOption(help=_COMMENT_HELP, show_default=False),
    **options,
):
    """
    Sets one or more properties for the service.
    """
    cursor = ServiceManager().set_property(
        service_name=name.identifier,
        min_instances=min_instances,
        max_instances=max_instances,
        query_warehouse=query_warehouse,
        auto_resume=auto_resume,
        auto_suspend_secs=auto_suspend_secs,
        external_access_integrations=external_access_integrations,
        comment=comment,
    )
    return SingleQueryResult(cursor)


@app.command("unset", requires_connection=True)
def unset_property(
    name: FQN = ServiceNameArgument,
    min_instances: bool = MinInstancesOption(
        default=False,
        help=f"Reset the MIN_INSTANCES property - {_MIN_INSTANCES_HELP}",
        show_default=False,
    ),
    max_instances: bool = MaxInstancesOption(
        default=False,
        help=f"Reset the MAX_INSTANCES property - {_MAX_INSTANCES_HELP}",
        show_default=False,
    ),
    query_warehouse: bool = QueryWarehouseOption(
        default=False,
        help=f"Reset the QUERY_WAREHOUSE property - {_QUERY_WAREHOUSE_HELP}",
        show_default=False,
    ),
    auto_resume: bool = AutoResumeOption(
        default=False,
        param_decls=["--auto-resume"],
        help=f"Reset the AUTO_RESUME property - {_AUTO_RESUME_HELP}",
        show_default=False,
    ),
    auto_suspend_secs: bool = AutoSuspendSecsOption(
        default=False,
        param_decls=["--auto-suspend-secs"],
        help=f"Reset the AUTO_SUSPEND_SECS property - {_AUTO_SUSPEND_SECS_HELP}",
        show_default=False,
    ),
    comment: bool = CommentOption(
        default=False,
        help=f"Reset the COMMENT property - {_COMMENT_HELP}",
        callback=None,
        show_default=False,
    ),
    **options,
):
    """
    Resets one or more properties for the service to their default value(s).
    """
    cursor = ServiceManager().unset_property(
        service_name=name.identifier,
        min_instances=min_instances,
        max_instances=max_instances,
        query_warehouse=query_warehouse,
        auto_resume=auto_resume,
        auto_suspend_secs=auto_suspend_secs,
        comment=comment,
    )
    return SingleQueryResult(cursor)


@app.command(
    "build-image",
    requires_connection=True,
    hidden=not FeatureFlag.ENABLE_SPCS_BUILD_IMAGE.is_enabled(),
)
def build_image(
    compute_pool: str = typer.Option(
        ...,
        "--compute-pool",
        help="Compute pool to run the image build job on.",
        show_default=False,
    ),
    image_repository: str = typer.Option(
        ...,
        "--image-repository",
        help="Image repository in format [db.][schema.]repo_name (e.g., my_db.my_schema.my_repo or my_repo).",
        show_default=False,
    ),
    image_name: str = typer.Option(
        ...,
        "--image-name",
        help="Name for the built image.",
        show_default=False,
    ),
    image_tag: str = typer.Option(
        ...,
        "--image-tag",
        help="Tag for the built image.",
        show_default=False,
    ),
    build_context_dir: Path = typer.Option(
        ...,
        "--build-context-dir",
        help="Directory to use as build context. Must contain a Dockerfile.",
        file_okay=False,
        dir_okay=True,
        exists=True,
        show_default=False,
    ),
    stage: Optional[str] = typer.Option(
        None,
        "--stage",
        help="Stage to store build context files. Format: [db.][schema.]stage_name. If not provided, a temporary stage will be created and dropped automatically. If provided, only the uploaded build context files will be removed after the build completes.",
        show_default=False,
    ),
    job_name: str = typer.Option(
        None,
        "--job-name",
        help="Name for the build job service. If not provided, a name will be auto-generated.",
        show_default=False,
    ),
    external_access_integrations: Optional[List[str]] = typer.Option(
        None,
        "--eai-name",
        help="Identifies external access integrations (EAI) that the build job can access. This option may be specified multiple times for multiple EAIs.",
    ),
    **options,
) -> CommandResult:
    """
    [Experimental] Builds a container image using SPCS service.

    **Note:** This command is experimental and subject to change.

    This command is hidden by default. To make it visible in help output, enable the
    feature flag in your config.toml:
    [cli.feature_flags]
    enable_spcs_build_image = true

    Or set the environment variable:
    export SNOWFLAKE_CLI_FEATURES_ENABLE_SPCS_BUILD_IMAGE=true

    This command uploads the build context (Dockerfile and related files) to a stage,
    then executes a job service that builds the container image and pushes it to the
    specified image repository. The build logs are streamed to the terminal in real-time
    until the build completes or fails.

    If --stage is not provided, a stage will be automatically created using the
    current session's database and schema context, and dropped after the build completes.
    If your session doesn't have a database/schema set, you should provide --stage explicitly.
    """
    # Verify Dockerfile exists in build context directory
    dockerfile_path = build_context_dir / "Dockerfile"
    if not dockerfile_path.exists():
        raise CliArgumentError(
            f"Dockerfile not found in build context directory '{build_context_dir}'. "
            f"Expected to find: {dockerfile_path}"
        )

    if not dockerfile_path.is_file():
        raise CliArgumentError(f"'{dockerfile_path}' exists but is not a file.")

    # Validate image_name and image_tag (basic alphanumeric validation)
    if not image_name.replace("_", "").replace("-", "").replace(".", "").isalnum():
        raise CliArgumentError(
            f"Invalid image name '{image_name}'. Must contain only alphanumeric characters, hyphens, underscores, and dots."
        )

    if not image_tag.replace("_", "").replace("-", "").replace(".", "").isalnum():
        raise CliArgumentError(
            f"Invalid image tag '{image_tag}'. Must contain only alphanumeric characters, hyphens, underscores, and dots."
        )

    # Generate a unique identifier for this build operation
    build_uuid = uuid.uuid4().hex[:8]

    if job_name is None:
        job_name = f"build_image_{build_uuid}"
    else:
        # Validate job_name format
        if not is_valid_object_name(job_name, max_depth=0, allow_quoted=True):
            raise CliArgumentError(
                f"Invalid job name '{job_name}'. Must be a valid unquoted identifier."
            )

    stage_manager = StageManager()
    use_temporary_stage = stage is None

    if use_temporary_stage:
        # Create a stage
        stage = f"{job_name}_stage"
        cli_console.step(f"Creating temporary stage: {stage}")
        stage_fqn = FQN.from_string(stage).using_context()
        stage_manager.create(fqn=stage_fqn)
    else:
        # Use the provided stage (ensure it exists)
        stage_fqn = FQN.from_string(stage)
        cli_console.step(f"Using existing stage: {stage_fqn.identifier}")

    # Upload build context to stage
    build_context_stage_path = f"build_contexts/{job_name}"
    cli_console.step(
        f"Uploading build context from {build_context_dir} to @{stage_fqn.identifier}/{build_context_stage_path}"
    )

    stage_path = StagePath.from_stage_str(
        f"@{stage_fqn.identifier}/{build_context_stage_path}"
    )
    stage_manager.put(
        local_path=build_context_dir,
        stage_path=str(stage_path),
        overwrite=True,
    )

    # Execute the build job asynchronously so we can stream logs
    cli_console.step(f"Starting image build job: {job_name}")
    service_manager = ServiceManager()
    cursor = service_manager.build_image(
        job_service_name=job_name,
        compute_pool=compute_pool,
        image_repository=image_repository,
        image_name=image_name,
        image_tag=image_tag,
        stage=stage_fqn.identifier,
        build_context_path=build_context_stage_path,
        external_access_integrations=external_access_integrations,
        async_mode=True,  # Always async so we can stream logs
    )

    cli_console.step(f"Waiting for job to start...")

    # Wait for job to be ready (not PENDING) before streaming logs
    max_wait_time = 300  # 5 minutes
    poll_interval = 5  # seconds
    elapsed_time = 0
    current_status = None

    cli_console.message(f"Checking job status every {poll_interval} seconds...")

    while elapsed_time < max_wait_time:
        try:
            # Check service status using ObjectManager
            object_manager = ObjectManager()
            job_fqn = FQN.from_string(job_name)
            describe_result = object_manager.describe(
                object_type="service", fqn=job_fqn, cursor_class=DictCursor
            )
            # DESCRIBE SERVICE returns a single row with all properties as columns
            result_row = describe_result.fetchone()
            if result_row and "status" in result_row:
                current_status = result_row["status"]

            if current_status:
                cli_console.message(f"Current job status: {current_status}")

            # Only wait if status is PENDING, otherwise logs should be available
            if current_status and current_status != "PENDING":
                break

        except Exception as e:
            # Job might not be describable yet
            cli_console.message(
                f"Waiting for job to be available... ({elapsed_time}s elapsed)"
            )

        time.sleep(poll_interval)
        elapsed_time += poll_interval

    # Stream logs if job is no longer pending
    if current_status and current_status != "PENDING":
        cli_console.step(f"Job status: {current_status}. Streaming logs...")
        cli_console.message("")  # Empty line before logs

        # Stream logs with terminal status monitoring
        final_status = None
        try:
            log_stream = service_manager.stream_logs(
                service_name=job_name,
                instance_id="0",
                container_name="main",
                num_lines=1000,
                since_timestamp="",
                include_timestamps=False,
                interval_seconds=2,
                check_terminal_status=True,
            )

            for log_entry in log_stream:
                # Check if this is a terminal status signal (tuple) vs a log line (string)
                if (
                    isinstance(log_entry, tuple)
                    and log_entry[0] == "__TERMINAL_STATUS__"
                ):
                    final_status = log_entry[1]
                    break
                # Otherwise it's a log line
                cli_console.message(log_entry)

        except KeyboardInterrupt:
            cli_console.warning(
                f"\nBuild job '{job_name}' is still running in the background."
            )
            cli_console.message(
                f"Use 'snow spcs service logs {job_name} --container-name main --instance-id 0' to view logs."
            )
            cli_console.message(
                f"Use 'snow spcs service status {job_name}' to check status."
            )
            if use_temporary_stage:
                cli_console.warning(
                    f"Remember to manually clean up stage: DROP STAGE {stage_fqn.sql_identifier};"
                )
            else:
                cli_console.warning(
                    f"Remember to manually clean up build context: REMOVE @{stage_fqn.identifier}/{build_context_stage_path};"
                )
            return SingleQueryResult(cursor)
    else:
        # Status is still PENDING or couldn't be determined
        cli_console.warning(
            f"Job did not start within {max_wait_time}s (status: {current_status or 'UNKNOWN'})"
        )
        cli_console.message(
            f"Use 'snow spcs service status {job_name}' to check status."
        )
        if use_temporary_stage:
            cli_console.warning(
                f"Remember to manually clean up stage: DROP STAGE {stage_fqn.sql_identifier};"
            )
        else:
            cli_console.warning(
                f"Remember to manually clean up build context: REMOVE @{stage_fqn.identifier}/{build_context_stage_path};"
            )
        return SingleQueryResult(cursor)

    # Use final_status if available, otherwise fall back to current_status
    final_status = final_status or current_status

    # Display final status message
    cli_console.message("")  # Empty line after logs
    if final_status == "DONE":
        cli_console.message(f"✓ Image build job '{job_name}' completed successfully.")
    elif final_status == "FAILED":
        cli_console.warning(f"✗ Image build job '{job_name}' failed.")
    elif final_status == "CANCELLED":
        cli_console.warning(f"✗ Image build job '{job_name}' was cancelled.")

    # Cleanup after build completes
    if use_temporary_stage:
        # Drop the stage
        cli_console.step(f"Cleaning up stage: {stage}")
        try:
            object_manager = ObjectManager()
            object_manager.drop(object_type="stage", fqn=stage_fqn, if_exists=True)
            cli_console.message(f"✓ Dropped stage {stage}")
        except ProgrammingError as e:
            cli_console.warning(f"Failed to clean up stage: {e}")
    else:
        # Remove only the uploaded build context files from customer-provided stage
        cli_console.step(f"Cleaning up build context files from stage: {stage}")
        try:
            stage_manager.remove(
                stage_name=stage_fqn.identifier, path=build_context_stage_path
            )
            cli_console.message(
                f"✓ Removed build context files from {stage}/{build_context_stage_path}"
            )
        except ProgrammingError as e:
            cli_console.warning(f"Failed to clean up build context files: {e}")

    return SingleQueryResult(cursor)
