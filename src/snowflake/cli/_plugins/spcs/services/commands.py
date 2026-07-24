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

import base64
import itertools
import signal
import time
import uuid
from pathlib import Path
from typing import Generator, Iterable, List, Optional, cast

import typer
from click import ClickException
from snowflake.cli._plugins.object.command_aliases import (
    add_object_command_aliases,
    scope_option,
)
from snowflake.cli._plugins.object.common import CommentOption, Tag, TagOption
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli._plugins.spcs.common import (
    filter_log_timestamp,
    new_logs_only,
    validate_and_set_instances,
)
from snowflake.cli._plugins.spcs.services.manager import ServiceManager
from snowflake.cli._plugins.spcs.services.remote_build_manager import (
    RemoteBuildJobStatus,
    RemoteBuildManager,
    RemoteBuildPermanentError,
    RemoteBuildStatus,
)
from snowflake.cli._plugins.spcs.services.service_entity_model import ServiceEntityModel
from snowflake.cli._plugins.spcs.services.service_project_paths import (
    ServiceProjectPaths,
)
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.flags import (
    IfExistsOption,
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
    CliError,
    IncompatibleParametersError,
)
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    ObjectResult,
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

events_container_name_option = typer.Option(
    None,
    "--container-name",
    help="Narrow events to this container. Requires --instance-id.",
    show_default=False,
)

events_instance_id_option = typer.Option(
    None,
    "--instance-id",
    help="Narrow events to this service instance, starting with 0.",
    show_default=False,
)

# Overall wait budget for the whole submit -> terminal-status wait in `remote_build`,
# covering REST polling and best-effort SQL log streaming alike. Replaces what used to
# be three independently-timed phases (a 300s REST pending-poll, a SQL-readiness
# sub-loop, and a 60s post-streaming fallback poll).
_REMOTE_BUILD_OVERALL_WAIT_SECONDS = 1800
_REMOTE_BUILD_POLL_INTERVAL_SECONDS = 5
_REMOTE_BUILD_MAX_CONSECUTIVE_REST_ERRORS = 3
# Hard cap on history pages fetched by remote-build-history. With healthy paging
# (50 jobs/page) this is far above any realistic 30-day window.
_REMOTE_BUILD_HISTORY_MAX_PAGES = 1000


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
    ommit_commands=["drop"],
)


@app.command(requires_connection=True)
def drop(
    name: FQN = ServiceNameArgument,
    if_exists: bool = IfExistsOption(),
    force: bool = typer.Option(
        False,
        "--force",
        help="Drops the service along with any block storage volumes it contains. Without this flag, dropping a service that contains block storage volumes fails.",
        is_flag=True,
    ),
    **options,
) -> CommandResult:
    """
    Drops a service.
    """
    return SingleQueryResult(
        ServiceManager().drop(
            service_name=name.sql_identifier, if_exists=if_exists, force=force
        )
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
)
def events(
    name: FQN = ServiceNameArgument,
    container_name: Optional[str] = events_container_name_option,
    instance_id: Optional[str] = events_instance_id_option,
    since: str = since_option,
    until: str = until_option,
    first: Optional[int] = typer.Option(
        default=None,
        show_default=False,
        help="Fetch only the first N events. Cannot be used with --last.",
    ),
    last: Optional[int] = typer.Option(
        default=None,
        show_default=False,
        help="Fetch only the last N events. Cannot be used with --first.",
    ),
    show_all_columns: bool = show_all_columns_option,
    **options,
):
    """
    Retrieve platform events for a service.

    By default, all platform events for the service are returned. The following
    filters narrow the results:

    * --instance-id restricts events to a single service instance.
    * --container-name restricts events to a single container (requires
      --instance-id).
    * --since / --until restrict events to a time window, in Snowflake interval
      syntax.
    * --first / --last return only the first / last N events.
    """

    if first is not None and last is not None:
        raise IncompatibleParametersError(["--first", "--last"])

    if container_name is not None and instance_id is None:
        raise CliArgumentError(
            "--instance-id is required when --container-name is specified."
        )

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
    [cli.features]
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
    if stage is None:
        use_temporary_stage = True
        # Create a stage
        stage = f"{job_name}_stage"
        cli_console.step(f"Creating temporary stage: {stage}")
        stage_fqn = FQN.from_string(stage).using_context()
        stage_manager.create(fqn=stage_fqn)
    else:
        use_temporary_stage = False
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
    for _ in stage_manager.put_recursive(
        local_path=build_context_dir,
        stage_path=str(stage_path),
        overwrite=True,
    ):
        pass

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


@app.command(
    "remote-build",
    requires_connection=True,
    hidden=not FeatureFlag.ENABLE_SPCS_REMOTE_BUILD.is_enabled(),
)
def remote_build(
    build_context_dir: Path = typer.Option(
        ...,
        "--build-context-dir",
        help="Directory to use as build context. Must contain a Dockerfile (image) or project root (app).",
        file_okay=False,
        dir_okay=True,
        exists=True,
        show_default=False,
    ),
    location: str = typer.Option(
        None,
        "--location",
        help=(
            "Target repository for the build output. "
            "For image builds: IMAGE REPOSITORY in [db.][schema.]repo format (optional, account default used when omitted). "
            "For app builds: ARTIFACT REPOSITORY in db.schema.repo format (required)."
        ),
        show_default=False,
    ),
    name: str = typer.Option(
        None,
        "--name",
        help=(
            "Output name. "
            "For image builds: short image name without a tag. "
            "For app builds: artifact package name. "
            "Auto-generated when omitted."
        ),
        show_default=False,
    ),
    image_tag: str = typer.Option(
        None,
        "--image-tag",
        help="Tag for the built image. Applies to image builds only. Defaults to 'latest' when omitted.",
        show_default=False,
    ),
    project_type: str = typer.Option(
        None,
        "--project-type",
        help="Project type hint for app builds (e.g. 'node', 'python'). Ignored for image builds.",
        show_default=False,
    ),
    compute_pool: str = typer.Option(
        None,
        "--compute-pool",
        help="Compute pool to run the build job on. Uses the platform default when omitted.",
        show_default=False,
    ),
    stage: Optional[str] = typer.Option(
        None,
        "--stage",
        help="Stage to store build context files. Format: [db.][schema.]stage_name. If not provided, a temporary stage will be created and dropped automatically.",
        show_default=False,
    ),
    build_type: str = typer.Option(
        "image",
        "--build-type",
        help="Type of build to execute: 'image' (default) or 'app'.",
        show_default=True,
    ),
    validation_profile: Optional[str] = typer.Option(
        None,
        "--validation-profile",
        help=(
            "Validation profile for image builds (e.g. ML_JOB, NOTEBOOK). "
            "Selects the pre-baked ruleset the image builder runs before publish. "
            "Ignored for --build-type app. When omitted, no image validation is requested."
        ),
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """
    Builds an image or app artifact using the Snowflake remote build REST API.

    This command is hidden by default. To make it visible in help output, enable the
    feature flag in your config.toml:
    [cli.features]
    enable_spcs_remote_build = true

    Or set the environment variable:
    export SNOWFLAKE_CLI_FEATURES_ENABLE_SPCS_REMOTE_BUILD=true

    Unlike ``build-image`` (which runs an ``EXECUTE JOB SERVICE`` system function),
    this command calls the GS REST API directly:

    - ``POST /api/v2/remote-build/execute`` to submit the build
    - ``GET  /api/v2/remote-build/jobs/<name>`` to poll status

    **Build types:**

    - ``--build-type image`` (default): builds an OCI container image and pushes it to an IMAGE REPOSITORY.
      Equivalent to ``SYSTEM$SPCS_TEST_REMOTE_BUILD``.
    - ``--build-type app``: builds an application tarball and uploads it to an ARTIFACT REPOSITORY.
      ``--location`` (ARTIFACT REPOSITORY) is required. Equivalent to ``SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO``.

    **Required account parameters (must be set to 'enable'):**

    - ``ENABLE_SNOW_API_FOR_REMOTE_BUILD`` — gates the REST API endpoints (all build types).
    - ``ENABLE_SPCS_RUNTIME_IMAGE_BUILDER_FUNCTIONS`` — gates image builds (``--build-type image``).
    - ``ENABLE_SPCS_RUNTIME_APP_BUILDER_FUNCTIONS`` — gates app/tarball builds (``--build-type app``).

    On qualification and test deployments all three parameters default to ``true``.
    On a standard account you must explicitly enable the relevant parameters before this command works.
    """
    if build_type not in ("image", "app"):
        raise CliArgumentError(
            f"Invalid build type '{build_type}'. Must be 'image' or 'app'."
        )

    # For app builds, location (ARTIFACT REPOSITORY) is required.
    if build_type == "app" and not location:
        raise CliArgumentError(
            "--location is required for app builds. "
            "Provide the fully-qualified ARTIFACT REPOSITORY name (db.schema.repo)."
        )

    # Verify build context directory has the expected entry point.
    if build_type == "image":
        dockerfile_path = build_context_dir / "Dockerfile"
        if not dockerfile_path.exists() or not dockerfile_path.is_file():
            raise CliArgumentError(
                f"Dockerfile not found in build context directory '{build_context_dir}'. "
                f"Expected to find: {dockerfile_path}"
            )

    # Validate name (image name or artifact package name) when provided.
    if name and not name.replace("_", "").replace("-", "").replace(".", "").isalnum():
        raise CliArgumentError(
            f"Invalid name '{name}'. Must contain only alphanumeric characters, hyphens, underscores, and dots."
        )

    if (
        image_tag
        and not image_tag.replace("_", "").replace("-", "").replace(".", "").isalnum()
    ):
        raise CliArgumentError(
            f"Invalid image tag '{image_tag}'. Must contain only alphanumeric characters, hyphens, underscores, and dots."
        )

    # Generate a unique identifier for this build operation (used for stage naming only;
    # the server assigns its own job name independently).
    build_uuid = uuid.uuid4().hex[:8]
    local_job_name = f"remote_build_{build_uuid}"

    stage_manager = StageManager()
    if stage is None:
        use_temporary_stage = True
        stage = f"{local_job_name}_stage"
        cli_console.step(f"Creating temporary stage: {stage}")
        stage_fqn = FQN.from_string(stage).using_context()
        stage_manager.create(fqn=stage_fqn)
    else:
        use_temporary_stage = False
        stage_fqn = FQN.from_string(stage)
        cli_console.step(f"Using existing stage: {stage_fqn.identifier}")

    # Upload build context to stage
    build_context_stage_path = f"build_contexts/{local_job_name}"
    cli_console.step(
        f"Uploading build context from {build_context_dir} to @{stage_fqn.identifier}/{build_context_stage_path}"
    )

    stage_path = StagePath.from_stage_str(
        f"@{stage_fqn.identifier}/{build_context_stage_path}"
    )
    # Derive build_source from the same StagePath used for the upload so the server
    # always reads from the location we actually wrote to (quoted identifiers, @/slash
    # normalization, etc.).
    build_source = stage_path.absolute_path()

    # Upload and submit are wrapped together so that any failure — including a mid-upload
    # error from put_recursive — triggers stage cleanup before re-raising.
    remote_build_manager = RemoteBuildManager()
    try:
        for _ in stage_manager.put_recursive(
            local_path=build_context_dir,
            stage_path=str(stage_path),
            overwrite=True,
        ):
            pass

        # Submit the build via the GS REST API
        cli_console.step("Submitting remote build via REST API...")
        assigned_job_name = remote_build_manager.create_remote_builder(
            build_source=build_source,
            location=location,
            name=name,
            image_tag=image_tag,
            project_type=project_type,
            compute_pool=compute_pool,
            build_type=build_type,
            validation_profile=validation_profile,
        )
    except ClickException:
        # CliError / ClickException already carry a user-friendly message; clean up then
        # re-raise as-is.
        _cleanup_stage(
            use_temporary_stage,
            stage,
            stage_fqn,
            build_context_stage_path,
            stage_manager,
        )
        raise
    except Exception as exc:
        # Unexpected errors (e.g. network failure during upload) — clean up, then surface
        # as a CliError so the CLI can print the message and exit non-zero cleanly.
        _cleanup_stage(
            use_temporary_stage,
            stage,
            stage_fqn,
            build_context_stage_path,
            stage_manager,
        )
        raise CliError(f"Remote build failed: {exc}") from exc

    cli_console.step(f"Build job submitted: {assigned_job_name}")
    cli_console.message(
        f"Waiting for job to complete (polling REST every "
        f"{_REMOTE_BUILD_POLL_INTERVAL_SECONDS}s; streaming logs via SQL once available)..."
    )
    cli_console.message("")

    try:
        final_status, interrupted = _wait_for_remote_build_completion(
            remote_build_manager=remote_build_manager,
            service_manager=ServiceManager(),
            job_name=assigned_job_name,
        )
    except RemoteBuildPermanentError:
        # Non-retryable REST failure (see error below) — e.g. auth, a disabled feature
        # flag, or a bad request. The job was already submitted and may still be
        # running server-side, so don't touch the stage; point the user at
        # remote-build-status once the underlying issue is resolved.
        cli_console.warning(
            f"\nRemote build job '{assigned_job_name}' was already submitted; its status "
            "could not be confirmed (see error below)."
        )
        cli_console.message(
            "Once resolved, check its status with: "
            f"snow spcs service remote-build-status --job-name {assigned_job_name}"
        )
        if use_temporary_stage:
            cli_console.warning(
                f"Remember to manually clean up stage: DROP STAGE {stage_fqn.sql_identifier};"
            )
        else:
            cli_console.warning(
                f"Remember to manually clean up build context: REMOVE @{stage_fqn.identifier}/{build_context_stage_path};"
            )
        raise

    if interrupted:
        cli_console.warning(
            f"\nRemote build job '{assigned_job_name}' is still running in the background."
        )
        cli_console.message(
            f"Use 'snow spcs service remote-build-status --job-name {assigned_job_name}' to check its progress."
        )
        cli_console.message(
            f"Use 'snow spcs service logs {assigned_job_name} --container-name main --instance-id 0' to view logs."
        )
        if use_temporary_stage:
            cli_console.warning(
                f"Remember to manually clean up stage: DROP STAGE {stage_fqn.sql_identifier};"
            )
        else:
            cli_console.warning(
                f"Remember to manually clean up build context: REMOVE @{stage_fqn.identifier}/{build_context_stage_path};"
            )
        return MessageResult(
            f"Remote build job '{assigned_job_name}' is running in the background."
        )

    final_job_status = final_status.job_status if final_status is not None else None

    cli_console.message("")
    if final_job_status == RemoteBuildStatus.DONE:
        cli_console.message(
            f"✓ Remote build job '{assigned_job_name}' completed successfully."
        )
    elif final_job_status == RemoteBuildStatus.FAILED:
        cli_console.warning(f"✗ Remote build job '{assigned_job_name}' failed.")
    elif final_job_status == RemoteBuildStatus.CANCELLED:
        cli_console.warning(f"✗ Remote build job '{assigned_job_name}' was cancelled.")

    # Only clean up the stage once the job is in a terminal state. If we couldn't
    # confirm a terminal status (e.g. the overall wait budget elapsed first), the build
    # may still be running server-side and the stage must be kept so the job can read
    # its build context.
    if final_status is not None and final_status.is_terminal:
        _cleanup_stage(
            use_temporary_stage,
            stage,
            stage_fqn,
            build_context_stage_path,
            stage_manager,
        )
        if final_job_status == RemoteBuildStatus.DONE:
            return MessageResult(
                f"Remote build job '{assigned_job_name}' completed successfully."
            )
        # FAILED or CANCELLED — non-zero exit so automation can detect the failure.
        raise CliError(
            f"Remote build job '{assigned_job_name}' finished with status: {final_job_status}."
        )
    else:
        # Could not confirm terminal status — stage is kept, non-zero exit so the caller
        # knows the outcome is unconfirmed and should not proceed as if the build succeeded.
        stage_hint = (
            f"DROP STAGE {stage_fqn.sql_identifier};"
            if use_temporary_stage
            else f"REMOVE @{stage_fqn.identifier}/{build_context_stage_path};"
        )
        raise CliError(
            f"Remote build job '{assigned_job_name}' did not reach a terminal state within the "
            f"{_REMOTE_BUILD_OVERALL_WAIT_SECONDS}s wait budget "
            f"(last known status: {final_job_status!r}). The job may still be running.\n"
            f"  • Check status: snow spcs service remote-build-status --job-name {assigned_job_name}\n"
            f"  • Clean up stage manually once done: {stage_hint}"
        )


@app.command(
    "remote-build-status",
    requires_connection=True,
    hidden=not FeatureFlag.ENABLE_SPCS_REMOTE_BUILD.is_enabled(),
)
def remote_build_status(
    job_name: str = typer.Option(
        ...,
        "--job-name",
        help="Fully-qualified name of the remote build job to look up (e.g. MYDB.PUBLIC.SPCS_IMAGE_BUILDER_JOB_abc123).",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """
    Displays the current status of a remote build job.

    Looks up the job in the live service store first; falls back to the 30-day job history
    for completed jobs.

    This command is hidden by default. Enable it with:

        [cli.features]
        enable_spcs_remote_build = true
    """
    manager = RemoteBuildManager()
    job = manager.get_remote_builder(job_name)
    if job is None:
        return MessageResult(f"No remote build job found with name '{job_name}'.")
    return ObjectResult(
        {
            "job_name": job.job_name,
            "status": job.job_status,
            "creation_time": job.creation_time,
            "end_time": job.end_time,
        }
    )


@app.command(
    "remote-build-history",
    requires_connection=True,
    hidden=not FeatureFlag.ENABLE_SPCS_REMOTE_BUILD.is_enabled(),
)
def remote_build_history(
    page_size: int = typer.Option(
        50,
        "--page-size",
        help="Number of jobs to request per server page (1–200).",
        show_default=True,
        min=1,
        max=200,
    ),
    start_token: Optional[str] = typer.Option(
        None,
        "--start-token",
        help=(
            "Resume from a specific page token (printed by a prior capped run as "
            "'Resume with: --start-token <token>'). "
            "When omitted, starts from the most recent job and walks back ~30 days."
        ),
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """
    Lists all remote build jobs for the current account from the past ~30 days,
    newest first.

    Automatically follows pagination tokens until the server reports no further results,
    collecting every page into a single table.  Use --page-size to control how many records
    are requested per round-trip (default 50).

    To resume from a known point, pass the token printed by a previous interrupted run via
    --start-token.

    This command is hidden by default. Enable it with:

        [cli.features]
        enable_spcs_remote_build = true
    """
    manager = RemoteBuildManager()
    all_rows: list[dict] = []
    token: Optional[str] = start_token

    # Lower bound: jobs created before this point are outside the 30-day window.
    _thirty_days_ms = 30 * 24 * 60 * 60 * 1000
    cutoff_ms = int(time.time() * 1000) - _thirty_days_ms

    pages_fetched = 0

    while True:
        response = manager.list_remote_build_jobs(limit=page_size, page_token=token)
        pages_fetched += 1
        page_jobs = response.get("jobs", [])
        for j in page_jobs:
            all_rows.append(
                {
                    "job_name": j.get("job_name", ""),
                    "status": j.get("job_status", ""),
                    "creation_time": j.get("creation_time", ""),
                    "end_time": j.get("end_time", ""),
                }
            )
        token = response.get("next_page_token")
        if not token:
            break
        if pages_fetched >= _REMOTE_BUILD_HISTORY_MAX_PAGES:
            cli_console.warning(
                f"Stopped after {_REMOTE_BUILD_HISTORY_MAX_PAGES} pages to avoid an unbounded loop. "
                f"Resume with: --start-token {token}"
            )
            break
        # The server page token is Base64url(str(createdOnMs)).
        # If the token can't be decoded, treat it as untrustworthy and stop — continuing
        # with an opaque token could loop forever if the server is misbehaving.
        token_ms = _decode_history_token(token)
        if token_ms is None:
            cli_console.warning(
                "Received an unrecognised page token from the server; stopping pagination."
            )
            break
        # If the next window's end already lies past our 30-day horizon there can be no
        # remaining jobs within the retention window — stop here.
        if token_ms < cutoff_ms:
            break

    if not all_rows:
        return MessageResult("No remote build jobs found in the past 30 days.")

    return CollectionResult(all_rows)


class _InterruptFlag:
    """Mutable flag set by a SIGINT handler so a polling loop can detect Ctrl+C
    reliably on every iteration.

    This is necessary instead of a plain ``except KeyboardInterrupt`` around the
    loop because ``ServiceManager.stream_logs`` catches and swallows
    ``KeyboardInterrupt`` internally (by design, so that ``snow spcs service logs
    --follow`` can exit its tail loop cleanly) — that exception would never reach
    a caller-side handler wrapping a call into it. Installing our own SIGINT
    handler intercepts the signal before Python turns it into a KeyboardInterrupt
    exception at all, so Ctrl+C is detected the same way regardless of which part
    of the wait loop happens to be executing (REST call, SQL log fetch, sleep).
    """

    def __init__(self) -> None:
        self.is_set = False

    def handler(self, signum, frame) -> None:  # noqa: ARG002
        self.is_set = True


def _wait_for_remote_build_completion(
    remote_build_manager: RemoteBuildManager,
    service_manager: ServiceManager,
    job_name: str,
) -> tuple[Optional[RemoteBuildJobStatus], bool]:
    """
    Waits for a submitted remote build job to reach a terminal status.

    This is a single consolidated wait loop. It replaces an earlier design with three
    independently-timed phases (a REST pending-poll, a SQL-readiness sub-loop, and a
    post-streaming fallback poll) which proved hard to get right: each phase had its own
    timeout and its own error handling, and fixing an edge case in one phase repeatedly
    surfaced a new edge case in another (e.g. Ctrl+C only being honored in one phase,
    a timeout in one phase silently succeeding, transient REST errors aborting the
    command outright).

    Design:

    - REST status (via ``get_remote_builder``) is the single, authoritative source of
      truth for job completion. Transient REST errors (network blips, unclassified/5xx
      responses) are retried in place rather than aborting the whole command — the loop
      keeps going even after several consecutive failures, it just stops polling REST
      that iteration and relies on the last known status. Definitive, non-retryable
      failures (auth, bad request, not found, disabled feature flag — raised as
      ``RemoteBuildPermanentError``) are *not* retried: they propagate immediately so
      the command fails fast instead of burning the whole wait budget on an outcome
      that cannot change.
    - Log streaming is opportunistic/best-effort: once the service becomes describable
      via SQL, new log lines are fetched and printed each iteration. SQL availability
      never gates completion detection — the loop still relies solely on REST for that.
    - A single overall timeout budget covers the whole wait (submission acceptance
      through terminal status), rather than several stacked, separately-timed phases.
    - Ctrl+C is detected via a SIGINT flag (see ``_InterruptFlag``) checked once per
      iteration, so it is honored uniformly no matter which part of the loop body is
      currently executing.

    Raises:
        RemoteBuildPermanentError: if ``get_remote_builder`` reports a definitive,
            non-retryable failure. Callers should treat the job as submitted-but-
            unconfirmed (do not clean up the stage) rather than as failed.

    Returns:
        A tuple of ``(final_status, interrupted)``:
          - ``final_status``: the last known :class:`RemoteBuildJobStatus`. Its
            ``is_terminal`` property is ``True`` if a terminal status (``DONE``/
            ``FAILED``/``CANCELLED``) was observed before the timeout; otherwise it
            reflects the last observed non-terminal status, or is ``None`` if the job
            was never observed at all.
          - ``interrupted``: ``True`` if the user pressed Ctrl+C during the wait.
    """
    interrupt_flag = _InterruptFlag()
    previous_handler = signal.signal(signal.SIGINT, interrupt_flag.handler)

    current_status: Optional[RemoteBuildJobStatus] = None
    consecutive_rest_errors = 0
    since_timestamp = ""
    prev_log_records: List[str] = []
    elapsed = 0

    try:
        while elapsed < _REMOTE_BUILD_OVERALL_WAIT_SECONDS:
            if interrupt_flag.is_set:
                return current_status, True

            try:
                job_info = remote_build_manager.get_remote_builder(job_name)
                consecutive_rest_errors = 0
            except RemoteBuildPermanentError:
                # A definitive, non-retryable failure (auth, bad request, not found,
                # disabled feature flag) — retrying for the length of the wait budget
                # would just waste time on an outcome that will never change. Let it
                # propagate immediately so the command fails fast.
                raise
            except Exception as exc:
                consecutive_rest_errors += 1
                cli_console.warning(
                    f"REST status check failed ({exc}); retrying "
                    f"({consecutive_rest_errors}/{_REMOTE_BUILD_MAX_CONSECUTIVE_REST_ERRORS})..."
                )
                job_info = None
                if consecutive_rest_errors == _REMOTE_BUILD_MAX_CONSECUTIVE_REST_ERRORS:
                    cli_console.warning(
                        f"REST status check has failed {consecutive_rest_errors} times in a "
                        "row; will keep waiting, but status updates may be stale."
                    )

            if job_info is not None:
                if (
                    current_status is None
                    or job_info.job_status != current_status.job_status
                ):
                    cli_console.message(f"Current job status: {job_info.job_status}")
                current_status = job_info
                if job_info.is_terminal:
                    return current_status, False
            elif consecutive_rest_errors == 0:
                cli_console.message("Waiting for job to appear...")

            # Opportunistically stream any new log lines via SQL. This is best-effort: the
            # service may not be describable yet (e.g. still PENDING, or the SQL plane
            # briefly lagging the REST-observed state) — any failure here is silently
            # ignored and retried next iteration. REST remains authoritative for
            # completion regardless of whether this succeeds.
            try:
                raw_log_blocks = list(
                    service_manager.logs(
                        service_name=job_name,
                        instance_id="0",
                        container_name="main",
                        num_lines=1000,
                        since_timestamp=since_timestamp,
                        include_timestamps=True,
                    )
                )
                new_log_records = [
                    line
                    for block in raw_log_blocks
                    for line in block.split("\n")
                    if line.strip()
                ]
                if new_log_records:
                    dedup_records = new_logs_only(prev_log_records, new_log_records)
                    if dedup_records:
                        for log in dedup_records:
                            cli_console.message(
                                filter_log_timestamp(log, include_timestamps=False)
                            )
                        since_timestamp = dedup_records[-1].split(" ", 1)[0]
                        prev_log_records = dedup_records
            except Exception:
                pass

            if interrupt_flag.is_set:
                return current_status, True

            time.sleep(_REMOTE_BUILD_POLL_INTERVAL_SECONDS)
            elapsed += _REMOTE_BUILD_POLL_INTERVAL_SECONDS

        return current_status, False
    finally:
        signal.signal(signal.SIGINT, previous_handler)


def _decode_history_token(token: str) -> Optional[int]:
    """Decode a server-issued history page token to a millisecond UTC timestamp.

    The server encodes page tokens as Base64url(str(createdOnMs)).  Returns None
    if the token cannot be parsed, in which case the caller should not apply the
    timestamp guard.
    """
    try:
        # urlsafe_b64decode requires padding; add it back before decoding.
        padded = token + "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(padded).decode()
        return int(raw)
    except Exception:
        return None


def _cleanup_stage(
    use_temporary_stage: bool,
    stage: str,
    stage_fqn: FQN,
    build_context_stage_path: str,
    stage_manager: StageManager,
) -> None:
    """Clean up stage resources after a build (temporary or customer-provided)."""
    if use_temporary_stage:
        cli_console.step(f"Cleaning up stage: {stage}")
        try:
            object_manager = ObjectManager()
            object_manager.drop(object_type="stage", fqn=stage_fqn, if_exists=True)
            cli_console.message(f"✓ Dropped stage {stage}")
        except ProgrammingError as e:
            cli_console.warning(f"Failed to clean up stage: {e}")
    else:
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
