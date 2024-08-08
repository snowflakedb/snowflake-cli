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

import sys
from pathlib import Path
from typing import List, Optional

import typer
from click import ClickException
from snowflake.cli._plugins.object.command_aliases import (
    add_object_command_aliases,
    scope_option,
)
from snowflake.cli._plugins.object.common import CommentOption, Tag, TagOption
from snowflake.cli._plugins.spcs.common import (
    print_log_lines,
    validate_and_set_instances,
)
from snowflake.cli._plugins.spcs.services.manager import ServiceManager
from snowflake.cli.api.commands.flags import (
    IfNotExistsOption,
    OverrideableOption,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CommandResult,
    QueryJsonValueResult,
    QueryResult,
    SingleQueryResult,
)
from snowflake.cli.api.project.util import is_valid_object_name

app = SnowTyperFactory(
    name="service",
    help="Manages Snowpark Container Services services.",
    short_help="Manages services.",
)


def _service_name_callback(name: FQN) -> FQN:
    if not is_valid_object_name(name.identifier, max_depth=2, allow_quoted=False):
        raise ClickException(
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
        help="Identifies External Access Integrations(EAI) that the service can access. This option may be specified multiple times for multiple EAIs.",
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
        help="Identifies External Access Integrations(EAI) that the job service can access. This option may be specified multiple times for multiple EAIs.",
    ),
    query_warehouse: Optional[str] = QueryWarehouseOption(),
    comment: Optional[str] = CommentOption(help=_COMMENT_HELP),
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
    )
    return SingleQueryResult(cursor)


@app.command(requires_connection=True)
def status(name: FQN = ServiceNameArgument, **options) -> CommandResult:
    """
    Retrieves the status of a service.
    """
    cursor = ServiceManager().status(service_name=name.identifier)
    return QueryJsonValueResult(cursor)


@app.command(requires_connection=True)
def logs(
    name: FQN = ServiceNameArgument,
    container_name: str = typer.Option(
        ...,
        "--container-name",
        help="Name of the container.",
        show_default=False,
    ),
    instance_id: str = typer.Option(
        ...,
        "--instance-id",
        help="ID of the service instance, starting with 0.",
        show_default=False,
    ),
    num_lines: int = typer.Option(
        500, "--num-lines", help="Number of lines to retrieve."
    ),
    **options,
):
    """
    Retrieves local logs from a service container.
    """
    results = ServiceManager().logs(
        service_name=name.identifier,
        instance_id=instance_id,
        container_name=container_name,
        num_lines=num_lines,
    )
    cursor = results.fetchone()
    logs = next(iter(cursor)).split("\n")
    print_log_lines(sys.stdout, name, "0", logs)


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
        comment=comment,
    )
    return SingleQueryResult(cursor)
