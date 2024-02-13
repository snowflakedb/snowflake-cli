import sys
from pathlib import Path
from typing import List, Optional

import typer
from click import ClickException
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import (
    CommandResult,
    QueryJsonValueResult,
    QueryResult,
    SingleQueryResult,
)
from snowflake.cli.api.project.util import is_valid_object_name
from snowflake.cli.plugins.object.common import CommentOption, Tag, TagOption
from snowflake.cli.plugins.spcs.common import (
    print_log_lines,
    validate_and_set_instances,
)
from snowflake.cli.plugins.spcs.services.manager import ServiceManager

app = SnowTyper(
    name="service",
    help="Manages Snowpark services.",
)


def _service_name_callback(name: str) -> str:
    if not is_valid_object_name(name, 2):
        raise ClickException(f"'{name}' is not a valid service name.")
    return name


ServiceNameArgument = typer.Argument(
    ..., help="Name of the service.", callback=_service_name_callback
)

SpecPathOption = typer.Option(
    ...,
    "--spec-path",
    help="Path to service specification file.",
    file_okay=True,
    dir_okay=False,
    exists=True,
)


@app.command(requires_connection=True)
def create(
    name: str = ServiceNameArgument,
    compute_pool: str = typer.Option(
        ..., "--compute-pool", help="Compute pool to run the service on."
    ),
    spec_path: Path = SpecPathOption,
    min_instances: int = typer.Option(
        1, "--min-instances", help="Minimum number of service instances to run."
    ),
    max_instances: Optional[int] = typer.Option(
        None, "--max-instances", help="Maximum number of service instances to run."
    ),
    auto_resume: bool = typer.Option(
        True,
        "--auto-resume/--no-auto-resume",
        help="The service will automatically resume when a service function or ingress is called.",
    ),
    external_access_integrations: Optional[List[str]] = typer.Option(
        None,
        "--eai-name",
        help="Identifies External Access Integrations(EAI) that the service can access. This option may be specified multiple times for multiple EAIs.",
    ),
    query_warehouse: Optional[str] = typer.Option(
        None,
        "--query-warehouse",
        help="Warehouse to use if a service container connects to Snowflake to execute a query without explicitly specifying a warehouse to use.",
    ),
    tags: Optional[List[Tag]] = TagOption(help="Tag for the service."),
    comment: Optional[str] = CommentOption(help="Comment for the service."),
    **options,
) -> CommandResult:
    """
    Creates a new Snowpark Container Services service in the current schema.
    """
    max_instances = validate_and_set_instances(
        min_instances, max_instances, "instances"
    )
    cursor = ServiceManager().create(
        service_name=name,
        min_instances=min_instances,
        max_instances=max_instances,
        compute_pool=compute_pool,
        spec_path=spec_path,
        external_access_integrations=external_access_integrations,
        auto_resume=auto_resume,
        query_warehouse=query_warehouse,
        tags=tags,
        comment=comment,
    )
    return SingleQueryResult(cursor)


@app.command(requires_connection=True)
def status(name: str = ServiceNameArgument, **options) -> CommandResult:
    """
    Retrieves status of a Snowpark Container Services service.
    """
    cursor = ServiceManager().status(service_name=name)
    return QueryJsonValueResult(cursor)


@app.command(requires_connection=True)
def logs(
    name: str = ServiceNameArgument,
    container_name: str = typer.Option(
        ..., "--container-name", help="Name of the container."
    ),
    instance_id: str = typer.Option(..., "--instance-id", help="Instance Id"),
    num_lines: int = typer.Option(500, "--num-lines", help="Number of lines"),
    **options,
):
    """
    Retrieves local logs from a Snowpark Container Services service container.
    """
    results = ServiceManager().logs(
        service_name=name,
        instance_id=instance_id,
        container_name=container_name,
        num_lines=num_lines,
    )
    cursor = results.fetchone()
    logs = next(iter(cursor)).split("\n")
    print_log_lines(sys.stdout, name, "0", logs)


@app.command(requires_connection=True)
def upgrade(
    name: str = ServiceNameArgument,
    spec_path: Path = SpecPathOption,
    **options,
):
    """
    Updates an existing service with a new specification file.
    """
    return SingleQueryResult(
        ServiceManager().upgrade_spec(service_name=name, spec_path=spec_path)
    )


@app.command("list-endpoints", requires_connection=True)
def list_endpoints(name: str = ServiceNameArgument, **options):
    """
    Lists the endpoints in a Snowpark Container Services service.
    """
    return QueryResult(ServiceManager().list_endpoints(service_name=name))
