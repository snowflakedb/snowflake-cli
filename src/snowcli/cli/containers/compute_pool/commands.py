import typer
from snowcli.api.output.decorators import with_output
from snowcli.api.output.types import CommandResult, SingleQueryResult
from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.containers.compute_pool.manager import ComputePoolManager

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="pool",
    help="Manages compute pools.",
)


@app.command()
@with_output
@global_options_with_connection
def create(
    name: str = typer.Option(..., "--name", help="Name of the compute pool."),
    num_instances: int = typer.Option(
        ..., "--num", help="Number of compute pool instances."
    ),
    instance_family: str = typer.Option(
        ...,
        "--family",
        help="Name of the instance family. For more information about instance families, refer to the SQL CREATE COMPUTE POOL command.",
    ),
    **options,
) -> CommandResult:
    """
    Creates a compute pool with a specified number of instances.
    """
    cursor = ComputePoolManager().create(
        pool_name=name, num_instances=num_instances, instance_family=instance_family
    )
    return SingleQueryResult(cursor)


@app.command()
@with_output
@global_options_with_connection
def stop(
    name: str = typer.Argument(..., help="Name of the compute pool."), **options
) -> CommandResult:
    """
    Stops a compute pool and deletes all services running on the pool.
    """
    cursor = ComputePoolManager().stop(pool_name=name)
    return SingleQueryResult(cursor)
