import typer

from snowcli.cli.common.alias import build_alias
from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark.compute_pool.manager import ComputePoolManager
from snowcli.output.decorators import with_output
from snowcli.output.printing import OutputData

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="compute-pool",
    help="Manage compute pools. You can also use cp as alias for this command",
)


@app.command()
@with_output
@global_options_with_connection
def create(
    name: str = typer.Option(..., "--name", "-n", help="Compute pool name"),
    num_instances: int = typer.Option(..., "--num", "-d", help="Number of instances"),
    instance_family: str = typer.Option(..., "--family", "-f", help="Instance family"),
    **options,
) -> OutputData:
    """
    Create compute pool
    """
    cursor = ComputePoolManager().create(
        pool_name=name, num_instances=num_instances, instance_family=instance_family
    )
    return OutputData.from_cursor(cursor)


@app.command()
@with_output
@global_options_with_connection
def list(**options) -> OutputData:
    """
    List compute pools
    """
    cursor = ComputePoolManager().show()
    return OutputData.from_cursor(cursor)


@app.command()
@with_output
@global_options_with_connection
def drop(
    name: str = typer.Argument(..., help="Compute Pool Name"), **options
) -> OutputData:
    """
    Drop compute pool
    """
    cursor = ComputePoolManager().drop(pool_name=name)
    return OutputData.from_cursor(cursor)


@app.command()
@with_output
@global_options_with_connection
def stop(
    name: str = typer.Argument(..., help="Compute Pool Name"), **options
) -> OutputData:
    """
    Stop and delete all services running on Compute Pool
    """
    cursor = ComputePoolManager().stop(pool_name=name)
    return OutputData.from_cursor(cursor)


app_cp = build_alias(
    app,
    name="cp",
    help_str="Manage compute pools. This is alias for compute-pool command",
)
