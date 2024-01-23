from typing import Optional

import typer
from snowflake.cli.api.commands.decorators import (
    global_options_with_connection,
    with_output,
)
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS
from snowflake.cli.api.output.types import CommandResult, SingleQueryResult
from snowflake.cli.plugins.containers.compute_pool.manager import ComputePoolManager
from snowflake.cli.plugins.object.common import comment_option

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
    auto_resume: bool = typer.Option(
        True,
        "--auto-resume/--no-auto-resume",
        help="The compute pool will automatically resume when a service or job is submitted to it.",
    ),
    initially_suspended: bool = typer.Option(
        False,
        "--init-suspend",
        help="The compute pool will start in a suspended state.",
    ),
    auto_suspend_secs: int = typer.Option(
        3600,
        "--auto-suspend-secs",
        help="Number of seconds of inactivity after which you want Snowflake to automatically suspend the compute pool.",
    ),
    comment: Optional[str] = comment_option("compute pool"),
    **options,
) -> CommandResult:
    """
    Creates a compute pool with a specified number of instances.
    """
    cursor = ComputePoolManager().create(
        pool_name=name,
        num_instances=num_instances,
        instance_family=instance_family,
        auto_resume=auto_resume,
        initially_suspended=initially_suspended,
        auto_suspend_secs=auto_suspend_secs,
        comment=comment,
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
