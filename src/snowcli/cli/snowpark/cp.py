import typer
from snowcli import config
from snowcli.cli import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.common.alias import build_alias
from snowcli.cli.common.flags import ConnectionOption
from snowcli.config import connect_to_snowflake
from snowcli.output.printing import print_db_cursor

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="compute-pool",
    help="Manage compute pools. You can also use cp as alias for this command",
)


@app.command()
def create(
    environment: str = ConnectionOption,
    name: str = typer.Option(..., "--name", "-n", help="Compute pool name"),
    num_instances: int = typer.Option(..., "--num", "-d", help="Number of instances"),
    instance_family: str = typer.Option(..., "--family", "-f", help="Instance family"),
):
    """
    Create compute pool
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = conn.create_compute_pool(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
            num_instances=num_instances,
            instance_family=instance_family,
        )
        print_db_cursor(results)


@app.command()
def list(environment: str = ConnectionOption):
    """
    List compute pools
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = conn.list_compute_pools(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
        )
        print_db_cursor(results)


@app.command()
def drop(
    environment: str = ConnectionOption,
    name: str = typer.Argument(..., help="Compute Pool Name"),
):
    """
    Drop compute pool
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = conn.drop_compute_pool(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
        )
        print_db_cursor(results)


@app.command()
def stop(
    environment: str = ConnectionOption,
    name: str = typer.Argument(..., help="Compute Pool Name"),
):
    """
    Stop and delete all services running on Compute Pool
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = conn.stop_compute_pool(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
        )
        print_db_cursor(results)


app_cp = build_alias(
    app,
    name="cp",
    help_str="Manage compute pools. This is alias for compute-pool command",
)
