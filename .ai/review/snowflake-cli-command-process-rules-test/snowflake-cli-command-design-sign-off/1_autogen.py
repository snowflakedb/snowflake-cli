import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.api.plugins.command.interface import (
    CommandDef,
    CommandGroupSpec,
    ParamDef,
    ParamKind,
)

app = SnowTyperFactory(
    name="snapshots",
    help="Manages snapshots.",
)


@app.command(requires_connection=True)
def delete_snapshot(
    name: str = typer.Argument(..., help="Snapshot name"),
    retain_days: int = typer.Option(7, "--retain-days", help="Days to retain"),
):
    """Delete a snapshot."""
    return MessageResult(f"Deleting {name}")


@app.command("export")
def export_snapshot(
    name: str,
    fmt: str = typer.Option("json", "--format", "-f", help="Output format"),
):
    """Export a snapshot."""
    return MessageResult(f"export {name} as {fmt}")


ANALYTICS_SPEC = CommandGroupSpec(
    name="analytics",
    help="Run analytics queries.",
    parent_path=(),
    commands=(
        CommandDef(
            name="run",
            help="Run an analytics query.",
            handler_method="run",
            requires_connection=True,
            params=(
                ParamDef(
                    name="limit",
                    type=int,
                    kind=ParamKind.OPTION,
                    cli_names=("--limit", "-l"),
                    default=100,
                ),
            ),
            output_type="QueryResult",
        ),
    ),
)


def get_builtin_plugin_name_to_plugin_spec():
    from snowflake.cli._plugins.analytics import plugin_spec as analytics_plugin_spec
    from snowflake.cli._plugins.git import plugin_spec as git_plugin_spec

    return {
        "git": git_plugin_spec,
        "analytics": analytics_plugin_spec,
    }
