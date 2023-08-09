import typer

from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS

app: typer.Typer = typer.Typer(
    name="coverage", context_settings=DEFAULT_CONTEXT_SETTINGS
)

from snowcli.cli.snowpark.procedure_coverage import clear, report
