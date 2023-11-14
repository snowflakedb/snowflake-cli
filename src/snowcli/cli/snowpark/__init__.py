import typer

from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark.function.commands import app as function_app
from snowcli.cli.snowpark.package.commands import app as package_app
from snowcli.cli.snowpark.procedure.commands import app as procedure_app

app = typer.Typer(
    name="snowpark",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manages functions, procedures, and Snowpark objects.",
)

app.add_typer(function_app)  # type: ignore
app.add_typer(package_app)  # type: ignore
app.add_typer(procedure_app)  # type: ignore
