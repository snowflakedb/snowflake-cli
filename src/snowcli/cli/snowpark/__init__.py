import typer

from snowcli.cli.snowpark.function import app as function_app
from snowcli.cli.snowpark.package import app as package_app
from snowcli.cli.snowpark.procedure import app as procedure_app

app = typer.Typer(
    name="snowpark",
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Manage functions, procedures and Snowpark objects",
)

app.add_typer(function_app)
app.add_typer(package_app)
app.add_typer(procedure_app)
