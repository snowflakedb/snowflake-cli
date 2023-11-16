from snowcli.cli.snowpark.commands import app
from snowcli.cli.snowpark.package.commands import app as package_app
from snowcli.cli.snowpark.procedure_coverage.commands import (
    app as procedure_coverage_app,
)

app.add_typer(package_app)
app.add_typer(procedure_coverage_app)
