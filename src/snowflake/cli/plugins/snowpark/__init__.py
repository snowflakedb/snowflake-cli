from snowflake.cli.plugins.snowpark.commands import app
from snowflake.cli.plugins.snowpark.package.commands import app as package_app

app.add_typer(package_app)
