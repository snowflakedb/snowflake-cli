from snowcli.plugins.snowpark.commands import app
from snowcli.plugins.snowpark.package.commands import app as package_app

app.add_typer(package_app)
