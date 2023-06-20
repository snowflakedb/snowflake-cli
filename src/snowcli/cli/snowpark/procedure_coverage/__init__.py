from snowcli.cli.common.snow_cli_typer import SnowCliTyper
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS

app: SnowCliTyper = SnowCliTyper(
    name="coverage", context_settings=DEFAULT_CONTEXT_SETTINGS
)

from snowcli.cli.snowpark.procedure_coverage import clear, report
