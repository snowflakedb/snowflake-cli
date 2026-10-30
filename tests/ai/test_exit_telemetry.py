from unittest import mock

import pytest
import typer
from snowflake.cli.api.commands.execution_metadata import ExecutionStatus
from snowflake.cli.api.commands.snow_typer import SnowTyper
from typer.testing import CliRunner


@pytest.mark.parametrize("code", [0, 7])
def test_explicit_exit_telemetry(code):
    app = SnowTyper()

    @app.command(requires_global_options=False)
    def finish():
        raise typer.Exit(code)

    with mock.patch.object(SnowTyper, "pre_execute"), mock.patch.object(
        SnowTyper, "post_execute"
    ) as post, mock.patch.object(
        SnowTyper, "exception_handler", side_effect=lambda error, execution: error
    ) as handler:
        result = CliRunner().invoke(app, [])
    assert result.exit_code == code
    assert post.call_args.args[0].status == (
        ExecutionStatus.SUCCESS if code == 0 else ExecutionStatus.FAILURE
    )
    assert handler.call_count == (0 if code == 0 else 1)
