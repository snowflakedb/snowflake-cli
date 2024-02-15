from typer import Typer

from snowflake.cli.api.commands.flags import PLAIN_PASSWORD_MSG, PasswordOption
from typer.testing import CliRunner


def test_format(runner):
    result = runner.invoke(
        ["object", "stage", "list", "stage_name", "--format", "invalid_format"]
    )

    assert result.output == (
        """Usage: default object stage list [OPTIONS] STAGE_NAME
Try 'default object stage list --help' for help.
╭─ Error ──────────────────────────────────────────────────────────────────────╮
│ Invalid value for '--format': 'invalid_format' is not one of 'TABLE',        │
│ 'JSON'.                                                                      │
╰──────────────────────────────────────────────────────────────────────────────╯
"""
    )


def test_password_flag():
    app = Typer()

    @app.command()
    def _(password: str = PasswordOption):
        return "ok"

    runner = CliRunner()
    result = runner.invoke(app, ["--password", "dummy"], catch_exceptions=False)
    assert result.exit_code == 0
    assert PLAIN_PASSWORD_MSG in result.output
