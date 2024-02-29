import pytest
from snowflake.cli.api.commands.flags import (
    PLAIN_PASSWORD_MSG,
    OverrideableOption,
    PasswordOption,
)
from typer import Typer
from typer.testing import CliRunner


def test_format(runner, snapshot):
    result = runner.invoke(
        ["object", "stage", "list", "stage_name", "--format", "invalid_format"]
    )

    assert result.output == snapshot


def test_password_flag():
    app = Typer()

    @app.command()
    def _(password: str = PasswordOption):
        return "ok"

    runner = CliRunner()
    result = runner.invoke(app, ["--password", "dummy"], catch_exceptions=False)
    assert result.exit_code == 0
    assert PLAIN_PASSWORD_MSG in result.output


def test_overrideable_option_is_overrideable(snapshot):
    original = OverrideableOption(1, "--option", help="original help")
    app = Typer()

    @app.command()
    def _(option: int = OverrideableOption(default=2, help="new help")):
        return "ok"

    runner = CliRunner()
    result = runner.invoke(app, ["--help"], catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output == snapshot


@pytest.mark.parametrize("set1, set2", [(False, False), (False, True), (True, False)])
def test_mutually_exclusive_options_no_error(set1, set2):
    option1 = OverrideableOption(
        False, "--option1", mutually_exclusive=["option_1, option_2"]
    )
    option2 = OverrideableOption(
        False, "--option2", mutually_exclusive=["option_1, option_2"]
    )
    app = Typer()

    @app.command()
    def _(option_1: bool = option1(), option_2: bool = option2()):
        return "ok"

    command = []
    if set1:
        command.append("--option1")
    if set2:
        command.append("--option2")
    runner = CliRunner()
    result = runner.invoke(app, command)
    print(result.output)
    assert result.exit_code == 0


def test_mutually_exclusive_options_error(snapshot):
    option1 = OverrideableOption(
        False, "--option1", mutually_exclusive=["option_1", "option_2"]
    )
    option2 = OverrideableOption(
        False, "--option2", mutually_exclusive=["option_1", "option_2"]
    )
    app = Typer()

    @app.command()
    def _(option_1: bool = option1(), option_2: bool = option2()):
        return "ok"

    command = ["--option1", "--option2"]
    runner = CliRunner()
    result = runner.invoke(app, command)
    assert result.exit_code == 1
    assert result.output == snapshot


def test_overrideable_option_invalid_callback_signature():
    pass
