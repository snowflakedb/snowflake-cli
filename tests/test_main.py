"""These tests verify that the CLI runs work as expected."""
from __future__ import annotations

import json
import typing as t

import click
from click import Command
from typer.core import TyperArgument
from typer.main import get_command

from snowcli.__about__ import VERSION
from snowcli.config import cli_config
from tests.testing_utils.fixtures import *


def test_help_option(runner):
    result = runner.invoke(["--help"])
    assert result.exit_code == 0


def test_streamlit_help(runner):
    result = runner.invoke(["streamlit", "--help"], catch_exceptions=False)
    assert result.exit_code == 0, result.output


@mock.patch("snowflake.connector.connect")
@mock.patch.dict(os.environ, {}, clear=True)
def test_custom_config_path(mock_conn, runner, mock_cursor):
    config_file = Path(__file__).parent / "test.toml"
    mock_conn.return_value.execute_string.return_value = [
        None,
        mock_cursor(["row"], []),
    ]
    result = runner.invoke(
        ["--config-file", str(config_file), "warehouse", "status"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    mock_conn.assert_called_once_with(
        application="SNOWCLI.WAREHOUSE.STATUS",
        database="db_for_test",
        schema="test_public",
        role="test_role",
        warehouse="xs",
        password="dummy_password",
    )


def test_info_callback(runner):
    result = runner.invoke(["--info"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == [
        {"key": "version", "value": VERSION},
        {"key": "default_config_file_path", "value": str(cli_config.file_path)},
    ]


def test_all_commands_has_proper_documentation():
    from snowcli.app.cli_app import app

    ctx = click.Context(get_command(app))
    errors = []

    def _check(command: Command, path: t.Optional[t.List] = None):
        path = path or []
        if hasattr(command, "commands"):
            for command_name, command_info in command.commands.items():
                _check(command_info, [*path, command_name])
        else:
            # This is end command
            if not command.help:
                errors.append(
                    f"Command `snow {' '.join(path)}` is missing help in docstring"
                )

            for param in command.params:
                if not param.help:  # type: ignore
                    if isinstance(param, TyperArgument):
                        errors.append(
                            f"Command `snow {' '.join(path)}` is missing help for `{param.name}` argument"
                        )
                    else:
                        errors.append(
                            f"Command `snow {' '.join(path)}` is missing help for `{param.name}` option"
                        )

    _check(ctx.command)

    assert len(errors) == 0, "\n".join(errors)
