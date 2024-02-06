"""These tests verify that the CLI runs work as expected."""
from __future__ import annotations

import json
import platform
import sys
import typing as t

from click import Command
from snowflake.cli.__about__ import VERSION
from snowflake.cli.app.cli_app import app_context_holder
from snowflake.connector.config_manager import CONFIG_MANAGER
from typer.core import TyperArgument, TyperOption

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
    mock_conn.return_value.execute_stream.return_value = [
        None,
        mock_cursor(["row"], []),
    ]
    result = runner.invoke_with_config_file(
        config_file,
        ["object", "list", "warehouse"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    mock_conn.assert_called_once_with(
        application="SNOWCLI.OBJECT.LIST",
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
        {"key": "default_config_file_path", "value": str(CONFIG_MANAGER.file_path)},
        {"key": "python_version", "value": sys.version},
        {"key": "system_info", "value": platform.platform()},
    ]


def test_docs_callback(runner):
    result = runner.invoke(["--docs"])
    assert result.exit_code == 0, result.output


def test_all_commands_have_proper_documentation(runner):
    # invoke any command to populate app context (plugins registration)
    runner.invoke("--help")

    ctx = app_context_holder.app_context
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

            long_options = []
            for param in command.params:
                is_argument = isinstance(param, TyperArgument)

                long_options = [opt for opt in param.opts if opt.startswith("--")]
                if not is_argument and len(long_options) == 0:
                    errors.append(
                        f"Command `snow {' '.join(path)}` is missing --long option for `{param.name}` option"
                    )

                if not param.help:  # type: ignore
                    if is_argument:
                        errors.append(
                            f"Command `snow {' '.join(path)}` is missing help for `{param.name}` argument"
                        )
                    else:
                        errors.append(
                            f"Command `snow {' '.join(path)}` is missing help for `{param.name}` option"
                        )

    _check(ctx.command)

    assert len(errors) == 0, "\n".join(errors)


def test_if_there_are_no_option_duplicates(runner):
    runner.invoke("--help")

    ctx = app_context_holder.app_context
    duplicates = {}

    def _check(command: Command, path: t.Optional[t.List] = None):
        path = path or ["snow"]

        if duplicated_params := check_options_for_duplicates(command.params):
            duplicates[" ".join(path)] = duplicated_params

        if hasattr(command, "commands"):
            for command_name, command_info in command.commands.items():
                _check(command_info, [*path, command_name])

    def check_options_for_duplicates(params: t.List[TyperOption]) -> t.Set[str]:
        RESERVED_FLAGS = ["--help"]  # noqa: N806

        flags = [flag for param in params for flag in param.opts]
        return set(
            [
                flag
                for flag in flags
                if (flags.count(flag) > 1 or flag in RESERVED_FLAGS)
            ]
        )

    _check(ctx.command)

    assert duplicates == {}, "\n".join(duplicates)
