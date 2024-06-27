# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from functools import partial
from unittest import mock
from unittest.mock import MagicMock

import pytest
import typer
from snowflake.cli.api.commands.snow_typer import SnowTyper, SnowTyperFactory
from snowflake.cli.api.output.types import MessageResult
from typer.testing import CliRunner


def class_factory(
    pre_execute=None,
    result_handler=None,
    exception_handler=None,
    post_execute=None,
):
    class _CustomTyper(SnowTyper):
        @staticmethod
        def pre_execute(execution):
            if pre_execute:
                pre_execute(execution)

        @staticmethod
        def post_execute(execution):
            if post_execute:
                post_execute(execution)

        @staticmethod
        def process_result(result):
            if result_handler:
                result_handler(result)

        @staticmethod
        def exception_handler(err, execution):
            if exception_handler:
                exception_handler(err, execution)

        def create_instance(self):
            return self

    return _CustomTyper


_ENABLED_FLAG = False


def app_factory(typer_cls):
    app = typer_cls(name="snow")

    @app.command("simple_cmd", requires_global_options=False, requires_connection=False)
    def simple_cmd(name: str = typer.Argument()):
        return MessageResult(f"hello {name}")

    @app.command("fail_cmd", requires_global_options=False, requires_connection=False)
    def fail_cmd(name: str = typer.Argument()):
        raise Exception("err")

    @app.command(
        "cmd_with_global_options",
        requires_global_options=True,
        requires_connection=False,
    )
    def cmd_with_global_options(name: str = typer.Argument()):
        return MessageResult(f"hello {name}")

    @app.command("cmd_with_connection_options", requires_connection=True)
    def cmd_with_connection_options(name: str = typer.Argument()):
        return MessageResult(f"hello {name}")

    @app.command("switchable_cmd", is_enabled=lambda: _ENABLED_FLAG)
    def cmd_witch_enabled_switch():
        return MessageResult("Enabled")

    return app.create_instance()


@pytest.fixture
def cli():
    def mock_cli(app):
        return partial(CliRunner().invoke, app)

    return mock_cli


def test_no_callbacks(cli):
    result = cli(app_factory(class_factory()))(["simple_cmd", "Norman"])
    assert result.exit_code == 0, result.output
    assert result.output == ""


def test_result_callbacks(cli):
    result = cli(app_factory(class_factory(result_handler=lambda x: print(x.message))))(
        ["simple_cmd", "Norman"]
    )
    assert result.exit_code == 0, result.output
    assert result.output.strip() == "hello Norman"


def test_pre_callback_green_path(cli):
    pre_execute = MagicMock()
    post_execute = MagicMock()
    exception_callback = MagicMock()
    result = cli(
        app_factory(
            class_factory(
                pre_execute=pre_execute,
                post_execute=post_execute,
                exception_handler=exception_callback,
            )
        )
    )(["simple_cmd", "Norman"])
    assert result.exit_code == 0, result.output

    assert pre_execute.called
    assert post_execute.called
    assert not exception_callback.called


def test_pre_callback_error_path(cli):
    pre_execute = MagicMock()
    post_execute = MagicMock()
    exception_callback = MagicMock()
    result_handler = MagicMock()

    result = cli(
        app_factory(
            class_factory(
                pre_execute=pre_execute,
                post_execute=post_execute,
                exception_handler=exception_callback,
                result_handler=result_handler,
            )
        )
    )(["fail_cmd", "Norman"])
    assert result.exit_code == 1, result.output

    assert pre_execute.called
    assert post_execute.called
    assert not result_handler.called
    assert exception_callback.called
    assert len(exception_callback.call_args_list) == 1


def test_command_without_any_options(cli, os_agnostic_snapshot):
    result = cli(app_factory(SnowTyperFactory))(["simple_cmd", "--help"])
    assert result.output == os_agnostic_snapshot


def test_command_with_global_options(cli, os_agnostic_snapshot):
    result = cli(app_factory(SnowTyperFactory))(["cmd_with_global_options", "--help"])
    assert result.output == os_agnostic_snapshot


def test_command_with_connection_options(cli, os_agnostic_snapshot):
    result = cli(app_factory(SnowTyperFactory))(
        ["cmd_with_connection_options", "--help"]
    )
    assert result.output == os_agnostic_snapshot


def test_enabled_command_is_visible(cli, os_agnostic_snapshot):
    global _ENABLED_FLAG
    _ENABLED_FLAG = True
    result = cli(app_factory(SnowTyperFactory))(["switchable_cmd", "--help"])
    assert result.exit_code == 0
    assert result.output == os_agnostic_snapshot


def test_enabled_command_is_not_visible(cli, os_agnostic_snapshot):
    global _ENABLED_FLAG
    _ENABLED_FLAG = False
    result = cli(app_factory(SnowTyperFactory))(["switchable_cmd", "--help"])
    assert result.exit_code == 2
    assert result.output == os_agnostic_snapshot


@mock.patch("snowflake.cli.app.telemetry.log_command_usage")
def test_snow_typer_pre_execute_sends_telemetry(mock_log_command_usage, cli):
    result = cli(app_factory(SnowTyperFactory))(["simple_cmd", "Norma"])

    assert result.exit_code == 0
    mock_log_command_usage.assert_called_once_with(mock.ANY)


@mock.patch("snowflake.cli.app.telemetry.flush_telemetry")
def test_snow_typer_post_execute_sends_telemetry(mock_flush_telemetry, cli):
    result = cli(app_factory(SnowTyperFactory))(["simple_cmd", "Norma"])
    assert result.exit_code == 0
    mock_flush_telemetry.assert_called_once_with()


@mock.patch("snowflake.cli.app.printing.print_result")
def test_snow_typer_result_callback_sends_telemetry(mock_print_result, cli):
    result = cli(app_factory(SnowTyperFactory))(["simple_cmd", "Norma"])
    assert result.exit_code == 0
    assert mock_print_result.call_count == 1
    assert mock_print_result.call_args.args[0].message == "hello Norma"
