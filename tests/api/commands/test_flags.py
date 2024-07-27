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

from unittest import mock
from unittest.mock import Mock, create_autospec, patch

import click.core
import pytest
import typer
from snowflake.cli.api.commands.flags import (
    PLAIN_PASSWORD_MSG,
    OverrideableOption,
    PasswordOption,
)
from typer import Typer
from typer.core import TyperOption
from typer.testing import CliRunner


def test_format(runner, os_agnostic_snapshot):
    result = runner.invoke(
        ["stage", "list-files", "stage_name", "--format", "invalid_format"]
    )

    assert result.output == os_agnostic_snapshot


def test_password_flag():
    app = Typer()

    @app.command()
    def _(password: str = PasswordOption):
        return "ok"

    runner = CliRunner()
    result = runner.invoke(app, ["--password", "dummy"], catch_exceptions=False)
    assert result.exit_code == 0
    assert PLAIN_PASSWORD_MSG in result.output


@patch("snowflake.cli.api.commands.flags.typer.Option")
def test_overrideable_option_returns_typer_option(mock_option):
    mock_option_info = Mock(spec=typer.models.OptionInfo)
    mock_option.return_value = mock_option_info
    default = 1
    param_decls = ["--option"]
    help_message = "help message"

    option = OverrideableOption(default, *param_decls, help=help_message)()
    mock_option.assert_called_once_with(default, *param_decls, help=help_message)
    assert option == mock_option_info


def test_overrideable_option_is_overrideable():
    original_param_decls = ("--option",)
    original = OverrideableOption(1, *original_param_decls, help="original help")

    new_default = 2
    new_help = "new help"
    modified = original(default=new_default, help=new_help)

    assert modified.default == new_default
    assert modified.help == new_help
    assert modified.param_decls == original_param_decls


_MUTEX_OPTION_1 = OverrideableOption(
    False, "--option1", mutually_exclusive=["option_1", "option_2"]
)
_MUTEX_OPTION_2 = OverrideableOption(
    False, "--option2", mutually_exclusive=["option_1", "option_2"]
)


@pytest.mark.parametrize("set1, set2", [(False, False), (False, True), (True, False)])
def test_mutually_exclusive_options_no_error(set1, set2):
    app = Typer()

    @app.command()
    def _(option_1: bool = _MUTEX_OPTION_1(), option_2: bool = _MUTEX_OPTION_2()):
        pass

    command = []
    if set1:
        command.append("--option1")
    if set2:
        command.append("--option2")
    runner = CliRunner()
    result = runner.invoke(app, command)
    assert result.exit_code == 0


def test_mutually_exclusive_options_error(os_agnostic_snapshot):
    app = Typer()

    @app.command()
    def _(option_1: bool = _MUTEX_OPTION_1(), option_2: bool = _MUTEX_OPTION_2()):
        pass

    command = ["--option1", "--option2"]
    runner = CliRunner()
    result = runner.invoke(app, command)
    assert result.exit_code == 2
    assert result.output == os_agnostic_snapshot


def test_overrideable_option_callback_passthrough():
    def callback(value):
        return value + 1

    app = Typer()

    @app.command()
    def _(option: int = OverrideableOption(..., "--option", callback=callback)()):
        print(option)

    runner = CliRunner()
    result = runner.invoke(app, ["--option", "0"])
    assert result.exit_code == 0
    assert result.output.strip() == "1"


def test_overrideable_option_callback_with_context():
    # tests that generated_callback will correctly map ctx and param arguments to the original callback
    def callback(value, param: typer.CallbackParam, ctx: typer.Context):
        assert isinstance(value, int)
        assert isinstance(param, TyperOption)
        assert isinstance(ctx, click.core.Context)
        return value

    app = Typer()

    @app.command()
    def _(option: int = OverrideableOption(..., "--option", callback=callback)()):
        pass

    runner = CliRunner()
    result = runner.invoke(app, ["--option", "0"])
    assert result.exit_code == 0


class _InvalidCallbackSignatureNamespace:
    # dummy functions for test_overrideable_option_invalid_callback_signature

    # too many parameters
    @staticmethod
    def callback1(
        ctx: typer.Context, param: typer.CallbackParam, value1: int, value2: float
    ):
        pass

    # untyped Context and CallbackParam
    @staticmethod
    def callback2(ctx, param, value):
        pass

    # multiple untyped values
    @staticmethod
    def callback3(ctx: typer.Context, value1, value2):
        pass


@pytest.mark.parametrize(
    "callback",
    [
        _InvalidCallbackSignatureNamespace.callback1,
        _InvalidCallbackSignatureNamespace.callback2,
        _InvalidCallbackSignatureNamespace.callback3,
    ],
)
def test_overrideable_option_invalid_callback_signature(callback):
    invalid_callback_option = OverrideableOption(None, "--option", callback=callback)
    with pytest.raises(OverrideableOption.InvalidCallbackSignature):
        invalid_callback_option()


def test_overrideable_option_callback_with_mutually_exclusive(os_agnostic_snapshot):
    """
    Tests that is both 'callback' and 'mutually_exclusive' are passed to OverrideableOption, both are respected. This
    is mainly for the rare use case where you are using 'mutually_exclusive' with non-flag options.
    """

    def passthrough(value):
        return value

    mock_callback = create_autospec(passthrough)
    app = Typer()

    @app.command()
    def _(
        option_1: int = _MUTEX_OPTION_1(default=None, callback=mock_callback),
        option_2: int = _MUTEX_OPTION_2(default=None, callback=mock_callback),
    ):
        pass

    runner = CliRunner()

    # test that callback is called on the option values
    runner.invoke(app, ["--option1", "1"])
    mock_callback.assert_has_calls([mock.call(value=1), mock.call(value=None)])

    # test that we can't provide both options as non-falsey values without throwing error
    result = runner.invoke(app, ["--option1", "1", "--option2", "2"])
    assert result.exit_code == 2
    assert result.output == os_agnostic_snapshot
