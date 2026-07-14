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
from snowflake.cli.api.commands.snow_typer import (
    PREVIEW_PREFIX,
    SnowTyper,
    SnowTyperFactory,
    SortedTyperGroup,
)
from snowflake.cli.api.output.types import MessageResult
from typer.main import get_command
from typer.testing import CliRunner


def class_factory(
    pre_execute=None,
    result_handler=None,
    exception_handler=None,
    post_execute=None,
):
    class _CustomTyper(SnowTyper):
        @staticmethod
        def pre_execute(execution, require_warehouse):
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

    @app.command(
        "interrupted_cmd",
        requires_global_options=False,
        requires_connection=False,
    )
    def interrupted_cmd(name: str = typer.Argument()):
        raise KeyboardInterrupt("err")

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

    # ensure duration metric captured is greater than or equal to 0
    assert post_execute.call_args.args[0].get_duration() >= 0


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

    # ensure duration metric captured is greater than or equal to 0
    assert post_execute.call_args.args[0].get_duration() >= 0


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


@mock.patch("snowflake.cli._app.telemetry.log_command_usage")
def test_snow_typer_pre_execute_sends_telemetry(mock_log_command_usage, cli):
    result = cli(app_factory(SnowTyperFactory))(["simple_cmd", "Norma"])

    assert result.exit_code == 0
    mock_log_command_usage.assert_called_once_with(mock.ANY)


@mock.patch("snowflake.cli._app.telemetry.flush_telemetry")
def test_snow_typer_post_execute_sends_telemetry(mock_flush_telemetry, cli):
    result = cli(app_factory(SnowTyperFactory))(["simple_cmd", "Norma"])
    assert result.exit_code == 0
    mock_flush_telemetry.assert_called_once_with()


@mock.patch("snowflake.cli._app.printing.print_result")
def test_snow_typer_result_callback_sends_telemetry(mock_print_result, cli):
    result = cli(app_factory(SnowTyperFactory))(["simple_cmd", "Norma"])
    assert result.exit_code == 0
    assert mock_print_result.call_count == 1
    assert mock_print_result.call_args.args[0].message == "hello Norma"


def test_snow_typer_with_keyboard_interrupt(cli):
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
    )(["interrupted_cmd", "Norman"])
    assert result.exit_code == 1, result.output

    assert pre_execute.called
    assert post_execute.called
    assert not result_handler.called
    assert exception_callback.called
    assert len(exception_callback.call_args_list) == 1

    # ensure duration metric captured is greater than or equal to 0
    assert post_execute.call_args.args[0].get_duration() >= 0


# Tests for the preview functionality
def test_snow_typer_factory_preview_adds_prefix_to_app_help():
    """Test that preview=True on SnowTyperFactory adds PREVIEW_PREFIX to app help text."""
    app = SnowTyperFactory(
        name="test_app",
        help="This is a test app.",
        preview=True,
    )

    instance = app.create_instance()
    assert instance.info.help == f"{PREVIEW_PREFIX}This is a test app."


def test_snow_typer_factory_preview_prevents_double_prefix():
    """Test that preview=True does not add double PREVIEW_PREFIX prefix."""
    app = SnowTyperFactory(
        name="test_app",
        help=f"{PREVIEW_PREFIX}This is already prefixed.",
        preview=True,
    )

    instance = app.create_instance()
    assert instance.info.help == f"{PREVIEW_PREFIX}This is already prefixed."


def test_snow_typer_factory_preview_propagates_to_commands():
    """Test that preview=True on SnowTyperFactory propagates to individual commands."""
    app = SnowTyperFactory(
        name="test_app",
        help="This is a test app.",
        preview=True,
    )

    @app.command("test_cmd", requires_global_options=False, requires_connection=False)
    def test_cmd():
        """This is a test command."""
        return MessageResult("test")

    instance = app.create_instance()

    # Get the command and check its help text
    registered_cmds = instance.registered_commands
    assert len(registered_cmds) == 1
    cmd_info = registered_cmds[0]
    assert cmd_info.callback.__doc__ == f"{PREVIEW_PREFIX}This is a test command."


def test_snow_typer_individual_command_preview():
    """Test that individual commands can have preview=True."""
    app = SnowTyperFactory(
        name="test_app",
        help="This is a test app.",
        preview=False,  # App itself is not preview
    )

    @app.command(
        "preview_cmd",
        requires_global_options=False,
        requires_connection=False,
        preview=True,
    )
    def preview_cmd():
        """This command is in preview."""
        return MessageResult("test")

    @app.command("normal_cmd", requires_global_options=False, requires_connection=False)
    def normal_cmd():
        """This command is normal."""
        return MessageResult("test")

    instance = app.create_instance()

    # Check that only the preview command has the prefix
    registered_cmds = instance.registered_commands
    assert len(registered_cmds) == 2

    # Find commands by name
    preview_cmd_info = None
    normal_cmd_info = None
    for cmd_info in registered_cmds:
        if cmd_info.name == "preview_cmd":
            preview_cmd_info = cmd_info
        elif cmd_info.name == "normal_cmd":
            normal_cmd_info = cmd_info

    assert preview_cmd_info is not None
    assert normal_cmd_info is not None
    assert (
        preview_cmd_info.callback.__doc__
        == f"{PREVIEW_PREFIX}This command is in preview."
    )
    assert normal_cmd_info.callback.__doc__ == "This command is normal."


def test_snow_typer_preview_works_with_help_parameter():
    """Test that preview=True works with help parameter in addition to docstrings."""
    app = SnowTyperFactory(
        name="test_app",
        help="This is a test app.",
        preview=False,  # App itself is not preview
    )

    @app.command(
        "help_param_cmd",
        requires_global_options=False,
        requires_connection=False,
        preview=True,
        help="This command uses help parameter.",
    )
    def help_param_cmd():
        return MessageResult("test")

    @app.command(
        "docstring_cmd",
        requires_global_options=False,
        requires_connection=False,
        preview=True,
    )
    def docstring_cmd():
        """This command uses docstring."""
        return MessageResult("test")

    instance = app.create_instance()

    # Check both commands have preview prefix
    registered_cmds = instance.registered_commands
    assert len(registered_cmds) == 2

    # Find commands by name
    help_param_cmd_info = None
    docstring_cmd_info = None
    for cmd_info in registered_cmds:
        if cmd_info.name == "help_param_cmd":
            help_param_cmd_info = cmd_info
        elif cmd_info.name == "docstring_cmd":
            docstring_cmd_info = cmd_info

    assert help_param_cmd_info is not None
    assert docstring_cmd_info is not None

    # The help parameter command should have PREVIEW_PREFIX in help
    assert (
        help_param_cmd_info.help == f"{PREVIEW_PREFIX}This command uses help parameter."
    )
    # The docstring command should have PREVIEW_PREFIX in docstring
    assert (
        docstring_cmd_info.callback.__doc__
        == f"{PREVIEW_PREFIX}This command uses docstring."
    )


# Tests for injectable group_class on SnowTyperFactory / SnowTyper


class _MyGroup(SortedTyperGroup):
    """Trivial SortedTyperGroup subclass used to verify group_class injection."""


def _factory_with_two_commands(**factory_kwargs) -> SnowTyperFactory:
    # Two commands are required so that Typer keeps a real group (a single
    # command app is collapsed into a bare command by Typer).
    app = SnowTyperFactory(name="grouped_app", **factory_kwargs)

    @app.command("first_cmd", requires_global_options=False, requires_connection=False)
    def first_cmd():
        return MessageResult("first")

    @app.command("second_cmd", requires_global_options=False, requires_connection=False)
    def second_cmd():
        return MessageResult("second")

    return app


def test_factory_honors_custom_group_class():
    """A custom group_class is used for the underlying Click group."""
    app = _factory_with_two_commands(group_class=_MyGroup).create_instance()

    group = get_command(app)
    assert isinstance(group, _MyGroup)
    assert type(group) is _MyGroup


def test_factory_defaults_to_sorted_typer_group():
    """Without group_class the default SortedTyperGroup is used."""
    app = _factory_with_two_commands().create_instance()

    group = get_command(app)
    assert isinstance(group, SortedTyperGroup)
    assert not isinstance(group, _MyGroup)


def test_snow_typer_honors_custom_group_class_directly():
    """SnowTyper itself forwards group_class to the Click group."""
    app = SnowTyper(name="direct", group_class=_MyGroup)

    @app.command("first_cmd", requires_global_options=False, requires_connection=False)
    def first_cmd():
        return MessageResult("first")

    @app.command("second_cmd", requires_global_options=False, requires_connection=False)
    def second_cmd():
        return MessageResult("second")

    group = get_command(app)
    assert isinstance(group, _MyGroup)


# Tests for add_typer kwargs forwarding


def _subapp_factory(name: str) -> SnowTyperFactory:
    sub = SnowTyperFactory(name=name)

    @sub.command("sub_cmd", requires_global_options=False, requires_connection=False)
    def sub_cmd():
        return MessageResult("sub")

    return sub


def test_add_typer_forwards_kwargs_to_subgroup():
    """add_typer kwargs (e.g. rich_help_panel) are forwarded to add_typer."""
    parent = SnowTyperFactory(name="parent")

    @parent.command(
        "parent_cmd", requires_global_options=False, requires_connection=False
    )
    def parent_cmd():
        return MessageResult("parent")

    parent.add_typer(_subapp_factory("sub"), rich_help_panel="My Panel")

    group = get_command(parent.create_instance())
    subcommand = group.commands["sub"]
    assert subcommand.rich_help_panel == "My Panel"


def test_add_typer_without_kwargs_has_no_rich_help_panel():
    """A subapp added without kwargs registers with no rich_help_panel."""
    parent = SnowTyperFactory(name="parent")

    @parent.command(
        "parent_cmd", requires_global_options=False, requires_connection=False
    )
    def parent_cmd():
        return MessageResult("parent")

    parent.add_typer(_subapp_factory("sub"))

    group = get_command(parent.create_instance())
    subcommand = group.commands["sub"]
    assert getattr(subcommand, "rich_help_panel", None) is None


def test_add_typer_subcommands_are_invokable(cli):
    """Regression: a factory with a subapp still builds and is invokable."""
    parent = SnowTyperFactory(name="parent")

    @parent.command(
        "parent_cmd", requires_global_options=False, requires_connection=False
    )
    def parent_cmd():
        return MessageResult("parent")

    parent.add_typer(_subapp_factory("sub"))

    app = parent.create_instance()
    result = cli(app)(["sub", "--help"])
    assert result.exit_code == 0, result.output

    sub_result = cli(app)(["sub", "sub_cmd", "--help"])
    assert sub_result.exit_code == 0, sub_result.output
