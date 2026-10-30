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

from __future__ import annotations

from functools import partial
from io import StringIO
from unittest.mock import patch

import click
import pytest
from rich.console import Console
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    PlainText,
    RelatedLink,
)
from snowflake.cli.api.commands.docs_help import (
    render_condensed_shared_options_panel,
    render_shared_configuration_panels,
)
from snowflake.cli.api.commands.flags import (
    CONNECTION_CONFIGURATION_PANEL,
    GLOBAL_CONFIGURATION_PANEL,
)
from snowflake.cli.api.commands.help_options import (
    CONDENSED_HELP_FOOTER,
    iter_shared_configuration_params,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.output.types import MessageResult
from typer.main import get_command
from typer.testing import CliRunner

from tests_common.feature_flag_utils import with_feature_flags

_MINIMAL_DOCS = CommandDocs(
    related=(RelatedLink(href="/developer-guide/snowflake-cli/index"),),
    usage_notes=(PlainText(parts=("Test usage note.",)),),
    examples=(Example(command="snow demo cmd"),),
)


def _fake_command(
    *,
    include_connection: bool,
    include_global: bool,
) -> click.Command:
    params: list[click.Parameter] = []
    if include_connection:
        params.extend(iter_shared_configuration_params(CONNECTION_CONFIGURATION_PANEL))
    if include_global:
        params.extend(iter_shared_configuration_params(GLOBAL_CONFIGURATION_PANEL))

    class _FakeCommand(click.Command):
        def get_params(self, ctx: click.Context) -> list[click.Parameter]:
            return params

    return _FakeCommand(name="fake")


def _capture_rich_output(callback, *args, **kwargs) -> str:
    buffer = StringIO()
    console = Console(file=buffer, width=120, color_system=None)
    with patch(
        "snowflake.cli.api.commands.docs_help._get_rich_console", return_value=console
    ):
        callback(*args, **kwargs)
    return buffer.getvalue()


def _global_only_app() -> SnowTyperFactory:
    app = SnowTyperFactory(name="demo")

    @app.command("cmd", requires_global_options=True, requires_connection=False)
    def cmd():
        return MessageResult("ok")

    return app


def _connection_and_global_app() -> SnowTyperFactory:
    app = SnowTyperFactory(name="demo")

    @app.command("cmd", requires_connection=True)
    def cmd():
        return MessageResult("ok")

    return app


def _connection_and_global_with_docs_app() -> SnowTyperFactory:
    app = SnowTyperFactory(name="demo")

    @app.command("cmd", requires_connection=True, docs=_MINIMAL_DOCS)
    def cmd():
        return MessageResult("ok")

    return app


def _root_app() -> SnowTyperFactory:
    app = SnowTyperFactory(name="snow")

    @app.command("leaf", requires_connection=True)
    def leaf():
        return MessageResult("ok")

    return app


@pytest.fixture
def cli():
    def mock_cli(app):
        return partial(CliRunner().invoke, app)

    return mock_cli


def test_condensed_panel_lists_global_options_only(cli):
    result = cli(_global_only_app().create_instance())(["cmd", "--help"])

    assert result.exit_code == 0, result.output
    assert CONNECTION_CONFIGURATION_PANEL not in result.output
    assert GLOBAL_CONFIGURATION_PANEL not in result.output
    assert "[global options]" in result.output
    assert "[connection options]" not in result.output
    assert "Global options" in result.output
    assert "Global options: --format" in result.output
    assert "Connection options:" not in result.output
    assert "Run `snow --help` for full descriptions" in result.output


def test_condensed_panel_lists_connection_options_only():
    command = _fake_command(include_connection=True, include_global=False)
    ctx = click.Context(command)
    output = _capture_rich_output(render_condensed_shared_options_panel, command, ctx)

    assert "Connection options" in output
    assert "Connection options: --connection" in output
    assert "Global options:" not in output
    assert "Run `snow --help` for full descriptions" in output


def test_condensed_panel_lists_connection_and_global_options(cli):
    result = cli(_connection_and_global_app().create_instance())(["cmd", "--help"])

    assert result.exit_code == 0, result.output
    assert CONNECTION_CONFIGURATION_PANEL not in result.output
    assert GLOBAL_CONFIGURATION_PANEL not in result.output
    assert "[global options]" in result.output
    assert "[connection options]" in result.output
    assert "Global and connection options" in result.output
    assert "Connection options: --connection" in result.output
    assert "Global options: --format" in result.output
    assert "Run `snow --help` for full descriptions" in result.output


def test_root_help_appends_full_shared_option_panels():
    group = get_command(_root_app().create_instance())
    ctx = click.Context(group)
    output = _capture_rich_output(render_shared_configuration_panels, ctx)

    assert CONNECTION_CONFIGURATION_PANEL in output
    assert GLOBAL_CONFIGURATION_PANEL in output
    assert "--connection" in output
    assert "--format" in output
    assert CONDENSED_HELP_FOOTER not in output


def test_root_snow_help_appends_full_shared_option_panels(runner):
    result = runner.invoke(["--help"])

    assert result.exit_code == 0, result.output
    assert CONNECTION_CONFIGURATION_PANEL in result.output
    assert GLOBAL_CONFIGURATION_PANEL in result.output
    assert "--connection" in result.output
    assert "--format" in result.output
    assert CONDENSED_HELP_FOOTER not in result.output


def test_root_snow_no_args_appends_full_shared_option_panels(runner):
    result = runner.invoke([])

    assert result.exit_code == 0, result.output
    assert CONNECTION_CONFIGURATION_PANEL in result.output
    assert GLOBAL_CONFIGURATION_PANEL in result.output
    assert "--connection" in result.output
    assert "--format" in result.output
    assert CONDENSED_HELP_FOOTER not in result.output


@with_feature_flags({FeatureFlag.ENABLE_CONDENSED_COMMAND_HELP: False})
def test_root_snow_help_unchanged_when_flag_off(runner):
    result = runner.invoke(["--help"])

    assert result.exit_code == 0, result.output
    assert CONNECTION_CONFIGURATION_PANEL not in result.output
    assert GLOBAL_CONFIGURATION_PANEL not in result.output


@with_feature_flags(
    {
        FeatureFlag.ENABLE_CONDENSED_COMMAND_HELP: True,
        FeatureFlag.ENABLE_COMMAND_DOCS_IN_HELP: True,
    }
)
def test_condensed_help_keeps_command_docs_panels(cli):
    result = cli(_connection_and_global_with_docs_app().create_instance())(
        ["cmd", "--help"]
    )

    assert result.exit_code == 0, result.output
    assert CONNECTION_CONFIGURATION_PANEL not in result.output
    assert GLOBAL_CONFIGURATION_PANEL not in result.output
    assert "Global and connection options" in result.output
    assert "Run `snow --help` for full descriptions" in result.output
    assert "Usage notes" in result.output
    assert "Test usage note." in result.output
    assert "Examples" in result.output
    assert "snow demo cmd" in result.output
    assert "Related topics" in result.output
