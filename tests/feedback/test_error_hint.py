# Copyright (c) 2026 Snowflake Inc.
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

import io
import re
from pathlib import Path
from unittest import mock

import click
import pytest
import typer.rich_utils as rich_utils
from snowflake.cli._app.main_typer import _handle_exception
from snowflake.cli._plugins.feedback.error_hint import (
    FEEDBACK_HINT,
    arm_feedback_hint,
    install_feedback_error_hint,
    print_armed_feedback_hint,
)
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.commands.execution_metadata import ExecutionMetadata
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.exceptions import (
    CliError,
    CliSqlError,
    install_plain_remediation_error_renderer,
)
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.connector import DatabaseError

_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    return _ANSI.sub("", text)


def _render(exc: BaseException, monkeypatch: pytest.MonkeyPatch) -> str:
    install_plain_remediation_error_renderer()
    install_feedback_error_hint()
    monkeypatch.setenv("COLUMNS", "80")
    buffer = io.StringIO()
    monkeypatch.setattr("sys.stderr", buffer)
    rich_utils.rich_format_error(exc)
    return _plain(buffer.getvalue())


def _patch_home(tmp_path: Path):
    fake_manager = mock.Mock()
    fake_manager.file_path = tmp_path / "config.toml"
    return mock.patch(
        "snowflake.cli._plugins.feedback.last_error.get_config_manager",
        return_value=fake_manager,
    )


def _handle(tmp_path: Path, command_name: str, exc: BaseException) -> BaseException:
    @click.group()
    def snow():
        pass

    @snow.command(command_name)
    def _cmd():
        pass

    parent = click.Context(snow, info_name="snow")
    child = click.Context(_cmd, parent=parent, info_name=command_name)
    with _patch_home(tmp_path):
        with child.scope():
            return SnowTyper.exception_handler(exc, ExecutionMetadata())


def test_hint_is_one_line_after_the_error_panel(monkeypatch):
    exc = CliError("compilation failed")
    arm_feedback_hint(exc)

    rendered = _render(exc, monkeypatch)
    lines = [line for line in rendered.splitlines() if line.strip()]

    assert lines[-1] == FEEDBACK_HINT
    assert "compilation failed" in rendered
    assert "│" not in lines[-1]
    assert lines[-2].startswith(("╰", "+"))


def test_sql_errors_hint(monkeypatch):
    exc = CliSqlError("invalid identifier 'SELCT'")
    arm_feedback_hint(exc)

    rendered = _render(exc, monkeypatch)

    assert rendered.splitlines()[-1] == FEEDBACK_HINT


@pytest.mark.parametrize(
    "exc",
    [
        KeyboardInterrupt(),
        click.exceptions.Exit(1),
        click.exceptions.Abort(),
        SystemExit(1),
    ],
)
def test_interrupts_and_exits_are_not_armed(exc):
    arm_feedback_hint(exc)
    assert not getattr(exc, "_snow_feedback_hint", False)


@pytest.mark.parametrize(
    "exc",
    [
        click.UsageError("missing argument"),
        click.ClickException("not a cli error"),
    ],
)
def test_usage_errors_do_not_hint(monkeypatch, exc):
    arm_feedback_hint(exc)

    rendered = _render(exc, monkeypatch)

    assert FEEDBACK_HINT not in rendered


@pytest.mark.parametrize(
    "output_format",
    [OutputFormat.JSON, OutputFormat.JSON_EXT, OutputFormat.CSV],
)
def test_structured_formats_skip_the_line(monkeypatch, output_format):
    exc = CliError("boom")
    arm_feedback_hint(exc)
    get_cli_context_manager().output_format = output_format

    rendered = _render(exc, monkeypatch)

    assert "boom" in rendered
    assert FEEDBACK_HINT not in rendered


def test_table_format_keeps_the_line(monkeypatch):
    exc = CliError("boom")
    arm_feedback_hint(exc)
    get_cli_context_manager().output_format = OutputFormat.TABLE

    assert _render(exc, monkeypatch).splitlines()[-1] == FEEDBACK_HINT


def test_feedback_command_does_not_hint(tmp_path):
    exc = CliError("send failed")
    returned = _handle(tmp_path, "feedback", exc)

    assert returned is exc
    assert not getattr(exc, "_snow_feedback_hint", False)
    buffer = io.StringIO()
    with mock.patch("sys.stderr", buffer):
        print_armed_feedback_hint(exc)
    assert buffer.getvalue() == ""


def test_exception_handler_arms_cli_and_unexpected_errors(tmp_path, monkeypatch):
    cli_error = CliError("boom")
    assert _handle(tmp_path, "sql", cli_error) is cli_error
    assert getattr(cli_error, "_snow_feedback_hint", False) is True

    unexpected = RuntimeError("nope")
    assert _handle(tmp_path, "sql", unexpected) is unexpected
    assert getattr(unexpected, "_snow_feedback_hint", False) is True

    database_error = DatabaseError(
        "000904 (42000): invalid identifier", send_telemetry=False
    )
    migrated = _handle(tmp_path, "sql", database_error)
    assert isinstance(migrated, CliSqlError)
    assert getattr(migrated, "_snow_feedback_hint", False) is True


def test_unexpected_exception_prints_the_hint_after_the_warning(monkeypatch):
    exc = RuntimeError("nope")
    arm_feedback_hint(exc)
    seen = []
    monkeypatch.setattr(
        "snowflake.cli._app.main_typer.cli_console.warning",
        lambda message: seen.append(message),
    )
    buffer = io.StringIO()
    monkeypatch.setattr("sys.stderr", buffer)

    with pytest.raises(SystemExit):
        _handle_exception(exc)

    assert seen and "unexpected exception" in seen[0].lower()
    assert buffer.getvalue().strip() == FEEDBACK_HINT


def test_renderer_install_is_idempotent():
    install_feedback_error_hint()
    first = rich_utils.rich_format_error
    install_feedback_error_hint()
    assert rich_utils.rich_format_error is first


def test_paired_renderer_install_does_not_stack(monkeypatch):
    """create_or_get_app installs both wrappers on every lookup.

    The permissions wrapper hides this one, and the next lookup used to
    wrap the pair again. A few hundred commands then recurse in
    rich_format_error.
    """
    install_plain_remediation_error_renderer()
    install_feedback_error_hint()
    first = rich_utils.rich_format_error
    for _ in range(1000):
        install_plain_remediation_error_renderer()
        install_feedback_error_hint()
    assert rich_utils.rich_format_error is first

    exc = CliError("compilation failed")
    arm_feedback_hint(exc)
    rendered = _render(exc, monkeypatch)
    assert rendered.count(FEEDBACK_HINT) == 1


def test_cli_app_installs_the_hint(app_factory, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "snowflake.cli._app.cli_app.install_feedback_error_hint",
        lambda: calls.append(True),
    )

    app_factory.create_or_get_app()

    assert calls == [True]
