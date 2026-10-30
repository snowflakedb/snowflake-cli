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

import io
import re
from pathlib import Path

import pytest
import typer.rich_utils as rich_utils
from snowflake.cli.api.exceptions import (
    CliError,
    ConfigFileTooWidePermissionsError,
    install_plain_remediation_error_renderer,
)

# Long enough that a 40-column error panel wraps the path in the middle.
_LONG_PATH = Path("/Users/edendulk/Library/Application Support/snowflake/config.toml")


_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    return _ANSI.sub("", text)


def _render(exc: Exception, columns: str, monkeypatch: pytest.MonkeyPatch) -> str:
    install_plain_remediation_error_renderer()
    monkeypatch.setenv("COLUMNS", columns)
    buffer = io.StringIO()
    monkeypatch.setattr("sys.stderr", buffer)
    rich_utils.rich_format_error(exc)
    return buffer.getvalue()


def test_unix_command_is_one_line_outside_the_error_panel(monkeypatch):
    monkeypatch.setattr("snowflake.cli.api.exceptions.IS_WINDOWS", False)
    exc = ConfigFileTooWidePermissionsError(_LONG_PATH)
    command = f'chmod 0600 "{_LONG_PATH}"'

    assert exc.plain_remediation_commands == (command,)
    assert "chmod" not in exc.format_message()
    assert "too wide permissions" in exc.format_message()

    rendered = _render(exc, "40", monkeypatch)
    lines = rendered.splitlines()
    assert lines.count(command) == 1
    assert lines[-1] == command
    assert all("│" not in line or "chmod" not in line for line in lines)


def test_windows_commands_are_plain_lines(monkeypatch):
    monkeypatch.setattr("snowflake.cli.api.exceptions.IS_WINDOWS", True)
    exc = ConfigFileTooWidePermissionsError(_LONG_PATH)
    deny = f'icacls "{_LONG_PATH}" /deny <USER_ID>:F'
    listing = f'icacls "{_LONG_PATH}"'

    assert exc.plain_remediation_commands == (deny, listing)
    assert "icacls" not in exc.format_message()

    rendered = _render(exc, "40", monkeypatch)
    lines = rendered.splitlines()
    assert lines[-2:] == [deny, listing]
    assert all("│" not in line or "icacls" not in line for line in lines)


def test_other_click_errors_do_not_gain_a_trailing_command(monkeypatch):
    rendered = _plain(_render(CliError("boom"), "40", monkeypatch))
    lines = [line for line in rendered.splitlines() if line.strip()]
    # Non-tty Rich uses ASCII boxes; a real terminal uses the unicode corner.
    assert lines[-1].startswith(("╰", "+"))
    assert "boom" in rendered
    assert "chmod" not in rendered


def test_string_form_keeps_the_command_for_handlers_that_only_print_the_exception(
    monkeypatch,
):
    """--config-file checks permissions before Typer, which prints str(exception)."""
    monkeypatch.setattr("snowflake.cli.api.exceptions.IS_WINDOWS", False)
    exc = ConfigFileTooWidePermissionsError(_LONG_PATH)
    command = f'chmod 0600 "{_LONG_PATH}"'

    assert command not in exc.format_message()
    assert str(exc).splitlines()[-1] == command

    seen = []
    monkeypatch.setattr(
        "snowflake.cli._app.main_typer.cli_console.warning",
        lambda message: seen.append(message),
    )
    from snowflake.cli._app.main_typer import _handle_exception

    with pytest.raises(SystemExit):
        _handle_exception(exc)
    assert command in seen[0]
    assert seen[0].rstrip().endswith(command)


def test_show_writes_the_command_when_rich_panels_are_off(monkeypatch):
    monkeypatch.setattr("snowflake.cli.api.exceptions.IS_WINDOWS", False)
    exc = ConfigFileTooWidePermissionsError(_LONG_PATH)
    command = f'chmod 0600 "{_LONG_PATH}"'
    buffer = io.StringIO()

    exc.show(file=buffer)

    assert command in buffer.getvalue().splitlines()


def test_renderer_install_is_idempotent():
    install_plain_remediation_error_renderer()
    first = rich_utils.rich_format_error
    install_plain_remediation_error_renderer()
    assert rich_utils.rich_format_error is first


def test_cli_app_installs_the_renderer(app_factory, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "snowflake.cli._app.cli_app.install_plain_remediation_error_renderer",
        lambda: calls.append(True),
    )

    app_factory.create_or_get_app()

    assert calls == [True]
