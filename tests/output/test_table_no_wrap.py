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

"""Wrapping of TABLE output cells (GH#3170).

Long values are folded onto several terminal lines by default. With wrapping
turned off every record keeps a single physical row, even when the table
becomes wider than the terminal.
"""

import pytest
from rich.console import Console
from snowflake.cli._app import ascii_table, printing
from snowflake.cli._app.printing import print_result
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.config import config_init
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import CollectionResult

from tests.testing_utils.conversion import get_output

_LONG_VALUE = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_TERMINAL_WIDTH = 40


def _force_tty(monkeypatch, width: int = _TERMINAL_WIDTH):
    """Make the table renderer behave like a terminal of the given width."""
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    monkeypatch.setattr(printing, "_stdout_is_terminal", lambda: True)

    def _console(needed_width=None):
        # needed_width is set when wrapping is off: the table is then as wide
        # as its content and the console has to grow with it.
        return Console(
            width=needed_width or width,
            height=25,
            soft_wrap=True,
            markup=False,
            force_terminal=False,
            legacy_windows=False,
            color_system=None,
            no_color=True,
        )

    monkeypatch.setattr(printing, "_render_console_for_table", _console)


def _print_rows(rows):
    print_result(CollectionResult(rows), output_format=OutputFormat.TABLE)


def _data_lines(out: str) -> list[str]:
    """Lines of the table that hold a value (no border, no header rule)."""
    return [line for line in out.splitlines() if line.startswith("|")]


def test_tty_table_wraps_long_value_by_default(capsys, monkeypatch):
    _force_tty(monkeypatch)
    _print_rows([{"c": _LONG_VALUE}])
    out = get_output(capsys)
    assert _LONG_VALUE not in out
    assert any(_LONG_VALUE[:20] in line for line in _data_lines(out))
    # The value is folded, so it occupies more than one data line.
    assert len(_data_lines(out)) >= 2


def test_tty_table_keeps_long_value_on_one_line_without_wrap(capsys, monkeypatch):
    _force_tty(monkeypatch)
    get_cli_context_manager().wrap = False
    _print_rows([{"c": _LONG_VALUE}])
    out = get_output(capsys)
    assert _LONG_VALUE in out
    matching = [line for line in _data_lines(out) if _LONG_VALUE in line]
    assert len(matching) == 1


def test_tty_table_keeps_every_record_on_one_line_without_wrap(capsys, monkeypatch):
    _force_tty(monkeypatch)
    get_cli_context_manager().wrap = False
    _print_rows([{"c": _LONG_VALUE, "n": "1"}, {"c": _LONG_VALUE, "n": "2"}])
    out = get_output(capsys)
    matching = [line for line in _data_lines(out) if _LONG_VALUE in line]
    assert len(matching) == 2


def test_tty_table_without_wrap_keeps_the_grid_aligned(capsys, monkeypatch):
    _force_tty(monkeypatch)
    get_cli_context_manager().wrap = False
    _print_rows([{"c": _LONG_VALUE, "n": "1"}])
    out = get_output(capsys)
    widths = {len(line) for line in out.splitlines() if line}
    assert len(widths) == 1


def test_streamed_table_wraps_long_value_by_default(capsys, monkeypatch):
    _force_tty(monkeypatch)
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    _print_rows([{"c": _LONG_VALUE}])
    out = get_output(capsys)
    assert _LONG_VALUE not in out


def test_streamed_table_keeps_long_value_on_one_line_without_wrap(capsys, monkeypatch):
    _force_tty(monkeypatch)
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    get_cli_context_manager().wrap = False
    _print_rows([{"c": _LONG_VALUE}])
    out = get_output(capsys)
    assert _LONG_VALUE in out
    matching = [line for line in _data_lines(out) if _LONG_VALUE in line]
    assert len(matching) == 1


def test_rich_and_streamed_paths_agree_without_wrap(capsys, monkeypatch):
    rows = [{"c": _LONG_VALUE, "n": "1"}]
    _force_tty(monkeypatch)
    get_cli_context_manager().wrap = False
    _print_rows(rows)
    rich = get_output(capsys)

    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    _print_rows(rows)
    streamed = get_output(capsys)

    assert rich == streamed


def test_wrap_is_enabled_by_default(capsys, monkeypatch):
    _force_tty(monkeypatch)
    _print_rows([{"c": _LONG_VALUE}])
    assert _LONG_VALUE not in get_output(capsys)


def test_config_wrap_false_disables_wrapping(capsys, monkeypatch, config_file):
    _force_tty(monkeypatch)
    with config_file("[cli]\nwrap = false\n") as cfg:
        config_init(cfg)
        _print_rows([{"c": _LONG_VALUE}])
    assert _LONG_VALUE in get_output(capsys)


def test_config_wrap_true_keeps_wrapping(capsys, monkeypatch, config_file):
    _force_tty(monkeypatch)
    with config_file("[cli]\nwrap = true\n") as cfg:
        config_init(cfg)
        _print_rows([{"c": _LONG_VALUE}])
    assert _LONG_VALUE not in get_output(capsys)


def test_env_var_wrap_false_disables_wrapping(capsys, monkeypatch):
    _force_tty(monkeypatch)
    monkeypatch.setenv("SNOWFLAKE_CLI_WRAP", "false")
    _print_rows([{"c": _LONG_VALUE}])
    assert _LONG_VALUE in get_output(capsys)


def test_explicit_choice_wins_over_config(capsys, monkeypatch, config_file):
    _force_tty(monkeypatch)
    with config_file("[cli]\nwrap = false\n") as cfg:
        config_init(cfg)
        get_cli_context_manager().wrap = True
        _print_rows([{"c": _LONG_VALUE}])
    assert _LONG_VALUE not in get_output(capsys)


@pytest.mark.parametrize("value", ["maybe", "", "yes please"])
def test_invalid_config_wrap_value_is_rejected(config_file, value):
    with config_file(f'[cli]\nwrap = "{value}"\n') as cfg:
        config_init(cfg)
        with pytest.raises(Exception, match="Expected boolean value"):
            get_cli_context_manager().wrap = None
            _print_rows([{"c": _LONG_VALUE}])
