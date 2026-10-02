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

import pytest

from snowflake.cli._app.main_typer import _handle_exception
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.output.formats import OutputFormat

_UNEXPECTED_EXCEPTION_HEADER = "An unexpected exception occurred"


@pytest.mark.parametrize(
    "output_format",
    [OutputFormat.JSON, OutputFormat.JSON_EXT, OutputFormat.CSV],
)
def test_unexpected_exception_is_reported_when_intermediate_output_is_muted(
    output_format, capsys
):
    """Structured formats mute intermediate console output.

    A fatal error must not be muted with it, otherwise the command exits 1 with
    no explanation on either stream.
    """
    get_cli_context_manager().output_format = output_format

    with pytest.raises(SystemExit) as exc_info:
        _handle_exception(ValueError("boom"))

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert _UNEXPECTED_EXCEPTION_HEADER in captured.err
    assert "boom" in captured.err
    assert captured.out == ""


def test_unexpected_exception_is_reported_on_stderr_for_table_format(capsys):
    """Errors belong on stderr even when nothing is muted, so that structured
    stdout stays parseable regardless of the selected format."""
    get_cli_context_manager().output_format = OutputFormat.TABLE

    with pytest.raises(SystemExit) as exc_info:
        _handle_exception(ValueError("boom"))

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert _UNEXPECTED_EXCEPTION_HEADER in captured.err
    assert "boom" in captured.err
    assert captured.out == ""


def test_enable_tracebacks_reraises_instead_of_reporting():
    """--debug keeps its existing behaviour: propagate the original exception."""
    context_manager = get_cli_context_manager()
    context_manager.output_format = OutputFormat.JSON
    context_manager.enable_tracebacks = True

    with pytest.raises(ValueError, match="boom"):
        _handle_exception(ValueError("boom"))
