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
import json
from io import StringIO
from typing import Any, List, Optional, Tuple
from unittest import mock

from rich.console import Console, RenderableType
from rich.style import Style
from snowflake.cli._plugins.dcm.reporters.base import Reporter
from snowflake.cli.api.exceptions import CliError

CLI_CONSOLE_PATH = (
    "snowflake.cli._plugins.dcm.reporters.base.cli_console.styled_message"
)
RENDERABLE_PATH = "snowflake.cli._plugins.dcm.reporters.base.cli_console.renderable"

RENDER_WIDTH = 200


def render_to_text(renderable: RenderableType, width: int = RENDER_WIDTH) -> str:
    """The text a rich renderable prints, with guide glyphs pinned by
    ``legacy_windows=False`` so assertions do not depend on the host."""
    output = StringIO()
    _render_console(width, output).print(renderable, soft_wrap=False)
    return output.getvalue()


def render_to_styled_runs(
    renderable: RenderableType, width: int = RENDER_WIDTH
) -> List[Tuple[str, Optional[Style]]]:
    """Every ``(text, style)`` run a rich renderable prints, rows concatenated."""
    console = _render_console(width, StringIO())
    return [
        (segment.text, segment.style)
        for line in console.render_lines(renderable, pad=False)
        for segment in line
    ]


def _render_console(width: int, file: StringIO) -> Console:
    return Console(
        file=file, width=width, no_color=True, legacy_windows=False, markup=False
    )


class FakeCursor:
    """Fake cursor that returns JSON data like a real Snowflake cursor."""

    def __init__(self, data):
        self._data = data
        self._fetched = False

    def fetchone(self):
        if self._fetched:
            return None
        self._fetched = True
        if self._data is None:
            return None
        return (json.dumps(self._data) if isinstance(self._data, dict) else self._data,)


def capture_reporter_renderables(
    reporter: Reporter[Any], cursor: FakeCursor
) -> List[RenderableType]:
    """The rich renderables a reporter prints, as objects rather than as text."""
    captured: List[RenderableType] = []

    def capture(renderable: RenderableType, **_: Any) -> None:
        captured.append(renderable)

    with mock.patch(CLI_CONSOLE_PATH), mock.patch(RENDERABLE_PATH, side_effect=capture):
        reporter.process(cursor)
    return captured


def capture_reporter_output(reporter: Reporter[Any], cursor: FakeCursor) -> str:
    """Capture the output from a reporter's process method, both console paths
    into one buffer so the order is the one the user sees."""
    output = StringIO()

    def mock_print(text, style=""):
        if hasattr(text, "plain"):
            output.write(text.plain)
        else:
            output.write(str(text))

    def mock_renderable(renderable: RenderableType, **_: Any) -> None:
        output.write(render_to_text(renderable))

    error_message = ""
    with mock.patch(
        CLI_CONSOLE_PATH,
        side_effect=mock_print,
    ), mock.patch(RENDERABLE_PATH, side_effect=mock_renderable):
        try:
            reporter.process(cursor)
        except CliError as e:
            error_message = e.message

    result = output.getvalue()
    if error_message:
        result += f"\n{error_message}\n"
    return result
