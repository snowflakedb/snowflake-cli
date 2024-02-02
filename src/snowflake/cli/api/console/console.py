from __future__ import annotations

from contextlib import contextmanager
from typing import Optional

from rich.style import Style
from rich.text import Text
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.console.enum import Output

PHASE_STYLE: Style = Style(color="grey93", bold=True)
STEP_STYLE: Style = Style(color="grey89", italic=True)
IMPORTANT_STYLE: Style = Style(color="red", bold=True, italic=True)
INDENTATION_LEVEL: int = 2


class CliConsole(AbstractConsole):
    """An utility for displayinf intermediate output."""

    _indentation_level: int = INDENTATION_LEVEL
    _styles: dict = {
        "default": "",
        Output.PHASE: PHASE_STYLE,
        Output.STEP: STEP_STYLE,
        Output.IMPORTANT: IMPORTANT_STYLE,
    }

    def _format_message(self, message: str, output: Output) -> Text:
        """Wraps message in rich Text object and applys formatting."""
        style = self._styles.get(output, "default")
        text = Text(message, style=style)

        if self.in_phase and output in {Output.STEP, Output.IMPORTANT}:
            text.pad_left(self._indentation_level)

        return text

    @contextmanager
    def phase(self, enter_message: str, exit_message: Optional[str] = None):
        """Displays unindented message formatted with PHASE style."""
        self._print(self._format_message(enter_message, Output.PHASE))
        self._in_phase = True

        yield self.step

        self._in_phase = False
        if exit_message:
            self._print(self._format_message(exit_message, Output.PHASE))

    def step(self, message: str):
        """Displays 2 spaces indented message with STEP style."""
        text = self._format_message(message, Output.STEP)
        self._print(text)

    def warning(self, message: str):
        """Displays unindented message formated with IMPORTANT style."""
        text = self._format_message(message, Output.IMPORTANT)
        self._print(text)


def get_cli_console() -> AbstractConsole:
    console = CliConsole()
    return console


cli_console = get_cli_console()
