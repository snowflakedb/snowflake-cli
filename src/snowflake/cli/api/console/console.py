from __future__ import annotations

from rich.style import Style
from rich.text import Text
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.console.enum import Output

PHASE_STYLE: Style = Style(color="grey93", bold=True)
STEP_STYLE: Style = Style(color="grey89", italic=True)
ERROR_STYLE: Style = Style(color="red", bold=True, italic=True)
INDENTATION_LEVEL: int = 2


class CliConsole(AbstractConsole):
    """An utility for displayinf intermediate output."""

    _indentation_level: int = INDENTATION_LEVEL
    _styles: dict = {
        "default": "",
        Output.PHASE: PHASE_STYLE,
        Output.STEP: STEP_STYLE,
        Output.ERROR: ERROR_STYLE,
    }

    def _format_message(self, message: str, output: Output) -> Text:
        """Wraps message in rich Text object and applys formatting."""
        style = self._styles.get(output, "default")
        text = Text(message, style=style)

        if output == Output.STEP:
            text.pad_left(self._indentation_level)

        return text

    def _print(self, text: Text):
        if self.is_silent:
            return
        print(text)

    def phase(self, message: str):
        """Displays unindented message formatted with PHASE style."""
        text = self._format_message(message, Output.PHASE)
        self._print(text)

    def step(self, message: str):
        """Displays 2 spaces indented message with STEP style."""
        text = self._format_message(message, Output.STEP)
        self._print(text)

    def warning(self, message: str):
        """Displays unindented message formated with ERROR style."""
        text = self._format_message(message, Output.ERROR)
        self._print(text)


def get_cli_console() -> AbstractConsole:
    console = CliConsole(cli_context=cli_context)
    return console


cli_console = get_cli_console()
