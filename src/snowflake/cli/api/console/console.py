from __future__ import annotations

from contextlib import contextmanager
from typing import Optional

from rich.style import Style
from rich.text import Text
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.console.enum import Output

PHASE_STYLE: Style = Style(bold=True)
STEP_STYLE: Style = Style(italic=True)
INFO_STYLE: Style = Style()
IMPORTANT_STYLE: Style = Style(bold=True, italic=True)
INDENTATION_LEVEL: int = 2


class CliConsoleNestingProhibitedError(RuntimeError):
    """CliConsole phase nesting not allowed."""


class CliConsole(AbstractConsole):
    """An utility for displaying intermediate output.

    Provides following methods for handling displaying messages:
    - `step` - for more detailed information on steps
    - `warning` - for displaying messages in a style that makes it
      visually stand out from other output
    - `phase` a context manager for organising steps into logical group
    """

    _indentation_level: int = INDENTATION_LEVEL
    _styles: dict = {
        "default": "",
        Output.PHASE: PHASE_STYLE,
        Output.STEP: STEP_STYLE,
        Output.INFO: INFO_STYLE,
        Output.IMPORTANT: IMPORTANT_STYLE,
    }

    def _format_message(self, message: str, output: Output) -> Text:
        """Wraps message in rich Text object and applies formatting."""
        style = self._styles.get(output, "default")
        text = Text(message, style=style)

        if self.in_phase and output in {Output.STEP, Output.INFO, Output.IMPORTANT}:
            text.pad_left(self._indentation_level)

        return text

    @contextmanager
    def phase(self, enter_message: str, exit_message: Optional[str] = None):
        """A context manager for organising steps into logical group."""
        if self.in_phase:
            raise CliConsoleNestingProhibitedError("Only one phase allowed at a time.")

        self._print(self._format_message(enter_message, Output.PHASE))
        self._in_phase = True

        yield self.step

        self._in_phase = False
        if exit_message:
            self._print(self._format_message(exit_message, Output.PHASE))

    def step(self, message: str):
        """Displays a message to output.

        If called within a phase, the output will be indented.
        """
        text = self._format_message(message, Output.STEP)
        self._print(text)

    def message(self, _message: str):
        """Displays an informational message to output.

        If called within a phase, the output will be indented."""
        text = self._format_message(_message, Output.INFO)
        self._print(text)

    def warning(self, message: str):
        """Displays message in a style that makes it visually stand out from other output.

        This should be used to display important messages to the console."""
        text = self._format_message(message, Output.IMPORTANT)
        self._print(text)


def get_cli_console() -> AbstractConsole:
    console = CliConsole()
    return console


cli_console = get_cli_console()
