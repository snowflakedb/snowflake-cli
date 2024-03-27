from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Callable, Iterator, Optional

from rich import print as rich_print
from rich.text import Text
from snowflake.cli.api.cli_global_context import _CliGlobalContextAccess, cli_context


class AbstractConsole(ABC):
    """Interface for cli console implementation.

    Each console should have three methods implemented:
    - `step` - for more detailed information on steps
    - `warning` - for displaying messages in a style that makes it
      visually stand out from other output
    - `phase` a context manager for organising steps into logical group
    """

    _print_fn: Callable[[str], None]
    _cli_context: _CliGlobalContextAccess
    _in_phase: bool

    def __init__(self):
        super().__init__()
        self._cli_context = cli_context
        self._in_phase = False

    @property
    def is_silent(self) -> bool:
        """Returns information whether intermediate output is muted."""
        return self._cli_context.silent

    @property
    def in_phase(self) -> bool:
        """Indicated whether output should be grouped."""
        return self._in_phase

    def _print(self, text: Text):
        if self.is_silent:
            return
        rich_print(text)

    @contextmanager
    @abstractmethod
    def phase(
        self,
        enter_message: str,
        exit_message: Optional[str] = None,
    ) -> Iterator[Callable[[str], None]]:
        """A context manager for organising steps into logical group."""

    @abstractmethod
    def step(self, message: str):
        """Displays a message to output."""

    @abstractmethod
    def message(self, _message: str):
        """Displays an informational message to output."""

    @abstractmethod
    def warning(self, message: str):
        """Displays message in a style that makes it visually stand out from other output.

        Intended for displaying messages related to important messages."""
