from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Callable, Iterator

from rich import print as rich_print
from rich.text import Text
from snowflake.cli.api.cli_global_context import _CliGlobalContextAccess, cli_context


class AbstractConsole(ABC):
    """Interface for cli console implementation.

    Each console should have two methods implemented:
    - `phase` for major informations on command actions
    - `step` for more detailed informations on step
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
        self, enter_message: str, exit_message: str
    ) -> Iterator[Callable[[str], None]]:
        """Displays not indented message."""

    @abstractmethod
    def step(self, message: str):
        """Displays indented message."""

    @abstractmethod
    def warning(self, message: str):
        """Displays message with distinct style.

        Intended for diplaying messeges related to important messages."""
