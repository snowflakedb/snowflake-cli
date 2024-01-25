from abc import ABC, abstractmethod
from typing import Callable

from snowflake.cli.api.cli_global_context import _CliGlobalContextAccess
from snowflake.cli.api.console.context import CliConsoleContext


class AbstractConsole(ABC):
    """Interface for cli console implementation.

    Each console should have two methods implemented:
    - `phase` for major informations on command actions
    - `step` for more detailed informations on step
    """

    _context: CliConsoleContext
    _print_fn: Callable
    _cli_context: _CliGlobalContextAccess

    def __init__(self, cli_context: _CliGlobalContextAccess):
        super().__init__()
        self._context = CliConsoleContext()
        self._cli_context = cli_context

    @property
    def is_silent(self) -> bool:
        """Returns information whether intermediate output is muted."""
        return self._cli_context.silent

    @abstractmethod
    def phase(self, message: str):
        """Displays not indented message."""
        ...

    @abstractmethod
    def step(self, message: str):
        """Displays indented message."""
        ...

    @abstractmethod
    def error(self, message: str):
        """Displays message with distinct style.

        Intended for diplaying messeges related to failures."""
        ...
