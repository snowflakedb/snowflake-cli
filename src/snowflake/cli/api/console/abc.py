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

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional

from rich import get_console
from rich.console import RenderableType
from rich.text import Text
from snowflake.cli.api.cli_global_context import (
    _CliGlobalContextAccess,
    get_cli_context,
)
from snowflake.cli.api.sanitizers import sanitize_for_terminal


class AbstractConsole(ABC):
    """Interface for cli console implementation.

    Each console should have the following methods implemented:
    - `step` - for more detailed information on steps
    - `warning` - for displaying messages in a style that makes it
      visually stand out from other output
    - `phase` - a context manager for organising steps into logical group
    - `indented` - a context manager for temporarily indenting messages and warnings
    - 'message' - informational output; parses Rich markup
    - 'plain_message' - untrusted text without markup parsing
    - 'panel' - displays visually separated messages
    - 'spinner' - context manager for indicating a long-running operation
    """

    _print_fn: Callable[[str], None]
    _in_phase: bool

    def __init__(self):
        super().__init__()
        self._in_phase = False

    @property
    def _cli_context(self) -> _CliGlobalContextAccess:
        return get_cli_context()

    @property
    def is_silent(self) -> bool:
        """Returns information whether intermediate output is muted."""
        return self._cli_context.silent

    @property
    def in_phase(self) -> bool:
        """Indicated whether output should be grouped."""
        return self._in_phase

    def _print(
        self,
        text: RenderableType,
        end: str = "\n",
        soft_wrap: Optional[bool] = None,
    ):
        if self.is_silent:
            return
        get_console().print(text, end=end, soft_wrap=soft_wrap)

    @contextmanager
    @abstractmethod
    def phase(
        self,
        enter_message: str,
        exit_message: Optional[str] = None,
    ) -> Iterator[Callable[[str], None]]:
        """A context manager for organising steps into logical group."""

    @contextmanager
    @abstractmethod
    def indented(self):
        """
        A context manager for temporarily indenting messages and warnings. Phases and steps cannot be used in indented blocks,
        but multiple indented blocks can be nested (use sparingly).
        """

    @abstractmethod
    def step(self, message: str):
        """Displays a message to output."""

    @abstractmethod
    def message(self, _message: str):
        """Displays an informational message to output.

        Parses Rich markup. Do not pass SQL, logs, object names, or other
        untrusted text; use ``plain_message`` for those.
        """

    @abstractmethod
    def warning(self, message: str):
        """Displays message in a style that makes it visually stand out from other output.

        Intended for displaying messages related to important messages."""

    @abstractmethod
    def panel(self, message: str):
        """Displays message in a panel that makes it visually stand out from other output.

        Intended for displaying visually separated messages."""

    @contextmanager
    @abstractmethod
    def spinner(self):
        """
        A context manager for indicating a long-running operation.
        """

    @abstractmethod
    def styled_message(self, message: str, style: Any):
        """Displays a message with provided style.

        Does not add a trailing newline (``end=""``). Untrusted full lines
        should use ``plain_message`` instead.
        """

    def plain_message(self, message: str):
        """Displays untrusted text without interpreting Rich markup.

        Concrete rather than abstract: this is the public plugin surface, so an
        external subclass must keep working across an upgrade. Sanitizes for the
        terminal, always adds a newline, and respects ``--silent``. ``CliConsole``
        also applies ``phase()`` / ``indented()`` padding; this default does not.
        """
        self._print(Text(sanitize_for_terminal(message)))

    def renderable(self, renderable: RenderableType, soft_wrap: Optional[bool] = None):
        """Displays a rich renderable, such as a tree or a table.

        Concrete rather than abstract: this is the public plugin surface, so an
        external subclass must keep working across an upgrade. For content that
        is not a plain message, it is displayed as given - indentation, styling
        and sanitizing belong to whoever built it.

        Pass ``soft_wrap=False`` to wrap a renderable wider than the console
        instead of cropping it."""
        self._print(renderable, soft_wrap=soft_wrap)
