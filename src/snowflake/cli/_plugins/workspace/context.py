from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Callable

from snowflake.cli.api.console.abc import AbstractConsole


@dataclass
class WorkspaceContext:
    """
    An object that is passed to each entity when instantiated by WorkspaceManager
    to allow access to the CLI context without requiring the entities to use
    get_cli_context().
    """

    console: AbstractConsole
    project_root: Path
    get_default_role: Callable[[], str]
    get_default_warehouse: Callable[[], str | None]

    @cached_property
    def default_role(self) -> str:
        return self.get_default_role()

    @cached_property
    def default_warehouse(self) -> str | None:
        return self.get_default_warehouse()


@dataclass
class ActionContext:
    """
    An object that is passed to each action when called by WorkspaceManager
    to provide access to metadata about the entity and project being acted upon.
    """

    get_entity: Callable
