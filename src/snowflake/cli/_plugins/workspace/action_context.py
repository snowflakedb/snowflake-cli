from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from snowflake.cli.api.console.abc import AbstractConsole


@dataclass
class ActionContext:
    """
    An object that is passed to each action when called by WorkspaceManager
    """

    console: AbstractConsole
    project_root: Path
    default_role: str
    default_warehouse: Optional[str]
