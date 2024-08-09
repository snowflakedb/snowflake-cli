from dataclasses import dataclass
from pathlib import Path


@dataclass
class ActionContext:
    """
    An object that is passed to each action when called by WorkspaceManager
    """

    project_root: Path
