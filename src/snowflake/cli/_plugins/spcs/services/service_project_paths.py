from dataclasses import dataclass
from pathlib import Path

from snowflake.cli.api.project.project_paths import ProjectPaths, bundle_root


@dataclass
class ServiceProjectPaths(ProjectPaths):
    """
    This class allows you to manage files paths related to given project.
    """

    @property
    def bundle_root(self) -> Path:
        return bundle_root(self.project_root, "service")
