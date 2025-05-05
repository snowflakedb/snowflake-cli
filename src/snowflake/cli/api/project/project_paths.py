from dataclasses import dataclass
from pathlib import Path

from snowflake.cli.api.secure_path import SecurePath


@dataclass
class ProjectPaths:
    """
    This class allows you to manage files paths related to given project.
    Class provides bundle root path and allows to remove it.
    """

    project_root: Path

    @property
    def bundle_root(self) -> Path:
        return bundle_root(self.project_root)

    def remove_up_bundle_root(self) -> None:
        if self.bundle_root.exists():
            SecurePath(self.bundle_root).rmdir(recursive=True)

    def clean_up_output(self):
        output = SecurePath(self.project_root / "output")
        if output.exists():
            output.rmdir(recursive=True)


def bundle_root(root: Path, app_type: str | None = None) -> Path:
    if app_type:
        return root / "output" / "bundle" / app_type
    return root / "output" / "bundle"
