from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree


@dataclass
class ProjectPaths:
    project_root: Path

    @property
    def bundle_root(self) -> Path:
        return bundle_root(self.project_root)

    def remove_up_bundle_root(self) -> None:
        if self.bundle_root.exists():
            rmtree(self.bundle_root)


def bundle_root(root: Path, app_type: str | None = None) -> Path:
    if app_type:
        return root / "output" / "bundle" / app_type
    return root / "output" / "bundle"
