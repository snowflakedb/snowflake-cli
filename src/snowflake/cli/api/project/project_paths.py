from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree


@dataclass
class ProjectPaths:
    project_root: Path

    @property
    def deploy_root(self) -> Path:
        return self.project_root / "output"

    def remove_up_deploy_root(self) -> None:
        if self.deploy_root.exists():
            rmtree(self.deploy_root)
