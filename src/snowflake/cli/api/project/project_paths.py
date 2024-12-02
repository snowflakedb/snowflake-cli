from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree


@dataclass
class ProjectPaths:
    project_root: Path

    @property
    def deploy_root(self) -> Path:
        return deploy_root(self.project_root)

    def remove_up_deploy_root(self) -> None:
        if self.deploy_root.exists():
            rmtree(self.deploy_root)


def deploy_root(root: Path, app_type: str | None = None) -> Path:
    if app_type:
        return root / "output" / "deploy" / app_type
    return root / "output" / "deploy"
