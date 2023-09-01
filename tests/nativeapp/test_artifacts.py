import pytest
import os
from typing import Optional
from unittest import mock
from tests.project.fixtures import *
from tests.testing_utils.fixtures import *

from snowcli.cli.nativeapp.artifacts import build_bundle, translate_artifact
from snowcli.cli.project.definition import load_project_definition


def trimmed_contents(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    with open(path, "r") as handle:
        return handle.read().strip()


def dir_structure(path: Path, prefix="") -> List[str]:
    if not path.is_dir():
        return None

    parts: List[str] = []
    for child in sorted(path.iterdir()):
        if child.is_dir():
            parts += dir_structure(child, f"{prefix}{child.name}/")
        else:
            parts.append(f"{prefix}{child.name}")

    return parts


@pytest.mark.parametrize(
    "project_definition_files",
    ["napp_project_1"],
    indirect=True,
)
def test_napp_project_1_artifacts(project_definition_files):
    project_root = project_definition_files[0].parent
    native_app = load_project_definition(project_definition_files)["native_app"]

    deploy_root = Path(project_root, native_app["deploy_root"])
    build_bundle(
        project_root,
        deploy_root,
        artifacts=[translate_artifact(item) for item in native_app["artifacts"]],
    )

    assert dir_structure(deploy_root) == [
        "app/README.md",
        "setup.sql",
        "ui/config.py",
        "ui/main.py",
    ]
    assert (
        trimmed_contents(deploy_root / "setup.sql")
        == "create versioned schema myschema;"
    )
    assert trimmed_contents(deploy_root / "app" / "README.md") == "app/README.md"
    assert trimmed_contents(deploy_root / "ui" / "main.py") == "# main.py"
    assert trimmed_contents(deploy_root / "ui" / "config.py") == "# config.py"
