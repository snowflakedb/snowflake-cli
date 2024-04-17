import os
from pathlib import Path
from typing import List, Optional

import pytest
from snowflake.cli.api.project.definition import load_project_definition
from snowflake.cli.plugins.nativeapp.artifacts import (
    ArtifactMapping,
    DeployRootError,
    GlobMatchedNothingError,
    NotInDeployRootError,
    SourceNotFoundError,
    TooManyFilesError,
    build_bundle,
    project_path_to_deploy_path,
    translate_artifact,
)

from tests.nativeapp.utils import touch


def trimmed_contents(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    with open(path, "r") as handle:
        return handle.read().strip()


def dir_structure(path: Path, prefix="") -> List[str]:
    if not path.is_dir():
        raise ValueError("Path must point to a directory")

    parts: List[str] = []
    for child in sorted(path.iterdir()):
        if child.is_dir():
            parts += dir_structure(child, f"{prefix}{child.name}/")
        else:
            parts.append(f"{prefix}{child.name}")

    return parts


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_napp_project_1_artifacts(project_definition_files):
    project_root = project_definition_files[0].parent
    native_app = load_project_definition(project_definition_files).native_app

    deploy_root = Path(project_root, native_app.deploy_root)
    artifacts = [translate_artifact(item) for item in native_app.artifacts]
    build_bundle(project_root, deploy_root, artifacts)

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

    # we should be able to re-bundle without any errors happening
    build_bundle(project_root, deploy_root, artifacts)

    # any additional files created in the deploy root will be obliterated by re-bundle
    with open(deploy_root / "unknown_file.txt", "w") as handle:
        handle.write("I am an unknown file!")

    assert dir_structure(deploy_root) == [
        "app/README.md",
        "setup.sql",
        "ui/config.py",
        "ui/main.py",
        "unknown_file.txt",
    ]

    build_bundle(project_root, deploy_root, artifacts)

    assert dir_structure(deploy_root) == [
        "app/README.md",
        "setup.sql",
        "ui/config.py",
        "ui/main.py",
    ]


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_source_not_found(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(SourceNotFoundError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[ArtifactMapping("NOTFOUND.md", "NOTFOUND.md")],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_glob_matched_nothing(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(GlobMatchedNothingError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[ArtifactMapping("**/*.jar", ".")],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_outside_deploy_root_three_ways(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(NotInDeployRootError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[ArtifactMapping("setup.sql", "..")],
        )

    with pytest.raises(NotInDeployRootError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[ArtifactMapping("setup.sql", "/")],
        )

    with pytest.raises(NotInDeployRootError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[ArtifactMapping("app", ".")],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_bad_deploy_root(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(DeployRootError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "..", "deploy"),
            artifacts=[],
        )

    with pytest.raises(DeployRootError):
        with open(project_root / "deploy", "w") as handle:
            handle.write("Deploy root should not be a file...")

        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_too_many_files(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(TooManyFilesError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[
                ArtifactMapping("app/streamlit/*.py", "somehow_combined_streamlits.py")
            ],
        )


@pytest.mark.parametrize(
    "project_paths,expected_paths,expected_exception",
    [
        [
            ["file", "dir"],
            ["deploy/file", "deploy/dir"],
            None,
        ],
        [
            ["dir/nested_file1", "dir/nested_dir/nested_file2"],
            ["deploy/dir/nested_file1", "deploy/dir/nested_dir/nested_file2"],
            None,
        ],
        [
            ["non_existing_file"],
            [],
            FileNotFoundError,
        ],
        [
            ["dir/nested_dir/non_existing_file"],
            [],
            FileNotFoundError,
        ],
    ],
)
def test_project_path_to_deploy_path(
    temp_dir,
    project_paths,
    expected_paths,
    expected_exception,
):
    # Source files
    touch("file")
    touch("dir/nested_file1")
    touch("dir/nested_dir/nested_file2")
    # Build
    os.mkdir("deploy")
    os.symlink("file", "deploy/file")
    os.symlink(Path("dir").absolute(), Path("deploy/dir"))

    files_mapping = {
        "dir": Path("deploy/dir"),
        "file": Path("deploy/file"),
    }

    if expected_exception is None:
        result = project_path_to_deploy_path(
            [Path(p) for p in project_paths], files_mapping
        )
        assert result == [Path(p) for p in expected_paths]
    else:
        with pytest.raises(expected_exception):
            project_path_to_deploy_path([Path(p) for p in project_paths], files_mapping)
