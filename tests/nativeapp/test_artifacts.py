from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional, Union

import pytest
from snowflake.cli.api.project.definition import load_project_definition
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.plugins.nativeapp.artifacts import (
    ArtifactError,
    ArtifactMapping,
    ArtifactPredicate,
    BundleMap,
    DeployRootError,
    NotInDeployRootError,
    SourceNotFoundError,
    TooManyFilesError,
    build_bundle,
    resolve_without_follow,
    translate_artifact,
)

from tests.nativeapp.utils import touch
from tests.testing_utils.files_and_dirs import temp_local_dir


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


@pytest.fixture
def bundle_map():
    project_files = {
        "snowflake.yml": "# empty",
        "README.md": "# Test Project",
        "app/setup.sql": "-- empty",
        "app/manifest.yml": "# empty",
        "src/snowpark/main.py": "# empty",
        "src/snowpark/a/file1.py": "# empty",
        "src/snowpark/a/file2.py": "# empty",
        "src/snowpark/a/b/file3.py": "# empty",
        "src/snowpark/a/b/file4.py": "# empty",
        "src/snowpark/a/c/file5.py": "# empty",
        "src/streamlit/main_ui.py": "# empty",
        "src/streamlit/helpers/file1.py": "# empty",
        "src/streamlit/helpers/file2.py": "# empty",
    }
    with temp_local_dir(project_files) as project_root:
        deploy_root = project_root / "output" / "deploy"
        yield BundleMap(project_root=project_root, deploy_root=deploy_root)


def ensure_path(path: Union[Path, str]) -> Path:
    if isinstance(path, str):
        return Path(path)
    return path


def verify_mappings(
    bundle_map: BundleMap,
    expected_mappings: Dict[Union[str, Path], Optional[Union[str, Path]]],
    **kwargs,
):
    for src, dest in expected_mappings.items():
        if dest is None:
            assert bundle_map.to_deploy_path(ensure_path(src)) is None
        else:
            assert bundle_map.to_deploy_path(ensure_path(src)) == ensure_path(dest)

    actual_path_mappings = dict(bundle_map.all_mappings(**kwargs))
    expected_path_mappings = {
        ensure_path(src): ensure_path(dest)
        for (src, dest) in expected_mappings.items()
        if dest is not None
    }
    assert actual_path_mappings == expected_path_mappings


def path_mapping_factory(src: str, dest: Optional[str] = None) -> PathMapping:
    return PathMapping(src=src, dest=dest)


def test_empty_bundle_map(bundle_map):
    mappings = list(bundle_map.all_mappings())
    assert mappings == []

    verify_mappings(
        bundle_map,
        {
            "app/setup.sql": None,
            ".": None,
            "/not/in/project": None,
        },
    )


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_handles_file_to_file_mappings(mapping_factory, bundle_map):
    bundle_map.add(mapping_factory("README.md", "deployed_readme.md"))
    bundle_map.add(mapping_factory("app/setup.sql", "app_setup.sql"))
    bundle_map.add(mapping_factory("app/manifest.yml", "manifest.yml"))

    verify_mappings(
        bundle_map,
        {
            "README.md": "deployed_readme.md",
            "app/setup.sql": "app_setup.sql",
            "app/manifest.yml": "manifest.yml",
        },
    )


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_supports_double_star_glob(mapping_factory, bundle_map):
    bundle_map.add(mapping_factory("src/snowpark/**/*.py", "deployed/"))

    verify_mappings(
        bundle_map,
        {
            "src/snowpark/main.py": "deployed/main.py",
            "src/snowpark/a/file1.py": "deployed/file1.py",
            "src/snowpark/a/file2.py": "deployed/file2.py",
            "src/snowpark/a/b/file3.py": "deployed/file3.py",
            "src/snowpark/a/b/file4.py": "deployed/file4.py",
            "src/snowpark/a/c/file5.py": "deployed/file5.py",
        },
    )


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_supports_complex_globbing(mapping_factory, bundle_map):
    bundle_map.add(mapping_factory("src/s*/**/file[3-5].py", "deployed/"))

    verify_mappings(
        bundle_map,
        {
            "src/snowpark/main.py": None,
            "src/snowpark/a/file1.py": None,
            "src/snowpark/a/file2.py": None,
            "src/snowpark/a/b/file3.py": "deployed/file3.py",
            "src/snowpark/a/b/file4.py": "deployed/file4.py",
            "src/snowpark/a/c/file5.py": "deployed/file5.py",
        },
    )


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_handles_mapping_to_deploy_root(mapping_factory, bundle_map):
    bundle_map.add(mapping_factory("app/*", "./"))
    bundle_map.add(mapping_factory("README.md", "./"))

    verify_mappings(
        bundle_map,
        {
            "app/setup.sql": "setup.sql",
            "app/manifest.yml": "manifest.yml",
            "README.md": "README.md",
        },
    )


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_can_rename_directories(mapping_factory, bundle_map):
    bundle_map.add(mapping_factory("app", "deployed"))

    verify_mappings(
        bundle_map,
        {
            "app": "deployed",
        },
        walk_directories=False,
    )

    verify_mappings(
        bundle_map,
        {
            "app/setup.sql": "deployed/setup.sql",
            "app/manifest.yml": "deployed/manifest.yml",
        },
        walk_directories=True,
    )


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_honours_trailing_slashes(mapping_factory, bundle_map):
    bundle_map.add(mapping_factory("app", "deployed/"))
    bundle_map.add(mapping_factory("README.md", "deployed/"))
    bundle_map.add(
        # src trailing slash has no effect
        mapping_factory("src/snowpark/", "deployed/")
    )

    verify_mappings(
        bundle_map,
        {
            "app": "deployed/app",
            "src/snowpark": "deployed/snowpark",
            "README.md": "deployed/README.md",
        },
    )

    verify_mappings(
        bundle_map,
        {
            "app/manifest.yml": "deployed/app/manifest.yml",
            "app/setup.sql": "deployed/app/setup.sql",
            "src/snowpark/main.py": "deployed/snowpark/main.py",
            "src/snowpark/a/file1.py": "deployed/snowpark/a/file1.py",
            "src/snowpark/a/file2.py": "deployed/snowpark/a/file2.py",
            "src/snowpark/a/b/file3.py": "deployed/snowpark/a/b/file3.py",
            "src/snowpark/a/b/file4.py": "deployed/snowpark/a/b/file4.py",
            "src/snowpark/a/c/file5.py": "deployed/snowpark/a/c/file5.py",
            "README.md": "deployed/README.md",
        },
        walk_directories=True,
    )


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_disallows_overwriting_deploy_root(mapping_factory, bundle_map):
    with pytest.raises(NotInDeployRootError):
        bundle_map.add(mapping_factory("app/*", "."))


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_disallows_unknown_sources(mapping_factory, bundle_map):
    with pytest.raises(SourceNotFoundError):
        bundle_map.add(mapping_factory("missing/*", "deployed/"))

    with pytest.raises(SourceNotFoundError):
        bundle_map.add(mapping_factory("missing", "deployed/"))

    with pytest.raises(SourceNotFoundError):
        bundle_map.add(mapping_factory("**/*.missing", "deployed/"))


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_disallows_mapping_multiple_to_file(mapping_factory, bundle_map):
    with pytest.raises(TooManyFilesError):
        # multiple files named 'file1.py' would collide
        bundle_map.add(mapping_factory("**/file1.py", "deployed/"))

    with pytest.raises(TooManyFilesError):
        bundle_map.add(mapping_factory("**/file1.py", "deployed/"))


def test_bundle_map_handles_missing_dest(bundle_map):
    bundle_map.add(PathMapping(src="app"))
    bundle_map.add(PathMapping(src="README.md"))
    bundle_map.add(PathMapping(src="src/streamlit/"))

    verify_mappings(
        bundle_map,
        {"app": "app", "README.md": "README.md", "src/streamlit": "src/streamlit"},
    )

    verify_mappings(
        bundle_map,
        {
            "app/setup.sql": "app/setup.sql",
            "app/manifest.yml": "app/manifest.yml",
            "README.md": "README.md",
            "src/streamlit/main_ui.py": "src/streamlit/main_ui.py",
            "src/streamlit/helpers/file1.py": "src/streamlit/helpers/file1.py",
            "src/streamlit/helpers/file2.py": "src/streamlit/helpers/file2.py",
        },
        walk_directories=True,
    )


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_disallows_conflicting_dest_types(mapping_factory, bundle_map):
    bundle_map.add(mapping_factory("app", "deployed/"))
    with pytest.raises(ArtifactError):
        bundle_map.add(mapping_factory("**/main.py", "deployed"))


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_disallows_mapping_outside_deploy_root(mapping_factory, bundle_map):
    with pytest.raises(NotInDeployRootError):
        bundle_map.add(mapping_factory("app", "deployed/../../"))

    with pytest.raises(NotInDeployRootError):
        bundle_map.add(mapping_factory("app", Path().resolve().root))

    with pytest.raises(NotInDeployRootError):
        bundle_map.add(mapping_factory("app", "/////"))


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_disallows_absolute_src(mapping_factory, bundle_map):
    with pytest.raises(ArtifactError):
        absolute_src = bundle_map.project_root() / "app"
        assert absolute_src.is_absolute()
        bundle_map.add(mapping_factory(str(absolute_src), "deployed"))


@pytest.mark.parametrize("mapping_factory", [ArtifactMapping, path_mapping_factory])
def test_bundle_map_disallows_absolute_dest(mapping_factory, bundle_map):
    with pytest.raises(ArtifactError):
        absolute_dest = bundle_map.deploy_root() / "deployed"
        assert absolute_dest.is_absolute()
        bundle_map.add(mapping_factory("app", str(absolute_dest)))


def test_bundle_map_checks_mapping_type(bundle_map):
    with pytest.raises(RuntimeError):
        bundle_map.add({"src": "app", "dest": "deployed"})


def test_bundle_map_all_mappings_can_generates_absolute_directories_when_requested(
    bundle_map,
):
    project_root = bundle_map.project_root()
    assert project_root.is_absolute()
    deploy_root = bundle_map.deploy_root()
    assert deploy_root.is_absolute()

    bundle_map.add(PathMapping(src="app", dest="deployed_app"))
    bundle_map.add(PathMapping(src="README.md", dest="deployed_README.md"))
    bundle_map.add(PathMapping(src="src/streamlit", dest="deployed_streamlit"))

    verify_mappings(
        bundle_map,
        {
            "app": "deployed_app",
            "README.md": "deployed_README.md",
            "src/streamlit": "deployed_streamlit",
        },
    )

    verify_mappings(
        bundle_map,
        {
            project_root / "app": deploy_root / "deployed_app",
            project_root / "README.md": deploy_root / "deployed_README.md",
            project_root / "src/streamlit": deploy_root / "deployed_streamlit",
        },
        absolute=True,
        walk_directories=False,
    )

    verify_mappings(
        bundle_map,
        {
            project_root / "app/setup.sql": deploy_root / "deployed_app/setup.sql",
            project_root
            / "app/manifest.yml": deploy_root
            / "deployed_app/manifest.yml",
            project_root / "README.md": deploy_root / "deployed_README.md",
            project_root
            / "src/streamlit/main_ui.py": deploy_root
            / "deployed_streamlit/main_ui.py",
            project_root
            / "src/streamlit/helpers/file1.py": deploy_root
            / "deployed_streamlit/helpers/file1.py",
            project_root
            / "src/streamlit/helpers/file2.py": deploy_root
            / "deployed_streamlit/helpers/file2.py",
        },
        absolute=True,
        walk_directories=True,
    )


def test_bundle_map_all_mappings_accepts_predicates(bundle_map):
    project_root = bundle_map.project_root()
    assert project_root.is_absolute()
    deploy_root = bundle_map.deploy_root()
    assert deploy_root.is_absolute()

    bundle_map.add(PathMapping(src="app", dest="deployed_app"))
    bundle_map.add(PathMapping(src="README.md", dest="deployed_README.md"))
    bundle_map.add(PathMapping(src="src/streamlit", dest="deployed_streamlit"))

    collected: Dict[Path, Path] = {}

    def collecting_predicate(predicate: ArtifactPredicate) -> ArtifactPredicate:
        def _predicate(src: Path, dest: Path) -> bool:
            collected[src] = dest
            return predicate(src, dest)

        return _predicate

    verify_mappings(
        bundle_map,
        {
            project_root
            / "src/streamlit/main_ui.py": deploy_root
            / "deployed_streamlit/main_ui.py",
            project_root
            / "src/streamlit/helpers/file1.py": deploy_root
            / "deployed_streamlit/helpers/file1.py",
            project_root
            / "src/streamlit/helpers/file2.py": deploy_root
            / "deployed_streamlit/helpers/file2.py",
        },
        absolute=True,
        walk_directories=True,
        predicate=collecting_predicate(lambda src, dest: src.suffix == ".py"),
    )

    assert collected == {
        project_root / "app/setup.sql": deploy_root / "deployed_app/setup.sql",
        project_root / "app/manifest.yml": deploy_root / "deployed_app/manifest.yml",
        project_root / "README.md": deploy_root / "deployed_README.md",
        project_root
        / "src/streamlit/main_ui.py": deploy_root
        / "deployed_streamlit/main_ui.py",
        project_root
        / "src/streamlit/helpers/file1.py": deploy_root
        / "deployed_streamlit/helpers/file1.py",
        project_root
        / "src/streamlit/helpers/file2.py": deploy_root
        / "deployed_streamlit/helpers/file2.py",
    }

    collected = {}

    verify_mappings(
        bundle_map,
        {
            "src/streamlit/main_ui.py": "deployed_streamlit/main_ui.py",
            "src/streamlit/helpers/file1.py": "deployed_streamlit/helpers/file1.py",
            "src/streamlit/helpers/file2.py": "deployed_streamlit/helpers/file2.py",
        },
        absolute=False,
        walk_directories=True,
        predicate=collecting_predicate(lambda src, dest: src.suffix == ".py"),
    )

    assert collected == {
        Path("app/setup.sql"): Path("deployed_app/setup.sql"),
        Path("app/manifest.yml"): Path("deployed_app/manifest.yml"),
        Path("README.md"): Path("deployed_README.md"),
        Path("src/streamlit/main_ui.py"): Path("deployed_streamlit/main_ui.py"),
        Path("src/streamlit/helpers/file1.py"): Path(
            "deployed_streamlit/helpers/file1.py"
        ),
        Path("src/streamlit/helpers/file2.py"): Path(
            "deployed_streamlit/helpers/file2.py"
        ),
    }


def test_bundle_map_to_deploy_path(bundle_map):
    bundle_map.add(PathMapping(src="app", dest="deployed_app"))
    bundle_map.add(PathMapping(src="README.md", dest="deployed_README.md"))
    bundle_map.add(PathMapping(src="src/streamlit", dest="deployed_streamlit"))

    # to_deploy_path returns relative paths when relative paths are given as input
    assert bundle_map.to_deploy_path(Path("app")) == Path("deployed_app")
    assert bundle_map.to_deploy_path(Path("README.md")) == Path("deployed_README.md")
    assert bundle_map.to_deploy_path(Path("src/streamlit")) == Path(
        "deployed_streamlit"
    )
    assert bundle_map.to_deploy_path(Path("src/streamlit/main_ui.py")) == Path(
        "deployed_streamlit/main_ui.py"
    )
    assert bundle_map.to_deploy_path(Path("src/streamlit/helpers")) == Path(
        "deployed_streamlit/helpers"
    )
    assert bundle_map.to_deploy_path(Path("src/streamlit/helpers/file1.py")) == Path(
        "deployed_streamlit/helpers/file1.py"
    )
    assert bundle_map.to_deploy_path(Path("src/streamlit/missing.py")) is None

    # to_deploy_path returns absolute paths when absolute paths are given as input
    project_root = bundle_map.project_root()
    deploy_root = bundle_map.deploy_root()
    assert (
        bundle_map.to_deploy_path(project_root / "app") == deploy_root / "deployed_app"
    )
    assert (
        bundle_map.to_deploy_path(project_root / "README.md")
        == deploy_root / "deployed_README.md"
    )
    assert (
        bundle_map.to_deploy_path(project_root / "src/streamlit")
        == deploy_root / "deployed_streamlit"
    )
    assert (
        bundle_map.to_deploy_path(project_root / "src/streamlit/main_ui.py")
        == deploy_root / "deployed_streamlit/main_ui.py"
    )
    assert (
        bundle_map.to_deploy_path(project_root / "src/streamlit/helpers")
        == deploy_root / "deployed_streamlit/helpers"
    )
    assert (
        bundle_map.to_deploy_path(project_root / "src/streamlit/helpers/file1.py")
        == deploy_root / "deployed_streamlit/helpers/file1.py"
    )
    assert bundle_map.to_deploy_path(project_root / "src/streamlit/missing.py") is None


def test_bundle_map_ignores_sources_in_deploy_root(bundle_map):
    bundle_map.deploy_root().mkdir(parents=True, exist_ok=True)
    deploy_root_source = bundle_map.deploy_root() / "should_not_match.yml"
    touch(str(deploy_root_source))

    bundle_map.add(PathMapping(src="**/*.yml", dest="deployed/"))

    verify_mappings(
        bundle_map,
        {
            "app/manifest.yml": "deployed/manifest.yml",
            "snowflake.yml": "deployed/snowflake.yml",
        },
    )


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
    with pytest.raises(SourceNotFoundError):
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
    "project_path,expected_path",
    [
        [
            "srcfile",
            "deploy/file",
        ],
        [
            "srcdir",
            "deploy/dir",
        ],
        [
            "srcdir/nested_file1",
            "deploy/dir/nested_file1",
        ],
        [
            "srcdir/nested_dir/nested_file2",
            "deploy/dir/nested_dir/nested_file2",
        ],
        [
            "srcdir/nested_dir",
            "deploy/dir/nested_dir",
        ],
        [
            "not-in-deploy",
            None,
        ],
    ],
)
def test_source_path_to_deploy_path(
    temp_dir,
    project_path,
    expected_path,
):
    # Source files
    touch("srcfile")
    touch("srcdir/nested_file1")
    touch("srcdir/nested_dir/nested_file2")
    touch("not-in-deploy")
    # Build
    os.mkdir("deploy")
    os.symlink("srcfile", "deploy/file")
    os.symlink(Path("srcdir").resolve(), Path("deploy/dir"))

    bundle_map = BundleMap(
        project_root=Path().resolve(), deploy_root=Path("deploy").resolve()
    )
    bundle_map.add(ArtifactMapping("srcdir", "./dir"))
    bundle_map.add(ArtifactMapping("srcfile", "./file"))

    result = bundle_map.to_deploy_path(resolve_without_follow(Path(project_path)))
    if expected_path:
        assert result == resolve_without_follow(Path(expected_path))
    else:
        assert result is None
